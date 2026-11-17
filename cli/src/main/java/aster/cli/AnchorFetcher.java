package aster.cli;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@code --jwks https://…} 的取回（README「信任锚」；参照实现 {@code argv.mjs} 的 {@code fetchJwks}）：
 * <ul>
 *   <li>总时限 10 s（连接、TLS、重定向与读完响应体）；只认 2xx；响应体 ≤ 1 MiB；</li>
 *   <li>只有 301 / 302 / 303 / 307 / 308 算重定向，至多 3 跳，每一跳都必须是 https（scheme 不区分大小写）；Location 必须是合法的
 *       RFC 3986 URI-reference，按 RFC 3986 §5.2 相对当前请求 URI 解析（{@link Rfc3986}，不用 RFC 2396 的 {@code URI.resolve}）；其他 3xx 失败。
 *       手工处理（{@link HttpClient.Redirect#NEVER} + 循环）：{@code NORMAL} 既不能限制次数，也不能拒绝降级到 http；</li>
 *   <li>请求带 {@code Accept-Encoding: identity}，响应的 {@code Content-Encoding} 不是 identity 即失败；</li>
 *   <li>TLS 用运行时缺省信任库（cacerts）；环境变量 {@code ASTER_ANCHOR_CA_FILE}（仅供测试）指向 PEM 时<b>追加</b>信任其中的证书；</li>
 *   <li>v1 不支持代理：显式直连（{@link HttpClient.Builder#NO_PROXY}），忽略 {@code HTTPS_PROXY} 等环境变量与 {@code https.proxyHost}；</li>
 *   <li>任何失败都是 {@link ErrorCode#USAGE}。</li>
 * </ul>
 */
final class AnchorFetcher {

    static final Duration TOTAL_TIMEOUT = Duration.ofSeconds(10);
    static final int MAX_REDIRECTS = 3;
    static final int MAX_BYTES = 1024 * 1024;
    private static final java.util.Set<Integer> REDIRECTS = java.util.Set.of(301, 302, 303, 307, 308);

    private AnchorFetcher() {}

    static byte[] fetch(String url, Map<String, String> env) throws CliException {
        long deadline = System.nanoTime() + TOTAL_TIMEOUT.toNanos();
        FutureTask<byte[]> task = new FutureTask<>(() -> fetchFollowing(url, env, deadline));
        Thread t = new Thread(task, "aster-anchor-fetch");
        t.setDaemon(true);
        t.start();
        try {
            return task.get(TOTAL_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            task.cancel(true);
            throw usage("取回 JWKS 超过 10 s");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof CliException ce) {
                throw ce;
            }
            throw usage("取回 JWKS 失败: " + cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw usage("取回 JWKS 被中断");
        }
    }

    private static byte[] fetchFollowing(String url, Map<String, String> env, long deadline) throws Exception {
        HttpClient client = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(remaining(deadline))
            .sslContext(sslContext(env))
            .proxy(HttpClient.Builder.NO_PROXY)
            .build();
        try {
            return follow(client, url, deadline);
        } finally {
            // 不等在途请求：超时取消时立即释放连接与选择器线程
            client.shutdownNow();
        }
    }

    private static byte[] follow(HttpClient client, String url, long deadline) throws Exception {
        String current = url;
        for (int hop = 0; ; hop++) {
            // 重定向目标的 scheme 已由 Rfc3986.resolve 归一为小写（scheme 不区分大小写）
            if (!current.startsWith("https://")) {
                throw usage("重定向到非 https 地址: " + current);
            }
            HttpRequest request = HttpRequest.newBuilder(URI.create(current)).timeout(remaining(deadline))
                .header("Accept-Encoding", "identity").GET().build();
            HttpResponse<InputStream> resp = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            int status = resp.statusCode();
            String location = resp.headers().firstValue("location").orElse(null);
            if (status >= 300 && status < 400) {
                resp.body().close();
                if (!REDIRECTS.contains(status) || location == null) {
                    throw usage("不支持的 HTTP " + status);
                }
                if (hop + 1 > MAX_REDIRECTS) {
                    throw usage("重定向超过 " + MAX_REDIRECTS + " 次");
                }
                if (!Rfc3986.isReference(location)) {
                    throw usage("Location 不是合法的 RFC 3986 URI-reference: " + location);
                }
                current = Rfc3986.resolve(current, location);
                continue;
            }
            try (InputStream body = resp.body()) {
                if (status < 200 || status > 299) {
                    throw usage("HTTP " + status);
                }
                String encoding = resp.headers().firstValue("content-encoding").orElse("identity");
                if (!encoding.trim().equalsIgnoreCase("identity")) {
                    throw usage("不支持的 Content-Encoding: " + encoding);
                }
                byte[] bytes = body.readNBytes(MAX_BYTES + 1);
                if (bytes.length > MAX_BYTES) {
                    throw usage("响应超过 1 MiB");
                }
                return bytes;
            }
        }
    }

    private static Duration remaining(long deadline) throws CliException {
        long left = deadline - System.nanoTime();
        if (left <= 0) {
            throw usage("取回 JWKS 超过 10 s");
        }
        return Duration.ofNanos(left);
    }

    /** 缺省信任库；{@code ASTER_ANCHOR_CA_FILE} 给出时另外信任其中的证书（两者任一通过即可）。 */
    private static SSLContext sslContext(Map<String, String> env) throws GeneralSecurityException, IOException {
        String caFile = env.get("ASTER_ANCHOR_CA_FILE");
        if (caFile == null || caFile.isBlank()) {
            return SSLContext.getDefault();
        }
        KeyStore extra = KeyStore.getInstance(KeyStore.getDefaultType());
        extra.load(null, null);
        try (InputStream in = Files.newInputStream(Path.of(caFile))) {
            int k = 0;
            for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                extra.setCertificateEntry("aster-anchor-ca-" + k++, c);
            }
        }
        X509TrustManager system = trustManager(null);
        X509TrustManager test = trustManager(extra);
        X509TrustManager either = new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                system.checkClientTrusted(chain, authType);
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                try {
                    system.checkServerTrusted(chain, authType);
                } catch (CertificateException e) {
                    test.checkServerTrusted(chain, authType);
                }
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                List<X509Certificate> all = new ArrayList<>(Arrays.asList(system.getAcceptedIssuers()));
                all.addAll(Arrays.asList(test.getAcceptedIssuers()));
                return all.toArray(X509Certificate[]::new);
            }
        };
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, new TrustManager[]{either}, null);
        return ctx;
    }

    private static X509TrustManager trustManager(KeyStore ks) throws GeneralSecurityException {
        TrustManagerFactory f = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        f.init(ks);
        for (TrustManager tm : f.getTrustManagers()) {
            if (tm instanceof X509TrustManager x) {
                return x;
            }
        }
        throw new GeneralSecurityException("没有 X509TrustManager");
    }

    private static CliException usage(String why) {
        return new CliException(ErrorCode.USAGE, why);
    }
}
