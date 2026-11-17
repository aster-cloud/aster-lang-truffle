package aster.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@code tamper/test-time.json} 的 {@code httpsServer} 配方：在 127.0.0.1 上起只用于一个用例的 https 服务（自签名证书，SAN 为
 * localhost 与 127.0.0.1，由 JDK {@code keytool} 生成），按 routes 应答，全程离线。{@link #caPem()} 是该证书的 PEM，
 * 供 {@code ASTER_ANCHOR_CA_FILE}（仅供测试的信任覆盖）使用。路由语义与 aster-lang-test {@code scripts/test-package-v1.mjs} 相同。
 */
final class HttpsFixtureServer implements AutoCloseable {

    private final HttpsServer server;
    private final Path caPem;

    HttpsFixtureServer(JsonNode routes, Path workDir) throws Exception {
        Path ks = workDir.resolve("server.p12");
        caPem = workDir.resolve("ca.pem");
        keytool("-genkeypair", "-alias", "srv", "-keyalg", "EC", "-groupname", "secp256r1", "-dname", "CN=localhost",
            "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-validity", "2", "-keystore", ks.toString(),
            "-storetype", "PKCS12", "-storepass", "changeit", "-keypass", "changeit");
        keytool("-exportcert", "-rfc", "-alias", "srv", "-keystore", ks.toString(), "-storetype", "PKCS12",
            "-storepass", "changeit", "-file", caPem.toString());

        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(ks)) {
            store.load(in, "changeit".toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(store, "changeit".toCharArray());
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(kmf.getKeyManagers(), null, null);

        byte[] jwks = Files.readAllBytes(Fixtures.PACKAGE_V1.resolve("keys/test-jwks.json"));
        server = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(ssl));
        server.createContext("/", ex -> {
            // 与参照测试服务同口径：按原始 path + query 匹配路由（重定向 "?q" 落到 "/a/b?q"）
            String raw = ex.getRequestURI().getRawPath()
                + (ex.getRequestURI().getRawQuery() == null ? "" : "?" + ex.getRequestURI().getRawQuery());
            JsonNode r = routes.get(raw);
            byte[] body = new byte[0];
            int status = 404;
            if (r != null) {
                status = r.path("status").asInt();
                if (r.has("location")) {
                    ex.getResponseHeaders().add("Location",
                        r.path("location").asText().replace("<PORT>", String.valueOf(port())));
                } else if (r.has("file")) {
                    body = jwks;
                } else if (r.has("zeros")) {
                    body = spaces(r.path("zeros").asInt());
                } else if (r.has("jwksPaddedTo")) {
                    body = Arrays.copyOf(jwks, r.path("jwksPaddedTo").asInt());
                    Arrays.fill(body, jwks.length, body.length, (byte) ' ');
                } else if (r.has("body")) {
                    body = r.path("body").asText().getBytes(StandardCharsets.UTF_8);
                }
            }
            for (String key : new String[]{"contentEncoding", "contentEncodingHeader"}) {
                if (r != null && r.has(key)) {
                    ex.getResponseHeaders().add("Content-Encoding", r.path(key).asText());
                }
            }
            ex.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
    }

    int port() {
        return server.getAddress().getPort();
    }

    Path caPem() {
        return caPem;
    }

    private static byte[] spaces(int n) {
        byte[] b = new byte[n];
        Arrays.fill(b, (byte) ' ');
        return b;
    }

    private static void keytool(String... args) throws Exception {
        List<String> cmd = new java.util.ArrayList<>();
        cmd.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        p.getInputStream().transferTo(out);
        if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IllegalStateException("keytool 失败: " + out.toString(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
