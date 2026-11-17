package aster.cli;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 信任锚的 TLS 信任 = 平台缺省信任库（{@code TrustManagerFactory} 以 null keystore 初始化：cacerts，或 {@code javax.net.ssl.trustStore}）
 * <b>加上</b> {@code ASTER_ANCHOR_CA_FILE} 的 PEM；PEM 只追加、从不替换缺省信任（ts#349 评审的跨 CLI 裁决）。
 *
 * <p>两个各自自签名的服务：A 的证书放进经 {@code javax.net.ssl.trustStore} 指定的「平台」信任库（模拟装进 JVM 的企业 CA），
 * B 的证书经 {@code ASTER_ANCHOR_CA_FILE} 给出。设置了 CA 文件时两者必须同时可信。
 */
class AnchorTrustStoreTest {

    @TempDir
    Path tmp;

    private static final String STDIN = "{\"applicant\":{\"activeDefaults\":0,\"creditScore\":760,\"monthlyDebt\":\"2000\","
        + "\"monthlyIncome\":\"9000\",\"requestedAmount\":\"20000\"}}";

    @Test
    void extraCaAddsToPlatformTrustInsteadOfReplacingIt() throws Exception {
        JsonNode routes = Fixtures.MAPPER.readTree("{\"/jwks.json\": {\"status\": 200, \"file\": \"keys/test-jwks.json\"}}");
        try (HttpsFixtureServer platform = new HttpsFixtureServer(routes, Files.createDirectory(tmp.resolve("a")));
             HttpsFixtureServer extra = new HttpsFixtureServer(routes, Files.createDirectory(tmp.resolve("b")))) {
            Path trustStore = tmp.resolve("platform-truststore.p12");
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(null, null);
            try (InputStream in = Files.newInputStream(platform.caPem())) {
                ks.setCertificateEntry("corporate-ca", CertificateFactory.getInstance("X.509").generateCertificate(in));
            }
            try (OutputStream out = Files.newOutputStream(trustStore)) {
                ks.store(out, "changeit".toCharArray());
            }
            String old = System.getProperty("javax.net.ssl.trustStore");
            String oldPass = System.getProperty("javax.net.ssl.trustStorePassword");
            System.setProperty("javax.net.ssl.trustStore", trustStore.toString());
            System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
            try {
                Map<String, String> env = Map.of("ASTER_ANCHOR_CA_FILE", extra.caPem().toString());
                assertEquals(0, fetchExit(platform, env), "设置 ASTER_ANCHOR_CA_FILE 时平台信任库仍须生效");
                assertEquals(0, fetchExit(extra, env), "ASTER_ANCHOR_CA_FILE 的证书须被信任");
                assertEquals(2, fetchExit(extra, Map.of("ASTER_ANCHOR_CA_FILE", platform.caPem().toString())),
                    "未列入任何信任来源的证书须被拒（USAGE）");
            } finally {
                restore("javax.net.ssl.trustStore", old);
                restore("javax.net.ssl.trustStorePassword", oldPass);
            }
        }
    }

    private static int fetchExit(HttpsFixtureServer server, Map<String, String> env) {
        return Fixtures.run(Fixtures.PACKAGE_V1.resolve("packages"), STDIN.getBytes(StandardCharsets.UTF_8), env,
            "run", "b-credit-limit", "--input", "-", "--jwks", "https://localhost:" + server.port() + "/jwks.json").exit();
    }

    private static void restore(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
