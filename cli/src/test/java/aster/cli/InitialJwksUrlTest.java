package aster.cli;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 初始 {@code --jwks} https URL 与重定向目标同一组检查（终审 V-1），失败在 usage 阶段：输出与其他 USAGE 相同的错误信封
 * （meta 全为 null / false），且不发请求（这里的 host 都不存在或不可达，若发出请求也只会以连接失败告终，故另断言耗时很短）。
 */
class InitialJwksUrlTest {

    private static final String USAGE_ENVELOPE = "{\"error\":{\"code\":\"USAGE\",\"message\":\"invalid command line\"},"
        + "\"meta\":{\"cliVersion\":\"<CLI_VERSION>\",\"coreIrSha256\":null,\"engine\":\"<ENGINE>\",\"entry\":null,"
        + "\"irRecompiled\":false,\"signed\":false},\"ok\":false}\n";

    @Test
    void invalidInitialUrlsAreUsageBeforeAnyRequest() {
        String[] bad = {
            "https://hé.example/jwks.json",
            "https://h.example/a b.json",
            "https://h.example/%zz",
            "https://h.example\\jwks.json",
            "https://a:b:443/jwks.json",
            "https://h::443/jwks.json",
            "https:///jwks.json",
            "https://u:p@h.example/jwks.json",
            "https://@h.example/jwks.json",
            "https://h.example:/jwks.json",
            "https://h.example:65536/jwks.json",
            "https://h.example/a[b]",
        };
        for (String url : bad) {
            long start = System.nanoTime();
            Fixtures.Result r = Fixtures.run(Fixtures.PACKAGE_V1.resolve("packages"), "{}".getBytes(StandardCharsets.UTF_8),
                Map.of(), "run", "b-credit-limit", "--input", "-", "--jwks", url);
            long ms = (System.nanoTime() - start) / 1_000_000;
            assertEquals(2, r.exit(), url + " " + r.stderr());
            assertEquals(USAGE_ENVELOPE, StdoutNormalizer.normalize(r.stdout()), url);
            org.junit.jupiter.api.Assertions.assertTrue(ms < 2_000, url + " 不应发出请求，耗时 " + ms + "ms");
        }
    }
}
