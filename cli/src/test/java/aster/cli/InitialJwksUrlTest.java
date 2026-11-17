package aster.cli;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 初始 {@code --jwks} https URL 与重定向目标同一组检查（终审 V-1），失败在 usage 阶段：输出与其他 USAGE 相同的错误信封
 * （meta 全为 null / false），且<b>不发请求</b>。
 *
 * <p>非法 URL 尽量指向本测试的 https 服务（同一 host:port），服务端计数证明请求从未发出（终审 T-1；ts#355 同一做法），
 * 合法的 {@code /ok} 计 1 次作为正对照。无法指向服务的形态（{@code a:b:443}、空 host 等）只断言信封。
 */
class InitialJwksUrlTest {

    @TempDir
    static Path tmp;

    private static HttpsFixtureServer server;

    private static final String USAGE_ENVELOPE = "{\"error\":{\"code\":\"USAGE\",\"message\":\"invalid command line\"},"
        + "\"meta\":{\"cliVersion\":\"<CLI_VERSION>\",\"coreIrSha256\":null,\"engine\":\"<ENGINE>\",\"entry\":null,"
        + "\"irRecompiled\":false,\"signed\":false},\"ok\":false}\n";
    private static final String STDIN = "{\"applicant\":{\"activeDefaults\":0,\"creditScore\":760,\"monthlyDebt\":\"2000\","
        + "\"monthlyIncome\":\"9000\",\"requestedAmount\":\"20000\"}}";

    @BeforeAll
    static void start() throws Exception {
        server = new HttpsFixtureServer(Fixtures.MAPPER.readTree("{\"/ok\": {\"status\": 200, \"file\": \"keys/test-jwks.json\"}}"),
            Files.createTempDirectory(tmp, "srv"));
    }

    @AfterAll
    static void stop() {
        server.close();
    }

    private static Fixtures.Result run(String url) {
        return Fixtures.run(Fixtures.PACKAGE_V1.resolve("packages"), STDIN.getBytes(StandardCharsets.UTF_8),
            Map.of("ASTER_ANCHOR_CA_FILE", server.caPem().toString()), "run", "b-credit-limit", "--input", "-", "--jwks", url);
    }

    private static void assertUsage(Fixtures.Result r, String url) {
        assertEquals(2, r.exit(), url + " " + r.stderr());
        assertEquals(USAGE_ENVELOPE, StdoutNormalizer.normalize(r.stdout()), url);
    }

    @Test
    void invalidInitialUrlsSendNoRequest() {
        String base = "https://localhost:" + server.port();
        String[] bad = {
            base + "/é",          // 未编码的非 ASCII
            base + "/%zz",             // 残缺的百分号编码
            base + "/a\\ok",           // 反斜杠
            base + "/a b",             // 空格
            "https://u@localhost:" + server.port() + "/ok",       // userinfo
            "https://u:p@localhost:" + server.port() + "/ok",     // userinfo
            "https://@localhost:" + server.port() + "/ok",        // 空 userinfo
            base + "/ok#[f]",          // 片段中的 [ ]
            base + "/a[b]",            // 路径中的 [ ]
        };
        for (String url : bad) {
            int before = server.hits();
            assertUsage(run(url), url);
            assertEquals(before, server.hits(), url + "：被拒绝的目标不得发出请求");
        }
        // 正对照：合法 URL 恰好发出一次请求并成功
        int before = server.hits();
        Fixtures.Result ok = run(base + "/ok");
        assertEquals(0, ok.exit(), ok.stderr());
        assertEquals(before + 1, server.hits(), "合法 URL 应恰好请求一次");
    }

    @Test
    void invalidAuthorityFormsAreUsage() {
        for (String url : new String[]{"https://a:b:443/jwks.json", "https://h::443/jwks.json", "https:///jwks.json",
                "https://h.example:/jwks.json", "https://h.example:65536/jwks.json"}) {
            assertUsage(run(url), url);
        }
    }
}
