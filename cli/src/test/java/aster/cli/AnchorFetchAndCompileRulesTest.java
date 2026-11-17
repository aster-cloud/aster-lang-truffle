package aster.cli;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #218 补充裁决中夹具尚未覆盖的边角：https 取回（非重定向 3xx、Content-Encoding、忽略代理环境变量）、本地锚文件 1 MiB 上限、
 * compile 的源码编码与 {@code --out} 指向目录。
 */
class AnchorFetchAndCompileRulesTest {

    @TempDir
    static Path tmp;

    private static HttpsFixtureServer server;
    private static final Path PACKAGES = Fixtures.PACKAGE_V1.resolve("packages");
    private static final String STDIN = "{\"applicant\":{\"activeDefaults\":0,\"creditScore\":760,\"monthlyDebt\":\"2000\","
        + "\"monthlyIncome\":\"9000\",\"requestedAmount\":\"20000\"}}";

    @BeforeAll
    static void start() throws Exception {
        JsonNode routes = Fixtures.MAPPER.readTree("""
            {"/jwks.json": {"status": 200, "file": "keys/test-jwks.json"},
             "/r300": {"status": 300, "location": "/jwks.json"},
             "/r307": {"status": 307, "location": "jwks.json"},
             "/gzip": {"status": 200, "file": "keys/test-jwks.json", "contentEncoding": "gzip"},
             "/created": {"status": 201, "file": "keys/test-jwks.json"}}
            """);
        server = new HttpsFixtureServer(routes, Files.createTempDirectory(tmp, "srv"));
    }

    @AfterAll
    static void stop() {
        server.close();
    }

    private static Fixtures.Result run(String path, Map<String, String> extraEnv) {
        Map<String, String> env = new java.util.HashMap<>(extraEnv);
        env.put("ASTER_ANCHOR_CA_FILE", server.caPem().toString());
        return Fixtures.run(PACKAGES, STDIN.getBytes(StandardCharsets.UTF_8), env,
            "run", "b-credit-limit", "--input", "-", "--jwks", "https://localhost:" + server.port() + path);
    }

    private static String code(Fixtures.Result r) {
        int i = r.stdout().indexOf("\"code\":\"");
        return i < 0 ? null : r.stdout().substring(i + 8, r.stdout().indexOf('"', i + 8));
    }

    @Test
    void relativeRedirectAndAny2xxAreAccepted() {
        assertEquals(0, run("/r307", Map.of()).exit());
        assertEquals(0, run("/created", Map.of()).exit());
    }

    @Test
    void nonRedirect3xxIsUsage() {
        assertEquals("USAGE", code(run("/r300", Map.of())));
    }

    @Test
    void contentEncodingOtherThanIdentityIsUsage() {
        assertEquals("USAGE", code(run("/gzip", Map.of())));
    }

    @Test
    void proxyEnvironmentIsIgnored() {
        // 指向不存在的代理：若被采用，请求必然失败
        Fixtures.Result r = run("/jwks.json", Map.of("HTTPS_PROXY", "http://127.0.0.1:9", "https_proxy", "http://127.0.0.1:9"));
        assertEquals(0, r.exit(), r.stderr());
    }

    @Test
    void localAnchorOverOneMiBIsUsage() throws Exception {
        Path big = tmp.resolve("big-jwks.json");
        byte[] jwks = Files.readAllBytes(Fixtures.PACKAGE_V1.resolve("keys/test-jwks.json"));
        byte[] padded = java.util.Arrays.copyOf(jwks, 1024 * 1024 + 1);
        java.util.Arrays.fill(padded, jwks.length, padded.length, (byte) ' ');
        Files.write(big, padded);
        Fixtures.Result r = Fixtures.run(PACKAGES, STDIN.getBytes(StandardCharsets.UTF_8), Map.of(),
            "run", "b-credit-limit", "--input", "-", "--jwks", big.toString());
        assertEquals("USAGE", code(r));
    }

    @Test
    void sourceEncodingRules() throws Exception {
        Path dir = Files.createTempDirectory(tmp, "src");
        String src = "Module demo.\n\nRule f given x as Int, produce Int:\n  Return x times 2.\n";
        Files.writeString(dir.resolve("ok.aster"), src);
        Files.write(dir.resolve("bom.aster"), ("\uFEFF" + src).getBytes(StandardCharsets.UTF_8));
        Files.write(dir.resolve("bad.aster"), new byte[]{'M', (byte) 0xC3, (byte) 0x28});
        Files.writeString(dir.resolve("ideographic-space.aster"), "\u3000");

        Fixtures.Result ok = Fixtures.run(dir, null, Map.of(), "compile", "ok.aster");
        Fixtures.Result bom = Fixtures.run(dir, null, Map.of(), "compile", "bom.aster");
        assertEquals(0, bom.exit(), bom.stderr());
        assertEquals(ok.stdout(), bom.stdout(), "去掉一个 BOM 后与无 BOM 源码同一 IR");
        assertEquals("USAGE", code(Fixtures.run(dir, null, Map.of(), "compile", "bad.aster")));
        assertEquals("COMPILE_ERROR", code(Fixtures.run(dir, null, Map.of(), "compile", "ideographic-space.aster")));
    }

    @Test
    void outPointingAtDirectoryIsUsage() throws Exception {
        Path dir = Files.createTempDirectory(tmp, "outdir");
        Files.writeString(dir.resolve("ok.aster"), "Module demo.\n\nRule f given x as Int, produce Int:\n  Return x.\n");
        Files.createDirectory(dir.resolve("target"));
        Fixtures.Result r = Fixtures.run(dir, null, Map.of(), "compile", "ok.aster", "--out", "target");
        assertEquals("USAGE", code(r));
        assertTrue(Files.isDirectory(dir.resolve("target")));
    }

    @Test
    void emptySeparateArgumentIsUsage() {
        assertEquals("USAGE", code(Fixtures.run(PACKAGES, STDIN.getBytes(StandardCharsets.UTF_8), Map.of(),
            "run", "b-credit-limit", "--entry", "", "--input", "-", "--jwks", "../keys/test-jwks.json")));
    }
}
