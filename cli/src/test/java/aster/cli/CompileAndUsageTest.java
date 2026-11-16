package aster.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** compile 子命令与命令行边角（与 Node CLI 的同一裁决，T11 交叉核对）。 */
class CompileAndUsageTest {

    @TempDir
    Path tmp;

    private static final Path PACKAGES = Fixtures.PACKAGE_V1.resolve("packages");

    private static String code(Fixtures.Result r) {
        int i = r.stdout().indexOf("\"code\":\"");
        return i < 0 ? null : r.stdout().substring(i + 8, r.stdout().indexOf('"', i + 8));
    }

    @Test
    void optionFollowedByKnownOptionIsMissingValue() {
        Fixtures.Result r = Fixtures.run(PACKAGES, "{}".getBytes(StandardCharsets.UTF_8), Map.of(),
            "run", "a-multi-entry", "--entry", "--input", "-", "--jwks", "../keys/test-jwks.json");
        assertEquals(2, r.exit(), r.stdout());
        assertEquals("USAGE", code(r));
    }

    @Test
    void httpJwksIsUsage() {
        Fixtures.Result r = Fixtures.run(PACKAGES, "{}".getBytes(StandardCharsets.UTF_8), Map.of(),
            "run", "a-multi-entry", "--input", "-", "--jwks", "http://example.invalid/jwks.json");
        assertEquals("USAGE", code(r));
    }

    @Test
    void compilePackagePrintsPlatformIrWithNewline() throws Exception {
        Fixtures.Result r = Fixtures.run(PACKAGES, null, Map.of(),
            "compile", "b-credit-limit", "--jwks", "../keys/test-jwks.json");
        assertEquals(0, r.exit(), r.stderr());
        assertTrue(r.stdout().endsWith("\n"));
        String ir = r.stdout().substring(0, r.stdout().length() - 1);
        String expected = Fixtures.json("packages/b-credit-limit/manifest.json").path("coreIr").path("sha256").asText();
        assertEquals(expected, HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
            .digest(ir.getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void compileWithOutWritesFileAndPrintsNothing() throws Exception {
        Path out = tmp.resolve("core.json");
        Fixtures.Result r = Fixtures.run(PACKAGES, null, Map.of(),
            "compile", "b-credit-limit", "--jwks", "../keys/test-jwks.json", "--out", out.toString());
        assertEquals(0, r.exit(), r.stderr());
        assertEquals("", r.stdout());
        assertTrue(Files.readString(out).startsWith("{") && !Files.readString(out).endsWith("\n"));
    }

    @Test
    void compileOptionConflictsAreUsage() throws Exception {
        assertEquals("USAGE", code(Fixtures.run(PACKAGES, null, Map.of(),
            "compile", "b-credit-limit", "--jwks", "../keys/test-jwks.json", "--locale", "de-DE")));
        Path src = tmp.resolve("p.aster");
        Files.writeString(src, "Module demo.\n\nRule f given x as Int, produce Int:\n  Return x times 2.\n");
        assertEquals("USAGE", code(Fixtures.run(tmp, null, Map.of(), "compile", "p.aster", "--allow-unsigned")));
        assertEquals("USAGE", code(Fixtures.run(tmp, null, Map.of(), "compile", "missing.aster")));
    }

    @Test
    void compileSourceAndCompileError() throws Exception {
        Files.writeString(tmp.resolve("ok.aster"), "Module demo.\n\nRule f given x as Int, produce Int:\n  Return x times 2.\n");
        Fixtures.Result ok = Fixtures.run(tmp, null, Map.of(), "compile", "ok.aster");
        assertEquals(0, ok.exit(), ok.stderr());
        assertTrue(ok.stdout().startsWith("{") && ok.stdout().endsWith("}\n"));

        Files.writeString(tmp.resolve("bad.aster"), "Module demo.\n\nRule f given x as Int produce:\n  Return.\n");
        Fixtures.Result bad = Fixtures.run(tmp, null, Map.of(), "compile", "bad.aster");
        assertEquals(4, bad.exit());
        assertEquals("COMPILE_ERROR", code(bad));

        Files.writeString(tmp.resolve("alias.json"), "{\"NOPE\":[\"x y\"]}");
        Fixtures.Result alias = Fixtures.run(tmp, null, Map.of(), "compile", "ok.aster", "--alias-set", "alias.json");
        assertEquals("COMPILE_ERROR", code(alias));
    }
}
