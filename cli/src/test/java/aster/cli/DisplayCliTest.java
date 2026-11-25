package aster.cli;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 包格式 1.1 显示排版的 CLI 行为（ADR 0059 修订 4 D5–D8；计划 T4 Step 1，与 aster-lang-ts {@code display-cli.test.ts}
 * 逐条对应）：黄金包 {@code display/packages/b-jingyesi}（《静夜思》）与 {@code display/edits/b-jingyesi/*.txt}。
 * stdout / 退出码由 {@link ConformanceCasesTest} 按夹具逐字节核对；这里补 stderr 说明与关键 meta 字段。
 */
class DisplayCliTest {

    private static final Path DIR = Fixtures.PACKAGE_V1.resolve("display/packages");
    private static final String JWKS = "../../keys/test-jwks.json";
    private static final String POEM = "床前明月光，\n疑是地上霜。\n举头望明月，\n低头思故乡。";

    @Test
    void showPrintsThePoemAndExplainsTheLayoutOnStderr() {
        Fixtures.Result r = Fixtures.run(DIR, null, Map.of(), "show", "b-jingyesi", "--jwks", JWKS);
        assertEquals(0, r.exit(), r.stderr());
        assertEquals(POEM + "\n", r.stdout());
        assertTrue(r.stderr().contains("\"，\\n\" ⇐ \"。\\n\""), r.stderr());
        assertTrue(r.stderr().contains("\"\" ⇐ \" \""), r.stderr());
        assertTrue(r.stderr().contains("思故乡 → 静夜思"), r.stderr());
    }

    @Test
    void showListsHiddenEscapesOnStderr() {
        // ADR 0059 修订 5 §4.3：每个被隐藏反引号的名字转义一行，JSON 字符串形式，按出现顺序
        Fixtures.Result r = Fixtures.run(DIR, null, Map.of(), "show", "b-display-escape", "--jwks", JWKS);
        assertEquals(0, r.exit(), r.stderr());
        assertTrue(!r.stdout().contains("`"), r.stdout());
        assertEquals(2, r.stderr().lines().filter(l -> l.trim().equals("hidden escape: \"`当`\" (name)")).count(), r.stderr());
    }

    @Test
    void showCanonicalPrintsTheSource() {
        Fixtures.Result r = Fixtures.run(DIR, null, Map.of(), "show", "b-jingyesi", "--canonical", "--jwks", JWKS);
        assertEquals(0, r.exit(), r.stderr());
        assertEquals("床前 明月光。\n疑是 地上霜，举头 望明月：\n  低头 思故乡。\n\n", r.stdout());
    }

    @Test
    void runPlainGivesJingyesi() throws Exception {
        Fixtures.Result r = Fixtures.run(DIR, "{}".getBytes(StandardCharsets.UTF_8), Map.of(), "run", "b-jingyesi", "--input", "-", "--jwks", JWKS);
        assertEquals(0, r.exit(), r.stderr());
        JsonNode env = Fixtures.MAPPER.readTree(r.stdout());
        assertEquals("静夜思", env.path("value").asText());
        assertTrue(env.path("meta").path("display").isMissingNode(), "不带 --display 时 meta 没有 display");
    }

    @Test
    void runDisplayRunsTheEditedPoemAndPrintsTheDerivedSourceFirst() throws Exception {
        Fixtures.Result r = Fixtures.run(DIR, "{}".getBytes(StandardCharsets.UTF_8), Map.of(),
            "run", "b-jingyesi", "--display", "../edits/b-jingyesi/last-line-literal.txt", "--input", "-", "--jwks", JWKS);
        assertEquals(0, r.exit(), r.stderr());
        JsonNode env = Fixtures.MAPPER.readTree(r.stdout());
        assertEquals("月光", env.path("value").asText());
        assertTrue(env.path("meta").path("coreIrSha256").isNull());
        assertTrue(env.path("meta").path("irRecompiled").asBoolean());
        assertEquals(false, env.path("meta").path("display").path("matchesPackage").asBoolean(true));
        int notice = r.stderr().indexOf("不在包签名覆盖之内");
        int source = r.stderr().indexOf("低头 「月光」。");
        assertTrue(notice >= 0 && source > notice, r.stderr());
    }

    @Test
    void crlfFileIsUnmappableWithAHint() {
        Fixtures.Result r = Fixtures.run(DIR, "{}".getBytes(StandardCharsets.UTF_8), Map.of(),
            "run", "b-jingyesi", "--display", "../edits/b-jingyesi/crlf.txt", "--input", "-", "--jwks", JWKS);
        assertEquals(2, r.exit());
        assertTrue(r.stdout().contains("\"DISPLAY_UNMAPPABLE\""), r.stdout());
        assertTrue(r.stderr().contains("CRLF"), r.stderr());
    }

    @Test
    void showOnALegacyPackageSaysThereIsNoLayout() {
        Fixtures.Result r = Fixtures.run(DIR, null, Map.of(), "show", "../../packages/b-credit-limit", "--jwks", JWKS);
        assertEquals(0, r.exit(), r.stderr());
        assertTrue(r.stderr().contains("没有显示排版"), r.stderr());
    }

    @Test
    void unsignedShowWarnsThatTheLayoutIsUnverified() {
        Fixtures.Result r = Fixtures.run(Fixtures.PACKAGE_V1.resolve("display/tamper"), null, Map.of(), "show", "display-unsigned", "--allow-unsigned");
        assertEquals(0, r.exit(), r.stderr());
        assertEquals(POEM + "\n", r.stdout());
        assertTrue(r.stderr().contains("未经签名核验"), r.stderr());
    }

    @Test
    void versionListsFormat11AndNewCodesMatchTheTable() throws Exception {
        Fixtures.Result r = Fixtures.run(DIR, null, Map.of(), "version");
        assertEquals("[\"1.0\",\"1.1\"]", Fixtures.MAPPER.readTree(r.stdout()).path("formatVersions").toString());
        JsonNode since = Fixtures.json("exit-codes.json").path("codesSince").path("1.1");
        for (ErrorCode c : new ErrorCode[]{ErrorCode.PKG_DISPLAY_INVALID, ErrorCode.DISPLAY_UNMAPPABLE, ErrorCode.DISPLAY_AMBIGUOUS, ErrorCode.DISPLAY_REJECTED}) {
            assertEquals(since.path(c.name()).path("exit").asInt(), c.exit(), c.name());
            assertEquals(since.path(c.name()).path("message").asText(), c.message(), c.name());
        }
    }
}
