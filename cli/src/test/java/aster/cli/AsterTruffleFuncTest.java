package aster.cli;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 环境变量 ASTER_TRUFFLE_FUNC 会改写 Truffle 的缺省入口：JVM CLI 在 usage 阶段拒绝它（README「检查顺序」usage 行，修订 1 A5）。 */
class AsterTruffleFuncTest {

    @Test
    void rejectsAsterTruffleFunc() {
        Fixtures.Result r = Fixtures.run(Fixtures.PACKAGE_V1.resolve("packages"),
            "{}".getBytes(StandardCharsets.UTF_8), Map.of("ASTER_TRUFFLE_FUNC", "label"),
            "run", "a-multi-entry", "--input", "-", "--jwks", "../keys/test-jwks.json");
        assertEquals(2, r.exit());
        assertTrue(r.stdout().startsWith("{\"error\":{\"code\":\"USAGE\""), r.stdout());
        assertTrue(r.stdout().contains("\"entry\":null"), r.stdout());
    }

    @Test
    void versionOutput() {
        Fixtures.Result r = Fixtures.run(Fixtures.PACKAGE_V1, null, Map.of(), "version");
        assertEquals(0, r.exit());
        assertTrue(r.stdout().matches("\\{\"cliVersion\":\"[^\"]+\",\"coreIrVersions\":\\[2],\"engine\":\"aster-lang-truffle\","
            + "\"formatVersions\":\\[\"1.0\",\"1.1\"],\"toolchainId\":\"abi=2.0;[^\"]+\",\"trustAnchorKids\":\\[\"export-2026-11a\"]}\n"), r.stdout());
    }
}
