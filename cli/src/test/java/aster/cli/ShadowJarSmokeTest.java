package aster.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * fat jar 冒烟（ADR 0059 P1 T9）：以子进程 {@code java -jar aster-cli-<ver>.jar} 跑 conformance 用例，验证 shadow 后的运行时闭包
 * （排除 quarkus / ANTLR 工具链 / icu4j 之后）仍能编译 B 包、求值、超时与深递归，且 stdout 只有信封。
 * 由 {@code :cli:shadowJarSmokeTest} 设置 {@code aster.cli.jar} 后运行；普通 {@code :cli:test} 跳过。
 */
@EnabledIfSystemProperty(named = "aster.cli.jar", matches = ".+")
class ShadowJarSmokeTest {

    private record Proc(int exit, String stdout, String stderr) {}

    private static Proc java(Path cwd, String stdin, String... args) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-jar", System.getProperty("aster.cli.jar")));
        cmd.addAll(List.of(args));
        Path out = Files.createTempFile("aster-cli", ".out");
        Path err = Files.createTempFile("aster-cli", ".err");
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(cwd.toFile())
            .redirectOutput(out.toFile()).redirectError(err.toFile());
        pb.environment().remove("ASTER_TRUFFLE_FUNC");
        Process p = pb.start();
        try (var in = p.getOutputStream()) {
            if (stdin != null) {
                in.write(stdin.getBytes(StandardCharsets.UTF_8));
            }
        }
        assertTrue(p.waitFor(60, TimeUnit.SECONDS), "进程未在 60 s 内退出");
        return new Proc(p.exitValue(), Files.readString(out), Files.readString(err));
    }

    private static void assertCase(String casesFile, String name) throws Exception {
        Path file = Fixtures.PACKAGE_V1.resolve(casesFile);
        for (var c : Fixtures.MAPPER.readTree(file.toFile()).path("cases")) {
            if (!c.path("name").asText().equals(name)) {
                continue;
            }
            List<String> args = new ArrayList<>();
            c.path("args").forEach(a -> args.add(a.asText()));
            Proc r = java(file.getParent(), c.has("stdin") ? c.path("stdin").asText() : null, args.toArray(String[]::new));
            assertEquals(c.path("expectedStdout").asText(), StdoutNormalizer.normalize(r.stdout()), "stderr: " + r.stderr());
            assertEquals(c.path("expectedExit").asInt(), r.exit(), "stderr: " + r.stderr());
            return;
        }
        throw new AssertionError("找不到用例 " + casesFile + " :: " + name);
    }

    @Test
    void bPackageRecompilesAndEvaluates() throws Exception {
        assertCase("packages/b-credit-limit.cases.json", "allow");
    }

    @Test
    void zhZipPackage() throws Exception {
        assertCase("packages/b-zh.cases.json", "zip-nested-entry");
    }

    @Test
    void timeoutIsEnforcedInTheJar() throws Exception {
        long start = System.nanoTime();
        assertCase("packages/a-timeout.cases.json", "timeout-1s");
        long ms = (System.nanoTime() - start) / 1_000_000;
        assertTrue(ms < 10_000, "超时用例进程耗时 " + ms + "ms");
    }

    @Test
    void deepRecursionOnDefaultStack() throws Exception {
        assertCase("packages/a-deep-recursion.cases.json", "depth-20000");
    }

    @Test
    void versionOutput() throws Exception {
        Proc r = java(Fixtures.PACKAGE_V1, null, "version");
        assertEquals(0, r.exit(), r.stderr());
        assertTrue(r.stdout().startsWith("{\"cliVersion\":\"") && r.stdout().endsWith("]}\n"), r.stdout());
        assertTrue(!r.stderr().contains("SLF4J"), "slf4j-nop 应消除 SLF4J 警告: " + r.stderr());
        assertTrue(!r.stderr().contains("restricted method"), "Enable-Native-Access 应消除受限方法警告: " + r.stderr());
    }
}
