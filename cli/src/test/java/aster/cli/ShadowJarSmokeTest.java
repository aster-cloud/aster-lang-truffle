package aster.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.condition.OS;

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
 * 另以 Release 附带的 POSIX 启动脚本 {@code aster}（{@code aster.cli.launcher}，aster-cloud/aster-lang-truffle#188）跑同一组用例：
 * stdout 与退出码与直接 {@code java -jar} 相同，且 stderr 不含 JEP 498 的 {@code sun.misc.Unsafe} 警告。
 * 由 {@code :cli:shadowJarSmokeTest} 设置 {@code aster.cli.jar} 与 {@code aster.cli.launcher} 后运行；普通 {@code :cli:test} 跳过。
 */
@EnabledIfSystemProperty(named = "aster.cli.jar", matches = ".+")
class ShadowJarSmokeTest {

    private record Proc(int exit, String stdout, String stderr) {}

    /** JEP 498 警告四行里都含这个类名；启动脚本运行时 stderr 不得出现。 */
    private static final String UNSAFE_WARNING = "sun.misc.Unsafe";

    private static Proc java(Path cwd, String stdin, String... args) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-jar", System.getProperty("aster.cli.jar")));
        return exec(cmd, cwd, stdin, args);
    }

    /** 经启动脚本运行：用 /bin/sh 执行（Release 下载的资产不带可执行位），JAVA_HOME 指向运行测试的 JDK。 */
    private static Proc launcher(Path cwd, String stdin, String... args) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>(List.of("/bin/sh", System.getProperty("aster.cli.launcher")));
        return exec(cmd, cwd, stdin, args);
    }

    private static Proc exec(List<String> cmd, Path cwd, String stdin, String... args) throws IOException, InterruptedException {
        cmd.addAll(List.of(args));
        Path out = Files.createTempFile("aster-cli", ".out");
        Path err = Files.createTempFile("aster-cli", ".err");
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(cwd.toFile())
            .redirectOutput(out.toFile()).redirectError(err.toFile());
        pb.environment().remove("ASTER_TRUFFLE_FUNC");
        pb.environment().remove("ASTER_CLI_JAR");
        pb.environment().remove("JDK_JAVA_OPTIONS");
        pb.environment().remove("JAVA_TOOL_OPTIONS");
        pb.environment().put("JAVA_HOME", System.getProperty("java.home"));
        Process p = pb.start();
        try (var in = p.getOutputStream()) {
            if (stdin != null) {
                in.write(stdin.getBytes(StandardCharsets.UTF_8));
            }
        }
        assertTrue(p.waitFor(60, TimeUnit.SECONDS), "进程未在 60 s 内退出");
        return new Proc(p.exitValue(), Files.readString(out), Files.readString(err));
    }

    @FunctionalInterface
    private interface Runner {
        Proc run(Path cwd, String stdin, String... args) throws IOException, InterruptedException;
    }

    private static Proc assertCase(String casesFile, String name) throws Exception {
        return assertCase(ShadowJarSmokeTest::java, casesFile, name);
    }

    private static Proc assertCase(Runner runner, String casesFile, String name) throws Exception {
        Path file = Fixtures.PACKAGE_V1.resolve(casesFile);
        for (var c : Fixtures.MAPPER.readTree(file.toFile()).path("cases")) {
            if (!c.path("name").asText().equals(name)) {
                continue;
            }
            List<String> args = new ArrayList<>();
            c.path("args").forEach(a -> args.add(a.asText()));
            Proc r = runner.run(file.getParent(), c.has("stdin") ? c.path("stdin").asText() : null, args.toArray(String[]::new));
            assertEquals(c.path("expectedStdout").asText(), StdoutNormalizer.normalize(r.stdout()), "stderr: " + r.stderr());
            assertEquals(c.path("expectedExit").asInt(), r.exit(), "stderr: " + r.stderr());
            return r;
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

    /** 经启动脚本求值 B 包：stdout / 退出码与 java -jar 相同（assertCase 比对夹具期望），stderr 无 Unsafe 警告。 */
    @Test
    @DisabledOnOs(OS.WINDOWS)
    void launcherRunHasNoUnsafeWarning() throws Exception {
        Proc r = assertCase(ShadowJarSmokeTest::launcher, "packages/b-credit-limit.cases.json", "allow");
        assertTrue(!r.stderr().contains(UNSAFE_WARNING), "启动脚本应消除 sun.misc.Unsafe 警告: " + r.stderr());
    }

    /** 超时退出码（5）经 exec 原样透传；Truffle 在超时路径上同样会触发 Unsafe 调用。 */
    @Test
    @DisabledOnOs(OS.WINDOWS)
    void launcherPassesTimeoutExitCode() throws Exception {
        Proc r = assertCase(ShadowJarSmokeTest::launcher, "packages/a-timeout.cases.json", "timeout-1s");
        assertTrue(!r.stderr().contains(UNSAFE_WARNING), "启动脚本应消除 sun.misc.Unsafe 警告: " + r.stderr());
    }

    /** version 经启动脚本输出与直接 java -jar 逐字节相同。 */
    @Test
    @DisabledOnOs(OS.WINDOWS)
    void launcherVersionMatchesJar() throws Exception {
        Proc viaJar = java(Fixtures.PACKAGE_V1, null, "version");
        Proc viaLauncher = launcher(Fixtures.PACKAGE_V1, null, "version");
        assertEquals(0, viaLauncher.exit(), viaLauncher.stderr());
        assertEquals(viaJar.stdout(), viaLauncher.stdout());
        assertEquals("", viaLauncher.stderr());
    }

    /** USAGE 等非零退出码与 stderr 诊断原样透传：启动脚本不吞、不改写错误输出。 */
    @Test
    @DisabledOnOs(OS.WINDOWS)
    void launcherPassesUsageErrorThrough() throws Exception {
        Proc viaJar = java(Fixtures.PACKAGE_V1, null, "no-such-command");
        Proc viaLauncher = launcher(Fixtures.PACKAGE_V1, null, "no-such-command");
        assertTrue(viaJar.exit() != 0, viaJar.stderr());
        assertEquals(viaJar.exit(), viaLauncher.exit(), viaLauncher.stderr());
        assertEquals(viaJar.stdout(), viaLauncher.stdout());
        assertEquals(viaJar.stderr(), viaLauncher.stderr());
    }
}
