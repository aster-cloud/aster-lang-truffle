package aster.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/** 兄弟仓 aster-lang-test {@code conformance/package-v1/} 的定位与进程内调用 CLI 的工具。 */
final class Fixtures {

    static final ObjectMapper MAPPER = new ObjectMapper();
    static final Path PACKAGE_V1 = locate();

    private Fixtures() {}

    /** CI：兄弟仓检出在根项目同级（workspace/aster-lang-test）；本地：并列目录。两种布局下都在根项目的上一级。 */
    private static Path locate() {
        Path root = Path.of(System.getProperty("aster.root.dir", ".."));
        for (Path base : new Path[]{root.resolve("aster-lang-test"), root.resolve("..").resolve("aster-lang-test")}) {
            Path p = base.resolve("conformance/package-v1").normalize();
            if (Files.isDirectory(p)) {
                return p;
            }
        }
        throw new IllegalStateException("缺少兄弟仓 aster-lang-test 的 conformance/package-v1（根项目 " + root + "）");
    }

    static JsonNode json(String rel) throws IOException {
        return MAPPER.readTree(PACKAGE_V1.resolve(rel).toFile());
    }

    /** 一次进程内调用的结果。 */
    record Result(int exit, String stdout, String stderr) {}

    /**
     * 调用一次 CLI。系统属性 {@code aster.cli.native} 给出时以子进程运行该原生可执行文件（原生一致性门，ADR 0063）；
     * 否则 {@code aster.cli.jar} 给出时以子进程 {@code java -jar <jar>} 运行（fat jar 一致性门，T9）；
     * 都没有则进程内调用 {@link AsterCli#run}。{@code env} 是额外设置的环境变量；子进程模式下 ASTER_TRUFFLE_FUNC 一律清除。
     */
    static Result run(Path cwd, byte[] stdin, Map<String, String> env, String... args) {
        String exe = System.getProperty("aster.cli.native");
        if (exe != null && !exe.isBlank()) {
            return runProcess(java.util.List.of(exe), cwd, stdin, env, args);
        }
        String jar = System.getProperty("aster.cli.jar");
        if (jar != null && !jar.isBlank()) {
            return runProcess(java.util.List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-jar", jar), cwd, stdin, env, args);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exit = AsterCli.run(args, new ByteArrayInputStream(stdin == null ? new byte[0] : stdin),
            new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8),
            cwd, env);
        return new Result(exit, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    private static Result runProcess(java.util.List<String> launcher, Path cwd, byte[] stdin, Map<String, String> env,
                                     String... args) {
        java.util.List<String> cmd = new java.util.ArrayList<>(launcher);
        cmd.addAll(java.util.List.of(args));
        try {
            Path outFile = Files.createTempFile("aster-cli", ".out");
            Path errFile = Files.createTempFile("aster-cli", ".err");
            ProcessBuilder pb = new ProcessBuilder(cmd).directory(cwd.toFile())
                .redirectOutput(outFile.toFile()).redirectError(errFile.toFile());
            pb.environment().remove("ASTER_TRUFFLE_FUNC");
            pb.environment().putAll(env);
            Process p = pb.start();
            try (var in = p.getOutputStream()) {
                if (stdin != null) {
                    in.write(stdin);
                }
            } catch (IOException ignored) {
                // CLI 可能在读完 stdin 之前就已退出（usage 阶段失败）
            }
            if (!p.waitFor(120, java.util.concurrent.TimeUnit.SECONDS)) {
                p.destroyForcibly();
                throw new IllegalStateException("aster-cli 子进程 120 s 未退出: " + cmd);
            }
            Result r = new Result(p.exitValue(), Files.readString(outFile), Files.readString(errFile));
            Files.deleteIfExists(outFile);
            Files.deleteIfExists(errFile);
            return r;
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
