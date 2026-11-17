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

    static Result run(Path cwd, byte[] stdin, Map<String, String> env, String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exit = AsterCli.run(args, new ByteArrayInputStream(stdin == null ? new byte[0] : stdin),
            new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8),
            cwd, env);
        return new Result(exit, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }
}
