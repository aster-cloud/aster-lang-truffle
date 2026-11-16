package aster.cli;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 跑 aster-lang-test {@code conformance/package-v1} 的全部用例（黄金包、篡改、非法输入与运行时生成的体积夹具）：
 * 进程内调用 {@link AsterCli#run}，stdout 经 {@link StdoutNormalizer} 归一后与 {@code expectedStdout} 逐字节比较，退出码与
 * {@code expectedExit} 比较。v1 夹具没有只适用于一侧的用例，{@code engines} 标注的用例只在含 {@code jre} 时运行。
 */
class ConformanceCasesTest {

    @TempDir
    static Path tmp;

    @TestFactory
    Stream<DynamicTest> cases() throws IOException {
        List<DynamicTest> tests = new ArrayList<>();
        List<Path> files = new ArrayList<>();
        try (Stream<Path> s = Files.list(Fixtures.PACKAGE_V1.resolve("packages"))) {
            s.filter(p -> p.getFileName().toString().endsWith(".cases.json")).sorted().forEach(files::add);
        }
        files.add(Fixtures.PACKAGE_V1.resolve("tamper/cases.json"));
        files.add(Fixtures.PACKAGE_V1.resolve("inputs-invalid/cases.json"));
        int total = 0;
        for (Path f : files) {
            for (JsonNode c : Fixtures.MAPPER.readTree(f.toFile()).path("cases")) {
                if (!appliesToJre(c)) {
                    continue;
                }
                total++;
                Path cwd = f.getParent();
                String name = Fixtures.PACKAGE_V1.relativize(f) + " :: " + c.path("name").asText();
                tests.add(DynamicTest.dynamicTest(name, () -> assertCase(c, cwd, args(c, null), stdin(c))));
            }
        }
        for (JsonNode c : Fixtures.json("tamper/test-time.json").path("cases")) {
            total++;
            tests.add(DynamicTest.dynamicTest("tamper/test-time.json :: " + c.path("name").asText(), () -> {
                Path pkg = buildRecipe(c);
                assertCase(c, Fixtures.PACKAGE_V1, args(c, pkg), stdinRecipe(c));
            }));
        }
        assertTrue(total >= 400, "用例过少（" + total + "），夹具路径可能漂移");
        return tests.stream();
    }

    private static boolean appliesToJre(JsonNode c) {
        if (!c.has("engines")) {
            return true;
        }
        for (JsonNode e : c.path("engines")) {
            if (e.asText().equals("jre")) {
                return true;
            }
        }
        return false;
    }

    private static void assertCase(JsonNode c, Path cwd, String[] args, byte[] stdin) {
        Fixtures.Result r = Fixtures.run(cwd, stdin, Map.of(), args);
        String actual = StdoutNormalizer.normalize(r.stdout());
        assertEquals(c.path("expectedStdout").asText(), actual, "stdout（stderr: " + r.stderr() + "）");
        assertEquals(c.path("expectedExit").asInt(), r.exit(), "退出码（stderr: " + r.stderr() + "）");
    }

    private static String[] args(JsonNode c, Path pkg) {
        List<String> out = new ArrayList<>();
        for (JsonNode a : c.path("args")) {
            String s = a.asText().replace("<PACKAGE_V1>", Fixtures.PACKAGE_V1.toString());
            out.add(pkg == null ? s : s.replace("<PKG>", pkg.toString()));
        }
        return out.toArray(String[]::new);
    }

    private static byte[] stdin(JsonNode c) {
        if (c.has("stdinB64")) {
            return Base64.getDecoder().decode(c.path("stdinB64").asText());
        }
        return c.has("stdin") ? c.path("stdin").asText().getBytes(StandardCharsets.UTF_8) : null;
    }

    private static byte[] stdinRecipe(JsonNode c) {
        if (!c.has("stdinRecipe")) {
            return stdin(c);
        }
        JsonNode r = c.path("stdinRecipe");
        return (r.path("prefix").asText() + " ".repeat(r.path("padding").asInt()) + r.path("suffix").asText())
            .getBytes(StandardCharsets.UTF_8);
    }

    /** 按 test-time.json 的配方在临时目录生成包（复制目录 + 零字节文件，或 zip 末尾追加零字节）。 */
    private static Path buildRecipe(JsonNode c) throws IOException {
        JsonNode recipe = c.path("recipe");
        Path dir = Files.createTempDirectory(tmp, c.path("name").asText());
        if (recipe.has("copyPackage")) {
            Path src = Fixtures.PACKAGE_V1.resolve(recipe.path("copyPackage").asText());
            Path dst = dir.resolve("pkg");
            try (Stream<Path> walk = Files.walk(src)) {
                for (Path p : walk.toList()) {
                    Files.copy(p, dst.resolve(src.relativize(p).toString()));
                }
            }
            for (JsonNode add : recipe.path("add")) {
                try (RandomAccessFile f = new RandomAccessFile(dst.resolve(add.path("path").asText()).toFile(), "rw")) {
                    f.setLength(add.path("size").asLong());
                }
            }
            return dst;
        }
        if (recipe.has("copyZip")) {
            Path dst = dir.resolve("pkg.zip");
            Files.copy(Fixtures.PACKAGE_V1.resolve(recipe.path("copyZip").asText()), dst);
            try (RandomAccessFile f = new RandomAccessFile(dst.toFile(), "rw")) {
                f.setLength(f.length() + recipe.path("appendZeros").asLong());
            }
            return dst;
        }
        return Fixtures.PACKAGE_V1.resolve(recipe.path("usePackage").asText());
    }
}
