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
        files.add(Fixtures.PACKAGE_V1.resolve("compile/cases.json"));
        int total = 0;
        for (Path f : files) {
            for (JsonNode c : Fixtures.MAPPER.readTree(f.toFile()).path("cases")) {
                if (!appliesToJre(c)) {
                    continue;
                }
                total++;
                Path cwd = f.getParent();
                String name = Fixtures.PACKAGE_V1.relativize(f) + " :: " + c.path("name").asText();
                tests.add(DynamicTest.dynamicTest(name, () -> assertCase(c, cwd, args(c, null, null), stdin(c), Map.of())));
            }
        }
        for (JsonNode c : Fixtures.json("tamper/test-time.json").path("cases")) {
            total++;
            tests.add(DynamicTest.dynamicTest("tamper/test-time.json :: " + c.path("name").asText(), () -> {
                Path pkg = buildRecipe(c);
                JsonNode https = c.path("recipe").path("httpsServer");
                if (https.isMissingNode()) {
                    assertCase(c, Fixtures.PACKAGE_V1, args(c, pkg, null), stdinRecipe(c), Map.of());
                    return;
                }
                try (HttpsFixtureServer server = new HttpsFixtureServer(https.path("routes"),
                        Files.createTempDirectory(tmp, "https"))) {
                    Map<String, String> env = new java.util.HashMap<>();
                    c.path("recipe").path("env").fields().forEachRemaining(e ->
                        env.put(e.getKey(), e.getValue().asText().replace("<CA_PEM>", server.caPem().toString())));
                    assertCase(c, Fixtures.PACKAGE_V1, args(c, pkg, server.port()), stdinRecipe(c), env);
                }
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

    private static void assertCase(JsonNode c, Path cwd, String[] args, byte[] stdin, Map<String, String> env)
            throws Exception {
        // compile 用例的 <OUT> 换成临时路径（父目录存在、文件尚不存在）
        Path outDir = Files.createTempDirectory(tmp, "out");
        Path outFile = outDir.resolve("core.json");
        for (int k = 0; k < args.length; k++) {
            args[k] = args[k].replace("<OUT>", outFile.toString());
        }
        Fixtures.Result r = Fixtures.run(cwd, stdin, env, args);
        String diag = "（stderr: " + r.stderr() + "）";
        assertEquals(c.path("expectedExit").asInt(), r.exit(), "退出码" + diag);
        if (c.has("expectedPositionFreeSha256")) {
            // compile 成功：两侧 IR 只差 origin 列，只比较投影哈希（README「compile」）
            String ir;
            if (c.has("outFile")) {
                assertEquals("", r.stdout(), "--out 时 stdout 应为空");
                ir = Files.readString(outFile);
            } else {
                assertTrue(r.stdout().endsWith("\n"), "stdout 应以换行结尾");
                ir = r.stdout().substring(0, r.stdout().length() - 1);
            }
            assertEquals(c.path("expectedPositionFreeSha256").asText(),
                aster.core.embed.PolicySourceCompiler.positionFreeSha256(ir), diag);
        } else {
            assertEquals(c.path("expectedStdout").asText(), StdoutNormalizer.normalize(r.stdout()), "stdout" + diag);
        }
        if (c.path("outFileMustNotExist").asBoolean(false)) {
            try (Stream<Path> left = Files.list(outDir)) {
                assertEquals(List.of(), left.toList(), "失败时不得创建目标文件或留下临时文件");
            }
        }
    }

    private static String[] args(JsonNode c, Path pkg, Integer port) {
        List<String> out = new ArrayList<>();
        for (JsonNode a : c.path("args")) {
            String s = a.asText().replace("<PACKAGE_V1>", Fixtures.PACKAGE_V1.toString());
            if (pkg != null) {
                s = s.replace("<PKG>", pkg.toString());
            }
            if (port != null) {
                s = s.replace("<PORT>", String.valueOf(port));
            }
            out.add(s);
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
                Path file = dst.resolve(add.path("path").asText());
                Files.createDirectories(file.getParent());
                try (RandomAccessFile f = new RandomAccessFile(file.toFile(), "rw")) {
                    f.setLength(add.path("size").asLong());
                }
            }
            for (JsonNode d : recipe.path("addDirs")) {
                Files.createDirectories(dst.resolve(d.asText()));
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
