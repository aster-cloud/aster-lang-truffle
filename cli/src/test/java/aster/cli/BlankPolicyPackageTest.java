package aster.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 包内 {@code policy/policy.aster} 同样适用源码的空白规则（严格 UTF-8、去一个 BOM、只含 ASCII 空白即失败）：
 * {@code compile <pkg>} 为 COMPILE_ERROR，{@code verify --recompile} 为 PKG_IR_DRIFT。
 * 包由 b-credit-limit 派生：换掉源码、重算 manifest 中的哈希与 SHA256SUMS、去掉签名（以 --allow-unsigned 运行）。
 */
class BlankPolicyPackageTest {

    @TempDir
    Path tmp;

    @SuppressWarnings("unchecked")
    private Path packageWithPolicy(byte[] policy) throws Exception {
        Path src = Fixtures.PACKAGE_V1.resolve("packages/b-credit-limit");
        Path dst = tmp.resolve("pkg");
        try (Stream<Path> walk = Files.walk(src)) {
            for (Path p : walk.toList()) {
                Files.copy(p, dst.resolve(src.relativize(p).toString()));
            }
        }
        Files.delete(dst.resolve("manifest.sig.json"));
        Files.write(dst.resolve("policy/policy.aster"), policy);
        Map<String, Object> m = (Map<String, Object>) StrictJson.parse(Files.readAllBytes(dst.resolve("manifest.json")), 64);
        String sha = Integrity.sha256(policy);
        for (Map<String, Object> f : Json.objects(m.get("files"))) {
            if (f.get("path").equals("policy/policy.aster")) {
                f.put("sha256", sha);
                f.put("size", BigInteger.valueOf(policy.length));
            }
        }
        ((Map<String, Object>) m.get("source")).put("sha256", sha);
        byte[] manifest = ValueJson.write(m).getBytes(StandardCharsets.UTF_8);
        Files.write(dst.resolve("manifest.json"), manifest);
        TreeMap<String, String> sums = new TreeMap<>(ManifestCheck::compareUtf8);
        for (Map<String, Object> f : Json.objects(m.get("files"))) {
            String path = (String) f.get("path");
            sums.put(path, Integrity.sha256(Files.readAllBytes(dst.resolve(path))));
        }
        sums.put("manifest.json", Integrity.sha256(manifest));
        List<String> lines = new ArrayList<>();
        sums.forEach((p, h) -> lines.add(h + "  " + p + "\n"));
        Files.writeString(dst.resolve("SHA256SUMS"), String.join("", lines));
        return dst;
    }

    private static String code(Fixtures.Result r) {
        int i = r.stdout().indexOf("\"code\":\"");
        return i < 0 ? null : r.stdout().substring(i + 8, r.stdout().indexOf('"', i + 8));
    }

    @Test
    void blankPolicyFailsCompileAndRecompile() throws Exception {
        Path pkg = packageWithPolicy(" \t\r\n".getBytes(StandardCharsets.UTF_8));
        Fixtures.Result compile = Fixtures.run(tmp, null, Map.of(), "compile", pkg.toString(), "--allow-unsigned");
        assertEquals("COMPILE_ERROR", code(compile), compile.stdout() + compile.stderr());
        Fixtures.Result verify = Fixtures.run(tmp, null, Map.of(), "verify", pkg.toString(), "--recompile", "--allow-unsigned");
        assertEquals(4, verify.exit(), verify.stdout());
        assertEquals("PKG_IR_DRIFT", code(verify), verify.stdout());
    }

    @Test
    void invalidUtf8PolicyFailsCompile() throws Exception {
        Path pkg = packageWithPolicy(new byte[]{'M', (byte) 0xC3, (byte) 0x28});
        assertEquals("COMPILE_ERROR", code(Fixtures.run(tmp, null, Map.of(), "compile", pkg.toString(), "--allow-unsigned")));
    }

    @Test
    void bomIsStrippedFromPackagePolicy() throws Exception {
        byte[] original = Files.readAllBytes(Fixtures.PACKAGE_V1.resolve("packages/b-credit-limit/policy/policy.aster"));
        byte[] withBom = new byte[original.length + 3];
        withBom[0] = (byte) 0xEF;
        withBom[1] = (byte) 0xBB;
        withBom[2] = (byte) 0xBF;
        System.arraycopy(original, 0, withBom, 3, original.length);
        Path pkg = packageWithPolicy(withBom);
        Fixtures.Result verify = Fixtures.run(tmp, null, Map.of(), "verify", pkg.toString(), "--recompile", "--allow-unsigned");
        assertEquals(0, verify.exit(), verify.stdout() + verify.stderr());
    }
}
