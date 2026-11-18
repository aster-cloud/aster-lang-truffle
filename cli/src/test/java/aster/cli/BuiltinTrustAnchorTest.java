package aster.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 内置信任锚 {@code aster/cli/export-jwks.json} = 生产导出密钥的公钥（ADR 0059 P2 T25，§6.2「先发布锚，再签名」）。
 *
 * <p>与 aster-lang-ts {@code src/policy-package/trust/export-jwks.json} 逐字节相同：两侧测试断言同一 SHA-256
 * （本仓 CI 不检出 ts，以固定哈希代替兄弟仓比对）。测试密钥（kid 以 {@code test-} 开头，conformance keys/README.md M4）
 * 只能经 {@code --jwks} / {@code --pubkey} 注入，绝不内置。
 */
class BuiltinTrustAnchorTest {

    private static final String SHA256 = "7e24a91332d6bc04b8311d233aa09e2fb14b6a1fa40248815edc91e93a10a4f7";

    private static byte[] builtin() throws Exception {
        try (InputStream in = TrustAnchor.class.getResourceAsStream("export-jwks.json")) {
            assertNotNull(in, "classpath 上缺少 aster/cli/export-jwks.json");
            return in.readAllBytes();
        }
    }

    @Test
    void holdsOnlyTheProductionExportKey() throws Exception {
        byte[] bytes = builtin();
        assertEquals(SHA256, HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)), "与 ts 内置锚的字节不同");
        JsonNode keys = new ObjectMapper().readTree(new String(bytes, StandardCharsets.UTF_8)).get("keys");
        assertEquals(1, keys.size());
        for (JsonNode k : keys) {
            assertFalse(k.get("kid").asText().startsWith("test-"), "测试密钥不得内置：" + k.get("kid"));
            // conformance 测试密钥（keys/README.md）的公钥不得出现在内置锚中
            assertFalse(k.get("x").asText().equals("8dfz-daPQgc3ZDU3qhgH23T0RCgeJU6POxG70ZlJ9HA"), "测试公钥不得内置");
            assertEquals(List.of("OKP", "Ed25519", "EdDSA", "sig"),
                List.of(k.get("kty").asText(), k.get("crv").asText(), k.get("alg").asText(), k.get("use").asText()));
        }
    }

    /** 把 a-decimal 复制到临时目录并改写签名文件的 kid，不给 --jwks / --pubkey 跑 verify，返回失败码（成功为 null）。 */
    private static String verifyWithBuiltinAnchor(String kid) throws Exception {
        Path dir = Files.createTempDirectory("builtin-anchor-");
        try {
            Path pkg = dir.resolve("a-decimal");
            Path src = Fixtures.PACKAGE_V1.resolve("packages").resolve("a-decimal");
            try (Stream<Path> files = Files.walk(src)) {
                for (Path f : files.toList()) {
                    Path to = pkg.resolve(src.relativize(f).toString());
                    if (Files.isDirectory(f)) Files.createDirectories(to); else Files.copy(f, to);
                }
            }
            Path sig = pkg.resolve("manifest.sig.json");
            ObjectNode node = (ObjectNode) new ObjectMapper().readTree(sig.toFile());
            node.put("kid", kid);
            Files.writeString(sig, node.toString(), StandardCharsets.UTF_8);
            Fixtures.Result r = Fixtures.run(dir, null, Map.of(), "verify", "a-decimal");
            JsonNode out = new ObjectMapper().readTree(r.stdout());
            for (JsonNode c : out.path("checks")) {
                if (!c.path("ok").asBoolean()) return c.path("code").asText();
            }
            return out.path("error").path("code").isMissingNode() ? null : out.path("error").path("code").asText();
        } finally {
            try (Stream<Path> files = Files.walk(dir)) {
                for (Path f : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(f);
            }
        }
    }

    @Test
    void verifiesAgainstBuiltinAnchorByDefault() throws Exception {
        // 签名不是生产私钥签的：生产 kid 得 PKG_SIGNATURE（内置锚按 kid 取到了生产公钥），测试 kid 得 PKG_UNTRUSTED_KID
        assertEquals("PKG_SIGNATURE", verifyWithBuiltinAnchor("export-2026-11a"));
        assertEquals("PKG_UNTRUSTED_KID", verifyWithBuiltinAnchor("test-2026-11"));
    }

    @Test
    void defaultAnchorListsExportKid() throws Exception {
        assertEquals(List.of("export-2026-11a"), TrustAnchor.load(null, null, Path.of("."), Map.of()).kids());
    }
}
