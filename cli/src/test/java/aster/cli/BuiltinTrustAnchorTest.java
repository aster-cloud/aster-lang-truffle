package aster.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

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
            assertEquals(List.of("OKP", "Ed25519", "EdDSA", "sig"),
                List.of(k.get("kty").asText(), k.get("crv").asText(), k.get("alg").asText(), k.get("use").asText()));
        }
    }

    @Test
    void defaultAnchorListsExportKid() throws Exception {
        assertEquals(List.of("export-2026-11a"), TrustAnchor.load(null, null, Path.of("."), Map.of()).kids());
    }
}
