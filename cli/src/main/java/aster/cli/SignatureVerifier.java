package aster.cli;

import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.EdECPoint;
import java.security.spec.EdECPublicKeySpec;
import java.security.spec.NamedParameterSpec;
import java.util.Base64;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * signature 阶段（README「检查顺序」signature 行）：大小 ≤ 64 KiB → 严格 JSON → sig schema → {@code sig} 为规范 base64url →
 * 按信任锚取公钥 → Ed25519 验签 manifest 文件字节（JDK {@code Ed25519}，按 RFC 8032 拒绝 S ≥ L）。
 */
final class SignatureVerifier {

    static final int MAX_SIG_BYTES = 64 * 1024;
    private static final Pattern B64U_86 = Pattern.compile("[A-Za-z0-9_-]{86}");
    /** Ed25519 群阶 L。 */
    private static final BigInteger L = BigInteger.TWO.pow(252).add(new BigInteger("27742317777372353535851937790883648493"));

    private SignatureVerifier() {}

    /**
     * 校验签名文件；{@code sigBytes == null} 表示包内没有签名文件。
     *
     * @return 是否已验签（无签名且 {@code allowUnsigned} 时为 false）
     */
    static boolean verify(byte[] sigBytes, byte[] manifestBytes, TrustAnchor anchor, boolean allowUnsigned)
            throws CliException {
        if (sigBytes == null) {
            if (allowUnsigned) {
                return false;
            }
            throw new CliException(ErrorCode.PKG_UNSIGNED, "包内没有 manifest.sig.json");
        }
        if (sigBytes.length > MAX_SIG_BYTES) {
            throw bad("签名文件超过 64 KiB");
        }
        Object tree;
        try {
            tree = StrictJson.parse(sigBytes, 4);
        } catch (StrictJson.StrictJsonException e) {
            throw bad("签名文件不是严格 JSON: " + e.getMessage());
        }
        String schemaError = JsonSchemas.firstError(JsonSchemas.SIG, tree);
        if (schemaError != null) {
            throw bad("签名文件 schema: " + schemaError);
        }
        Map<?, ?> sig = (Map<?, ?>) tree;
        String sigText = (String) sig.get("sig");
        if (!B64U_86.matcher(sigText).matches()) {
            throw bad("签名编码");
        }
        byte[] sigRaw = Base64.getUrlDecoder().decode(sigText);
        if (sigRaw.length != 64 || !Base64.getUrlEncoder().withoutPadding().encodeToString(sigRaw).equals(sigText)) {
            throw bad("签名不是规范 base64url");
        }
        byte[] key = anchor.keyFor((String) sig.get("kid"));
        if (!ed25519Verify(key, manifestBytes, sigRaw)) {
            throw bad("验签失败");
        }
        return true;
    }

    /** Ed25519 验签；S ≥ L（可塑签名）显式拒绝，不依赖提供者的实现细节。 */
    static boolean ed25519Verify(byte[] rawKey, byte[] message, byte[] sig) {
        byte[] s = new byte[32];
        for (int k = 0; k < 32; k++) {
            s[k] = sig[63 - k];
        }
        if (new BigInteger(1, s).compareTo(L) >= 0) {
            return false;
        }
        try {
            Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(publicKey(rawKey));
            verifier.update(message);
            return verifier.verify(sig);
        } catch (GeneralSecurityException | RuntimeException e) {
            return false;
        }
    }

    /** RFC 8032 编码的 32 字节公钥 → JDK 公钥：小端 y，最高位为 x 的奇偶。 */
    private static PublicKey publicKey(byte[] raw) throws GeneralSecurityException {
        byte[] be = new byte[32];
        for (int k = 0; k < 32; k++) {
            be[k] = raw[31 - k];
        }
        boolean xOdd = (be[0] & 0x80) != 0;
        be[0] &= 0x7f;
        EdECPoint point = new EdECPoint(xOdd, new BigInteger(1, be));
        return KeyFactory.getInstance("Ed25519").generatePublic(new EdECPublicKeySpec(NamedParameterSpec.ED25519, point));
    }

    private static CliException bad(String why) {
        return new CliException(ErrorCode.PKG_SIGNATURE, why);
    }
}
