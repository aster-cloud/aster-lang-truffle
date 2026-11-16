package aster.cli;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 信任锚（README「信任锚」）。读取与校验属于 usage 阶段：锚文件不可读或不合法一律 {@link ErrorCode#USAGE}。
 *
 * <ul>
 *   <li>JWKS（内置 {@code export-jwks.json} 或 {@code --jwks <file|https url>}）：按签名文件的 kid 查找；</li>
 *   <li>{@code --pubkey <file>} 原始公钥：32 字节的规范 base64url，后面至多一个 {@code \n}；不看 kid；</li>
 *   <li>{@code --pubkey <file>} 单个 JWK：带 kid 时必须等于签名文件的 kid，不带时不看 kid。</li>
 * </ul>
 */
public final class TrustAnchor {

    private static final Pattern RAW = Pattern.compile("[A-Za-z0-9_-]{43}\\n?");

    /** kind：jwks 时 {@code keys} 有效；raw / jwk 时 {@code key} 有效，jwk 的 {@code kid} 可为 null。 */
    private final String kind;
    private final Map<String, byte[]> keys;
    private final byte[] key;
    private final String kid;

    private TrustAnchor(String kind, Map<String, byte[]> keys, byte[] key, String kid) {
        this.kind = kind;
        this.keys = keys;
        this.key = key;
        this.kid = kid;
    }

    /** 按命令行选项加载；二者都缺省时用内置锚。 */
    public static TrustAnchor load(String pubkey, String jwks, Path cwd) throws CliException {
        if (pubkey != null && jwks != null) {
            throw usage("--jwks 与 --pubkey 不能同时给出");
        }
        if (pubkey != null) {
            return fromPubkey(readFile(cwd, pubkey));
        }
        if (jwks != null) {
            byte[] bytes = jwks.startsWith("https://") ? fetch(jwks) : readFile(cwd, jwks);
            return fromJwks(bytes);
        }
        try (InputStream in = TrustAnchor.class.getResourceAsStream("export-jwks.json")) {
            return fromJwks(in.readAllBytes());
        } catch (IOException e) {
            throw new IllegalStateException("缺少内置 export-jwks.json", e);
        }
    }

    /** 内置锚与命令行锚中出现的 kid（{@code version} 输出用）。 */
    public List<String> kids() {
        return keys == null ? List.of() : new ArrayList<>(keys.keySet());
    }

    /** 按签名文件的 kid 取 32 字节公钥；取不到为 {@link ErrorCode#PKG_UNTRUSTED_KID}。 */
    byte[] keyFor(String sigKid) throws CliException {
        switch (kind) {
            case "raw":
                return key;
            case "jwk":
                if (kid != null && !kid.equals(sigKid)) {
                    throw new CliException(ErrorCode.PKG_UNTRUSTED_KID, "kid 与 --pubkey 的 JWK 不符");
                }
                return key;
            default:
                byte[] k = keys.get(sigKid);
                if (k == null) {
                    throw new CliException(ErrorCode.PKG_UNTRUSTED_KID, "kid " + sigKid + " 不在信任锚中");
                }
                return k;
        }
    }

    static TrustAnchor fromPubkey(byte[] bytes) throws CliException {
        String text = decode(bytes);
        String trimmed = text.stripLeading();
        if (trimmed.startsWith("{")) {
            Map<String, Object> jwk = object(parse(bytes), "JWK");
            return new TrustAnchor("jwk", null, jwkKey(jwk, false), (String) jwk.get("kid"));
        }
        if (!RAW.matcher(text).matches()) {
            throw usage("原始公钥必须是 32 字节的规范 base64url，后面至多一个 \\n");
        }
        return new TrustAnchor("raw", null, b64u32(text.endsWith("\n") ? text.substring(0, 43) : text), null);
    }

    static TrustAnchor fromJwks(byte[] bytes) throws CliException {
        Map<String, Object> doc = object(parse(bytes), "JWKS");
        if (!(doc.get("keys") instanceof List<?> list)) {
            throw usage("JWKS 缺少 keys 数组");
        }
        Map<String, byte[]> out = new LinkedHashMap<>();
        for (Object item : list) {
            Map<String, Object> jwk = object(item, "JWK");
            byte[] k = jwkKey(jwk, true);
            if (out.put((String) jwk.get("kid"), k) != null) {
                throw usage("JWKS 中 kid 重复: " + jwk.get("kid"));
            }
        }
        return new TrustAnchor("jwks", out, null, null);
    }

    private static byte[] jwkKey(Map<String, Object> jwk, boolean kidRequired) throws CliException {
        if (!"OKP".equals(jwk.get("kty")) || !"Ed25519".equals(jwk.get("crv"))) {
            throw usage("JWK 必须是 kty=OKP、crv=Ed25519");
        }
        if (jwk.containsKey("alg") && !"EdDSA".equals(jwk.get("alg"))) {
            throw usage("JWK 的 alg 必须是 EdDSA");
        }
        if (jwk.containsKey("use") && !"sig".equals(jwk.get("use"))) {
            throw usage("JWK 的 use 必须是 sig");
        }
        boolean hasKid = jwk.containsKey("kid");
        if ((kidRequired && !hasKid) || (hasKid && !(jwk.get("kid") instanceof String s && !s.isEmpty()))) {
            throw usage("JWK 的 kid 必须是非空字符串");
        }
        if (!(jwk.get("x") instanceof String x) || !x.matches("[A-Za-z0-9_-]{43}")) {
            throw usage("JWK 的 x 必须是 32 字节的规范 base64url");
        }
        return b64u32(x);
    }

    /** 43 字符 base64url → 32 字节，要求规范编码（重新编码与原文相同）。 */
    private static byte[] b64u32(String s) throws CliException {
        byte[] raw;
        try {
            raw = Base64.getUrlDecoder().decode(s);
        } catch (IllegalArgumentException e) {
            throw usage("不是 base64url");
        }
        if (raw.length != 32 || !Base64.getUrlEncoder().withoutPadding().encodeToString(raw).equals(s)) {
            throw usage("公钥不是 32 字节的规范 base64url");
        }
        return raw;
    }

    private static Object parse(byte[] bytes) throws CliException {
        try {
            return StrictJson.parse(bytes, 64);
        } catch (StrictJson.StrictJsonException e) {
            throw usage("信任锚不是严格 JSON: " + e.getMessage());
        }
    }

    private static String decode(byte[] bytes) throws CliException {
        try {
            return StrictJson.decodeUtf8(bytes);
        } catch (StrictJson.StrictJsonException e) {
            throw usage("信任锚不是合法 UTF-8");
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object v, String what) throws CliException {
        if (!(v instanceof Map<?, ?>)) {
            throw usage(what + " 不是对象");
        }
        return (Map<String, Object>) v;
    }

    private static byte[] readFile(Path cwd, String file) throws CliException {
        try {
            return Files.readAllBytes(cwd.resolve(file));
        } catch (IOException | RuntimeException e) {
            throw usage("无法读取信任锚文件 " + file);
        }
    }

    /** {@code --jwks <https url>}：唯一联网的路径，10 s 超时。 */
    private static byte[] fetch(String url) throws CliException {
        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            HttpResponse<byte[]> resp = client.send(
                HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() != 200) {
                throw usage("获取 JWKS 失败: HTTP " + resp.statusCode());
            }
            return resp.body();
        } catch (IOException | IllegalArgumentException e) {
            throw usage("获取 JWKS 失败: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw usage("获取 JWKS 被中断");
        }
    }

    private static CliException usage(String why) {
        return new CliException(ErrorCode.USAGE, why);
    }
}
