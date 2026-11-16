package aster.cli;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * integrity 阶段（README「检查顺序」integrity 行）：{@code files[]} 的存在、大小与 SHA-256；无多余文件；{@code SHA256SUMS}；
 * A 包 {@code compiled/core.json} 与 {@code coreIr.sha256}；{@code source.sha256}；{@code tests.probesSha256} 与条数。
 * 失败一律 {@link ErrorCode#PKG_INTEGRITY}。签名先于本阶段：没有可信 manifest 就不读载荷哈希。
 */
final class Integrity {

    private static final Set<String> META_FILES = Set.of("manifest.json", "manifest.sig.json", "SHA256SUMS");

    private Integrity() {}

    static void check(Map<String, byte[]> files, Map<String, Object> manifest, byte[] manifestBytes)
            throws CliException {
        List<Map<String, Object>> listed = Json.objects(manifest.get("files"));
        Map<String, byte[]> all = new LinkedHashMap<>();
        for (Map<String, Object> f : listed) {
            String path = (String) f.get("path");
            byte[] b = files.get(path);
            if (b == null) {
                throw bad("缺文件 " + path);
            }
            if (!BigInteger.valueOf(b.length).equals(f.get("size")) || !sha256(b).equals(f.get("sha256"))) {
                throw bad("哈希 / 大小不符 " + path);
            }
            all.put(path, b);
        }
        for (String p : files.keySet()) {
            if (!all.containsKey(p) && !META_FILES.contains(p)) {
                throw bad("多余文件 " + p);
            }
        }
        all.put("manifest.json", manifestBytes);
        List<String> paths = new ArrayList<>(all.keySet());
        paths.sort(ManifestCheck::compareUtf8);
        StringBuilder sums = new StringBuilder();
        for (String p : paths) {
            sums.append(sha256(all.get(p))).append("  ").append(p).append('\n');
        }
        byte[] actualSums = files.get("SHA256SUMS");
        if (actualSums == null || !java.util.Arrays.equals(actualSums, sums.toString().getBytes(StandardCharsets.UTF_8))) {
            throw bad("SHA256SUMS 与 files[] + manifest.json 不符");
        }
        if ("a".equals(manifest.get("mode"))
                && !sha256OrNull(files.get("compiled/core.json")).equals(Json.at(manifest, "coreIr", "sha256"))) {
            throw bad("compiled/core.json 与 coreIr.sha256 不符");
        }
        if (!sha256OrNull(files.get("policy/policy.aster")).equals(Json.at(manifest, "source", "sha256"))) {
            throw bad("policy/policy.aster 与 source.sha256 不符");
        }
        byte[] probes = files.get("tests/probes.json");
        if (!sha256OrNull(probes).equals(Json.at(manifest, "tests", "probesSha256"))) {
            throw bad("tests/probes.json 与 tests.probesSha256 不符");
        }
        Object doc;
        try {
            doc = StrictJson.parse(probes, 64);
        } catch (StrictJson.StrictJsonException e) {
            throw bad("tests/probes.json 不是严格 JSON: " + e.getMessage());
        }
        if (!(Json.at(doc, "probes") instanceof List<?> list)
                || !BigInteger.valueOf(list.size()).equals(Json.at(manifest, "tests", "probes"))) {
            throw bad("tests.probes 条数不符");
        }
    }

    static String sha256(byte[] b) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String sha256OrNull(byte[] b) {
        return b == null ? "" : sha256(b);
    }

    private static CliException bad(String why) {
        return new CliException(ErrorCode.PKG_INTEGRITY, why);
    }
}
