package aster.cli;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * manifest 阶段（README「检查顺序」manifest 行 ①–⑧ 与「manifest 语义规则」）。失败为 {@code PKG_INVALID_MANIFEST}，
 * 主 / 次版本不受支持为 {@code PKG_UNSUPPORTED_FORMAT}，{@code coreIr.version ≠ 2} 为 {@code PKG_UNSUPPORTED_IR}。
 */
final class ManifestCheck {

    static final int MAX_MANIFEST_BYTES = 1024 * 1024;
    private static final BigInteger MAX_SAFE = BigInteger.valueOf(9007199254740991L);
    /** 整体匹配（{@link Matcher#matches()}），末尾换行不被接受。 */
    private static final Pattern FORMAT_VERSION = Pattern.compile("(0|[1-9][0-9]{0,8})\\.(0|[1-9][0-9]{0,8})");

    private ManifestCheck() {}

    /** ①–③：大小、严格 JSON、数值域、文件字节 = 规范 JSON。返回解析后的树。 */
    static Object canonicalTree(byte[] bytes) throws CliException {
        if (bytes.length > MAX_MANIFEST_BYTES) {
            throw invalid("manifest 超过 1 MiB");
        }
        Object tree;
        try {
            tree = StrictJson.parse(bytes, 64);
        } catch (StrictJson.StrictJsonException e) {
            throw invalid("manifest 不是严格 JSON: " + e.getMessage());
        }
        requireSafeIntegers(tree);
        if (!Arrays.equals(ValueJson.write(tree).getBytes(StandardCharsets.UTF_8), bytes)) {
            throw invalid("manifest 文件字节不是规范 JSON");
        }
        return tree;
    }

    /** ①–⑧ 全部；返回 manifest 对象。 */
    @SuppressWarnings("unchecked")
    static Map<String, Object> check(byte[] bytes) throws CliException {
        Object tree = canonicalTree(bytes);
        if (!(tree instanceof Map<?, ?>)) {
            throw invalid("manifest 顶层不是对象");
        }
        Map<String, Object> m = (Map<String, Object>) tree;
        if (!"aster-policy-package".equals(m.get("format"))) {
            throw invalid("format 不是 aster-policy-package");
        }
        Matcher fv = m.get("formatVersion") instanceof String s ? FORMAT_VERSION.matcher(s) : null;
        if (fv == null || !fv.matches()) {
            throw invalid("formatVersion 语法");
        }
        // 主版本 1、次版本 ≤ 1（ADR 0059 修订 4 D2），再按该次版本自己的 schema 严格校验
        int minor = Integer.parseInt(fv.group(2));
        if (Integer.parseInt(fv.group(1)) != 1 || minor > 1) {
            throw new CliException(ErrorCode.PKG_UNSUPPORTED_FORMAT, "formatVersion " + m.get("formatVersion"));
        }
        String schemaError = JsonSchemas.firstError(minor == 1 ? JsonSchemas.MANIFEST_1_1 : JsonSchemas.MANIFEST, m);
        if (schemaError != null) {
            throw invalid("manifest schema: " + schemaError);
        }
        semanticRules(m);
        if (!BigInteger.TWO.equals(Json.at(m, "coreIr", "version"))) {
            throw new CliException(ErrorCode.PKG_UNSUPPORTED_IR, "coreIr.version " + Json.at(m, "coreIr", "version"));
        }
        return m;
    }

    private static void semanticRules(Map<String, Object> m) throws CliException {
        List<Map<String, Object>> files = Json.objects(m.get("files"));
        for (int k = 1; k < files.size(); k++) {
            if (compareUtf8((String) files.get(k - 1).get("path"), (String) files.get(k).get("path")) >= 0) {
                throw invalid("files[] 不是按字节严格升序（含重复）");
            }
        }
        Set<String> lower = new HashSet<>();
        for (Map<String, Object> f : files) {
            if (!lower.add(((String) f.get("path")).toLowerCase(Locale.ROOT))) {
                throw invalid("files[] 有大小写不敏感的重复");
            }
        }
        Set<String> fns = new HashSet<>();
        for (Map<String, Object> e : Json.objects(Json.at(m, "policy", "entries"))) {
            String fn = (String) e.get("function");
            if (!fns.add(fn)) {
                throw invalid("入口函数重复");
            }
            String ptr = "#/$defs/entry." + fn;
            if (!ptr.equals(e.get("inputSchema")) || !ptr.equals(e.get("outputSchema"))) {
                throw invalid("入口 schema 指针");
            }
            if (!Normalizer.isNormalized(fn, Normalizer.Form.NFC)) {
                throw invalid("函数名不是 NFC");
            }
        }
        if (!Normalizer.isNormalized((String) Json.at(m, "policy", "moduleName"), Normalizer.Form.NFC)) {
            throw invalid("模块名不是 NFC");
        }
        // 1.1：display 存在 ⇒ files[] 同时含两个显示排版路径（不存在时这两个路径只是普通载荷）
        if (m.containsKey("display")) {
            Set<String> paths = new HashSet<>();
            files.forEach(f -> paths.add((String) f.get("path")));
            if (!paths.contains(DisplayStage.LAYOUT_PATH) || !paths.contains(DisplayStage.DISPLAY_PATH)) {
                throw invalid("display 存在但 files[] 缺显示排版文件");
            }
        }
    }

    /** 规范 JSON 的数值域：只允许 ±(2⁵³−1) 内的整数。 */
    private static void requireSafeIntegers(Object v) throws CliException {
        switch (v) {
            case Double d -> throw invalid("manifest 含非整数数字 " + d);
            case BigInteger n -> {
                if (n.abs().compareTo(MAX_SAFE) > 0) {
                    throw invalid("manifest 含超出 ±(2^53-1) 的整数");
                }
            }
            case Map<?, ?> map -> {
                for (Object x : map.values()) {
                    requireSafeIntegers(x);
                }
            }
            case List<?> list -> {
                for (Object x : list) {
                    requireSafeIntegers(x);
                }
            }
            case null, default -> { }
        }
    }

    /** 按 UTF-8 字节序比较（无符号）。 */
    static int compareUtf8(String a, String b) {
        return Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    private static CliException invalid(String why) {
        return new CliException(ErrorCode.PKG_INVALID_MANIFEST, why);
    }
}
