package aster.cli;

import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * {@code --input} 的校验与宿主实参转换（README「输入」；参照实现 {@code scripts/lib/package-v1/input.mjs}）。
 *
 * <p>判定：≤ 16 MiB、严格 JSON（深度 ≤ 64，整数 token 无损）、顶层对象的键恰为入口全部形参名、逐形参按 Core IR 类型校验；
 * 任一不满足即 {@link ErrorCode#INPUT_INVALID}。通过后按 IR 形参顺序转为位置实参：Int → {@link Integer}、Long → {@link Long}、
 * Double → {@link Double}、Decimal → {@link BigDecimal}、List → {@link List}、Map / Data → {@link LinkedHashMap}、None → null、
 * 无载荷 Enum → 变体名字符串。Long 与 Decimal 的字符串形态由 CLI 自己转换，不照搬平台的 JSON 入参转换（aster-api#558）。
 */
final class InputParser {

    static final int MAX_INPUT_BYTES = 16 * 1024 * 1024;
    private static final int MAX_DEPTH = 64;
    private static final int DECIMAL_MAX_DIGITS = 80;
    private static final BigInteger INT32_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
    private static final BigInteger INT32_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final BigInteger INT64_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger INT64_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger SAFE = BigInteger.valueOf(9007199254740991L);
    private static final Pattern DECIMAL = Pattern.compile("-?(0|[1-9][0-9]*)(\\.[0-9]+)?");
    private static final Pattern LONG_STRING = Pattern.compile("-?(0|[1-9][0-9]{0,18})");

    private final Map<String, JsonNode> datas = new HashMap<>();
    private final Map<String, Set<String>> enums = new HashMap<>();

    private InputParser(JsonNode coreIr) {
        for (JsonNode d : coreIr.path("decls")) {
            String kind = d.path("kind").asText();
            if (kind.equals("Data")) {
                datas.put(d.path("name").asText(), d);
            } else if (kind.equals("Enum")) {
                Set<String> variants = new HashSet<>();
                d.path("variants").forEach(v -> variants.add(v.isTextual() ? v.asText() : v.path("name").asText()));
                enums.put(d.path("name").asText(), variants);
            }
        }
    }

    /** 入口函数的 IR 声明；不存在返回 null。 */
    static JsonNode func(JsonNode coreIr, String entry) {
        for (JsonNode d : coreIr.path("decls")) {
            if ("Func".equals(d.path("kind").asText()) && entry.equals(d.path("name").asText())) {
                return d;
            }
        }
        return null;
    }

    /** 校验原始输入字节并按形参顺序返回宿主实参。 */
    static Object[] parse(byte[] bytes, JsonNode coreIr, JsonNode func) throws CliException {
        if (bytes.length > MAX_INPUT_BYTES) {
            throw invalid("输入超过 16 MiB");
        }
        Object v;
        try {
            v = StrictJson.parse(bytes, MAX_DEPTH);
        } catch (StrictJson.StrictJsonException e) {
            throw invalid("输入不是严格 JSON: " + e.getMessage());
        }
        if (!(v instanceof Map<?, ?> obj)) {
            throw invalid("顶层必须是对象");
        }
        List<String> names = new ArrayList<>();
        func.path("params").forEach(p -> names.add(p.path("name").asText()));
        checkKeys(obj, names, "$");
        InputParser parser = new InputParser(coreIr);
        Object[] args = new Object[names.size()];
        int k = 0;
        for (JsonNode p : func.path("params")) {
            String name = p.path("name").asText();
            args[k++] = parser.convert(obj.get(name), p.path("type"), "$." + name);
        }
        return args;
    }

    private Object convert(Object v, JsonNode type, String path) throws CliException {
        switch (type.path("kind").asText()) {
            case "List" -> {
                if (!(v instanceof List<?> list)) {
                    throw invalid(path + ": 期望数组");
                }
                List<Object> out = new ArrayList<>(list.size());
                for (int k = 0; k < list.size(); k++) {
                    out.add(convert(list.get(k), type.path("type"), path + "[" + k + "]"));
                }
                return out;
            }
            case "Map" -> {
                if (!(v instanceof Map<?, ?> map)) {
                    throw invalid(path + ": 期望对象");
                }
                Map<String, Object> out = new LinkedHashMap<>();
                for (Map.Entry<?, ?> e : map.entrySet()) {
                    out.put((String) e.getKey(), convert(e.getValue(), type.path("val"), path + "." + e.getKey()));
                }
                return out;
            }
            case "Maybe", "Option" -> {
                return v == null ? null : convert(v, type.path("type"), path);
            }
            case "TypeName" -> {
                return named(v, type.path("name").asText(), path);
            }
            default -> throw invalid(path + ": 不支持的入参类型 " + type.path("kind").asText());
        }
    }

    private Object named(Object v, String name, String path) throws CliException {
        switch (name) {
            case "Int" -> {
                if (!(v instanceof BigInteger n)) {
                    throw invalid(path + ": Int 必须是整数 token");
                }
                if (n.compareTo(INT32_MIN) < 0 || n.compareTo(INT32_MAX) > 0) {
                    throw invalid(path + ": Int 超出 int32");
                }
                return n.intValue();
            }
            case "Long" -> {
                if (v instanceof BigInteger n) {
                    if (n.abs().compareTo(SAFE) > 0) {
                        throw invalid(path + ": Long 的整数 token 超出 ±(2^53-1)，须用十进制字符串");
                    }
                    return n.longValue();
                }
                if (!(v instanceof String s) || !LONG_STRING.matcher(s).matches()) {
                    throw invalid(path + ": Long 必须是安全整数 token 或十进制字符串");
                }
                BigInteger n = new BigInteger(s);
                if (n.compareTo(INT64_MIN) < 0 || n.compareTo(INT64_MAX) > 0) {
                    throw invalid(path + ": Long 超出 int64");
                }
                return n.longValue();
            }
            case "Double" -> {
                if (v instanceof BigInteger n) {
                    double d = n.doubleValue();
                    if (Double.isInfinite(d)) {
                        throw invalid(path + ": Double 溢出");
                    }
                    return d;
                }
                if (!(v instanceof Double d)) {
                    throw invalid(path + ": Double 必须是数字");
                }
                return d;
            }
            case "Decimal" -> {
                if (!(v instanceof String s) || !DECIMAL.matcher(s).matches()) {
                    throw invalid(path + ": Decimal 必须是十进制字符串");
                }
                if (s.replace("-", "").replace(".", "").length() > DECIMAL_MAX_DIGITS) {
                    throw invalid(path + ": Decimal 超过 " + DECIMAL_MAX_DIGITS + " 位数字");
                }
                return new BigDecimal(s);
            }
            case "Text" -> {
                if (!(v instanceof String)) {
                    throw invalid(path + ": 期望字符串");
                }
                return v;
            }
            case "Bool" -> {
                if (!(v instanceof Boolean)) {
                    throw invalid(path + ": 期望布尔");
                }
                return v;
            }
            default -> {
                return userType(v, name, path);
            }
        }
    }

    private Object userType(Object v, String name, String path) throws CliException {
        Set<String> variants = enums.get(name);
        if (variants != null) {
            if (!(v instanceof String s) || !variants.contains(s)) {
                throw invalid(path + ": 不是 " + name + " 的变体名");
            }
            return v;
        }
        JsonNode data = datas.get(name);
        if (data == null) {
            throw invalid(path + ": 未知类型 " + name);
        }
        if (!(v instanceof Map<?, ?> obj)) {
            throw invalid(path + ": 期望对象");
        }
        List<String> fields = new ArrayList<>();
        data.path("fields").forEach(f -> fields.add(f.path("name").asText()));
        checkKeys(obj, fields, path);
        Map<String, Object> out = new LinkedHashMap<>();
        for (JsonNode f : data.path("fields")) {
            String fname = f.path("name").asText();
            out.put(fname, convert(obj.get(fname), f.path("type"), path + "." + fname));
        }
        return out;
    }

    private static void checkKeys(Map<?, ?> obj, List<String> names, String path) throws CliException {
        Set<String> want = new HashSet<>(names);
        if (obj.size() != want.size() || !want.containsAll(obj.keySet())) {
            throw invalid(path + ": 键必须恰为 " + String.join(", ", names));
        }
    }

    private static CliException invalid(String why) {
        return new CliException(ErrorCode.INPUT_INVALID, why);
    }
}
