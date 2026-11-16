package aster.cli;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 值规范 JSON（aster-lang-test {@code conformance/package-v1/VALUE-FORMS.md}；参照实现 {@code value-json.mjs}）。
 *
 * <p>对象键按 Unicode 码点升序；无空白；字符串按 RFC 8785 §3.2.2.2 转义（与 ECMAScript {@code JSON.stringify} 相同）；
 * 整数（含超出 2⁵³ 的 Long）输出精确十进制；非整数 Double 按 ECMAScript {@code Number::toString}；{@code -0} 输出 {@code 0}；
 * NaN / ±Infinity、孤立代理项、嵌套超过 64 层（顶层值为第 0 层）一律失败（CLI 报 {@code EVAL_ERROR}）。
 *
 * <p>同一序列化器也用于 stdout 的信封与 manifest 的规范字节检查：manifest 的数值域只有安全整数，在该域内本格式与
 * 规范 JSON（ts {@code canonicalJson} / core {@code CanonicalJson}）字节相同。
 */
public final class ValueJson {

    /** 值的最大嵌套层级（与 ts {@code SERIALIZE_MAX_DEPTH} 相同）。 */
    public static final int MAX_VALUE_LEVEL = 64;

    /** 无法写出的值。 */
    public static final class ValueJsonException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        ValueJsonException(String message) {
            super(message);
        }
    }

    private ValueJson() {}

    public static String write(Object value) {
        StringBuilder sb = new StringBuilder();
        write(value, 0, sb);
        return sb.toString();
    }

    private static void write(Object value, int level, StringBuilder sb) {
        if (level > MAX_VALUE_LEVEL) {
            throw new ValueJsonException("值嵌套超过 " + MAX_VALUE_LEVEL + " 层");
        }
        switch (value) {
            case null -> sb.append("null");
            case Boolean b -> sb.append(b ? "true" : "false");
            case String s -> string(s, sb);
            case Integer n -> sb.append(n.intValue());
            case Long n -> sb.append(n.longValue());
            case Short n -> sb.append(n.shortValue());
            case Byte n -> sb.append(n.byteValue());
            case BigInteger n -> sb.append(n);
            case Double d -> sb.append(number(d));
            case Float f -> sb.append(number(f.doubleValue()));
            case BigDecimal d -> string(decimal(d), sb);
            case Map<?, ?> map -> object(map, level, sb);
            case List<?> list -> array(list, level, sb);
            case Object[] array -> array(Arrays.asList(array), level, sb);
            default -> throw new ValueJsonException("不支持的值类型 " + value.getClass().getName());
        }
    }

    private static void object(Map<?, ?> map, int level, StringBuilder sb) {
        List<String> keys = new ArrayList<>(map.size());
        for (Object k : map.keySet()) {
            if (!(k instanceof String s)) {
                throw new ValueJsonException("对象键不是字符串");
            }
            keys.add(s);
        }
        keys.sort(ValueJson::compareByCodePoint);
        sb.append('{');
        for (int k = 0; k < keys.size(); k++) {
            if (k > 0) {
                sb.append(',');
            }
            string(keys.get(k), sb);
            sb.append(':');
            write(map.get(keys.get(k)), level + 1, sb);
        }
        sb.append('}');
    }

    private static void array(List<?> list, int level, StringBuilder sb) {
        sb.append('[');
        for (int k = 0; k < list.size(); k++) {
            if (k > 0) {
                sb.append(',');
            }
            write(list.get(k), level + 1, sb);
        }
        sb.append(']');
    }

    /** 按 Unicode 码点比较（非 BMP 字符按其码点，而非 UTF-16 码元）。 */
    public static int compareByCodePoint(String a, String b) {
        int ia = 0;
        int ib = 0;
        while (ia < a.length() && ib < b.length()) {
            int ca = a.codePointAt(ia);
            int cb = b.codePointAt(ib);
            if (ca != cb) {
                return Integer.compare(ca, cb);
            }
            ia += Character.charCount(ca);
            ib += Character.charCount(cb);
        }
        return Boolean.compare(ia < a.length(), ib < b.length());
    }

    private static void string(String s, StringBuilder sb) {
        if (!StrictJson.isWellFormed(s)) {
            throw new ValueJsonException("字符串含孤立代理项");
        }
        sb.append('"');
        for (int k = 0; k < s.length(); k++) {
            char c = s.charAt(k);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    /** 把十进制数字串四舍五入到 {@code len} 位；进位溢出时返回 len + 1 位（首位为 1）。 */
    private static String roundDigits(String digits, int len) {
        char[] d = digits.substring(0, len).toCharArray();
        if (digits.charAt(len) >= '5') {
            int k = len - 1;
            while (k >= 0 && d[k] == '9') {
                d[k--] = '0';
            }
            if (k < 0) {
                return "1" + new String(d);
            }
            d[k]++;
        }
        return new String(d);
    }

    /** Decimal 的普通写法：去掉末尾 0、无指数、零为 "0"（Truffle {@code AsterDecimalValue} 同一写法）。 */
    private static String decimal(BigDecimal d) {
        return d.signum() == 0 ? "0" : d.stripTrailingZeros().toPlainString();
    }

    /**
     * ECMAScript {@code Number::toString(x)}（RFC 8785 §3.2.2.3）：有效数字取 {@link Double#toString}（JDK 19 起为最短往返位数），
     * 再按 ECMAScript 规则决定普通或指数写法。
     */
    public static String number(double x) {
        if (Double.isNaN(x) || Double.isInfinite(x)) {
            throw new ValueJsonException("值中不得出现 " + x);
        }
        if (x == 0) {
            return "0";
        }
        String sign = x < 0 ? "-" : "";
        String java = Double.toString(Math.abs(x));
        int ePos = java.indexOf('E');
        String mantissa = ePos < 0 ? java : java.substring(0, ePos);
        int exp = ePos < 0 ? 0 : Integer.parseInt(java.substring(ePos + 1));
        int dot = mantissa.indexOf('.');
        String intPart = mantissa.substring(0, dot);
        String fracPart = mantissa.substring(dot + 1);
        // 有效数字串 digits 与十进制指数 n：x = 0.d1d2…dk × 10^n
        String all = intPart + fracPart;
        int n = intPart.length() + exp;
        int lead = 0;
        while (lead < all.length() - 1 && all.charAt(lead) == '0') {
            lead++;
        }
        String digits = all.substring(lead);
        n -= lead;
        int end = digits.length();
        while (end > 1 && digits.charAt(end - 1) == '0') {
            end--;
        }
        digits = digits.substring(0, end);
        // Double.toString 对少数值（如 Double.MIN_VALUE → 4.9E-324）给出多于最短往返所需的位数：逐位缩短，仍往返即采用
        while (digits.length() > 1) {
            String shorter = roundDigits(digits, digits.length() - 1);
            int shift = shorter.length() > digits.length() - 1 ? 1 : 0;
            String candidate = "0." + shorter.substring(0, digits.length() - 1) + "E" + (n + shift);
            if (Double.parseDouble(candidate) != Math.abs(x)) {
                break;
            }
            String trimmed = shorter.substring(0, digits.length() - 1).replaceAll("0+$", "");
            digits = trimmed.isEmpty() ? "1" : trimmed;
            n += shift;
        }
        int k = digits.length();
        StringBuilder sb = new StringBuilder(sign);
        if (k <= n && n <= 21) {
            sb.append(digits).append("0".repeat(n - k));
        } else if (0 < n && n <= 21) {
            sb.append(digits, 0, n).append('.').append(digits, n, k);
        } else if (-6 < n && n <= 0) {
            sb.append("0.").append("0".repeat(-n)).append(digits);
        } else {
            int e = n - 1;
            sb.append(digits.charAt(0));
            if (k > 1) {
                sb.append('.').append(digits, 1, k);
            }
            sb.append('e').append(e >= 0 ? "+" : "-").append(Math.abs(e));
        }
        return sb.toString();
    }
}
