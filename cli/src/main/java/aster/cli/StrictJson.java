package aster.cli;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 包格式 v1 的严格 JSON 读取（README「严格 JSON」；参照实现 aster-lang-test {@code scripts/lib/package-v1/strict-json.mjs}）。
 *
 * <p>manifest、签名文件、{@code tests/probes.json}、{@code --input} 与信任锚文件都按同一语法读。不用 Jackson：它的缺省
 * 宽容度（跳过 BOM、接受尾随 token、重复键取后者、不查孤立代理项）与 Node 不同，逐项关掉不如照参照实现逐字符扫描。
 *
 * <p>树的表示：对象为 {@link LinkedHashMap}（保留键序）、数组为 {@link ArrayList}、字符串、{@link Boolean}、{@code null}；
 * 不含 {@code . e E} 的数字 token 为 {@link BigInteger}（无损），其余为 {@link Double}（溢出为 ±Infinity 即失败）。
 */
public final class StrictJson {

    /** 解析失败；{@link #getMessage()} 只供 stderr。 */
    public static final class StrictJsonException extends Exception {
        private static final long serialVersionUID = 1L;

        StrictJsonException(String message) {
            super(message);
        }
    }

    private final String text;
    private final int maxDepth;
    private int i;

    private StrictJson(String text, int maxDepth) {
        this.text = text;
        this.maxDepth = maxDepth;
    }

    /** 原始字节 → 值：先严格 UTF-8 解码并拒绝 BOM。 */
    public static Object parse(byte[] bytes, int maxDepth) throws StrictJsonException {
        if (bytes.length >= 3 && (bytes[0] & 0xff) == 0xef && (bytes[1] & 0xff) == 0xbb && (bytes[2] & 0xff) == 0xbf) {
            throw new StrictJsonException("以 UTF-8 BOM 开头");
        }
        return parse(decodeUtf8(bytes), maxDepth);
    }

    /** 已解码文本 → 值。 */
    public static Object parse(String text, int maxDepth) throws StrictJsonException {
        StrictJson p = new StrictJson(text, maxDepth);
        Object v = p.value(0);
        p.ws();
        if (p.i != text.length()) {
            throw new StrictJsonException("值之后有多余内容（位置 " + p.i + "）");
        }
        return v;
    }

    /** 严格 UTF-8 解码（非法字节序列、代理项编码一律失败）。 */
    public static String decodeUtf8(byte[] bytes) throws StrictJsonException {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
        } catch (CharacterCodingException e) {
            throw new StrictJsonException("不是合法 UTF-8");
        }
    }

    /** 字符串是否为合法 Unicode（无孤立代理项）。 */
    public static boolean isWellFormed(String s) {
        for (int k = 0; k < s.length(); k++) {
            char c = s.charAt(k);
            if (Character.isHighSurrogate(c)) {
                if (k + 1 >= s.length() || !Character.isLowSurrogate(s.charAt(k + 1))) {
                    return false;
                }
                k++;
            } else if (Character.isLowSurrogate(c)) {
                return false;
            }
        }
        return true;
    }

    private void ws() {
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                return;
            }
            i++;
        }
    }

    private StrictJsonException syntax(String what) {
        return new StrictJsonException(what + "（位置 " + i + "）");
    }

    private Object value(int depth) throws StrictJsonException {
        ws();
        if (i >= text.length()) {
            throw syntax("缺少值");
        }
        char c = text.charAt(i);
        if ((c == '{' || c == '[') && depth + 1 > maxDepth) {
            throw new StrictJsonException("嵌套深度超过 " + maxDepth);
        }
        return switch (c) {
            case '{' -> object(depth);
            case '[' -> array(depth);
            case '"' -> string();
            default -> scalar();
        };
    }

    private Map<String, Object> object(int depth) throws StrictJsonException {
        i++;
        Map<String, Object> out = new LinkedHashMap<>();
        ws();
        if (peek('}')) {
            i++;
            return out;
        }
        while (true) {
            ws();
            if (!peek('"')) {
                throw syntax("期望键");
            }
            String key = string();
            if (out.containsKey(key)) {
                throw new StrictJsonException("重复键 " + key);
            }
            ws();
            if (!peek(':')) {
                throw syntax("期望 :");
            }
            i++;
            out.put(key, value(depth + 1));
            ws();
            if (peek(',')) {
                i++;
            } else if (peek('}')) {
                i++;
                return out;
            } else {
                throw syntax("期望 , 或 }");
            }
        }
    }

    private List<Object> array(int depth) throws StrictJsonException {
        i++;
        List<Object> out = new ArrayList<>();
        ws();
        if (peek(']')) {
            i++;
            return out;
        }
        while (true) {
            out.add(value(depth + 1));
            ws();
            if (peek(',')) {
                i++;
            } else if (peek(']')) {
                i++;
                return out;
            } else {
                throw syntax("期望 , 或 ]");
            }
        }
    }

    private boolean peek(char c) {
        return i < text.length() && text.charAt(i) == c;
    }

    private String string() throws StrictJsonException {
        i++;
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (i >= text.length()) {
                throw syntax("字符串未结束");
            }
            char c = text.charAt(i);
            if (c == '"') {
                i++;
                break;
            }
            if (c < 0x20) {
                throw syntax("字符串内有未转义的控制字符");
            }
            if (c != '\\') {
                sb.append(c);
                i++;
                continue;
            }
            char e = i + 1 < text.length() ? text.charAt(i + 1) : 0;
            switch (e) {
                case '"' -> sb.append('"');
                case '\\' -> sb.append('\\');
                case '/' -> sb.append('/');
                case 'b' -> sb.append('\b');
                case 'f' -> sb.append('\f');
                case 'n' -> sb.append('\n');
                case 'r' -> sb.append('\r');
                case 't' -> sb.append('\t');
                case 'u' -> {
                    if (i + 6 > text.length() || !isHex4(text, i + 2)) {
                        throw syntax("非法 \\u 转义");
                    }
                    sb.append((char) Integer.parseInt(text.substring(i + 2, i + 6), 16));
                    i += 4;
                }
                default -> throw syntax("非法转义");
            }
            i += 2;
        }
        String s = sb.toString();
        if (!isWellFormed(s)) {
            throw new StrictJsonException("字符串含孤立代理项");
        }
        return s;
    }

    private static boolean isHex4(String s, int at) {
        for (int k = at; k < at + 4; k++) {
            if (Character.digit(s.charAt(k), 16) < 0) {
                return false;
            }
        }
        return true;
    }

    private Object scalar() throws StrictJsonException {
        for (String word : new String[]{"true", "false", "null"}) {
            if (text.startsWith(word, i)) {
                i += word.length();
                return word.equals("null") ? null : Boolean.valueOf(word.equals("true"));
            }
        }
        int start = i;
        boolean integer = true;
        if (peek('-')) {
            i++;
        }
        if (peek('0')) {
            i++;
        } else if (i < text.length() && text.charAt(i) >= '1' && text.charAt(i) <= '9') {
            digits();
        } else {
            i = start;
            throw syntax("非法值");
        }
        if (peek('.') && isDigitAt(i + 1)) {
            integer = false;
            i++;
            digits();
        }
        if ((peek('e') || peek('E'))) {
            int mark = i;
            i++;
            if (peek('+') || peek('-')) {
                i++;
            }
            if (isDigitAt(i)) {
                integer = false;
                digits();
            } else {
                i = mark;
            }
        }
        String tok = text.substring(start, i);
        if (integer) {
            return new BigInteger(tok);
        }
        double d = Double.parseDouble(tok);
        if (Double.isInfinite(d)) {
            throw new StrictJsonException("数字溢出 " + tok);
        }
        return d;
    }

    private boolean isDigitAt(int at) {
        return at < text.length() && text.charAt(at) >= '0' && text.charAt(at) <= '9';
    }

    private void digits() {
        while (isDigitAt(i)) {
            i++;
        }
    }
}
