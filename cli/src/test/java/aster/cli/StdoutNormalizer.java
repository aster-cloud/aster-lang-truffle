package aster.cli;

import java.util.ArrayList;
import java.util.List;

/**
 * 跨 CLI stdout 归一（修订 1 A0），与 aster-lang-test {@code conformance/package-v1/normalize-stdout.mjs} 同一规则：
 * 只把顶层成员 {@code meta} 的直接成员 {@code engine} / {@code cliVersion} 的字符串值替换为 {@code "<ENGINE>"} /
 * {@code "<CLI_VERSION>"}，不重新序列化；文本不是合法 JSON 对象时原样返回。
 */
final class StdoutNormalizer {

    private StdoutNormalizer() {}

    private record Span(int start, int end, String replacement) {}

    static String normalize(String stdout) {
        String body = stdout.endsWith("\n") ? stdout.substring(0, stdout.length() - 1) : stdout;
        Scanner sc = new Scanner(body);
        List<Span> spans = new ArrayList<>();
        try {
            sc.ws();
            if (!sc.peek('{')) {
                return stdout;
            }
            sc.topObject(spans);
            sc.ws();
            if (sc.i != body.length()) {
                return stdout;
            }
        } catch (IllegalArgumentException e) {
            return stdout;
        }
        if (spans.isEmpty()) {
            return stdout;
        }
        StringBuilder out = new StringBuilder();
        int at = 0;
        for (Span s : spans) {
            out.append(body, at, s.start()).append(s.replacement());
            at = s.end();
        }
        return out.append(body.substring(at)).append(stdout.substring(body.length())).toString();
    }

    private static final class Scanner {
        final String s;
        int i;

        Scanner(String s) {
            this.s = s;
        }

        boolean peek(char c) {
            return i < s.length() && s.charAt(i) == c;
        }

        void ws() {
            while (i < s.length() && " \t\n\r".indexOf(s.charAt(i)) >= 0) {
                i++;
            }
        }

        IllegalArgumentException fail() {
            return new IllegalArgumentException("非法 JSON（位置 " + i + "）");
        }

        void topObject(List<Span> spans) {
            object((key, start) -> {
                if (key.equals("meta") && peek('{')) {
                    object((k, st) -> {
                        String rep = k.equals("engine") ? "\"<ENGINE>\"" : k.equals("cliVersion") ? "\"<CLI_VERSION>\"" : null;
                        if (rep != null && peek('"')) {
                            int a = i;
                            string();
                            spans.add(new Span(a, i, rep));
                        } else {
                            value();
                        }
                    });
                } else {
                    value();
                }
            });
        }

        interface Member {
            void read(String key, int start);
        }

        void object(Member member) {
            if (!peek('{')) {
                throw fail();
            }
            i++;
            ws();
            if (peek('}')) {
                i++;
                return;
            }
            while (true) {
                ws();
                String key = string();
                ws();
                if (!peek(':')) {
                    throw fail();
                }
                i++;
                ws();
                member.read(key, i);
                ws();
                if (peek(',')) {
                    i++;
                } else if (peek('}')) {
                    i++;
                    return;
                } else {
                    throw fail();
                }
            }
        }

        void value() {
            ws();
            if (peek('{')) {
                object((k, st) -> value());
            } else if (peek('[')) {
                i++;
                ws();
                if (peek(']')) {
                    i++;
                    return;
                }
                while (true) {
                    value();
                    ws();
                    if (peek(',')) {
                        i++;
                    } else if (peek(']')) {
                        i++;
                        return;
                    } else {
                        throw fail();
                    }
                }
            } else if (peek('"')) {
                string();
            } else {
                java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("true|false|null|-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")
                    .matcher(s).region(i, s.length());
                if (!m.lookingAt()) {
                    throw fail();
                }
                i = m.end();
            }
        }

        /** 扫描并解码一个字符串（逐字符，不用正则）。 */
        String string() {
            if (!peek('"')) {
                throw fail();
            }
            int start = i;
            i++;
            while (true) {
                if (i >= s.length()) {
                    throw fail();
                }
                char c = s.charAt(i);
                if (c == '"') {
                    break;
                }
                if (c < 0x20) {
                    throw fail();
                }
                if (c == '\\') {
                    char e = i + 1 < s.length() ? s.charAt(i + 1) : 0;
                    if (e == 'u') {
                        if (i + 6 > s.length() || !s.substring(i + 2, i + 6).matches("[0-9a-fA-F]{4}")) {
                            throw fail();
                        }
                        i += 6;
                    } else if ("\"\\/bfnrt".indexOf(e) >= 0 && e != 0) {
                        i += 2;
                    } else {
                        throw fail();
                    }
                    continue;
                }
                i++;
            }
            i++;
            try {
                return (String) StrictJson.parse(s.substring(start, i), 1);
            } catch (StrictJson.StrictJsonException e) {
                // 孤立代理项等：归一只看键名，按原文比较
                return s.substring(start, i);
            }
        }
    }
}
