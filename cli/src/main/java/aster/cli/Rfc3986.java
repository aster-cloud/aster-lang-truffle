package aster.cli;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 重定向 {@code Location} 的 RFC 3986 处理（README「信任锚」https 取回；aster-lang-truffle#185 评审 M-1）。
 *
 * <p>不用 {@link java.net.URI#resolve}：它按 RFC 2396 解析（{@code ?q} 对 {@code /a/b} 得 {@code /a/?q}、{@code ../../x} 留下
 * {@code /../x}），与 Node 的 WHATWG {@code URL} 和 RFC 3986 都不同，同一个服务端重定向在两个 CLI 上会得到不同结果。
 * <ul>
 *   <li>{@link #isReference}：是否为合法的 RFC 3986 URI-reference（空串、空格、反斜杠、控制字符、未编码的非 ASCII、
 *       不完整的 {@code %} 编码都不是）；</li>
 *   <li>{@link #resolve}：§5.2.2 引用解析 + §5.2.3 合并 + §5.2.4 去点段；scheme 归一为小写（scheme 不区分大小写）。</li>
 * </ul>
 */
final class Rfc3986 {

    /** URI-reference 允许的字符：unreserved / sub-delims / gen-delims，或 {@code %HH}。 */
    private static final Pattern REFERENCE_CHARS =
        Pattern.compile("(?:[A-Za-z0-9\\-._~!$&'()*+,;=:@/?#\\[\\]]|%[0-9A-Fa-f]{2})+");
    /** RFC 3986 附录 B 的分解正则。 */
    private static final Pattern PARTS = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    private static final Pattern SCHEME = Pattern.compile("[A-Za-z][A-Za-z0-9+.\\-]*");

    private Rfc3986() {}

    private record Parts(String scheme, String authority, String path, String query, String fragment) {
        static Parts of(String ref) {
            Matcher m = PARTS.matcher(ref);
            if (!m.matches()) {
                throw new IllegalArgumentException("无法分解 " + ref);
            }
            return new Parts(m.group(2), m.group(3) == null ? null : m.group(4), m.group(5),
                m.group(6) == null ? null : m.group(7), m.group(8) == null ? null : m.group(9));
        }

        String recompose() {
            StringBuilder sb = new StringBuilder();
            if (scheme != null) {
                sb.append(scheme).append(':');
            }
            if (authority != null) {
                sb.append("//").append(authority);
            }
            sb.append(path);
            if (query != null) {
                sb.append('?').append(query);
            }
            if (fragment != null) {
                sb.append('#').append(fragment);
            }
            return sb.toString();
        }
    }

    /** 是否为合法的 URI-reference（非空，且只由允许的字符与完整的百分号编码组成；有 scheme 时 scheme 合法）。 */
    static boolean isReference(String ref) {
        if (ref == null || ref.isEmpty() || !REFERENCE_CHARS.matcher(ref).matches()) {
            return false;
        }
        String scheme = Parts.of(ref).scheme();
        return scheme == null || SCHEME.matcher(scheme).matches();
    }

    /**
     * 以 {@code base}（绝对 URI）解析 {@code ref}，返回目标 URI（scheme 小写；fragment 取自 {@code ref}，§5.2.2）。
     * 发请求时用 {@link #withoutFragment}。
     *
     * @throws IllegalArgumentException {@code ref} 不是合法的 URI-reference
     */
    static String resolve(String base, String ref) {
        if (!isReference(ref)) {
            throw new IllegalArgumentException("不是合法的 RFC 3986 URI-reference: " + ref);
        }
        Parts b = Parts.of(base);
        Parts r = Parts.of(ref);
        String scheme;
        String authority;
        String path;
        String query;
        if (r.scheme() != null) {
            scheme = r.scheme();
            authority = r.authority();
            path = removeDotSegments(r.path());
            query = r.query();
        } else {
            scheme = b.scheme();
            if (r.authority() != null) {
                authority = r.authority();
                path = removeDotSegments(r.path());
                query = r.query();
            } else {
                authority = b.authority();
                if (r.path().isEmpty()) {
                    path = b.path();
                    query = r.query() != null ? r.query() : b.query();
                } else {
                    path = r.path().startsWith("/") ? removeDotSegments(r.path()) : removeDotSegments(merge(b, r.path()));
                    query = r.query();
                }
            }
        }
        return new Parts(scheme.toLowerCase(Locale.ROOT), authority, path, query, r.fragment()).recompose();
    }

    /** 去掉 fragment（请求目标不带 fragment）。 */
    static String withoutFragment(String uri) {
        Parts p = Parts.of(uri);
        return new Parts(p.scheme(), p.authority(), p.path(), p.query(), null).recompose();
    }

    /** IP-literal：{@code [IPv6address]} 或 {@code [vX.…]}（IPvFuture）。 */
    private static final Pattern IP_LITERAL =
        Pattern.compile("\\[(?:[0-9A-Fa-f:.]+|[vV][0-9A-Fa-f]+\\.[A-Za-z0-9\\-._~!$&'()*+,;=:]+)]");

    /**
     * 请求目标（初始 URL 与每个解析后的重定向目标）是否可以发送（aster-lang-truffle#189 后续 N-1）：必须有非空 host；不得带 userinfo（Node 会把它变成
     * {@code Authorization: Basic} 发出，JVM 会静默丢掉——两侧都改为拒绝）；端口若出现须为十进制 1–65535；
     * {@code [ ]} 只允许出现在 IP-literal host 中。返回违例原因，合法返回 null。
     */
    static String targetViolation(String uri) {
        Parts p = Parts.of(uri);
        String authority = p.authority();
        if (authority == null || authority.isEmpty()) {
            return "缺少 host";
        }
        if (authority.indexOf('@') >= 0) {
            return "不得带 userinfo";
        }
        String host;
        String port;
        if (authority.startsWith("[")) {
            int close = authority.indexOf(']');
            if (close < 0) {
                return "IP-literal 未闭合";
            }
            host = authority.substring(0, close + 1);
            if (!IP_LITERAL.matcher(host).matches()) {
                return "不是合法的 IP-literal: " + host;
            }
            String rest = authority.substring(close + 1);
            if (!rest.isEmpty() && !rest.startsWith(":")) {
                return "IP-literal 之后只能是 :port";
            }
            port = rest.isEmpty() ? null : rest.substring(1);
        } else {
            int colon = authority.lastIndexOf(':');
            host = colon < 0 ? authority : authority.substring(0, colon);
            port = colon < 0 ? null : authority.substring(colon + 1);
            if (host.indexOf(':') >= 0) {
                return "host 含 ':'";
            }
        }
        if (host.isEmpty()) {
            return "缺少 host";
        }
        if (port != null && !validPort(port)) {
            return "端口须为十进制 1–65535: " + port;
        }
        String rest = p.path() + (p.query() == null ? "" : "?" + p.query())
            + (p.fragment() == null ? "" : "#" + p.fragment());
        if (!authority.startsWith("[") && (authority.indexOf('[') >= 0 || authority.indexOf(']') >= 0)
                || rest.indexOf('[') >= 0 || rest.indexOf(']') >= 0) {
            return "[ ] 只允许用于 IP-literal host";
        }
        return null;
    }

    private static boolean validPort(String port) {
        if (port.isEmpty() || port.length() > 5 || !port.chars().allMatch(c -> c >= '0' && c <= '9')) {
            return false;
        }
        int n = Integer.parseInt(port);
        return n >= 1 && n <= 65535;
    }

    /** §5.2.3。 */
    private static String merge(Parts base, String refPath) {
        if (base.authority() != null && base.path().isEmpty()) {
            return "/" + refPath;
        }
        int slash = base.path().lastIndexOf('/');
        return slash < 0 ? refPath : base.path().substring(0, slash + 1) + refPath;
    }

    /** §5.2.4。 */
    static String removeDotSegments(String path) {
        StringBuilder in = new StringBuilder(path);
        StringBuilder out = new StringBuilder();
        while (!in.isEmpty()) {
            if (startsWith(in, "../")) {
                in.delete(0, 3);
            } else if (startsWith(in, "./")) {
                in.delete(0, 2);
            } else if (startsWith(in, "/./")) {
                in.delete(0, 2);
            } else if (in.toString().equals("/.")) {
                in.replace(0, 2, "/");
            } else if (startsWith(in, "/../")) {
                in.delete(0, 3);
                removeLastSegment(out);
            } else if (in.toString().equals("/..")) {
                in.replace(0, 3, "/");
                removeLastSegment(out);
            } else if (in.toString().equals(".") || in.toString().equals("..")) {
                in.setLength(0);
            } else {
                int start = in.charAt(0) == '/' ? 1 : 0;
                int next = in.indexOf("/", start);
                int end = next < 0 ? in.length() : next;
                out.append(in, 0, end);
                in.delete(0, end);
            }
        }
        return out.toString();
    }

    private static boolean startsWith(StringBuilder sb, String prefix) {
        return sb.length() >= prefix.length() && sb.substring(0, prefix.length()).equals(prefix);
    }

    private static void removeLastSegment(StringBuilder out) {
        int slash = out.lastIndexOf("/");
        out.setLength(Math.max(slash, 0));
    }
}
