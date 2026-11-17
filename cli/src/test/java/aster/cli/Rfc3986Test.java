package aster.cli;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RFC 3986 §5.4 的示例与评审 M-1 的分歧样例。 */
class Rfc3986Test {

    private static final String BASE = "http://a/b/c/d;p?q";

    @Test
    void normalExamples() {
        String[][] cases = {
            {"g:h", "g:h"}, {"g", "http://a/b/c/g"}, {"./g", "http://a/b/c/g"}, {"g/", "http://a/b/c/g/"},
            {"/g", "http://a/g"}, {"//g", "http://g"}, {"?y", "http://a/b/c/d;p?y"}, {"g?y", "http://a/b/c/g?y"},
            {"#s", "http://a/b/c/d;p?q"}, {"g#s", "http://a/b/c/g"}, {"g?y#s", "http://a/b/c/g?y"}, {";x", "http://a/b/c/;x"},
            {"g;x", "http://a/b/c/g;x"}, {"", null}, {".", "http://a/b/c/"}, {"./", "http://a/b/c/"}, {"..", "http://a/b/"},
            {"../", "http://a/b/"}, {"../g", "http://a/b/g"}, {"../..", "http://a/"}, {"../../", "http://a/"},
            {"../../g", "http://a/g"},
        };
        for (String[] c : cases) {
            if (c[1] == null) {
                assertThrows(IllegalArgumentException.class, () -> Rfc3986.resolve(BASE, c[0]));
            } else {
                assertEquals(c[1], Rfc3986.resolve(BASE, c[0]), c[0]);
            }
        }
    }

    @Test
    void abnormalExamples() {
        String[][] cases = {
            {"../../../g", "http://a/g"}, {"../../../../g", "http://a/g"}, {"/./g", "http://a/g"}, {"/../g", "http://a/g"},
            {"g.", "http://a/b/c/g."}, {".g", "http://a/b/c/.g"}, {"g..", "http://a/b/c/g.."}, {"..g", "http://a/b/c/..g"},
            {"./../g", "http://a/b/g"}, {"./g/.", "http://a/b/c/g/"}, {"g/./h", "http://a/b/c/g/h"}, {"g/../h", "http://a/b/c/h"},
            {"g;x=1/./y", "http://a/b/c/g;x=1/y"}, {"g;x=1/../y", "http://a/b/c/y"},
        };
        for (String[] c : cases) {
            assertEquals(c[1], Rfc3986.resolve(BASE, c[0]), c[0]);
        }
    }

    @Test
    void reviewDivergences() {
        String base = "https://localhost:1/a/b";
        assertEquals("https://localhost:1/a/b?q", Rfc3986.resolve(base, "?q"));
        assertEquals("https://localhost:1/x", Rfc3986.resolve(base, "../../x"));
        assertEquals("https://localhost:1/x", Rfc3986.resolve(base, "HTTPS://localhost:1/x"));
        for (String bad : new String[]{"x y", "\\x", "", "é", "a\tb", "%zz", "%4", "1bad:x"}) {
            assertFalse(Rfc3986.isReference(bad), bad);
            assertThrows(IllegalArgumentException.class, () -> Rfc3986.resolve(base, bad), bad);
        }
        assertTrue(Rfc3986.isReference("%E2%82%AC/x?y=1#z"));
    }

    @Test
    void redirectTargetRules() {
        for (String ok : new String[]{"https://h/x", "https://h:1/x", "https://h:65535/x", "https://[::1]:8443/x",
                "https://[v1.fe]/x", "https://127.0.0.1/x?q=1"}) {
            assertEquals(null, Rfc3986.targetViolation(ok), ok);
        }
        for (String bad : new String[]{"https:///x", "https://:443/x", "https://u@h/x", "https://u:p@h/x", "https://@h/x",
                "https://h:0/x", "https://h:65536/x", "https://h:/x", "https://h:1a/x", "https://h:+1/x",
                "https://h/a[b]", "https://h/x?a=[1]", "https://h[1]/x", "https://[::1/x", "https://[zz]/x",
                "https://[::1]x/x", "https:/x"}) {
            assertTrue(Rfc3986.targetViolation(bad) != null, bad);
        }
    }
}
