package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Scope 内声明的局部变量写入子 Env，且遮蔽同名形参（形参住在 frame slot）：
 * Scope 内的读（Name）、写（Set）都必须落到同一个 Env 变量，Scope 结束后形参恢复可见。
 */
class ScopeShadowingTest {

    private static final String INT = "{ \"kind\": \"TypeName\", \"name\": \"Int\" }";

    private static String module(String body) {
        return """
            { "kind": "Module", "name": "m", "decls": [
              { "kind": "Func", "name": "entry",
                "params": [ { "name": "x", "type": %s } ],
                "ret": %s, "effects": [],
                "body": { "kind": "Block", "statements": %s } } ] }
            """.formatted(INT, INT, body);
    }

    private static String let(String name, int v) {
        return "{ \"kind\": \"Let\", \"name\": \"" + name + "\", \"expr\": { \"kind\": \"Int\", \"value\": " + v + " } }";
    }

    private static String set(String name, int v) {
        return "{ \"kind\": \"Set\", \"name\": \"" + name + "\", \"expr\": { \"kind\": \"Int\", \"value\": " + v + " } }";
    }

    private static String ret(String name) {
        return "{ \"kind\": \"Return\", \"expr\": { \"kind\": \"Name\", \"name\": \"" + name + "\" } }";
    }

    private static String scope(String... stmts) {
        return "{ \"kind\": \"Scope\", \"statements\": [" + String.join(",", stmts) + "] }";
    }

    private static Value run(String json, Object arg) throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value program = ctx.eval(Source.newBuilder("aster", json, "scope.json").build());
            return program.execute(arg);
        }
    }

    @Test
    void scopeLetShadowsSameNamedParameterForReads() throws IOException {
        String body = "[" + scope(let("x", 2), ret("x")) + "]";
        assertEquals(2, run(module(body), 5).asInt());
    }

    @Test
    void scopeSetTargetsTheShadowingLocalNotTheParameter() throws IOException {
        String body = "[" + scope(let("x", 2), set("x", 3), ret("x")) + "]";
        assertEquals(3, run(module(body), 5).asInt());
    }

    @Test
    void parameterIsVisibleAgainAfterScopeEnds() throws IOException {
        String body = "[" + scope(let("x", 2)) + "," + ret("x") + "]";
        assertEquals(5, run(module(body), 5).asInt());
    }

    @Test
    void parameterReadBeforeScopeLetStillSeesArgument() throws IOException {
        String body = "[" + scope(ret("x"), let("x", 2)) + "]";
        assertEquals(5, run(module(body), 5).asInt());
    }

    @Test
    void nestedScopeInnerLetShadowsOuterScopeLet() throws IOException {
        String body = "[" + scope(let("x", 2), scope(let("x", 7), ret("x"))) + "]";
        assertEquals(7, run(module(body), 5).asInt());
    }

    @Test
    void nestedScopeSeesOuterScopeLetOverParameter() throws IOException {
        String body = "[" + scope(let("x", 2), scope(ret("x"))) + "]";
        assertEquals(2, run(module(body), 5).asInt());
    }

    @Test
    void scopeLocalWithoutParameterCollisionStillWorks() throws IOException {
        String body = "[" + scope(let("y", 4), ret("y")) + "]";
        assertEquals(4, run(module(body), 5).asInt());
    }
}
