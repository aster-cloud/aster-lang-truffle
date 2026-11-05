package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 调用目标与单段 builtin 同名、又有同名局部时：局部是函数值就调用局部，不是函数值才调用 builtin。
 * 与 aster-lang-ts 解释器（ts#273）及 aster-lang-core 类型检查（core#231：局部遮蔽规则，builtin 同名的
 * 非函数局部不报 E001）一致。此前 Loader 见到 builtin 名就直接分派 builtin，函数类型形参 {@code gt}、
 * 闭包 {@code add} 都被忽略，静默算错。
 */
class LocalCallTargetTest {

    private static final String INT = "{ \"kind\": \"TypeName\", \"name\": \"Int\" }";
    private static final String BOOL = "{ \"kind\": \"TypeName\", \"name\": \"Bool\" }";

    private static String name(String n) {
        return "{ \"kind\": \"Name\", \"name\": \"" + n + "\" }";
    }

    private static String num(int v) {
        return "{ \"kind\": \"Int\", \"value\": " + v + " }";
    }

    private static String call(String target, String... args) {
        return "{ \"kind\": \"Call\", \"target\": " + name(target) + ", \"args\": [" + String.join(",", args) + "] }";
    }

    private static String ret(String expr) {
        return "{ \"kind\": \"Return\", \"expr\": " + expr + " }";
    }

    private static String let(String n, String expr) {
        return "{ \"kind\": \"Let\", \"name\": \"" + n + "\", \"expr\": " + expr + " }";
    }

    private static String param(String n, String type) {
        return "{ \"name\": \"" + n + "\", \"type\": " + type + " }";
    }

    private static String lambda(String[] params, String body) {
        StringBuilder ps = new StringBuilder();
        for (String p : params) {
            if (ps.length() > 0) ps.append(',');
            ps.append(param(p, INT));
        }
        return "{ \"kind\": \"Lambda\", \"params\": [" + ps + "], \"ret\": " + INT
            + ", \"captures\": [], \"body\": { \"kind\": \"Block\", \"statements\": [" + ret(body) + "] } }";
    }

    private static String func(String fname, String params, String ret, String... stmts) {
        return "{ \"kind\": \"Func\", \"name\": \"" + fname + "\", \"params\": [" + params + "], \"ret\": " + ret
            + ", \"effects\": [], \"body\": { \"kind\": \"Block\", \"statements\": [" + String.join(",", stmts) + "] } }";
    }

    private static String module(String... funcs) {
        return "{ \"kind\": \"Module\", \"name\": \"m\", \"decls\": [" + String.join(",", funcs) + "] }";
    }

    private static Value run(String json, String entry) throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value program = ctx.eval(Source.newBuilder("aster", json, "call.json").build());
            Value v = program.canExecute() ? program.execute() : program;
            return Value.asValue(v.isNumber() ? (Object) v.asInt() : (Object) v.asBoolean());
        }
    }

    @Test
    void functionTypedParameterNamedLikeABuiltinIsCalled() throws IOException {
        // check(gt) 中 gt 是函数类型形参：gt(1, 2) 调用传入的 plus2，得 3；此前分派 builtin gt 得 false
        String fnType = "{ \"kind\": \"FuncType\", \"params\": [" + INT + "," + INT + "], \"ret\": " + INT + " }";
        String json = module(
            func("plus2", param("a", INT) + "," + param("b", INT), INT, ret(call("+", name("a"), name("b")))),
            func("check", param("gt", fnType), INT, ret(call("gt", num(1), num(2)))),
            func("main", "", INT, ret(call("check", name("plus2")))));
        assertEquals(3, run(json, "main").asInt());
    }

    @Test
    void closureNamedAddIsCalledInsteadOfTheBuiltin() throws IOException {
        String json = module(func("main", "", INT,
            let("add", lambda(new String[]{"a", "b"}, call("*", name("a"), name("b")))),
            ret(call("add", num(2), num(3)))));
        assertEquals(6, run(json, "main").asInt());
    }

    @Test
    void singleArgumentClosureNamedAddIsCalled() throws IOException {
        String json = module(func("main", "", INT,
            let("add", lambda(new String[]{"x"}, call("+", name("x"), num(10)))),
            ret(call("add", num(5)))));
        assertEquals(15, run(json, "main").asInt());
    }

    @Test
    void nonFunctionLocalNamedAddStillCallsTheBuiltin() throws IOException {
        String json = module(func("main", "", INT,
            let("add", num(1)),
            ret(call("add", num(2), num(3)))));
        assertEquals(5, run(json, "main").asInt());
    }

    @Test
    void closureNamedOrIsCalledInsteadOfTheShortCircuit() throws IOException {
        // or(…) 原先走短路节点；局部 or 是函数值时调用它
        String body = "{ \"kind\": \"Bool\", \"value\": false }";
        String orLambda = "{ \"kind\": \"Lambda\", \"params\": [" + param("p", BOOL) + "," + param("q", BOOL)
            + "], \"ret\": " + BOOL + ", \"captures\": [], \"body\": { \"kind\": \"Block\", \"statements\": [" + ret(body) + "] } }";
        String json = module(func("main", "", BOOL,
            let("or", orLambda),
            ret(call("or", "{ \"kind\": \"Bool\", \"value\": true }", "{ \"kind\": \"Bool\", \"value\": true }"))));
        assertEquals(false, run(json, "main").asBoolean());
    }

    @Test
    void closureShadowsSameNamedRule() throws IOException {
        // #161：同名规则 add 与局部闭包 add 并存时按 ADR 0051 §6.2 LOCAL 优先调用局部，得 6 而不是 5
        String json = module(
            func("add", param("a", INT) + "," + param("b", INT), INT, ret(call("+", name("a"), name("b")))),
            func("main", "", INT,
                let("add", lambda(new String[]{"a", "b"}, call("*", name("a"), name("b")))),
                ret(call("add", num(2), num(3)))));
        assertEquals(6, run(json, "main").asInt());
    }

    @Test
    void closureShadowsSameNamedNonBuiltinRule() throws IOException {
        String json = module(
            func("combine", param("a", INT) + "," + param("b", INT), INT, ret(call("+", name("a"), name("b")))),
            func("main", "", INT,
                let("combine", lambda(new String[]{"a", "b"}, call("*", name("a"), name("b")))),
                ret(call("combine", num(2), num(3)))));
        assertEquals(6, run(json, "main").asInt());
    }

    @Test
    void ruleIsCalledWhenNoLocalShadowsIt() throws IOException {
        String json = module(
            func("combine", param("a", INT) + "," + param("b", INT), INT, ret(call("+", name("a"), name("b")))),
            func("main", "", INT, ret(call("combine", num(2), num(3)))));
        assertEquals(5, run(json, "main").asInt());
    }
}
