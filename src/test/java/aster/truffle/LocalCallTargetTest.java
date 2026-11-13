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

    private static final String INT = IrV2.typeName("Int");
    private static final String BOOL = IrV2.typeName("Bool");

    private static String name(String n) {
        return IrV2.name(n);
    }

    private static String num(int v) {
        return IrV2.intLit(v);
    }

    private static String call(String target, String... args) {
        return IrV2.call(name(target), args);
    }

    private static String ret(String expr) {
        return IrV2.ret(expr);
    }

    private static String let(String n, String expr) {
        return IrV2.let(n, expr);
    }

    private static String param(String n, String type) {
        return IrV2.param(n, type);
    }

    private static String lambda(String[] params, String body) {
        StringBuilder ps = new StringBuilder();
        for (String p : params) {
            if (ps.length() > 0) ps.append(',');
            ps.append(param(p, INT));
        }
        return "{ \"kind\": \"Lambda\", \"params\": [" + ps + "], \"ret\": " + INT
            + ", \"body\": " + IrV2.block(ret(body)) + ", \"captures\": [], \"origin\": " + IrV2.ORIGIN + " }";
    }

    /** {@code params} 为逗号拼接的 {@link #param} 列表（可为空串）。 */
    private static String func(String fname, String params, String ret, String... stmts) {
        java.util.List<String> ps = params.isEmpty() ? java.util.List.of() : java.util.List.of(params);
        return IrV2.func(fname, ps, ret, java.util.List.of(), IrV2.block(stmts));
    }

    private static String module(String... funcs) {
        return IrV2.module("m", funcs);
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
        String fnType = "{ \"kind\": \"FuncType\", \"params\": [" + INT + "," + INT + "], \"ret\": " + INT
            + ", \"origin\": " + IrV2.ORIGIN + " }";
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
        String orLambda = "{ \"kind\": \"Lambda\", \"params\": [" + param("p", BOOL) + "," + param("q", BOOL)
            + "], \"ret\": " + BOOL + ", \"body\": " + IrV2.block(ret(IrV2.bool(false)))
            + ", \"captures\": [], \"origin\": " + IrV2.ORIGIN + " }";
        String json = module(func("main", "", BOOL,
            let("or", orLambda),
            ret(call("or", IrV2.bool(true), IrV2.bool(true)))));
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
