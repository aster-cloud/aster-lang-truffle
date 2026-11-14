package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * ADR 0056 §4.2 / §5：Lambda.captures 只列外层局部。模块规则名、运算符、内建函数不再进 captures，
 * 也就不再占帧槽；Loader 构建 lambda 体时这些名字没有槽位，必须按全局（规则表 / 内建）查找。
 * 这里用与新版两引擎 lowering 相同形状的 IR（captures 只含外层局部）确认各种调用位置都能正确回落。
 */
class LambdaGlobalNameFallbackTest {

    private static final String INT = IrV2.typeName("Int");

    private static String lambda(List<String> params, List<String> captures, String... stmts) {
        StringBuilder ps = new StringBuilder();
        for (String p : params) {
            if (ps.length() > 0) ps.append(',');
            ps.append(IrV2.param(p, INT));
        }
        StringBuilder cs = new StringBuilder();
        for (String c : captures) {
            if (cs.length() > 0) cs.append(',');
            cs.append(IrV2.q(c));
        }
        return "{ \"kind\": \"Lambda\", \"params\": [" + ps + "], \"ret\": " + INT
            + ", \"body\": " + IrV2.block(stmts) + ", \"captures\": [" + cs + "], \"origin\": " + IrV2.ORIGIN + " }";
    }

    private static String func(String name, List<String> params, String... stmts) {
        return IrV2.func(name, params.stream().map(p -> IrV2.param(p, INT)).toList(), INT, List.of(), IrV2.block(stmts));
    }

    /** bump(x) = x + 1 */
    private static final String BUMP = func("bump", List.of("x"),
        IrV2.ret(IrV2.call(IrV2.name("+"), IrV2.name("x"), IrV2.intLit(1))));

    private static int run(String... decls) throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value program = ctx.eval(Source.newBuilder("aster", IrV2.module("m", decls), "fallback.json").build());
            Value v = program.canExecute() ? program.execute() : program;
            return v.asInt();
        }
    }

    @Test
    void ruleCalledInsideLambdaResolvesGlobally() throws IOException {
        // main: Let k be 5. Let f be (y) -> bump(y) + k [captures k]. Return f(1).  → 7
        assertEquals(7, run(BUMP, func("main", List.of(),
            IrV2.let("k", IrV2.intLit(5)),
            IrV2.let("f", lambda(List.of("y"), List.of("k"),
                IrV2.ret(IrV2.call(IrV2.name("+"), IrV2.call(IrV2.name("bump"), IrV2.name("y")), IrV2.name("k"))))),
            IrV2.ret(IrV2.call(IrV2.name("f"), IrV2.intLit(1))))));
    }

    @Test
    void ruleUsedAsValueInsideLambdaResolvesGlobally() throws IOException {
        // lambda 体内 Let g be bump（规则作函数值，未捕获）再调用 g(y)  → 4
        assertEquals(4, run(BUMP, func("main", List.of(),
            IrV2.let("f", lambda(List.of("y"), List.of(),
                IrV2.let("g", IrV2.name("bump")),
                IrV2.ret(IrV2.call(IrV2.name("g"), IrV2.name("y"))))),
            IrV2.ret(IrV2.call(IrV2.name("f"), IrV2.intLit(3))))));
    }

    @Test
    void lambdaInvokedFromAnotherRuleStillResolvesGlobalNames() throws IOException {
        // applyTo(fn, v) = fn(v)；main 把只捕获 k 的 lambda 交给 applyTo 调用  → bump(4) + 3 = 8
        String fnType = "{ \"kind\": \"FuncType\", \"params\": [" + INT + "], \"ret\": " + INT
            + ", \"origin\": " + IrV2.ORIGIN + " }";
        String applyTo = IrV2.func("applyTo", List.of(IrV2.param("fn", fnType), IrV2.param("v", INT)), INT, List.of(),
            IrV2.block(IrV2.ret(IrV2.call(IrV2.name("fn"), IrV2.name("v")))));
        assertEquals(8, run(BUMP, applyTo, func("main", List.of(),
            IrV2.let("k", IrV2.intLit(3)),
            IrV2.let("f", lambda(List.of("y"), List.of("k"),
                IrV2.ret(IrV2.call(IrV2.name("+"), IrV2.call(IrV2.name("bump"), IrV2.name("y")), IrV2.name("k"))))),
            IrV2.ret(IrV2.call(IrV2.name("applyTo"), IrV2.name("f"), IrV2.intLit(4))))));
    }

    @Test
    void nestedLambdaPassesThroughOuterLocalAndResolvesRuleGlobally() throws IOException {
        // outer(x) [captures k] { inner(z) [captures k] = bump(z) * k; Return inner(x) }；k = 2，outer(4) → 10
        String inner = lambda(List.of("z"), List.of("k"),
            IrV2.ret(IrV2.call(IrV2.name("*"), IrV2.call(IrV2.name("bump"), IrV2.name("z")), IrV2.name("k"))));
        String outer = lambda(List.of("x"), List.of("k"),
            IrV2.let("inner", inner),
            IrV2.ret(IrV2.call(IrV2.name("inner"), IrV2.name("x"))));
        assertEquals(10, run(BUMP, func("main", List.of(),
            IrV2.let("k", IrV2.intLit(2)),
            IrV2.let("outer", outer),
            IrV2.ret(IrV2.call(IrV2.name("outer"), IrV2.intLit(4))))));
    }

    @Test
    void methodCallRewriteInsideLambdaUsesBuiltin() throws IOException {
        // x.add(offset) 改写为 add(x, offset)：add 是内建，不在 captures 里  → 5 + 10 = 15
        assertEquals(15, run(func("main", List.of(),
            IrV2.let("offset", IrV2.intLit(10)),
            IrV2.let("f", lambda(List.of("x"), List.of("offset"),
                IrV2.ret(IrV2.call(IrV2.name("add"), IrV2.name("x"), IrV2.name("offset"))))),
            IrV2.ret(IrV2.call(IrV2.name("f"), IrV2.intLit(5))))));
    }

    @Test
    void enumMemberInsideLambdaIsNotCapturedAndStillResolves() throws IOException {
        // 枚举成员不是外层局部，不进 captures；lambda 体内的 Hoch 按枚举成员求值  → 1
        assertEquals(1, run(IrV2.enumDecl("Stufe", "Hoch", "Niedrig"), func("main", List.of(),
            IrV2.let("f", lambda(List.of("x"), List.of(), IrV2.ret(IrV2.name("Hoch")))),
            IrV2.match(IrV2.call(IrV2.name("f"), IrV2.intLit(0)),
                IrV2.caseOf(IrV2.patCtor("Hoch"), IrV2.ret(IrV2.intLit(1))),
                IrV2.caseOf(IrV2.patName("other"), IrV2.ret(IrV2.intLit(2)))))));
    }
}
