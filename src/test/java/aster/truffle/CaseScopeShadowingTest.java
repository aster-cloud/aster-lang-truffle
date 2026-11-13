package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * case 分支体与 Scope 是独立的词法作用域（ADR 0052 §2.1，aster-lang-truffle#156）：
 * 块内 {@code Let} 一个外层已有的名字只遮蔽外层，之后的 {@code Set} 写块内绑定，离开块后外层值不变；
 * 块内没有同名 {@code Let} 时 {@code Set} 写回外层。模式绑定同样只在本分支内遮蔽外层同名变量。
 * 语义与 aster-lang-ts 解释器（每个 case / Scope 一层子环境）一致。
 */
class CaseScopeShadowingTest {

    private static final String INT = IrV2.typeName("Int");

    /** 入口 {@code entry(n: Int): Int}，函数体为给定语句。 */
    private static String module(String... stmts) {
        return IrV2.module("m",
            IrV2.func("entry", java.util.List.of(IrV2.param("n", INT)), INT, java.util.List.of(), IrV2.block(stmts)));
    }

    private static String num(int v) {
        return IrV2.intLit(v);
    }

    private static String name(String n) {
        return IrV2.name(n);
    }

    private static String plus(String a, String b) {
        return IrV2.call(name("+"), a, b);
    }

    private static String let(String n, String expr) {
        return IrV2.let(n, expr);
    }

    private static String set(String n, String expr) {
        return IrV2.set(n, expr);
    }

    private static String ret(String expr) {
        return IrV2.ret(expr);
    }

    private static String block(String... stmts) {
        return IrV2.block(stmts);
    }

    private static String scope(String... stmts) {
        return IrV2.scope(stmts);
    }

    private static String whenInt(int v, String body) {
        return IrV2.caseOf(IrV2.patInt(v), body);
    }

    private static String whenName(String bind, String body) {
        return IrV2.caseOf(IrV2.patName(bind), body);
    }

    private static String match(String subject, String... cases) {
        return IrV2.match(name(subject), cases);
    }

    private static int run(String json, int arg) throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value program = ctx.eval(Source.newBuilder("aster", json, "shadow.json").build());
            return program.execute(arg).asInt();
        }
    }

    @Test
    void caseLetShadowsOuterLetAndOuterValueSurvivesTheCase() throws IOException {
        String json = module(
            let("x", num(1)),
            match("n",
                whenInt(0, block(let("x", num(100)), set("x", plus(name("x"), num(1))))),
                whenName("other", block(let("x", name("other"))))),
            ret(name("x")));
        assertEquals(1, run(json, 0));
        assertEquals(1, run(json, 5));
    }

    @Test
    void caseBodyReadsItsOwnShadowingBinding() throws IOException {
        String json = module(
            let("x", num(1)),
            let("y", num(0)),
            match("n", whenInt(0, block(let("x", num(100)), set("y", name("x"))))),
            ret(plus(name("x"), name("y"))));
        assertEquals(101, run(json, 0));
    }

    @Test
    void readBeforeTheShadowingLetSeesTheOuterBinding() throws IOException {
        String json = module(
            let("x", num(1)),
            let("y", num(0)),
            match("n", whenInt(0, block(set("y", name("x")), let("x", num(100))))),
            ret(plus(name("x"), name("y"))));
        assertEquals(2, run(json, 0));
    }

    @Test
    void setWithoutShadowingLetStillWritesBackTheOuterBinding() throws IOException {
        String json = module(
            let("x", num(1)),
            match("n", whenInt(0, block(set("x", num(7))))),
            ret(name("x")));
        assertEquals(7, run(json, 0));
    }

    @Test
    void nestedCaseShadowLeavesTheEnclosingCaseBindingIntact() throws IOException {
        String json = module(
            let("x", num(1)),
            match("n", whenInt(0, block(
                let("x", num(10)),
                match("n", whenInt(0, block(let("x", num(20)), set("x", plus(name("x"), num(1)))))),
                ret(name("x"))))),
            ret(name("x")));
        assertEquals(10, run(json, 0));
        assertEquals(1, run(json, 3));
    }

    @Test
    void caseLetShadowsAParameter() throws IOException {
        String json = module(
            match("n", whenInt(0, block(let("n", num(50)), set("n", plus(name("n"), num(1)))))),
            ret(name("n")));
        assertEquals(0, run(json, 0));
    }

    @Test
    void patternBindingShadowsASameNamedOuterLocalOnlyInsideTheCase() throws IOException {
        String json = module(
            let("other", num(7)),
            match("n",
                whenInt(0, ret(name("other"))),
                whenName("other", block(ret(name("other"))))),
            ret(num(-1)));
        assertEquals(7, run(json, 0));
        assertEquals(5, run(json, 5));
    }

    @Test
    void outerLocalIsVisibleAgainAfterAPatternBindingCase() throws IOException {
        String json = module(
            let("other", num(7)),
            let("seen", num(0)),
            match("n", whenName("other", block(set("seen", name("other"))))),
            ret(plus(name("other"), name("seen"))));
        assertEquals(12, run(json, 5));
    }

    @Test
    void nestedScopeLetDoesNotOverwriteTheOuterScopeLet() throws IOException {
        String json = module(scope(let("x", num(2)), scope(let("x", num(7))), ret(name("x"))));
        assertEquals(2, run(json, 0));
    }

    @Test
    void nestedScopeSetWritesBackTheOuterScopeLet() throws IOException {
        String json = module(scope(let("x", num(2)), scope(set("x", num(7))), ret(name("x"))));
        assertEquals(7, run(json, 0));
    }

    @Test
    void nestedScopeShadowThenSetOnlyTouchesTheInnerBinding() throws IOException {
        String json = module(scope(
            let("x", num(2)),
            scope(let("x", num(7)), set("x", plus(name("x"), num(1)))),
            ret(name("x"))));
        assertEquals(2, run(json, 0));
    }

    @Test
    void caseInsideScopeShadowsTheScopeLocal() throws IOException {
        String json = module(scope(
            let("x", num(2)),
            match("n", whenInt(0, block(let("x", num(9))))),
            ret(name("x"))));
        assertEquals(2, run(json, 0));
    }

    @Test
    void scopeLetInsideCaseShadowsTheFunctionLocal() throws IOException {
        String json = module(
            let("x", num(1)),
            // v2 的 CaseBody 只有 Return / Block（ADR 0055），Scope 放进分支块内
            match("n", whenInt(0, block(scope(let("x", num(9)), set("x", num(10)))))),
            ret(name("x")));
        assertEquals(1, run(json, 0));
    }

    @Test
    void recursiveCallsKeepTheirOwnShadowingBindings() throws IOException {
        // entry(n) = n == 0 ? 0 : (case 内 Let n 遮蔽形参后递归) —— 每次调用的块内绑定互不干扰
        String json = module(
            let("acc", name("n")),
            match("n",
                whenInt(0, ret(num(0))),
                whenName("k", block(
                    let("acc", num(100)),
                    set("acc", plus(name("acc"), IrV2.call(name("entry"), IrV2.call(name("-"), name("k"), num(1)))))))),
            ret(name("acc")));
        assertEquals(3, run(json, 3));
    }
}
