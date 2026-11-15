package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.util.List;

import static aster.truffle.IrV2.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 宿主调用入口时的 effect 权限（#174）与 workflow / Start worker 线程上的权限传递（#175）。
 *
 * <p>权限只来自宿主调用点的入口声明：有参入口经 {@code ctx.eval} 只取出函数值，真正计算发生在宿主
 * {@code Value.execute(args)} 时，此时必须按入口声明的 effects 安装权限；被调函数（嵌套调用）沿用调用方
 * 权限，不能给自己授权；效应变量不授权任何具体效应（fail-closed）。
 *
 * <p>Truffle 后端没有真实 IO：IO 内建在通过效应校验后报「不受支持」，未通过则报 "effect not permitted"，
 * 两种错误消息即「放行 / 拒绝」的可观测区分。
 */
class HostCallEffectPermissionTest {

    private static final String INT = typeName("Int");
    private static final String DENIED = "effect not permitted";
    private static final String UNSUPPORTED = "不受支持";

    private static String ioPrint() {
        return call(name("IO.print"), str("hi"));
    }

    /** 有参入口 main(x)，声明 {@code effects}，体内调 IO。 */
    private static String paramIoEntry(List<String> effects) {
        return module("m", func("main", List.of(param("x", INT)), INT, effects, block(ret(ioPrint()))));
    }

    private static String start(String name, String expr) {
        return "{\"kind\":\"Start\",\"name\":" + q(name) + ",\"expr\":" + expr + ",\"origin\":" + ORIGIN + "}";
    }

    private static String waitFor(String name) {
        return "{\"kind\":\"Wait\",\"names\":[" + q(name) + "],\"origin\":" + ORIGIN + "}";
    }

    private static String workflow(String stepBody) {
        String step = "{\"kind\":\"step\",\"name\":\"s\",\"body\":" + stepBody + ",\"compensate\":null,"
            + "\"dependencies\":[],\"effectCaps\":[],\"origin\":" + ORIGIN + "}";
        return "{\"kind\":\"workflow\",\"steps\":[" + step + "],\"effectCaps\":[],\"retry\":null,\"timeout\":null,"
            + "\"origin\":" + ORIGIN + "}";
    }

    private static String lambda(String... stmts) {
        return "{\"kind\":\"Lambda\",\"params\":[" + param("y", INT) + "],\"ret\":" + INT
            + ",\"body\":" + block(stmts) + ",\"captures\":[],\"origin\":" + ORIGIN + "}";
    }

    private static Source source(String json) throws Exception {
        return Source.newBuilder("aster", json, "host-call-effects.json").build();
    }

    /** eval 取出有参入口的函数值，再以宿主身份调用（#174 的宿主调用路径）。 */
    private static String hostCallFailure(String json) throws Exception {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value entry = ctx.eval(source(json));
            assertTrue(entry.canExecute(), "有参入口经 eval 应返回可执行函数值");
            return assertThrows(PolyglotException.class, () -> entry.execute(1)).getMessage();
        }
    }

    /** 无参入口：eval 直接执行。 */
    private static String evalFailure(String json) throws Exception {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            return assertThrows(PolyglotException.class, () -> ctx.eval(source(json))).getMessage();
        }
    }

    private static void assertDenied(String msg) {
        assertTrue(msg.contains(DENIED), "期望效应校验拒绝，实际: " + msg);
    }

    private static void assertPermitted(String msg) {
        assertFalse(msg.contains(DENIED), "期望效应校验放行，实际: " + msg);
        assertTrue(msg.contains(UNSUPPORTED), "放行后应报 Truffle 不支持 IO，实际: " + msg);
    }

    // ---------------- #174：有参入口经宿主调用 ----------------

    @Test
    void paramEntryWithoutDeclaredIoIsDeniedOnHostCall() throws Exception {
        assertDenied(hostCallFailure(paramIoEntry(List.of())));
    }

    @Test
    void paramEntryWithDeclaredIoIsPermittedOnHostCall() throws Exception {
        assertPermitted(hostCallFailure(paramIoEntry(List.of("io"))));
    }

    @Test
    void paramEntryDeclaringOnlyCpuIsDeniedIo() throws Exception {
        assertDenied(hostCallFailure(paramIoEntry(List.of("cpu"))));
    }

    @Test
    void paramEntryEffectVariableGrantsNoConcreteEffect() throws Exception {
        String json = paramIoEntry(List.of("E")).replace("\"effectParams\":[]", "\"effectParams\":[\"E\"]");
        assertDenied(hostCallFailure(json));
    }

    @Test
    void paramlessEntryFollowsDeclarationToo() throws Exception {
        String undeclared = module("m", func("main", List.of(), INT, List.of(), block(ret(ioPrint()))));
        String declared = module("m", func("main", List.of(), INT, List.of("io"), block(ret(ioPrint()))));
        assertDenied(evalFailure(undeclared));
        assertPermitted(evalFailure(declared));
    }

    @Test
    void nestedCalleeInheritsCallerPermissions() throws Exception {
        // 入口声明 io，调用未声明效应的 helper：helper 沿用调用方权限
        String json = module("m",
            func("helper", List.of(param("y", INT)), INT, List.of(), block(ret(ioPrint()))),
            func("main", List.of(param("x", INT)), INT, List.of("io"),
                block(ret(call(name("helper"), name("x"))))));
        assertPermitted(hostCallFailure(json));
    }

    @Test
    void nestedCalleeCannotGrantItselfEffects() throws Exception {
        // 入口未声明 io，helper 声明 io：被调函数不能给自己授权
        String json = module("m",
            func("helper", List.of(param("y", INT)), INT, List.of("io"), block(ret(ioPrint()))),
            func("main", List.of(param("x", INT)), INT, List.of(),
                block(ret(call(name("helper"), name("x"))))));
        assertDenied(hostCallFailure(json));
    }

    @Test
    void lambdaReturnedToHostDoesNotInheritEntryEffects() throws Exception {
        // 入口声明 io 并把内部 lambda 交给宿主；宿主之后直接调用该 lambda 时入口已返回，
        // lambda 自身没有效应声明 → 按 fail-closed 拒绝，不能沿用已结束的入口权限
        String json = module("m", func("main", List.of(param("x", INT)), typeName("Fn"), List.of("io"),
            block(ret(lambda(ret(ioPrint()))))));
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value entry = ctx.eval(source(json));
            Value fn = entry.execute(1);
            assertTrue(fn.canExecute(), "入口应返回可执行 lambda");
            assertDenied(assertThrows(PolyglotException.class, () -> fn.execute(2)).getMessage());
        }
    }

    @Test
    void lambdaCalledInsideDeclaredEntryInheritsPermissions() throws Exception {
        // 入口声明 io，体内调用自己的 lambda：lambda 是嵌套调用，沿用入口权限
        String json = module("m", func("main", List.of(param("x", INT)), INT, List.of("io"),
            block(let("f", lambda(ret(ioPrint()))), ret(call(name("f"), name("x"))))));
        assertPermitted(hostCallFailure(json));
    }

    @Test
    void permissionsDoNotLeakAcrossSequentialHostCalls() throws Exception {
        // 同一 context、同一宿主线程：先调声明 io 的入口，再调未声明的入口，后者仍被拒
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value withIo = ctx.eval(source(paramIoEntry(List.of("io"))));
            assertPermitted(assertThrows(PolyglotException.class, () -> withIo.execute(1)).getMessage());
            Value withoutIo = ctx.eval(Source.newBuilder("aster", paramIoEntry(List.of()), "second.json").build());
            assertDenied(assertThrows(PolyglotException.class, () -> withoutIo.execute(1)).getMessage());
            // 第一个入口仍按自己的声明放行，不受第二次调用影响
            assertPermitted(assertThrows(PolyglotException.class, () -> withIo.execute(1)).getMessage());
        }
    }

    @Test
    void reentrantHostCallOnlyNarrowsCallerPermissions() throws Exception {
        // 模拟「guest 执行中经宿主回调再入」：当前线程已在 guest 帧内时，宿主调用只能取
        // 声明 ∩ 调用方现有权限，不能借被调函数的声明放宽
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value withIo = ctx.eval(source(paramIoEntry(List.of("io"))));
            Value withoutIo = ctx.eval(Source.newBuilder("aster", paramIoEntry(List.of()), "second.json").build());
            ctx.enter();
            boolean outermost = aster.truffle.runtime.AllocationBudget.enterFrame();
            AsterContext asterContext = AsterLanguage.getContext();
            try {
                asterContext.setAllowedEffects(java.util.Set.of());
                assertDenied(assertThrows(PolyglotException.class, () -> withIo.execute(1)).getMessage());
                asterContext.setAllowedEffects(java.util.Set.of("io"));
                assertPermitted(assertThrows(PolyglotException.class, () -> withIo.execute(1)).getMessage());
                assertDenied(assertThrows(PolyglotException.class, () -> withoutIo.execute(1)).getMessage());
                // 再入调用结束后恢复调用方原有权限
                assertEquals(java.util.Set.of("io"), asterContext.getAllowedEffects());
            } finally {
                asterContext.clearAllowedEffects();
                aster.truffle.runtime.AllocationBudget.exitFrame(outermost);
                ctx.leave();
            }
        }
    }

    @Test
    void paramEntryReturnsValueWhenNoEffectNeeded() throws Exception {
        String json = module("m", func("main", List.of(param("x", INT)), INT, List.of(),
            block(ret(call(name("+"), name("x"), intLit(1))))));
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertEquals(42, ctx.eval(source(json)).execute(41).asInt());
        }
    }

    // ---------------- #175：workflow / Start 的 worker 线程 ----------------

    private static String paramlessMain(List<String> effects, String... stmts) {
        return module("m", func("main", List.of(), INT, effects, block(stmts)));
    }

    @Test
    void workflowStepIoPermittedWhenEntryDeclaresIo() throws Exception {
        assertPermitted(evalFailure(paramlessMain(List.of("io"), workflow(block(ret(ioPrint()))), ret(intLit(0)))));
    }

    @Test
    void workflowStepIoDeniedWhenEntryOmitsIo() throws Exception {
        assertDenied(evalFailure(paramlessMain(List.of(), workflow(block(ret(ioPrint()))), ret(intLit(0)))));
    }

    @Test
    void workflowStepIoDeniedForEffectVariable() throws Exception {
        String json = paramlessMain(List.of("E"), workflow(block(ret(ioPrint()))), ret(intLit(0)))
            .replace("\"effectParams\":[]", "\"effectParams\":[\"E\"]");
        assertDenied(evalFailure(json));
    }

    @Test
    void startTaskIoPermittedWhenEntryDeclaresIo() throws Exception {
        assertPermitted(evalFailure(paramlessMain(List.of("io"), start("t", ioPrint()), waitFor("t"), ret(intLit(0)))));
    }

    @Test
    void startTaskIoDeniedWhenEntryOmitsIo() throws Exception {
        assertDenied(evalFailure(paramlessMain(List.of(), start("t", ioPrint()), waitFor("t"), ret(intLit(0)))));
    }

    @Test
    void startTaskPickedUpByWorkflowExecutorFollowsDeclaration() throws Exception {
        // Start 任务注册后先跑一个 workflow：调度器会把仍 PENDING 的 Start 任务一并提交到 executor
        // worker 线程执行，此时任务体同样需要进入 context 才能按入口声明放行
        String declared = paramlessMain(List.of("io"),
            start("t", ioPrint()), workflow(block(ret(intLit(1)))), waitFor("t"), ret(intLit(0)));
        String undeclared = paramlessMain(List.of(),
            start("t", ioPrint()), workflow(block(ret(intLit(1)))), waitFor("t"), ret(intLit(0)));
        assertPermitted(evalFailure(declared));
        assertDenied(evalFailure(undeclared));
    }

    @Test
    void workflowStepIoPermittedInSandboxedHostContext() throws Exception {
        // 与 aster-api TrufflePolicyRuntime 同口径的受限 Context（不开 createThread / IO / 类查找）：
        // worker 线程进入 context 不依赖这些权限
        String json = module("m", func("main", List.of(param("x", INT)), INT, List.of("io"),
            block(workflow(block(ret(ioPrint()))), ret(name("x")))));
        try (Context ctx = Context.newBuilder("aster")
                .allowHostAccess(org.graalvm.polyglot.HostAccess.EXPLICIT)
                .allowIO(org.graalvm.polyglot.io.IOAccess.NONE)
                .allowNativeAccess(false)
                .allowHostClassLookup(n -> false)
                .allowCreateThread(false)
                .allowCreateProcess(false)
                .build()) {
            Value entry = ctx.eval(source(json));
            assertPermitted(assertThrows(PolyglotException.class, () -> entry.execute(1)).getMessage());
        }
    }

    @Test
    void workflowInParamEntryFollowsHostCallDeclaration() throws Exception {
        // #174 × #175：有参入口经宿主调用，step 在 worker 线程上沿用入口声明
        String declared = module("m", func("main", List.of(param("x", INT)), INT, List.of("io"),
            block(workflow(block(ret(ioPrint()))), ret(name("x")))));
        String undeclared = module("m", func("main", List.of(param("x", INT)), INT, List.of(),
            block(workflow(block(ret(ioPrint()))), ret(name("x")))));
        assertPermitted(hostCallFailure(declared));
        assertDenied(hostCallFailure(undeclared));
    }

    @Test
    void workflowStepWithoutEffectsStillCompletes() throws Exception {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value v = ctx.eval(source(paramlessMain(List.of(), workflow(block(ret(intLit(1)))), ret(intLit(7)))));
            assertEquals(7, v.asInt());
        }
    }
}
