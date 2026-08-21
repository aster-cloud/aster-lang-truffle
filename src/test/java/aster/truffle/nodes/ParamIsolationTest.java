package aster.truffle.nodes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 函数实参不得泄漏到**跨执行共享**的 globalEnv（#73）。
 *
 * <p>背景：{@code AsterRootNode} 原先同时做 {@code bindArgumentsToFrame} 与
 * {@code bindArgumentsToEnv}。后者把实参写进 globalEnv —— 那是**每个程序一份、
 * 跨所有执行共享**的对象，等于把「这次调用的输入」变成全局状态。
 *
 * <p>对合规决策引擎，这类污染的危害不在于会崩，而在于**静默给出错误答案**：
 * 一次决策可能用到另一次调用的输入，且没有任何异常提示。
 *
 * <p>★为什么此前没有暴露：那条写入对参数而言是**死写**。
 * {@code Loader.buildSimpleName} 只在名字**不在槽位表**里时才退回 {@code NameNodeEnv}，
 * 而每个参数都有槽位 → 参数永远走 frame，读不到 Env 里那份副本。
 * 但「读不到」不等于「没写」：只要将来有一条路径改从 Env 读，污染立刻变成错误答案。
 * 本测试把「不写进去」这一点直接钉住，而不是依赖读取顺序的巧合。
 */
class ParamIsolationTest {

    /** {@code func echo(x) { return x }} —— 参数直接回显，便于核对拿到的是不是自己的实参。 */
    private static final String ECHO_PROGRAM = """
        {
          "kind": "Module",
          "name": "test.param.isolation",
          "decls": [{
            "kind": "Func",
            "name": "echo",
            "params": [{ "name": "x", "type": { "kind": "TypeName", "name": "Int" } }],
            "ret": { "kind": "TypeName", "name": "Int" },
            "effects": [],
            "body": {
              "kind": "Block",
              "statements": [{ "kind": "Return", "expr": { "kind": "Name", "name": "x" } }]
            }
          }]
        }
        """;

    /**
     * 核心不变量：多次执行后，globalEnv 里**不应**出现参数名。
     *
     * <p>这是本次修复的直接断言。若有人把 bindArgumentsToEnv 加回来，此处立刻失败。
     */
    @Test
    @DisplayName("#73: 实参不得写入跨执行共享的 globalEnv")
    void argumentsMustNotLeakIntoGlobalEnv() throws Exception {
        try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
            context.initialize("aster");
            context.enter();
            try {
                // 经 Loader 建 Program 才能拿到与执行**同一份** Env 实例来核对；
                // 只走 context.eval 拿不到内部 Env，断言会变成空断言。
                // Loader 对 language 有 null 保护（Loader:134/416），测试传 null 即可。
                aster.truffle.Loader loader = new aster.truffle.Loader(null);
                aster.truffle.Loader.Program program = loader.buildProgram(ECHO_PROGRAM, "echo", null);

                // 有参入口的 program.root 是取出 lambda 的 NameNodeEnv，真正的调用
                // 由 LambdaRootNode 承担；这里只需驱动 AsterRootNode 执行一次，
                // 让它有机会（在修复前）把实参写进 globalEnv。
                AsterRootNode root = new AsterRootNode(
                    null, program.root, program.env, program.params, program.effects);
                root.getCallTarget().call(11);
                root.getCallTarget().call(22);

                assertFalse(program.env.contains("x"),
                    "★参数 x 不应出现在跨执行共享的 globalEnv 里——它是本次调用的输入，"
                        + "写进全局态会让两次执行互相覆盖（静默错答案）");
            } finally {
                context.leave();
            }
        }
    }

    /**
     * 串行交接：不同线程先后调用同一 CallTarget，各自必须读到自己的实参。
     *
     * <p>GraalVM 默认策略**拒绝真并发**（两线程同时进入会抛
     * {@code Multi threaded access requested ... not allowed}），但**允许串行交接**
     * ——实测线程 A 调完线程 B 可以接着调。所以「单线程策略」并不能保证参数隔离：
     * globalEnv 跨执行存活，串行交接下同样会读到上一次的残留。
     *
     * <p>本用例覆盖的正是这条真实可达的路径。真并发那条等语言开启多线程后再补。
     */
    @Test
    @DisplayName("#73: 跨线程串行交接时各次执行读到自己的实参")
    void sequentialHandoffAcrossThreadsKeepsOwnArgument() throws Exception {
        try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Source source = Source.newBuilder("aster", ECHO_PROGRAM, "param-handoff.json").build();
            Value program = context.eval(source);

            ExecutorService a = Executors.newSingleThreadExecutor();
            ExecutorService b = Executors.newSingleThreadExecutor();
            try {
                assertEquals(101, a.submit(() -> program.execute(101).asInt()).get(10, TimeUnit.SECONDS),
                    "线程 A 应读到自己的实参");
                assertEquals(202, b.submit(() -> program.execute(202).asInt()).get(10, TimeUnit.SECONDS),
                    "线程 B 应读到自己的实参，而不是 A 残留的值");
                assertEquals(303, a.submit(() -> program.execute(303).asInt()).get(10, TimeUnit.SECONDS),
                    "交回线程 A 后仍应读到本次实参");
            } finally {
                a.shutdownNow();
                b.shutdownNow();
            }
        }
    }

}
