package aster.truffle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * {@code start} 注册的任务必须在被消费后清理（issue aster-lang-truffle#103 正文）。
 *
 * <p>★真实缺陷：{@code StartNode.registerTask} 之后**没有任何路径**会清理它——
 * {@code removeTask} 的唯一调用点是 {@code WorkflowNode} 的 finally，
 * {@code gc()} 的唯一调用点在测试里。于是 start/await 注册的任务完成后，
 * {@code tasks} / {@code taskInfos} / {@code DependencyGraph.nodes} /
 * {@code completedNodes} 全部随 Context 终身存活。
 *
 * <p>实测（修复前，同一 Context 连跑 5 次 start+wait）：
 *
 * <pre>
 *   taskCount 1 → 2 → 3 → 4 → 5      // 每次 eval 线性泄漏一个，即便任务已被正确 await
 * </pre>
 *
 * <p>{@code TaskInfo.callable} 闭包持有 {@code MaterializedFrame} 与结果对象，
 * 宿主池化 Context（aster-api 用 pooled Context）下按 eval 次数**无界累积**。
 *
 * <p>顺带消除「跨 eval 幽灵执行」：残留的无依赖 PENDING 任务会被后续无关 eval 的
 * {@code AwaitNode → executeNext} 捞起来执行——它不按 eval/workflow 过滤。
 *
 * <p>★本测试走**真实产品路径**（polyglot Context → Loader → StartNode/WaitNode →
 * AsyncTaskRegistry），并直接断言 registry 的任务数——行为层看不见内存泄漏，
 * 只断言「结果对不对」会完全漏掉本缺陷。
 */
class StartTaskLeakTest {

  private String fixture(String name) throws Exception {
    try (InputStream in =
        getClass().getClassLoader().getResourceAsStream("start-task-leak/" + name)) {
      assertNotNull(in, "fixture 缺失: " + name);
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private Value run(Context ctx, String json, String name) throws Exception {
    Value r = ctx.eval(Source.newBuilder("aster", json, name).build());
    return r.canExecute() ? r.execute() : r;
  }

  /** 用 host builtin 从 guest 侧回读 registry 任务数（测试线程拿不到 AsterContext）。 */
  private int taskCount(Context ctx) throws Exception {
    final int[] captured = {-1};
    aster.truffle.runtime.Builtins.register(
        "__probeTaskCount",
        new aster.truffle.runtime.Builtins.BuiltinDef(
            args -> {
              captured[0] = AsterLanguage.getContext().getAsyncRegistry().getTaskCount();
              return captured[0];
            }));
    run(ctx, fixture("task-count-probe.json"), "probe");
    assertEquals(true, captured[0] >= 0, "探针 builtin 未被执行，后续断言无意义");
    return captured[0];
  }

  @Test
  void startWaitDoesNotAccumulateTasksAcrossEvals() throws Exception {
    // ★核心回归：连跑 5 次，任务数必须恒为 0。
    //   修复前是 1→2→3→4→5。
    String src = fixture("start-wait.json");
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      for (int i = 0; i < 5; i++) {
        assertEquals(0, run(ctx, src, "wait" + i).asInt(), "第 " + i + " 轮返回值应为 0");
        assertEquals(
            0, taskCount(ctx), "第 " + i + " 轮后 registry 不得残留任务（start 的任务已被 wait 消费）");
      }
    }
  }

  @Test
  void startAwaitDoesNotAccumulateTasksAcrossEvals() throws Exception {
    // ★AwaitNode 是与 WaitNode 并列的**另一条**消费路径，必须单独覆盖——
    //   只修/只测其中一条，另一条照样泄漏。
    String src = fixture("start-await.json");
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      for (int i = 0; i < 5; i++) {
        assertEquals(42, run(ctx, src, "await" + i).asInt(), "第 " + i + " 轮 await 应取到 42");
        assertEquals(0, taskCount(ctx), "第 " + i + " 轮后 registry 不得残留任务");
      }
    }
  }

  @Test
  void awaitStillReturnsCorrectResultAfterCleanup() throws Exception {
    // ★反向护栏：清理不得把结果一起清掉。
    //   removeTask 会清 taskInfos/tasks，若在 getResult **之前**调用就取不到值了——
    //   没有这条，把 removeTask 提到 getResult 前面也能让上面两条的"任务数"断言变绿。
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      assertEquals(42, run(ctx, fixture("start-await.json"), "await").asInt(),
          "await 必须返回任务的真实结果，清理不得先于取值");
      assertEquals(0, run(ctx, fixture("start-wait.json"), "wait").asInt(),
          "wait 路径同样必须先取值再清理");
    }
  }
}
