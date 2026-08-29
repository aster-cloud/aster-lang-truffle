package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Workflow 注册中途失败必须完全回滚（issue #109 正文）。
 *
 * <p>★真实缺陷有**两层**，缺一不可：
 *
 * <ol>
 *   <li>{@code WorkflowNode} 的注册循环在第 3 步 try/finally **之外**。循环第 i 步抛异常时
 *       （{@code resolveDependencyIds} 的 "Unknown workflow dependency"、或
 *       {@code DependencyGraph.addTask} 的循环依赖检测），第 0..i-1 步已注册的任务永远
 *       留在共享 registry。</li>
 *   <li>即便补上回滚调用 {@code removeTask}，它**只清 map 不扣 remainingTasks**——
 *       而 {@code registerInternal} 注册时是 incrementAndGet 的。计数器被永久抬高。</li>
 * </ol>
 *
 * <p>后果是**永久毒化该 Context**：此后任何 workflow 求值都撞
 * {@code executeUntilComplete} 的「死锁检测：无就绪任务但仍有 N 个任务待完成」。
 * 即一个含未知依赖的坏程序会打断该 Context 之后**所有** workflow。
 *
 * <p>★本测试跑的是**真实产品路径**（polyglot Context → Loader → WorkflowNode →
 * AsyncTaskRegistry），不是对回滚逻辑的复刻——复刻只能证明复刻件自洽。
 *
 * <p>实测记录：只补第 1 层（注册循环加 try/rollback）时，本测试的
 * {@code subsequentWorkflowStillWorksAfterRegistrationFailure} 仍然红，
 * 报的正是上述死锁检测异常——这是第 2 层缺陷被发现的过程。
 */
class WorkflowRegistrationRollbackTest {

  private String fixture(String name) throws Exception {
    try (InputStream in = getClass().getClassLoader()
        .getResourceAsStream("workflow-rollback/" + name)) {
      assertNotNull(in, "fixture 缺失: " + name);
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private Value run(Context ctx, String json, String name) throws Exception {
    Value r = ctx.eval(Source.newBuilder("aster", json, name).build());
    return r.canExecute() ? r.execute() : r;
  }

  @Test
  void subsequentWorkflowStillWorksAfterRegistrationFailure() throws Exception {
    // ★核心回归：同一 Context 内，坏 workflow 之后的**合法** workflow 必须照常成功。
    //   修复前它死于「死锁检测：无就绪任务但仍有 1 个任务待完成」——
    //   那 1 个正是坏 workflow 泄漏的 alpha。
    String bad = fixture("unknown-dependency.json");
    String good = fixture("valid.json");

    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      assertThrows(Exception.class, () -> run(ctx, bad, "bad"),
          "前置条件：未知依赖的 workflow 本就该失败");

      Value v = assertDoesNotThrow(() -> run(ctx, good, "good"),
          "坏 workflow 不得毒化 Context——后续合法 workflow 必须照常执行");
      assertEquals(0, v.asInt());
    }
  }

  @Test
  void registrationFailureStillReportsOriginalCause() throws Exception {
    // ★回滚绝不能吞掉或替换原始失败原因。
    //   没有这条，把回滚写成「catch 后吞掉」也能让上一条变绿——
    //   坏 workflow 静默成功，比毒化更糟。
    String bad = fixture("unknown-dependency.json");

    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      var ex = assertThrows(Exception.class, () -> run(ctx, bad, "bad"));
      assertTrue(String.valueOf(ex.getMessage()).contains("Unknown workflow dependency"),
          "必须原样报出未知依赖，而非被清理逻辑掩盖；实际: " + ex.getMessage());
    }
  }

  @Test
  void repeatedFailuresDoNotAccumulateResidue() throws Exception {
    // ★残骸是**累积**的：跑三次坏 workflow 后再跑合法的，仍须成功。
    //   只跑一次坏的，计数器只抬高 1，某些实现可能侥幸不触发死锁检测；
    //   累积三次能把「回滚不完全」与「完全不回滚」区分开。
    String bad = fixture("unknown-dependency.json");
    String good = fixture("valid.json");

    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      for (int i = 0; i < 3; i++) {
        final int round = i;
        assertThrows(Exception.class, () -> run(ctx, bad, "bad" + round));
      }

      Value v = assertDoesNotThrow(() -> run(ctx, good, "good"),
          "三次失败注册的残骸不得累积");
      assertEquals(0, v.asInt());
    }
  }

  @Test
  void failedRegistrationLeavesNoTaskResidueInRegistry() throws Exception {
    // ★上面三条只能观察「后续 workflow 还能不能跑」——那只覆盖 remainingTasks 这一层。
    //   实测：去掉 WorkflowNode 的注册回滚、只留 removeTask 的计数修正，
    //   上面三条**仍然全绿**，但 registry 的 tasks/taskInfos 里残骸逐次累积
    //   （插桩实测每次坏 workflow 后 getTaskCount 递增 0→1→2→3）。
    //   那是池化 Context 下的无界内存泄漏，行为层看不见，必须直接断言任务数。
    //
    //   TaskInfo.callable 闭包持有 MaterializedFrame 与结果对象，
    //   宿主（aster-api 用 pooled Context）按每次 eval 线性累积。
    String bad = fixture("unknown-dependency.json");

    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      // 先跑一个合法 workflow，确保 registry 已被初始化
      run(ctx, fixture("valid.json"), "warmup");

      for (int i = 0; i < 3; i++) {
        final int round = i;
        assertThrows(Exception.class, () -> run(ctx, bad, "bad" + round));
      }

      assertEquals(0, remainingTaskCount(ctx),
          "三次注册失败后 registry 不得残留任何任务（每次泄漏 1 个 alpha 会累积到 3）");
    }
  }

  /**
   * 读取当前 Context 的 registry 任务数。
   *
   * <p>用一个 host builtin 从 guest 侧回调——因为 {@code AsterContext} 只能在
   * Context 内部经 {@code AsterLanguage.getContext()} 取得，测试线程直接拿不到。
   * 这是本仓既有测试注册 builtin 的同一手法（见 FrameIntegrationTest.setup）。
   */
  private int remainingTaskCount(Context ctx) throws Exception {
    final int[] captured = {-1};
    aster.truffle.runtime.Builtins.register("__probeTaskCount",
        new aster.truffle.runtime.Builtins.BuiltinDef(args -> {
          captured[0] = AsterLanguage.getContext().getAsyncRegistry().getTaskCount();
          return captured[0];
        }));
    run(ctx, fixture("task-count-probe.json"), "probe");
    return captured[0];
  }

  @Test
  void validWorkflowWorksOnCleanContext() throws Exception {
    // ★反向护栏：合法 workflow 在干净 Context 上本就该成功。
    //   没有这条，若 valid.json 自身有问题（比如根本跑不通），
    //   上面几条的「后续仍成功」断言就无从谈起。
    String good = fixture("valid.json");

    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      assertEquals(0, run(ctx, good, "good").asInt());
    }
  }
}
