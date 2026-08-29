package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aster.workflow.DeterminismContext;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * 延迟重试 poller 的排空与锁粒度（issue aster-lang-truffle#108 正文）。
 *
 * <p>★两个缺陷：
 *
 * <ol>
 *   <li>{@code pollDelayedTasks} 每轮用 {@code if}（非 while）只处理**一个**到期任务，
 *       然后 {@code Thread.sleep(100)}——N 个同时到期的重试要 N×100ms 才排空。
 *       用户配的是 10ms backoff，实际却因排队变成几秒。</li>
 *   <li>{@code resumeTask → scheduleTask → submitTask}（含 {@code executor.submit}、
 *       {@code timeoutScheduler.schedule}）**全程在 delayQueueLock 临界区内**执行——
 *       把无关的调度 IO 圈进了本该只保护队列结构的锁。</li>
 * </ol>
 *
 * <p>★覆盖范围（如实标注）：本类只锁住第 1 条（批量排空）。第 2 条「提交移出临界区」
 * <b>没有</b>对应的自动化断言——我试过在任务体里查 {@code delayQueueLock.isLocked()}，
 * 但任务体跑在 <b>executor worker 线程</b>上，而锁由 poller 线程持有，
 * worker 观察到的状态反映不了「poller 此刻是否在临界区」（实测：把 resume 移回锁内，
 * 该断言照样绿）。真要锁住它需要在 resumeTask 入口埋断言或用
 * {@code isHeldByCurrentThread} 同线程判定——属改生产代码，超出本次范围。
 * 第 2 条的依据是代码结构（提交动作已在 finally 之外），由全量测试保证未引入回归。
 */
class DelayPollerBatchTest {

  @SuppressWarnings("unchecked")
  private java.util.Queue<Object> delayQueue(AsyncTaskRegistry r) throws Exception {
    Field f = AsyncTaskRegistry.class.getDeclaredField("delayQueue");
    f.setAccessible(true);
    return (java.util.Queue<Object>) f.get(r);
  }



  private long clockNow(AsyncTaskRegistry r) throws Exception {
    Field f = AsyncTaskRegistry.class.getDeclaredField("determinismContext");
    f.setAccessible(true);
    Object ctx = f.get(r);
    Object clock = ctx.getClass().getMethod("clock").invoke(ctx);
    Object inst = clock.getClass().getMethod("now").invoke(clock);
    return (long) inst.getClass().getMethod("toEpochMilli").invoke(inst);
  }

  /**
   * 跑**一轮** poller 主体。
   *
   * <p>{@code pollDelayedTasks} 是 {@code while (running)} 循环：
   * running=false 时循环体**一次都不执行**（我第一版就写错在这里——
   * 以为「设 false 让它跑完一轮即退出」，实际是根本不进循环）。
   * 正确做法：running=true 让它进循环，另起线程等第一轮做完再置 false + interrupt。
   */
  private void pollOnce(AsyncTaskRegistry r) throws Exception {
    Method m = AsyncTaskRegistry.class.getDeclaredMethod("pollDelayedTasks");
    m.setAccessible(true);
    Field running = AsyncTaskRegistry.class.getDeclaredField("running");
    running.setAccessible(true);
    running.setBoolean(r, true);

    Thread t = new Thread(() -> {
      try {
        m.invoke(r);
      } catch (Exception ignored) {
        // interrupt 导致的退出属预期
      }
    }, "poll-once");
    t.setDaemon(true);
    t.start();
    // 一轮排空 + sleep(100) —— 给足时间做完第一轮
    Thread.sleep(400);
    running.setBoolean(r, false);
    t.interrupt();
    t.join(2000);
  }

  /**
   * 直接把 DelayedTask 塞进队列——**不能用 scheduleRetry**：
   * 它会自动启动 poller（`if (!running) shouldStartPoller = true`），
   * 后台线程会在断言之前把队列排空，前置条件必然失败（实测踩到）。
   * 本测试要验证的是 poller **单轮**的排空行为，故必须自己控制入队时机。
   */
  private void enqueue(AsyncTaskRegistry r, String taskId, long triggerAtMs) throws Exception {
    Class<?> dt = Class.forName("aster.truffle.runtime.DelayedTask");
    var ctor = dt.getDeclaredConstructors()[0];
    ctor.setAccessible(true);
    Object task = ctor.newInstance(taskId, "wf-test", triggerAtMs, 1, "test");
    delayQueue(r).add(task);
  }

  @Test
  void allDueTasksDrainInOneRound() throws Exception {
    // ★核心回归：一轮必须排空**全部**到期任务，而不是每轮一个。
    var registry = new AsyncTaskRegistry(2, new DeterminismContext());
    try {
      for (int i = 0; i < 10; i++) {
        registry.registerTask("batch-" + i, () -> { /* no-op */ });
        enqueue(registry, "batch-" + i, clockNow(registry));   // 立即到期（用引擎自己的时钟）
      }
      assertTrue(delayQueue(registry).size() >= 10, "前置条件：应有 10 个待触发任务");

      pollOnce(registry);

      assertTrue(delayQueue(registry).isEmpty(),
          "一轮必须排空全部到期任务；剩余 " + delayQueue(registry).size()
              + " 个说明仍是「每轮只处理一个」——N 个重试要 N×100ms 才排空");
    } finally {
      registry.stopPolling();
      registry.shutdown();
    }
  }

  @Test
  void notYetDueTasksStayQueued() throws Exception {
    // ★反向护栏：批量排空不得把**未到期**的任务一并拽出。
    //   没有这条，把 while 条件写成「只要队列非空就 poll」也能让上面变绿。
    var registry = new AsyncTaskRegistry(2, new DeterminismContext());
    try {
      registry.registerTask("future-1", () -> { });
      enqueue(registry, "future-1", clockNow(registry) + 3_600_000L);  // 1 小时后
      int before = delayQueue(registry).size();
      assertTrue(before >= 1, "前置条件：应有待触发任务");

      pollOnce(registry);

      assertTrue(delayQueue(registry).size() == before,
          "未到期任务必须留在队列里，不得被批量排空拽出");
    } finally {
      registry.stopPolling();
      registry.shutdown();
    }
  }
}
