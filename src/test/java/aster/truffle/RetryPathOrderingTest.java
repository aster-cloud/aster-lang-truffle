package aster.truffle;

import aster.truffle.runtime.AsyncTaskRegistry;
import aster.truffle.runtime.WorkflowScheduler;
import io.aster.workflow.DeterminismContext;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 失败重试路径的状态推进顺序（issue aster-lang-truffle#105 正文）。
 *
 * <p>★真实缺陷：{@code runTask} 的失败路径「先调度重试、后置 PENDING」顺序倒置。
 * {@code onTaskFailed} 内部的 {@code scheduleRetry} 已把 DelayedTask 放进
 * {@code pendingRetryTasks} 并入队，返回后才 {@code state.status.set(PENDING)}。
 * 若 poller 恰在这两步之间触发，{@code resumeTask → scheduleTask} 看到状态仍是
 * RUNNING 就直接 return（对非 PENDING 静默丢弃且**不重新入队**）——此时 DelayedTask
 * 已被 {@code delayQueue.poll()} 出队且不回队，**重试被永久丢弃**；随后任务被置回
 * PENDING 且仍在 readyQueue 里，{@code executeUntilComplete} 反复 submit 但 CAS
 * {@code submitted} 失败只拿到一个永不完成的 future → {@code barrier.join()} 永久阻塞。
 *
 * <p>对照证据：{@code handleTaskTimeout} 的重试分支一直是「先 CAS RUNNING→PENDING
 * 再 onTaskFailed」的正确顺序——同一文件里两处顺序不一致，本身就是缺陷的指纹。
 *
 * <h2>★为什么不用「跑很多轮碰运气」的写法</h2>
 *
 * <p>我最初写的是「短 backoff + 20 轮 + 硬超时」的压力测试。它在**修复前的代码上
 * 同样全绿**——poller 轮询间隔 100ms，而竞态窗口是两条相邻语句间的亚微秒级间隙，
 * 靠随机调度撞上的概率实际为零。那种测试只是**看起来**在测竞态。
 *
 * <p>故这里改为**确定性地**把系统摆进竞态发生后的那一刻：手工把任务状态置成 RUNNING
 * （等价于「poller 抢在 status.set(PENDING) 之前跑进来」），再直接调用
 * {@code scheduleTask}，断言那个 DelayedTask **没有被丢弃**。不依赖时序，结论可复现。
 */
class RetryPathOrderingTest {

  private static final long ROUND_TIMEOUT_MS = 15_000L;

  @SuppressWarnings("unchecked")
  private Map<String, Object> pendingRetryTasks(AsyncTaskRegistry registry) throws Exception {
    Field f = AsyncTaskRegistry.class.getDeclaredField("pendingRetryTasks");
    f.setAccessible(true);
    return (Map<String, Object>) f.get(registry);
  }

  @SuppressWarnings("unchecked")
  private java.util.Queue<Object> delayQueue(AsyncTaskRegistry registry) throws Exception {
    Field f = AsyncTaskRegistry.class.getDeclaredField("delayQueue");
    f.setAccessible(true);
    return (java.util.Queue<Object>) f.get(registry);
  }

  @SuppressWarnings("unchecked")
  private Object taskState(AsyncTaskRegistry registry, String taskId) throws Exception {
    Field f = AsyncTaskRegistry.class.getDeclaredField("tasks");
    f.setAccessible(true);
    return ((Map<String, Object>) f.get(registry)).get(taskId);
  }

  @SuppressWarnings("unchecked")
  private void setStatus(Object state, AsyncTaskRegistry.TaskStatus status) throws Exception {
    Field f = state.getClass().getDeclaredField("status");
    f.setAccessible(true);
    ((AtomicReference<AsyncTaskRegistry.TaskStatus>) f.get(state)).set(status);
  }

  private void invokeScheduleTask(AsyncTaskRegistry registry, String taskId, Object delayedTask)
      throws Exception {
    // DelayedTask 的具体类名不写死——直接按方法名找，避免嵌套/顶层类名假设出错
    Method m = java.util.Arrays.stream(AsyncTaskRegistry.class.getDeclaredMethods())
        .filter(x -> x.getName().equals("scheduleTask") && x.getParameterCount() == 2)
        .findFirst()
        .orElseThrow(() -> new AssertionError("找不到 scheduleTask(String, DelayedTask)"));
    m.setAccessible(true);
    m.invoke(registry, taskId, delayedTask);
  }

  /** 建一个「必失败且配了重试」的 registry，跑一次让 DelayedTask 落进 pendingRetryTasks。 */
  private AsyncTaskRegistry armFailedTaskWithPendingRetry(String wf, String taskId)
      throws Exception {
    DeterminismContext ctx = new DeterminismContext();
    AsyncTaskRegistry registry = new AsyncTaskRegistry(2, ctx);
    new WorkflowScheduler(registry, wf, null, ctx);
    // backoff 取大值：确保 DelayedTask 不会在断言前被 poller 真的触发
    var policy = new AsyncTaskRegistry.RetryPolicy(3, "exponential", 5_000L);
    registry.registerTaskWithRetry(
        taskId,
        () -> {
          throw new RuntimeException("fail to trigger retry scheduling");
        },
        Collections.emptySet(),
        policy);
    registry.executeNext();   // 未 startPolling，DelayedTask 会停在 pendingRetryTasks
    return registry;
  }

  @Test
  void delayedRetryIsNotDroppedWhenStatusHasNotYetBecomePending() throws Exception {
    // ★确定性复现竞态发生后的那一刻。
    //   修复前：非 PENDING → 直接 return，DelayedTask 就此蒸发
    //   （pendingRetryTasks 里那一项再无任何路径处理）→ 重试永久丢失。
    //   修复后：非终态时 requeueDelayedTask 重排，条目仍在。
    String taskId = "racy-task";
    AsyncTaskRegistry registry = armFailedTaskWithPendingRetry("race-wf", taskId);
    try {
      Object delayed = pendingRetryTasks(registry).get(taskId);
      assertTrue(delayed != null,
          "前置条件：失败后应有 DelayedTask 待触发。若为空说明本用例根本没走到重试调度路径，"
              + "后面的断言就是空洞的");

      // ★完整模拟 poller 那一刻做的事：先 poll 出队（这是丢弃的关键——
      //   出队后除非有人重新 offer，这次重试就永久消失），再 resumeTask→scheduleTask。
      Object polled = delayQueue(registry).poll();
      assertTrue(polled != null, "前置条件：delayQueue 里应有待触发的 DelayedTask");
      assertTrue(delayQueue(registry).isEmpty(), "前置条件：出队后队列应为空");

      // 竞态：状态尚未落到 PENDING
      setStatus(taskState(registry, taskId), AsyncTaskRegistry.TaskStatus.RUNNING);

      invokeScheduleTask(registry, taskId, polled);

      assertFalse(delayQueue(registry).isEmpty(),
          "状态尚未变成 PENDING 时，出队的 DelayedTask 必须被重新入队——"
              + "否则这次重试永久消失，executeUntilComplete 会永久阻塞（issue #105）");
    } finally {
      registry.stopPolling();
      registry.shutdown();
    }
  }

  @Test
  void terminalTaskDelayedRetryIsDropped_notRequeuedForever() throws Exception {
    // ★反向护栏：终态任务的 DelayedTask **必须**丢弃，否则无限自旋重排。
    //   没有这条，把「非 PENDING 一律重排」写成无条件重排也能让上一条变绿——
    //   那样修好了竞态却引入活锁。
    String taskId = "terminal-task";
    AsyncTaskRegistry registry = armFailedTaskWithPendingRetry("terminal-wf", taskId);
    try {
      Object delayed = pendingRetryTasks(registry).get(taskId);
      assertTrue(delayed != null, "前置条件：应有 DelayedTask");

      Object polled = delayQueue(registry).poll();
      assertTrue(polled != null, "前置条件：delayQueue 里应有待触发的 DelayedTask");

      setStatus(taskState(registry, taskId), AsyncTaskRegistry.TaskStatus.CANCELLED);

      invokeScheduleTask(registry, taskId, polled);

      assertTrue(delayQueue(registry).isEmpty(),
          "终态任务的 DelayedTask 必须丢弃，不得重新入队——否则无限自旋成活锁");
    } finally {
      registry.stopPolling();
      registry.shutdown();
    }
  }

  @Test
  void normalRetryStillSucceedsEndToEnd() throws Exception {
    // ★端到端护栏：顺序调整不得打断正常重试。
    //   没有这条，把重试整个禁掉也能让上面两条变绿。
    DeterminismContext ctx = new DeterminismContext();
    AsyncTaskRegistry registry = new AsyncTaskRegistry(2, ctx);
    new WorkflowScheduler(registry, "normal-wf", null, ctx);
    registry.startPolling();
    AtomicInteger attempts = new AtomicInteger();
    try {
      var policy = new AsyncTaskRegistry.RetryPolicy(3, "exponential", 10L);
      registry.registerTaskWithRetry(
          "normal-task",
          () -> {
            if (attempts.incrementAndGet() == 1) {
              throw new RuntimeException("first attempt fails on purpose");
            }
            return "ok";
          },
          Collections.emptySet(),
          policy);

      Thread runner = new Thread(registry::executeUntilComplete, "normal-runner");
      runner.setDaemon(true);
      runner.start();
      runner.join(ROUND_TIMEOUT_MS);

      assertTrue(!runner.isAlive(), "正常重试路径不得阻塞");
      assertEquals(2, attempts.get(), "任务应执行 2 次（首次失败 + 重试成功）");
    } finally {
      registry.stopPolling();
      registry.shutdown();
    }
  }

  @Test
  void exhaustedRetriesStillTerminate_notStuckInPending() throws Exception {
    // ★锁住 MaxRetriesExceeded 分支里「把状态退回 RUNNING」那一步：
    //   PENDING 被提前设置后若不退回，handleFinalTaskFailure 的
    //   CAS(RUNNING→FAILED) 不成立 → 任务永不终结。
    DeterminismContext ctx = new DeterminismContext();
    AsyncTaskRegistry registry = new AsyncTaskRegistry(2, ctx);
    new WorkflowScheduler(registry, "exhaust-wf", null, ctx);
    registry.startPolling();
    try {
      var policy = new AsyncTaskRegistry.RetryPolicy(2, "exponential", 1L);
      registry.registerTaskWithRetry(
          "always-fails",
          () -> {
            throw new RuntimeException("always fails");
          },
          Collections.emptySet(),
          policy);

      Thread runner = new Thread(() -> {
        try {
          registry.executeUntilComplete();
        } catch (RuntimeException expected) {
          // 重试用尽后抛异常是正确行为
        }
      }, "exhaust-runner");
      runner.setDaemon(true);
      runner.start();
      runner.join(ROUND_TIMEOUT_MS);

      assertTrue(!runner.isAlive(),
          "重试用尽后必须终结并抛异常，不得卡在 PENDING 永久阻塞");
    } finally {
      registry.stopPolling();
      registry.shutdown();
    }
  }
}
