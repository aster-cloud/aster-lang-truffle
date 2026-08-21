package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * DelayedTask 与 AsyncTaskRegistry 延迟调度能力测试
 */
public class DelayedTaskTest {

  private AsyncTaskRegistry registry;

  @BeforeEach
  public void setUp() {
    registry = new AsyncTaskRegistry();
  }

  @AfterEach
  public void tearDown() {
    registry.stopPolling();
    registry.shutdown();
  }

  @Test
  public void testCompareTo() {
    DelayedTask early = new DelayedTask(null, "wf-a", 1_000L, 1, "fail");
    DelayedTask late = new DelayedTask(null, "wf-b", 2_000L, 2, "fail");

    assertTrue(early.compareTo(late) < 0, "较早触发的任务应排在前面");
    assertTrue(late.compareTo(early) > 0, "较晚触发的任务应排在后面");
    assertEquals(0, early.compareTo(new DelayedTask(null, "wf-c", 1_000L, 3, "x")),
        "触发时间一致时 compareTo 应返回 0");
  }

  @Test
  public void testPriorityQueueOrdering() {
    PriorityQueue<DelayedTask> queue = new PriorityQueue<>();
    queue.offer(new DelayedTask(null, "wf-late", 3_000L, 1, "late"));
    queue.offer(new DelayedTask(null, "wf-mid", 2_000L, 1, "mid"));
    queue.offer(new DelayedTask(null, "wf-early", 1_000L, 1, "early"));

    assertEquals("wf-early", queue.poll().workflowId);
    assertEquals("wf-mid", queue.poll().workflowId);
    assertEquals("wf-late", queue.poll().workflowId);
  }

  @Test
  public void testScheduleRetry() throws Exception {
    long delayMs = 200L;
    long before = System.currentTimeMillis();
    registry.scheduleRetry("wf-retry", delayMs, 2, "boom");
    long after = System.currentTimeMillis();

    PriorityQueue<DelayedTask> queue = getDelayQueue();
    ReentrantLock lock = getDelayQueueLock();
    lock.lock();
    try {
      assertEquals(1, queue.size(), "延迟任务应被加入队列");
      DelayedTask task = queue.peek();
      assertEquals("wf-retry", task.workflowId);
      assertEquals(2, task.attemptNumber);
      assertEquals("boom", task.failureReason);
      long minTrigger = before + delayMs;
      long maxTrigger = after + delayMs + 5; // 给定 5ms 余量应对时钟误差
      assertTrue(task.triggerAtMs >= minTrigger && task.triggerAtMs <= maxTrigger,
          "触发时间应在预计窗口内");
    } finally {
      lock.unlock();
    }
  }

  @Test
  public void testPollDelayedTasks() throws Exception {
    registry.startPolling();
    registry.scheduleRetry("wf-poll", 50L, 1, "fail");

    assertTrue(waitForQueueEmpty(2_000L), "轮询线程应在超时前触发任务");
  }

  @Test
  public void testConcurrentScheduling() throws Exception {
    int threads = 6;
    int perThread = 20;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threads);

    // ★不要吞掉 worker 的异常/中断（truffle#70）。
    //
    // 旧写法在 catch(InterruptedException) 里只 Thread.interrupt()，却在 finally 里
    // 照常 doneLatch.countDown()：worker 若在 startLatch.await() 处被中断，会**整批
    // 跳过 20 次调度**，而主线程的 latch 断言依然通过，最终只表现为「队列少了 40 个」。
    // CI 上那次 expected 120 / was 80 正是这个形状——恰好两个 worker 的整批。
    // 本地已用最小复现验证：cancel 掉两个 worker 后 doneLatch 仍为 true、计数正好 80。
    //
    // 于是失败被归错了对象：看起来像「延迟队列并发丢任务」，实为「worker 没跑」。
    // 改为收集 Future 并逐个 get()——worker 里的任何异常都会在这里原样抛出。
    java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
    for (int i = 0; i < threads; i++) {
      final int threadIndex = i;
      futures.add(executor.submit(() -> {
        try {
          startLatch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          // 中断即视为该 worker 失败，向上抛给 Future.get()，不再静默跳过整批。
          throw new IllegalStateException("worker " + threadIndex + " 在 startLatch 处被中断", e);
        } finally {
          doneLatch.countDown();
        }
        for (int j = 0; j < perThread; j++) {
          registry.scheduleRetry("wf-" + threadIndex + "-" + j, 1_000L, j + 1, "reason");
        }
      }));
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(5, TimeUnit.SECONDS), "并发调度应在超时前完成");
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "worker 应在超时前全部结束");

    // 逐个 get()：worker 内的异常在此暴露，而不是被算成「队列少了几个」。
    for (java.util.concurrent.Future<?> f : futures) {
      f.get(5, TimeUnit.SECONDS);
    }

    // 断言「每个 (threadIndex, j) 组合都在」而非只比总数——总数相等也可能是
    // 一批丢失、另一批重复。逐项核对能直接看出丢的是整批还是零散。
    java.util.Set<String> ids = new java.util.HashSet<>();
    ReentrantLock lock = getDelayQueueLock();
    lock.lock();
    try {
      for (DelayedTask t : getDelayQueue()) {
        ids.add(t.workflowId);
      }
    } finally {
      lock.unlock();
    }
    java.util.List<String> missing = new java.util.ArrayList<>();
    for (int i = 0; i < threads; i++) {
      for (int j = 0; j < perThread; j++) {
        String id = "wf-" + i + "-" + j;
        if (!ids.contains(id)) missing.add(id);
      }
    }
    assertTrue(missing.isEmpty(),
        "延迟队列缺少这些任务（前若干个）：" + missing.subList(0, Math.min(10, missing.size()))
            + "；共缺 " + missing.size() + " 个");
    assertEquals(threads * perThread, getQueueSize(), "延迟队列应包含全部并发调度的任务");
  }

  @SuppressWarnings("unchecked")
  private PriorityQueue<DelayedTask> getDelayQueue() throws Exception {
    Field field = AsyncTaskRegistry.class.getDeclaredField("delayQueue");
    field.setAccessible(true);
    return (PriorityQueue<DelayedTask>) field.get(registry);
  }

  private ReentrantLock getDelayQueueLock() throws Exception {
    Field field = AsyncTaskRegistry.class.getDeclaredField("delayQueueLock");
    field.setAccessible(true);
    return (ReentrantLock) field.get(registry);
  }

  private int getQueueSize() throws Exception {
    ReentrantLock lock = getDelayQueueLock();
    lock.lock();
    try {
      return getDelayQueue().size();
    } finally {
      lock.unlock();
    }
  }

  private boolean waitForQueueEmpty(long timeoutMs) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (System.nanoTime() < deadline) {
      if (getQueueSize() == 0) {
        return true;
      }
      Thread.sleep(20);
    }
    return getQueueSize() == 0;
  }
}
