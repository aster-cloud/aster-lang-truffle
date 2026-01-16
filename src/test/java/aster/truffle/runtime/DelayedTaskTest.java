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

    for (int i = 0; i < threads; i++) {
      final int threadIndex = i;
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < perThread; j++) {
            registry.scheduleRetry("wf-" + threadIndex + "-" + j, 1_000L, j + 1, "reason");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(5, TimeUnit.SECONDS), "并发调度应在超时前完成");
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.SECONDS);

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
