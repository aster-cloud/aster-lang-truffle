package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aster.workflow.DeterminismContext;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * executeUntilComplete 必须响应中断（aster-api#414 复审）。
 *
 * <p>等待延迟重试时它以 {@code parkNanos(1ms)} 轮询，等待批次时阻塞在 barrier 上。若不检查中断，
 * 调用方（如 JUnit {@code @Timeout}、宿主取消）中断线程后它仍无限等待，且中断标志置位时
 * {@code parkNanos} 立即返回，1ms 轮询退化为 100% CPU 空转。
 */
class ExecuteUntilCompleteInterruptTest {

  /** 在独立线程跑 executeUntilComplete，等待 startDelayMs 后中断它，返回其抛出的异常与中断标志。 */
  private static Outcome runAndInterrupt(AsyncTaskRegistry registry, long startDelayMs) throws Exception {
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    AtomicBoolean interruptFlag = new AtomicBoolean();
    Thread runner = new Thread(() -> {
      try {
        registry.executeUntilComplete();
      } catch (Throwable t) {
        thrown.set(t);
      } finally {
        interruptFlag.set(Thread.currentThread().isInterrupted());
      }
    }, "execute-until-complete");
    runner.setDaemon(true);
    runner.start();
    Thread.sleep(startDelayMs);
    long interruptedAt = System.nanoTime();
    runner.interrupt();
    runner.join(3000);
    long exitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - interruptedAt);
    return new Outcome(runner.isAlive(), exitMs, thrown.get(), interruptFlag.get());
  }

  private record Outcome(boolean stillAlive, long exitMs, Throwable thrown, boolean interruptFlag) { }

  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  void interruptWhileWaitingForDelayedRetryExitsPromptly() throws Exception {
    // 首次失败后重试排在 1 小时之后：executeUntilComplete 会一直轮询等待
    AsyncTaskRegistry registry = new AsyncTaskRegistry(2, new DeterminismContext());
    registry.setWorkflowId("wf-interrupt-retry");
    try {
      registry.registerTaskWithRetry("task-wait", () -> {
        throw new RuntimeException("transient");
      }, Collections.emptySet(), new AsyncTaskRegistry.RetryPolicy(3, "linear", 3_600_000L));

      Outcome o = runAndInterrupt(registry, 300);

      assertTrue(!o.stillAlive(), "中断后 executeUntilComplete 必须退出，而不是继续等待");
      assertTrue(o.exitMs() < 1000, "应在中断后及时退出，实际 " + o.exitMs() + "ms");
      assertInstanceOf(CancellationException.class, o.thrown());
      assertTrue(o.interruptFlag(), "必须保留调用线程的中断标志");
    } finally {
      registry.stopPolling();
      registry.shutdown();
    }
  }

  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  void interruptWhileWaitingOnRunningBatchExitsPromptly() throws Exception {
    AsyncTaskRegistry registry = new AsyncTaskRegistry(2, new DeterminismContext());
    CountDownLatch release = new CountDownLatch(1);
    try {
      registry.registerTask("task-block", () -> {
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      Outcome o = runAndInterrupt(registry, 300);

      assertTrue(!o.stillAlive(), "中断后不得继续阻塞在批次 barrier 上");
      assertTrue(o.exitMs() < 1000, "应在中断后及时退出，实际 " + o.exitMs() + "ms");
      assertInstanceOf(CancellationException.class, o.thrown());
      assertTrue(o.interruptFlag(), "必须保留调用线程的中断标志");
    } finally {
      release.countDown();
      registry.stopPolling();
      registry.shutdown();
    }
  }
}
