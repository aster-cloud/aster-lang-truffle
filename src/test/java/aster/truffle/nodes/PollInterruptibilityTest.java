package aster.truffle.nodes;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import aster.truffle.runtime.AsyncTaskRegistry;
import io.aster.workflow.DeterminismContext;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * await/wait 的轮询必须可中断（issue aster-lang-truffle#106 正文）。
 *
 * <p>★真实缺陷：轮询循环唯一的让步是 {@code Thread.yield()}——它<b>不响应中断</b>。于是
 *
 * <ul>
 *   <li>被 await 的任务若永远到不了终态，本线程 100% CPU 自旋；</li>
 *   <li>{@code executor.shutdownNow()} 发出的 interrupt 被完全无视，
 *       {@code awaitTermination(30s)} 必然超时 → 非守护线程<b>永久泄漏</b>、
 *       阻止 JVM 退出，{@code disposeContext} 被硬拖 30 秒。</li>
 * </ul>
 *
 * <p>改为可被 interrupt 打断的短睡眠，并在捕获后<b>恢复中断位</b>再抛，不吞掉信号。
 */
class PollInterruptibilityTest {

  /** 注册一个永不终结的任务，制造「被 await 的对象到不了终态」这一情形。 */
  private AsyncTaskRegistry registryWithNeverEndingTask(String taskId) {
    var registry = new AsyncTaskRegistry(1, new DeterminismContext());
    registry.registerTask(taskId, () -> {
      // 永不返回——但要能被 interrupt 终止，否则测试自身会泄漏线程
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    return registry;
  }

  @Test
  void awaitPollingIsInterruptible() throws Exception {
    // ★核心回归：修复前 Thread.yield() 无视 interrupt，本测试会超时。
    var registry = registryWithNeverEndingTask("never-1");
    var started = new CountDownLatch(1);
    var finished = new CountDownLatch(1);
    var thrown = new AtomicReference<Throwable>();

    Thread waiter = new Thread(() -> {
      started.countDown();
      try {
        AwaitNode.pollUntilTerminal(registry, "never-1");
      } catch (Throwable t) {
        thrown.set(t);
      } finally {
        finished.countDown();
      }
    }, "await-waiter");
    waiter.setDaemon(true);
    waiter.start();

    assertTrue(started.await(5, TimeUnit.SECONDS), "线程未启动");
    Thread.sleep(50);            // 让它确实进入轮询
    waiter.interrupt();

    assertTrue(finished.await(5, TimeUnit.SECONDS),
        "interrupt 后轮询必须终止——否则 shutdownNow 无法回收该线程，"
            + "非守护线程永久泄漏并阻止 JVM 退出");
    assertNotNull(thrown.get(), "应以异常方式退出，而不是静默返回");
    registry.shutdown();
  }

  @Test
  void waitPollingIsInterruptible() throws Exception {
    // ★WaitNode 是与 AwaitNode 并列的**另一条**轮询路径，必须单独覆盖——
    //   只修/只测其中一条，另一条照样自旋不可中断。
    var registry = registryWithNeverEndingTask("never-2");
    var started = new CountDownLatch(1);
    var finished = new CountDownLatch(1);
    var thrown = new AtomicReference<Throwable>();

    Thread waiter = new Thread(() -> {
      started.countDown();
      try {
        WaitNode.pollUntilAllTerminal(registry, new String[] {"never-2"});
      } catch (Throwable t) {
        thrown.set(t);
      } finally {
        finished.countDown();
      }
    }, "wait-waiter");
    waiter.setDaemon(true);
    waiter.start();

    assertTrue(started.await(5, TimeUnit.SECONDS), "线程未启动");
    Thread.sleep(50);
    waiter.interrupt();

    assertTrue(finished.await(5, TimeUnit.SECONDS),
        "WaitNode 的轮询同样必须可中断");
    assertNotNull(thrown.get(), "应以异常方式退出");
    registry.shutdown();
  }

  @Test
  void normalCompletionStillReturnsResult() throws Exception {
    // ★反向护栏：把「可中断」做进去后，正常路径必须照常返回结果。
    //   没有这条，把轮询写成「一进来就抛中断」也能让上面两条变绿。
    var registry = new AsyncTaskRegistry(1, new DeterminismContext());
    registry.registerTask("quick", () -> { /* 立即完成 */ });

    // 正常路径必须返回（不抛中断异常）——具体返回值由 Runnable 语义决定，此处只验「能返回」。
    AwaitNode.pollUntilTerminal(registry, "quick");
    registry.shutdown();
  }
}
