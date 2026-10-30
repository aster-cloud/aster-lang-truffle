package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aster.workflow.DeterminismContext;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * 延迟重试 poller 与确定性时钟的边界（aster-api#414）。
 *
 * <p>poller 每 100ms 判断一次「是否到期」，这是调度基础设施的轮询，不是 workflow 的时间决策。
 * 若它读取 workflow 的 {@link io.aster.workflow.ReplayDeterministicClock}：
 *
 * <ul>
 *   <li>重放模式下每轮消耗一条录制时间，录制序列很快耗尽，{@code now()} 抛
 *       「Replay exhausted」——到期任务永远触发不了，{@code executeUntilComplete} 永久阻塞；</li>
 *   <li>异常分支没有 sleep，poller 变成紧密循环刷 SEVERE 日志（测试日志曾膨胀到约 340MB）；</li>
 *   <li>录制模式下每轮往录制序列追加一条时间，poller 存活期间无界增长。</li>
 * </ul>
 */
class DelayPollerClockTest {

  private static final Logger REGISTRY_LOGGER = Logger.getLogger(AsyncTaskRegistry.class.getName());

  private final CountingHandler handler = new CountingHandler();
  private boolean previousUseParentHandlers;

  // 只计数、不转发到控制台：修复前的紧密循环会在几秒内刷出海量堆栈，拖垮测试进程的输出通道。
  @BeforeEach
  void attachHandler() {
    previousUseParentHandlers = REGISTRY_LOGGER.getUseParentHandlers();
    REGISTRY_LOGGER.setUseParentHandlers(false);
    REGISTRY_LOGGER.addHandler(handler);
  }

  @AfterEach
  void detachHandler() {
    REGISTRY_LOGGER.removeHandler(handler);
    REGISTRY_LOGGER.setUseParentHandlers(previousUseParentHandlers);
  }

  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  void replayModeRetryFiresWithoutExhaustingClock() throws Exception {
    // 重放上下文只录制了一条时间：恰好够 scheduleRetry 计算 triggerAt。
    DeterminismContext ctx = new DeterminismContext();
    ctx.clock().enterReplayMode(List.of(Instant.now()));
    AsyncTaskRegistry registry = new AsyncTaskRegistry(1, ctx);
    registry.setWorkflowId("wf-replay-poller");
    try {
      AtomicInteger attempts = new AtomicInteger();
      registry.registerTaskWithRetry("task-replay", () -> {
        if (attempts.incrementAndGet() == 1) {
          throw new RuntimeException("transient");
        }
        registry.setResult("task-replay", "ok");
        return "ok";
      }, Collections.emptySet(), new AsyncTaskRegistry.RetryPolicy(3, "linear", 50L));

      CompletableFuture<Void> run = CompletableFuture.runAsync(registry::executeUntilComplete);
      run.get(5, TimeUnit.SECONDS);

      assertEquals("ok", registry.getResult("task-replay"));
      assertEquals(2, attempts.get(), "重放模式下延迟重试必须按时触发");
      assertEquals(0, handler.severe.get(), "poller 不得因重放时钟耗尽而报错");
    } finally {
      registry.stopPolling();
      registry.shutdown();
    }
  }

  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  void pollerDoesNotRecordIntoWorkflowClock() throws Exception {
    DeterminismContext ctx = new DeterminismContext();
    AsyncTaskRegistry registry = new AsyncTaskRegistry(1, ctx);
    try {
      registry.startPolling();
      int before = ctx.clock().getRecordedTimes().size();
      Thread.sleep(600);
      int after = ctx.clock().getRecordedTimes().size();
      assertEquals(before, after, "空转的 poller 不得向 workflow 时钟追加录制（否则录制序列无界增长）");
    } finally {
      registry.stopPolling();
      registry.shutdown();
    }
  }

  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  void persistentPollErrorBacksOffInsteadOfSpinning() throws Exception {
    AsyncTaskRegistry registry = new AsyncTaskRegistry(1, new DeterminismContext());
    AtomicInteger peeks = new AtomicInteger();
    Field f = AsyncTaskRegistry.class.getDeclaredField("delayQueue");
    f.setAccessible(true);
    f.set(registry, new PriorityQueue<Object>() {
      @Override
      public Object peek() {
        peeks.incrementAndGet();
        throw new IllegalStateException("模拟不可恢复的轮询错误");
      }
    });
    try {
      registry.startPolling();
      Thread.sleep(1000);
      registry.stopPolling();

      // 正常节奏 1s 约 10 轮；紧密循环会是数十万轮。退避后只会更少。
      assertTrue(peeks.get() <= 12, "持续错误时 poller 必须退避，实际轮询 " + peeks.get() + " 次");
      assertTrue(handler.severe.get() <= 5,
          "持续错误的 SEVERE 日志必须限流，实际 " + handler.severe.get() + " 条");
      assertTrue(handler.severe.get() >= 1, "首个错误必须记录");
    } finally {
      registry.stopPolling();
      registry.shutdown();
    }
  }

  private static final class CountingHandler extends Handler {
    final AtomicInteger severe = new AtomicInteger();

    @Override
    public void publish(LogRecord record) {
      if (record.getLevel().intValue() >= Level.SEVERE.intValue()) {
        severe.incrementAndGet();
      }
    }

    @Override
    public void flush() { }

    @Override
    public void close() { }
  }
}
