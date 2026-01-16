package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aster.workflow.DeterminismContext;
import java.lang.reflect.Method;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Backoff 计算逻辑单元测试
 *
 * 测试 AsyncTaskRegistry.calculateBackoff 的正确性，包括：
 * - 指数退避公式（2^n）
 * - 线性退避公式（n*base）
 * - Jitter 范围验证
 * - 确定性 jitter（相同种子产生相同结果）
 */
public class BackoffCalculatorTest {

  private AsyncTaskRegistry registry;
  private Method calculateBackoffMethod;

  @BeforeEach
  public void setUp() throws Exception {
    registry = new AsyncTaskRegistry();
    calculateBackoffMethod = AsyncTaskRegistry.class.getDeclaredMethod(
        "calculateBackoff", int.class, String.class, long.class, DeterminismContext.class);
    calculateBackoffMethod.setAccessible(true);
  }

  @Test
  public void testExponentialBackoff() throws Exception {
    DeterminismContext ctx = new DeterminismContext();
    long baseDelayMs = 1000L;

    // 测试第1次重试：2^0 = 1
    long delay1 = (long) calculateBackoffMethod.invoke(registry, 1, "exponential", baseDelayMs, ctx);
    long expectedBase1 = baseDelayMs * 1; // 2^(1-1) = 2^0 = 1
    assertTrue(delay1 >= expectedBase1 && delay1 < expectedBase1 + baseDelayMs / 2,
        String.format("attempt=1: delay=%d should be in [%d, %d)", delay1, expectedBase1, expectedBase1 + baseDelayMs / 2));

    // 测试第2次重试：2^1 = 2
    ctx = new DeterminismContext();
    long delay2 = (long) calculateBackoffMethod.invoke(registry, 2, "exponential", baseDelayMs, ctx);
    long expectedBase2 = baseDelayMs * 2; // 2^(2-1) = 2^1 = 2
    assertTrue(delay2 >= expectedBase2 && delay2 < expectedBase2 + baseDelayMs / 2,
        String.format("attempt=2: delay=%d should be in [%d, %d)", delay2, expectedBase2, expectedBase2 + baseDelayMs / 2));

    // 测试第3次重试：2^2 = 4
    ctx = new DeterminismContext();
    long delay3 = (long) calculateBackoffMethod.invoke(registry, 3, "exponential", baseDelayMs, ctx);
    long expectedBase3 = baseDelayMs * 4; // 2^(3-1) = 2^2 = 4
    assertTrue(delay3 >= expectedBase3 && delay3 < expectedBase3 + baseDelayMs / 2,
        String.format("attempt=3: delay=%d should be in [%d, %d)", delay3, expectedBase3, expectedBase3 + baseDelayMs / 2));
  }

  @Test
  public void testLinearBackoff() throws Exception {
    DeterminismContext ctx = new DeterminismContext();
    long baseDelayMs = 500L;

    // 测试第1次重试：1 * base
    long delay1 = (long) calculateBackoffMethod.invoke(registry, 1, "linear", baseDelayMs, ctx);
    long expectedBase1 = baseDelayMs * 1;
    assertTrue(delay1 >= expectedBase1 && delay1 < expectedBase1 + baseDelayMs / 2,
        String.format("attempt=1: delay=%d should be in [%d, %d)", delay1, expectedBase1, expectedBase1 + baseDelayMs / 2));

    // 测试第2次重试：2 * base
    ctx = new DeterminismContext();
    long delay2 = (long) calculateBackoffMethod.invoke(registry, 2, "linear", baseDelayMs, ctx);
    long expectedBase2 = baseDelayMs * 2;
    assertTrue(delay2 >= expectedBase2 && delay2 < expectedBase2 + baseDelayMs / 2,
        String.format("attempt=2: delay=%d should be in [%d, %d)", delay2, expectedBase2, expectedBase2 + baseDelayMs / 2));

    // 测试第3次重试：3 * base
    ctx = new DeterminismContext();
    long delay3 = (long) calculateBackoffMethod.invoke(registry, 3, "linear", baseDelayMs, ctx);
    long expectedBase3 = baseDelayMs * 3;
    assertTrue(delay3 >= expectedBase3 && delay3 < expectedBase3 + baseDelayMs / 2,
        String.format("attempt=3: delay=%d should be in [%d, %d)", delay3, expectedBase3, expectedBase3 + baseDelayMs / 2));
  }

  @Test
  public void testJitterRange() throws Exception {
    long baseDelayMs = 2000L;
    long maxJitter = baseDelayMs / 2; // 1000ms

    // 测试 jitter 始终在 [0, base/2) 范围内
    for (int attempt = 1; attempt <= 5; attempt++) {
      DeterminismContext ctx = new DeterminismContext();
      long delay = (long) calculateBackoffMethod.invoke(registry, attempt, "exponential", baseDelayMs, ctx);
      long expectedBase = (long) (baseDelayMs * Math.pow(2, attempt - 1));
      long jitter = delay - expectedBase;

      assertTrue(jitter >= 0 && jitter < maxJitter,
          String.format("attempt=%d: jitter=%d should be in [0, %d)", attempt, jitter, maxJitter));
    }
  }

  @Test
  public void testDeterministicJitter() throws Exception {
    long baseDelayMs = 1000L;
    int attempt = 2;

    // 使用记录-重放机制测试确定性
    DeterminismContext ctx1 = new DeterminismContext();
    long delay1 = (long) calculateBackoffMethod.invoke(registry, attempt, "exponential", baseDelayMs, ctx1);

    // 进入重放模式，使用记录的随机值
    DeterminismContext ctx2 = new DeterminismContext();
    ctx2.random().enterReplayMode(ctx1.random().getRecordedRandoms());
    long delay2 = (long) calculateBackoffMethod.invoke(registry, attempt, "exponential", baseDelayMs, ctx2);

    assertEquals(delay1, delay2, "重放模式应产生相同的 backoff 延迟（确定性 jitter）");
  }

  @Test
  public void testZeroBaseDelay() throws Exception {
    DeterminismContext ctx = new DeterminismContext();

    // baseDelayMs = 0 时，jitter 也应该为 0
    long delay = (long) calculateBackoffMethod.invoke(registry, 1, "linear", 0L, ctx);
    assertEquals(0L, delay, "baseDelayMs=0 时延迟应为 0");
  }

  @Test
  public void testNullDeterminismContext() throws Exception {
    // calculateBackoff 应该使用 registry 的默认 determinismContext
    long delay1 = (long) calculateBackoffMethod.invoke(registry, 1, "linear", 1000L, null);
    long delay2 = (long) calculateBackoffMethod.invoke(registry, 1, "linear", 1000L, null);

    // 两次调用可能使用不同的随机数，但应该都在合法范围内
    assertTrue(delay1 >= 1000L && delay1 < 1500L);
    assertTrue(delay2 >= 1000L && delay2 < 1500L);
  }
}
