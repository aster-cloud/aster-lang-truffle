package aster.truffle;

import aster.truffle.nodes.LambdaValue;
import aster.truffle.nodes.parallel.ParallelListMapNode;
import com.oracle.truffle.api.CallTarget;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并行执行基准：验证纯函数 List.map 在大列表上真正分摊到多个工作线程。
 * <p>
 * 加速的前提是至少两个工作线程同时执行 lambda。这里用会合门闩直接证明并发，
 * 而不比较顺序与并行的墙钟耗时：后者在满负载机器上会抖动，造成与代码无关的失败。
 */
public final class ParallelExecutionBenchmark {
  private static final int DATA_SIZE = 8192;
  // 会合等待上限：只在并行路径失效时才会耗尽，正常情况下第二个线程毫秒级到达
  private static final long RENDEZVOUS_TIMEOUT_SECONDS = 30;

  @Test
  public void pureListMapShouldBenefitFromParallelExecution() {
    // 环境若只有 1 个工作线程，无法并行，直接跳过（避免在 CI 单核容器上失败）
    Assumptions.assumeTrue(
        ForkJoinPool.commonPool().getParallelism() > 1,
        "commonPool parallelism <= 1，跳过并行对比");

    List<Object> input = buildInput(DATA_SIZE);
    ParallelListMapNode parallelNode = ParallelListMapNode.create();

    Assumptions.assumeTrue(
        parallelNode.shouldParallelize(input.size()),
        "列表规模不足以触发并行路径");

    LambdaValue lambda = buildCpuIntensiveLambda();
    List<Object> sequentialResult = runSequential(input, lambda);
    List<Object> parallelResult = parallelNode.execute(input, lambda);
    assertEquals(sequentialResult, parallelResult, "并行路径必须保持与顺序执行一致的输出");

    RendezvousTarget rendezvous = new RendezvousTarget();
    List<Object> echoed = parallelNode.execute(input, new LambdaValue(List.of("x"), Map.of(), rendezvous));
    assertEquals(input, echoed, "会合 lambda 原样返回入参，输出必须与输入逐项一致");

    System.out.printf("ParallelExecutionBenchmark - distinct worker threads: %d%n", rendezvous.workers.size());
    assertFalse(rendezvous.timedOut.get(),
        "并行路径应让至少两个工作线程同时执行 lambda，但第一个线程等满 "
            + RENDEZVOUS_TIMEOUT_SECONDS + " 秒也没有等到第二个线程");
    assertTrue(rendezvous.workers.size() >= 2,
        "并行路径应使用多个工作线程，实际只有 " + rendezvous.workers.size() + " 个");
  }

  /**
   * 每个线程首次进入时登记并在门闩上等待第二个线程到达；门闩归零即证明两个线程同时在执行 lambda。
   */
  private static final class RendezvousTarget implements CallTarget {
    private final CountDownLatch bothArrived = new CountDownLatch(2);
    private final Set<Thread> workers = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean timedOut = new AtomicBoolean(false);

    @Override
    public Object call(Object... args) {
      if (workers.add(Thread.currentThread())) {
        bothArrived.countDown();
        awaitOtherWorker();
      }
      return args[0];
    }

    private void awaitOtherWorker() {
      try {
        if (!bothArrived.await(RENDEZVOUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          timedOut.set(true);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        timedOut.set(true);
      }
    }
  }

  private List<Object> buildInput(int size) {
    List<Object> data = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      data.add(i);
    }
    return data;
  }

  private LambdaValue buildCpuIntensiveLambda() {
    CallTarget callTarget = new CallTarget() {
      @Override
      public Object call(Object... args) {
        int base = ((Number) args[0]).intValue();
        double acc = base;
        for (int i = 0; i < 600; i++) {
          acc = Math.sin(acc) + Math.cos(acc);
        }
        return (int) Math.round(Math.abs(acc));
      }
    };
    return new LambdaValue(List.of("x"), Map.of(), callTarget);
  }

  private List<Object> runSequential(List<?> input, LambdaValue lambda) {
    CallTarget callTarget = lambda.getCallTarget();
    Object[] capturedValues = lambda.getCapturedValues();
    List<Object> output = new ArrayList<>(input.size());
    Object[] packed = new Object[1 + capturedValues.length];
    if (capturedValues.length > 0) {
      System.arraycopy(capturedValues, 0, packed, 1, capturedValues.length);
    }
    for (Object v : input) {
      packed[0] = v;
      output.add(callTarget.call(packed));
    }
    return output;
  }
}
