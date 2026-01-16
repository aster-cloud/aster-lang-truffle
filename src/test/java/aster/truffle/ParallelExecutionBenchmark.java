package aster.truffle;

import aster.truffle.nodes.LambdaValue;
import aster.truffle.nodes.parallel.ParallelListMapNode;
import com.oracle.truffle.api.CallTarget;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并行执行基准：验证纯函数 List.map 在大列表上能够获得可观加速。
 */
public final class ParallelExecutionBenchmark {
  private static final int DATA_SIZE = 8192;
  private static final int MEASURE_ITERATIONS = 5;
  private static volatile Object BLACKHOLE;

  @Test
  public void pureListMapShouldBenefitFromParallelExecution() {
    // 环境若只有 1 个工作线程，直接忽略性能对比（避免在 CI 单核容器上失败）
    Assumptions.assumeTrue(
        ForkJoinPool.commonPool().getParallelism() > 1,
        "commonPool parallelism <= 1，跳过并行对比");

    List<Object> input = buildInput(DATA_SIZE);
    LambdaValue lambda = buildCpuIntensiveLambda();
    ParallelListMapNode parallelNode = ParallelListMapNode.create();

    Assumptions.assumeTrue(
        parallelNode.shouldParallelize(input.size()),
        "列表规模不足以触发并行路径");

    List<Object> sequentialResult = runSequential(input, lambda);
    List<Object> parallelResult = parallelNode.execute(input, lambda);
    assertEquals(sequentialResult, parallelResult, "并行路径必须保持与顺序执行一致的输出");

    // 额外预热，避免首次调用干扰
    for (int i = 0; i < 3; i++) {
      BLACKHOLE = runSequential(input, lambda);
      BLACKHOLE = parallelNode.execute(input, lambda);
    }

    double sequentialMs = measureMs(() -> runSequential(input, lambda));
    double parallelMs = measureMs(() -> parallelNode.execute(input, lambda));

    System.out.printf(
        "ParallelExecutionBenchmark - sequential: %.3f ms, parallel: %.3f ms%n",
        sequentialMs,
        parallelMs);

    assertTrue(
        parallelMs < sequentialMs,
        String.format("并行路径应快于顺序执行 (sequential=%.3fms, parallel=%.3fms)", sequentialMs, parallelMs));
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

  private double measureMs(Supplier<List<Object>> action) {
    long totalNs = 0L;
    for (int i = 0; i < MEASURE_ITERATIONS; i++) {
      long start = System.nanoTime();
      BLACKHOLE = action.get();
      long end = System.nanoTime();
      totalNs += (end - start);
    }
    return (totalNs / (double) MEASURE_ITERATIONS) / 1_000_000.0;
  }
}
