package aster.truffle.nodes.parallel;

import aster.truffle.nodes.LambdaValue;
import aster.truffle.nodes.Profiler;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.nodes.Node;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * List.map 并行执行节点，使用 ForkJoinPool.commonPool() 调度任务。
 *
 * <p>设计要点：
 * <ul>
 *   <li>保持输入输出顺序一致；</li>
 *   <li>仅在大列表上触发并行，避免小任务分裂开销；</li>
 *   <li>内部复用 CallTarget.call()，无需额外 DSL 节点；</li>
 *   <li>线程安全：每个任务使用独立参数数组。</li>
 * </ul>
 */
public final class ParallelListMapNode extends Node {
  private static final ForkJoinPool POOL = ForkJoinPool.commonPool();
  private static final int MIN_PARALLEL_SIZE = 256;
  private static final int CHUNK_SIZE = 128;

  public static ParallelListMapNode create() {
    return new ParallelListMapNode();
  }

  /**
   * 判断给定列表是否值得并行化。
   */
  public boolean shouldParallelize(int size) {
    if (size < MIN_PARALLEL_SIZE) {
      return false;
    }
    // 当 commonPool 仍处于单线程配置时跳过并行路径
    return POOL.getParallelism() > 1;
  }

  /**
   * 并行执行 map 操作。
   *
   * @param source 待映射的列表
   * @param lambda 纯函数 lambda
   * @return 映射结果（保持顺序）
   */
  public List<Object> execute(List<?> source, LambdaValue lambda) {
    if (source == null || source.isEmpty()) {
      return new ArrayList<>();
    }
    CallTarget callTarget = lambda.getCallTarget();
    if (callTarget == null) {
      throw new IllegalStateException("ParallelListMapNode: lambda has no CallTarget");
    }
    Object[] capturedValues = lambda.getCapturedValues();
    int size = source.size();

    if (!shouldParallelize(size)) {
      return executeSequential(source, callTarget, capturedValues);
    }

    Profiler.inc("builtin_list_map_parallel");
    Object[] results = new Object[size];
    MapTask root = new MapTask(source, results, 0, size, callTarget, capturedValues);
    POOL.invoke(root);

    List<Object> output = new ArrayList<>(size);
    for (Object value : results) {
      output.add(value);
    }
    return output;
  }

  private List<Object> executeSequential(List<?> source, CallTarget target, Object[] capturedValues) {
    List<Object> result = new ArrayList<>(source.size());
    Object[] packedArgs = new Object[1 + capturedValues.length];
    if (capturedValues.length > 0) {
      System.arraycopy(capturedValues, 0, packedArgs, 1, capturedValues.length);
    }
    for (Object item : source) {
      packedArgs[0] = item;
      result.add(target.call(packedArgs));
    }
    return result;
  }

  private static final class MapTask extends RecursiveAction {
    private final List<?> source;
    private final Object[] result;
    private final int start;
    private final int end;
    private final CallTarget target;
    private final Object[] capturedValues;

    private MapTask(
        List<?> source,
        Object[] result,
        int start,
        int end,
        CallTarget target,
        Object[] capturedValues) {
      this.source = source;
      this.result = result;
      this.start = start;
      this.end = end;
      this.target = target;
      this.capturedValues = capturedValues;
    }

    @Override
    protected void compute() {
      int length = end - start;
      if (length <= CHUNK_SIZE) {
        applySequential();
        return;
      }
      int mid = start + (length / 2);
      MapTask left = new MapTask(source, result, start, mid, target, capturedValues);
      MapTask right = new MapTask(source, result, mid, end, target, capturedValues);
      right.fork();
      left.compute();
      right.join();
    }

    private void applySequential() {
      Object[] packedArgs = new Object[1 + capturedValues.length];
      if (capturedValues.length > 0) {
        System.arraycopy(capturedValues, 0, packedArgs, 1, capturedValues.length);
      }
      for (int i = start; i < end; i++) {
        packedArgs[0] = source.get(i);
        result[i] = target.call(packedArgs);
      }
    }
  }
}
