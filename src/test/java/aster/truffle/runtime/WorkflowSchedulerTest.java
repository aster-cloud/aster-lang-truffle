package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * WorkflowScheduler 单元测试（Phase 2.5）
 *
 * 覆盖要点：
 * 1. executeUntilComplete 应正确驱动 AsyncTaskRegistry
 * 2. RuntimeException 直接透传，非 RuntimeException 包装为 Workflow execution failed
 * 3. executeNext 仍可单步执行（兼容 start/await 语义）
 * 4. 基础构造与存取器行为
 */
public class WorkflowSchedulerTest {

  private AsyncTaskRegistry registry;
  private WorkflowScheduler scheduler;

  @BeforeEach
  public void setUp() {
    registry = new AsyncTaskRegistry();
    scheduler = new WorkflowScheduler(registry);
  }

  private void registerTask(String taskId, Runnable taskBody, Set<String> deps) {
    Set<String> normalized = deps == null ? Collections.emptySet() : deps;
    registry.registerTaskWithDependencies(taskId, () -> {
      taskBody.run();
      return null;
    }, normalized);
  }

  @Test
  public void testExecuteUntilCompleteRunsAllTasks() {
    AtomicInteger order = new AtomicInteger(0);
    List<Integer> observed = new ArrayList<>();

    registerTask("A", () -> {
      observed.add(order.incrementAndGet());
      registry.setResult("A", "alpha");
    }, Collections.emptySet());

    registerTask("B", () -> {
      observed.add(order.incrementAndGet());
      registry.setResult("B", "beta");
    }, Set.of("A"));

    registerTask("C", () -> {
      observed.add(order.incrementAndGet());
      registry.setResult("C", "gamma");
    }, Set.of("B"));

    scheduler.executeUntilComplete();

    assertEquals(List.of(1, 2, 3), observed, "执行顺序应符合拓扑排序");
    assertEquals("alpha", registry.getResult("A"));
    assertEquals("beta", registry.getResult("B"));
    assertEquals("gamma", registry.getResult("C"));
  }

  @Test
  public void testExecuteUntilCompleteWrapsFailure() {
    registerTask("A", () -> registry.setResult("A", "ok"), Collections.emptySet());
    registerTask("B", () -> {
      throw new RuntimeException("boom");
    }, Set.of("A"));
    registerTask("C", () -> registry.setResult("C", "skip"), Set.of("B"));

    // RuntimeException 直接透传，保留原始异常类型和消息
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> scheduler.executeUntilComplete());
    assertTrue(thrown.getMessage().contains("boom"), "应保留原始异常消息");

    assertTrue(registry.isCompleted("A"));
    assertTrue(registry.isFailed("B"));
    assertTrue(registry.isCancelled("C"));
  }

  @Test
  public void testExecuteNextDelegatesToRegistry() {
    registerTask("single", () -> registry.setResult("single", "done"), Collections.emptySet());
    assertDoesNotThrow(() -> scheduler.executeNext());
    assertTrue(registry.isCompleted("single"));
  }

  @Test
  public void testConstructorRejectsNullRegistry() {
    assertThrows(IllegalArgumentException.class, () -> new WorkflowScheduler(null));
  }

  @Test
  public void testGetRegistry() {
    assertSame(registry, scheduler.getRegistry());
  }

  /**
   * 测试 DAG 并发执行：init -> {fanout_a, fanout_b} -> merge
   * 验证 fanout_a 和 fanout_b 可以并发执行
   */
  @Test
  public void testConcurrentDAGExecution() {
    AtomicInteger concurrentCount = new AtomicInteger(0);
    AtomicInteger maxConcurrent = new AtomicInteger(0);
    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

    registerTask("init", () -> {
      executionOrder.add("init");
      registry.setResult("init", "initialized");
    }, Collections.emptySet());

    registerTask("fanout_a", () -> {
      executionOrder.add("fanout_a-start");
      int current = concurrentCount.incrementAndGet();
      maxConcurrent.updateAndGet(max -> Math.max(max, current));
      try {
        Thread.sleep(50); // 模拟并发窗口
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      executionOrder.add("fanout_a-end");
      concurrentCount.decrementAndGet();
      registry.setResult("fanout_a", "result_a");
    }, Set.of("init"));

    registerTask("fanout_b", () -> {
      executionOrder.add("fanout_b-start");
      int current = concurrentCount.incrementAndGet();
      maxConcurrent.updateAndGet(max -> Math.max(max, current));
      try {
        Thread.sleep(50); // 模拟并发窗口
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      executionOrder.add("fanout_b-end");
      concurrentCount.decrementAndGet();
      registry.setResult("fanout_b", "result_b");
    }, Set.of("init"));

    registerTask("merge", () -> {
      executionOrder.add("merge");
      registry.setResult("merge", "merged");
    }, Set.of("fanout_a", "fanout_b"));

    scheduler.executeUntilComplete();

    // 验证所有任务完成
    assertTrue(registry.isCompleted("init"));
    assertTrue(registry.isCompleted("fanout_a"));
    assertTrue(registry.isCompleted("fanout_b"));
    assertTrue(registry.isCompleted("merge"));

    // 验证并发执行：maxConcurrent 应该 >= 2（fanout_a 和 fanout_b 同时执行）
    assertTrue(maxConcurrent.get() >= 2, "fanout_a 和 fanout_b 应该并发执行，maxConcurrent=" + maxConcurrent.get());

    // 验证执行顺序：init 必须在 fanout 之前，merge 必须在所有 fanout 之后
    int initIndex = executionOrder.indexOf("init");
    int fanoutAStartIndex = executionOrder.indexOf("fanout_a-start");
    int fanoutBStartIndex = executionOrder.indexOf("fanout_b-start");
    int mergeIndex = executionOrder.indexOf("merge");

    assertTrue(initIndex < fanoutAStartIndex, "init 应在 fanout_a 之前");
    assertTrue(initIndex < fanoutBStartIndex, "init 应在 fanout_b 之前");
    assertTrue(fanoutAStartIndex < mergeIndex, "fanout_a 应在 merge 之前");
    assertTrue(fanoutBStartIndex < mergeIndex, "fanout_b 应在 merge 之前");
  }

  /**
   * 测试向后兼容：无 depends on 时仍保持串行执行
   */
  @Test
  public void testLinearWorkflowBackwardCompatibility() {
    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

    registerTask("step1", () -> {
      executionOrder.add("step1");
      registry.setResult("step1", "result1");
    }, Collections.emptySet());

    registerTask("step2", () -> {
      executionOrder.add("step2");
      registry.setResult("step2", "result2");
    }, Set.of("step1")); // 自动依赖前一个步骤

    registerTask("step3", () -> {
      executionOrder.add("step3");
      registry.setResult("step3", "result3");
    }, Set.of("step2")); // 自动依赖前一个步骤

    scheduler.executeUntilComplete();

    // 验证严格串行顺序
    assertEquals(List.of("step1", "step2", "step3"), executionOrder, "应保持串行执行顺序");
    assertTrue(registry.isCompleted("step1"));
    assertTrue(registry.isCompleted("step2"));
    assertTrue(registry.isCompleted("step3"));
  }

  /**
   * 测试 Diamond DAG：init -> {left, right} -> merge
   * 验证 Diamond 模式的正确性
   */
  @Test
  public void testDiamondDAGPattern() {
    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

    registerTask("init", () -> {
      executionOrder.add("init");
      registry.setResult("init", "start");
    }, Collections.emptySet());

    registerTask("left", () -> {
      executionOrder.add("left");
      registry.setResult("left", "left_result");
    }, Set.of("init"));

    registerTask("right", () -> {
      executionOrder.add("right");
      registry.setResult("right", "right_result");
    }, Set.of("init"));

    registerTask("merge", () -> {
      executionOrder.add("merge");
      // merge 应该能访问 left 和 right 的结果
      Object leftResult = registry.getResult("left");
      Object rightResult = registry.getResult("right");
      registry.setResult("merge", leftResult + "+" + rightResult);
    }, Set.of("left", "right"));

    scheduler.executeUntilComplete();

    assertTrue(registry.isCompleted("init"));
    assertTrue(registry.isCompleted("left"));
    assertTrue(registry.isCompleted("right"));
    assertTrue(registry.isCompleted("merge"));

    assertEquals("left_result+right_result", registry.getResult("merge"));

    // 验证拓扑顺序
    int initIndex = executionOrder.indexOf("init");
    int leftIndex = executionOrder.indexOf("left");
    int rightIndex = executionOrder.indexOf("right");
    int mergeIndex = executionOrder.indexOf("merge");

    assertTrue(initIndex < leftIndex);
    assertTrue(initIndex < rightIndex);
    assertTrue(leftIndex < mergeIndex);
    assertTrue(rightIndex < mergeIndex);
  }
}
