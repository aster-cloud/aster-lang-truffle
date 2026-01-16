package aster.truffle.runtime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DependencyGraph 单元测试（Phase 2.0）
 *
 * 测试目标：
 * 1. 线性链与菱形依赖的构建与就绪计算
 * 2. 循环依赖检测
 * 3. markCompleted 对依赖计数与就绪队列的影响
 * 4. 就绪任务查询正确性
 * 5. 100 任务性能基线（<10ms）
 */
public class DependencyGraphTest {

  private DependencyGraph graph;

  @BeforeEach
  public void setUp() {
    graph = new DependencyGraph();
  }

  /**
   * 测试：线性依赖链 A→B→C 的构建和就绪队列演进。
   */
  @Test
  public void testLinearChain() {
    graph.addTask("A", Set.of(), 0);
    graph.addTask("B", Set.of("A"), 0);
    graph.addTask("C", Set.of("B"), 0);

    assertEquals(3, graph.getTaskCount(), "应成功注册三个节点");
    assertEquals(1, graph.getReadyCount(), "初始仅有根节点就绪");
    assertEquals(List.of("A"), graph.getReadyTasks(), "首个就绪任务应为 A");

    graph.markCompleted("A");
    assertEquals(List.of("B"), graph.getReadyTasks(), "完成 A 后 B 就绪");
    assertFalse(graph.allCompleted(), "仍有未完成任务");

    graph.markCompleted("B");
    assertEquals(List.of("C"), graph.getReadyTasks(), "完成 B 后 C 就绪");

    graph.markCompleted("C");
    assertTrue(graph.getReadyTasks().isEmpty(), "所有任务完成后就绪队列应为空");
    assertTrue(graph.allCompleted(), "所有任务应完成");
  }

  /**
   * 测试：菱形依赖 A→B,C→D，验证就绪任务和反向依赖查询。
   */
  @Test
  public void testDiamondDependency() {
    graph.addTask("A", Set.of(), 0);
    graph.addTask("B", Set.of("A"), 0);
    graph.addTask("C", Set.of("A"), 0);
    graph.addTask("D", Set.of("B", "C"), 0);

    assertEquals(List.of("A"), graph.getReadyTasks(), "初始仅 A 可执行");

    graph.markCompleted("A");
    List<String> readyAfterA = graph.getReadyTasks();
    assertEquals(2, readyAfterA.size(), "A 完成后 B/C 同时就绪");
    assertTrue(readyAfterA.containsAll(Set.of("B", "C")));

    Set<String> dependents = graph.getDependents("A");
    assertEquals(Set.of("B", "C"), dependents, "反向依赖应列出 B 与 C");

    graph.markCompleted("B");
    assertEquals(List.of("C"), graph.getReadyTasks(), "B 完成后仅剩 C 就绪");

    graph.markCompleted("C");
    assertEquals(List.of("D"), graph.getReadyTasks(), "C 完成后 D 才能就绪");

    graph.markCompleted("D");
    assertTrue(graph.allCompleted(), "菱形依赖全部完成");
  }

  /**
   * 测试：循环依赖检测，包含互相依赖与自依赖两种情况。
   */
  @Test
  public void testCycleDetection() {
    graph.addTask("A", Set.of("B"), 0);  // 先注册 A，允许引用尚未存在的 B

    IllegalArgumentException mutualCycle = assertThrows(IllegalArgumentException.class, () -> {
      graph.addTask("B", Set.of("A"), 0);  // 注册 B 时应检测到 A↔B 循环
    });
    assertTrue(mutualCycle.getMessage().contains("Circular"), "应提示循环依赖");

    DependencyGraph fresh = new DependencyGraph();
    IllegalArgumentException selfCycle = assertThrows(IllegalArgumentException.class, () -> {
      fresh.addTask("Self", Set.of("Self"), 0);
    });
    assertTrue(selfCycle.getMessage().contains("Circular"), "自引用也应被阻止");
  }

  /**
   * 测试：markCompleted 正确更新依赖计数并推动下游任务。
   */
  @Test
  public void testMarkCompleted() {
    graph.addTask("root", Set.of(), 0);
    graph.addTask("left", Set.of("root"), 0);
    graph.addTask("right", Set.of("root"), 0);
    graph.addTask("leaf", Set.of("left", "right"), 0);

    assertEquals(List.of("root"), graph.getReadyTasks(), "开始仅 root 可执行");

    graph.markCompleted("root");
    List<String> ready = graph.getReadyTasks();
    assertEquals(2, ready.size(), "root 完成后 left/right 并行就绪");
    assertTrue(ready.containsAll(Set.of("left", "right")));

    graph.markCompleted("left");
    assertEquals(List.of("right"), graph.getReadyTasks(), "仅剩 right 未执行");

    graph.markCompleted("right");
    assertEquals(List.of("leaf"), graph.getReadyTasks(), "两条分支完成后 leaf 才能执行");
  }

  /**
   * 测试：就绪任务查询在混合依赖图中的表现，确保插入顺序与依赖满足情况解耦。
   */
  @Test
  public void testGetReadyTasks() {
    graph.addTask("A", null, 0);                 // null 依赖应视为空
    graph.addTask("B", Set.of(), 0);
    graph.addTask("C", Set.of("A"), 0);
    graph.addTask("D", Set.of("A", "B"), 0);
    graph.addTask("E", Set.of("D"), 0);

    List<String> initialReady = graph.getReadyTasks();
    assertEquals(2, initialReady.size(), "A/B 同时就绪");
    assertTrue(initialReady.containsAll(Set.of("A", "B")));

    graph.markCompleted("B");
    List<String> readyAfterB = graph.getReadyTasks();
    assertTrue(readyAfterB.contains("A"), "B 完成不应影响 A 的就绪状态");

    graph.markCompleted("A");
    List<String> readyAfterA = graph.getReadyTasks();
    assertEquals(2, readyAfterA.size(), "A 完成后 C 与 D 同时解锁");
    assertTrue(readyAfterA.containsAll(Set.of("C", "D")));
  }

  /**
   * 测试：100 任务线性链构建性能应低于 10ms。
   */
  @Test
  public void testPerformance() {
    long startNs = System.nanoTime();
    graph.addTask("task-0", Set.of(), 0);
    for (int i = 1; i < 100; i++) {
      graph.addTask("task-" + i, Set.of("task-" + (i - 1)), 0);
    }
    long durationMs = (System.nanoTime() - startNs) / 1_000_000;

    assertTrue(durationMs < 50, "100 节点构建需 <50ms，实际为 " + durationMs + "ms");
    assertEquals(100, graph.getTaskCount(), "应注册 100 个节点");
    assertEquals(1, graph.getReadyCount(), "线性链起点唯一就绪");
  }
}
