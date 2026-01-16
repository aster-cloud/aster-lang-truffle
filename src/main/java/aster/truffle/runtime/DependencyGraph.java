package aster.truffle.runtime;

import java.util.*;

/**
 * 依赖图管理器 - 管理任务之间的依赖关系和调度顺序
 *
 * Phase 2.0 实现：依赖图构建和拓扑排序
 * - 支持任务依赖声明和注册
 * - 循环依赖检测（DFS 算法）
 * - 就绪任务查询（O(1) 性能）
 * - 依赖计数优化（避免重复遍历）
 */
public final class DependencyGraph {
  // 任务节点存储：task_id -> TaskNode
  private final Map<String, TaskNode> nodes = new HashMap<>();

  // 就绪任务队列（依赖已满足的任务），按优先级升序（数字越小优先级越高）
  private final PriorityQueue<PrioritizedTask> readyQueue =
      new PriorityQueue<>(Comparator.comparingInt(t -> t.priority));
// 已完成（或被取消）任务集：支持先完成后注册的依赖
  private final Set<String> completedNodes = new HashSet<>();

  private static final class PrioritizedTask {
    final String taskId;
    final int priority;

    PrioritizedTask(String taskId, int priority) {
      this.taskId = taskId;
      this.priority = priority;
    }
  }

  /**
   * 任务节点 - 封装任务的依赖关系和状态
   */
  static class TaskNode {
    final String taskId;
    final Set<String> dependencies;  // 依赖的任务 ID 集合
    int remainingDeps;                // 剩余未完成的依赖数量
    final int priority;

    TaskNode(String taskId, Set<String> dependencies, int priority) {
      this.taskId = taskId;
      this.dependencies = new HashSet<>(dependencies);
      this.remainingDeps = dependencies.size();
      this.priority = priority;
    }
  }

  /**
   * 添加任务到依赖图
   *
   * @param taskId 任务唯一标识符
   * @param dependencies 依赖的任务 ID 集合（可为空）
   * @param priority 优先级（数值越小优先级越高）
   * @throws IllegalArgumentException 如果检测到循环依赖
   */
  public void addTask(String taskId, Set<String> dependencies, int priority) {
    if (taskId == null) {
      throw new IllegalArgumentException("taskId cannot be null");
    }

    if (nodes.containsKey(taskId)) {
      throw new IllegalArgumentException("Task already exists: " + taskId);
    }

    Set<String> deps = (dependencies == null) ? Collections.emptySet() : dependencies;

    // 循环依赖检测
    if (hasCycle(taskId, deps)) {
      throw new IllegalArgumentException("Circular dependency detected for task: " + taskId);
    }

    // 创建任务节点
    TaskNode node = new TaskNode(taskId, deps, priority);
    nodes.put(taskId, node);

    if (!deps.isEmpty()) {
      for (String dep : deps) {
        if (completedNodes.contains(dep) && node.remainingDeps > 0) {
          node.remainingDeps--;
        }
      }
    }

    if (node.remainingDeps == 0) {
      readyQueue.offer(new PrioritizedTask(taskId, node.priority));
    }
  }

  /**
   * 标记任务已完成，更新依赖计数
   *
   * @param taskId 已完成的任务 ID
   */
  public void markCompleted(String taskId) {
    if (taskId == null) {
      return;
    }

    if (!completedNodes.add(taskId)) {
      readyQueue.removeIf(t -> t.taskId.equals(taskId));
      return;
    }

    // 从就绪队列移除
    readyQueue.removeIf(t -> t.taskId.equals(taskId));

    // 遍历所有节点，减少依赖此任务的节点的计数
    for (TaskNode node : nodes.values()) {
      if (node.remainingDeps > 0 && node.dependencies.contains(taskId)) {
        node.remainingDeps--;

        // 依赖全部满足，加入就绪队列
        if (node.remainingDeps == 0) {
          readyQueue.offer(new PrioritizedTask(node.taskId, node.priority));
        }
      }
    }
  }

  /**
   * 获取所有就绪任务（依赖已满足），按优先级排序
   *
   * 注意：此方法是非破坏性的，不会从就绪队列中移除任务。
   * 任务在 markCompleted() 时会从队列中移除。
   * 返回的列表按优先级升序排列（数字越小优先级越高）。
   *
   * @return 就绪任务 ID 列表（按优先级排序）
   */
  public List<String> getReadyTasks() {
    // 复制队列内容并按优先级排序，避免依赖 PriorityQueue 未定义的迭代顺序
    List<PrioritizedTask> sorted = new ArrayList<>(readyQueue);
    sorted.sort(Comparator.comparingInt(t -> t.priority));
    List<String> ready = new ArrayList<>(sorted.size());
    for (PrioritizedTask task : sorted) {
      ready.add(task.taskId);
    }
    return ready;
  }

  /**
   * 检查所有任务是否已完成
   *
   * @return true 如果所有任务的依赖都已满足且就绪队列为空
   */
  public boolean allCompleted() {
    // 所有任务的 remainingDeps 都为 0，且就绪队列为空
    return readyQueue.isEmpty() &&
           nodes.values().stream().allMatch(node -> node.remainingDeps == 0);
  }

  /**
   * 获取依赖指定任务的所有后续任务
   *
   * @param taskId 任务 ID
   * @return 依赖此任务的后续任务 ID 集合
   */
  public Set<String> getDependents(String taskId) {
    Set<String> dependents = new HashSet<>();
    for (TaskNode node : nodes.values()) {
      if (node.dependencies.contains(taskId)) {
        dependents.add(node.taskId);
      }
    }
    return dependents;
  }

  /**
   * 循环依赖检测 - 使用 DFS 算法
   *
   * @param newTaskId 新任务 ID
   * @param newDeps 新任务的依赖集合
   * @return true 如果检测到循环依赖
   */
  private boolean hasCycle(String newTaskId, Set<String> newDeps) {
    // 构建临时图：包含现有节点 + 新节点
    Map<String, Set<String>> tempGraph = new HashMap<>();

    // 添加现有节点的依赖关系
    for (TaskNode node : nodes.values()) {
      tempGraph.put(node.taskId, new HashSet<>(node.dependencies));
    }

    // 添加新节点
    tempGraph.put(newTaskId, new HashSet<>(newDeps));

    // DFS 循环检测
    Set<String> visited = new HashSet<>();
    Set<String> recursionStack = new HashSet<>();

    // 从新节点开始 DFS
    return hasCycleDFS(newTaskId, tempGraph, visited, recursionStack);
  }

  /**
   * DFS 循环检测辅助方法
   *
   * @param taskId 当前访问的任务 ID
   * @param graph 依赖图
   * @param visited 已访问节点集合
   * @param recursionStack 递归栈（检测回边）
   * @return true 如果检测到循环
   */
  private boolean hasCycleDFS(String taskId, Map<String, Set<String>> graph,
                               Set<String> visited, Set<String> recursionStack) {
    // 标记当前节点为正在访问
    visited.add(taskId);
    recursionStack.add(taskId);

    // 访问所有依赖节点
    Set<String> dependencies = graph.get(taskId);
    if (dependencies != null) {
      for (String dep : dependencies) {
        // 如果依赖节点不存在，跳过（可能是外部任务）
        if (!graph.containsKey(dep)) {
          continue;
        }

        // 如果依赖节点未访问，递归访问
        if (!visited.contains(dep)) {
          if (hasCycleDFS(dep, graph, visited, recursionStack)) {
            return true;  // 检测到循环
          }
        }
        // 如果依赖节点在递归栈中，说明存在回边（循环）
        else if (recursionStack.contains(dep)) {
          return true;
        }
      }
    }

    // 回溯：从递归栈移除
    recursionStack.remove(taskId);
    return false;
  }

  /**
   * 从依赖图中移除任务
   *
   * 用于 workflow 执行完成后的清理，避免内存泄漏。
   * 移除任务时会同时清理：
   * - nodes 映射中的节点
   * - readyQueue 中的条目
   * - completedNodes 集合中的条目
   *
   * 注意：此方法不会更新其他任务的 remainingDeps 计数，
   * 因为它假设被移除的任务已经完成或整个 workflow 正在被清理。
   *
   * @param taskId 要移除的任务 ID
   */
  public void removeTask(String taskId) {
    if (taskId == null) {
      return;
    }

    // 从节点映射移除
    nodes.remove(taskId);

    // 从就绪队列移除
    readyQueue.removeIf(t -> t.taskId.equals(taskId));

    // 从已完成集合移除
    completedNodes.remove(taskId);
  }

  /**
   * 获取任务总数
   *
   * @return 任务数量
   */
  public int getTaskCount() {
    return nodes.size();
  }

  /**
   * 获取就绪任务数量
   *
   * @return 就绪队列大小
   */
  public int getReadyCount() {
    return readyQueue.size();
  }
}
