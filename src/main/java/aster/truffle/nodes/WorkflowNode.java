package aster.truffle.nodes;

import aster.truffle.AsterLanguage;
import aster.truffle.AsterContext;
import aster.truffle.runtime.AsyncTaskRegistry;
import aster.truffle.runtime.WorkflowScheduler;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.frame.MaterializedFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import aster.truffle.runtime.PostgresEventStore;

/**
 * WorkflowNode - 工作流编排节点
 *
 * Phase 2.0 实现：工作流编排的语法集成
 * - 接收任务列表和依赖声明
 * - 使用 StartNode 创建异步任务
 * - 构建 DependencyGraph 并使用 WorkflowScheduler 执行
 * - 收集并返回所有任务结果
 * - 支持全局超时控制
 *
 * Phase 2.1 升级路径：
 * - 编译器支持（从 AST 自动生成 WorkflowNode）
 * - 优化的依赖图序列化
 */
@NodeInfo(shortName = "workflow", description = "工作流编排节点")
public final class WorkflowNode extends Node {
  private static final Logger logger = Logger.getLogger(WorkflowNode.class.getName());
  private final Env env;
  @Children private final Node[] taskExprs;  // 任务表达式
  @Children private final Node[] compensateExprs;  // 补偿表达式（可为 null）
  private final String[] taskNames;
  private final Map<String, Set<String>> dependencies;  // name -> dep names
  private final long timeoutMs;

  /**
   * 构造工作流节点（向后兼容：无补偿）
   */
  public WorkflowNode(Env env, Node[] taskExprs, String[] taskNames,
                      Map<String, Set<String>> dependencies, long timeoutMs) {
    this(env, taskExprs, null, taskNames, dependencies, timeoutMs);
  }

  /**
   * 构造工作流节点
   *
   * @param env 环境对象（用于变量绑定）
   * @param taskExprs 任务表达式数组
   * @param compensateExprs 补偿表达式数组（可为 null，与 taskExprs 一一对应）
   * @param taskNames 任务名称数组（与 taskExprs 一一对应）
   * @param dependencies 依赖关系映射（任务名 -> 依赖的任务名集合）
   * @param timeoutMs 工作流全局超时时间（毫秒）
   */
  public WorkflowNode(Env env, Node[] taskExprs, Node[] compensateExprs, String[] taskNames,
                      Map<String, Set<String>> dependencies, long timeoutMs) {
    if (taskExprs == null || taskNames == null) {
      throw new IllegalArgumentException("taskExprs and taskNames cannot be null");
    }
    if (taskExprs.length != taskNames.length) {
      throw new IllegalArgumentException(
          "taskExprs and taskNames must have the same length"
      );
    }
    if (compensateExprs != null && compensateExprs.length != taskExprs.length) {
      throw new IllegalArgumentException(
          "compensateExprs must have the same length as taskExprs"
      );
    }
    this.env = env;
    this.taskExprs = taskExprs;
    this.compensateExprs = compensateExprs;
    this.taskNames = taskNames;
    this.dependencies = (dependencies == null) ? Collections.emptyMap() : dependencies;
    this.timeoutMs = timeoutMs;
  }

  /**
   * 执行工作流
   *
   * Phase 2.0: 协作式调度
   * 1. 为所有任务创建 StartNode 并获取 task_id
   * 2. 构建依赖图（将任务名映射为 task_id）
   * 3. 使用 WorkflowScheduler 执行工作流（带超时控制）
   * 4. 收集结果；如果有失败，执行补偿逻辑
   *
   * @param frame 当前执行帧
   * @return 结果数组（按 taskNames 顺序）
   */
  public Object execute(VirtualFrame frame) {
    Profiler.inc("workflow");

    AsterContext context = AsterLanguage.getContext();
    if (!context.isEffectAllowed("Async")) {
      throw new RuntimeException("workflow requires Async effect");
    }
    AsyncTaskRegistry registry = context.getAsyncRegistry();
    // 生成唯一的 workflowId 并获取事件存储（如已配置）
    String workflowId = "wf-" + UUID.randomUUID().toString().substring(0, 8);
    PostgresEventStore eventStore = registry.getEventStore();
    WorkflowScheduler scheduler = new WorkflowScheduler(registry, workflowId, eventStore);

    Map<String, String> nameToId = new LinkedHashMap<>();
    MaterializedFrame[] capturedFrames = new MaterializedFrame[taskExprs.length];
    Set<String>[] effectSnapshots = new Set[taskExprs.length];
    java.util.List<Integer> completedSteps = new java.util.ArrayList<>();

    // 1. 为所有任务生成 task_id 并捕获执行上下文
    for (int i = 0; i < taskExprs.length; i++) {
      Profiler.inc("start");
      MaterializedFrame materializedFrame = frame.materialize();
      // 深拷贝效果集合，避免后续修改污染其他任务的快照
      Set<String> currentEffects = context.getAllowedEffects();
      Set<String> capturedEffects = (currentEffects == null || currentEffects.isEmpty())
          ? Collections.emptySet()
          : new LinkedHashSet<>(currentEffects);
      String taskId = context.generateTaskId();

      // 检测重复任务名称，避免静默数据错乱
      if (taskNames[i] != null && nameToId.containsKey(taskNames[i])) {
        throw new IllegalArgumentException("Duplicate workflow step name: " + taskNames[i]);
      }
      nameToId.put(taskNames[i], taskId);
      if (taskNames[i] != null) {
        env.set(taskNames[i], taskId);
      }
      capturedFrames[i] = materializedFrame;
      effectSnapshots[i] = capturedEffects;
    }

    // 2. 注册任务并声明依赖关系
    for (int i = 0; i < taskExprs.length; i++) {
      Node expr = taskExprs[i];
      String stepName = taskNames[i];
      String taskId = nameToId.get(stepName);
      MaterializedFrame materializedFrame = capturedFrames[i];
      Set<String> capturedEffects = effectSnapshots[i];
      final int stepIndex = i;

      Callable<Object> callable = () -> {
        Set<String> previousEffects = context.getAllowedEffects();
        try {
          context.setAllowedEffects(capturedEffects);
          Object result = Exec.exec(expr, materializedFrame);
          synchronized (completedSteps) {
            completedSteps.add(stepIndex);
          }
          return result;
        } catch (ReturnNode.ReturnException rex) {
          synchronized (completedSteps) {
            completedSteps.add(stepIndex);
          }
          return rex.value;
        } catch (RuntimeException ex) {
          throw ex;
        } catch (Throwable t) {
          throw new RuntimeException("workflow step failed: " + stepName, t);
        } finally {
          context.setAllowedEffects(previousEffects);
        }
      };

      Set<String> depIds = resolveDependencyIds(stepName, nameToId);
      // 使用显式 workflowId 注册，避免并发 workflow 时全局字段被覆盖
      registry.registerTaskWithWorkflowId(taskId, callable, depIds, workflowId);
    }

    // 3. 执行工作流（带超时控制）
    boolean success = true;
    Throwable failure = null;
    Object[] results = new Object[taskNames.length];

    try {
      try {
        if (timeoutMs > 0) {
          scheduler.executeWithTimeout(timeoutMs);
        } else {
          scheduler.executeUntilComplete();
        }
      } catch (RuntimeException ex) {
        success = false;
        failure = ex;
        // 工作流失败时取消所有未完成的独立分支任务
        registry.cancelAll();
        // 等待任务真正停止，避免资源泄漏和状态污染
        boolean quiesced = registry.awaitQuiescent(5000);
        if (!quiesced) {
          logger.log(Level.WARNING,
              String.format("工作流 %s 取消后部分任务仍在运行", workflowId));
        }
      }

      // 4. 如果失败且有补偿逻辑，按逆序执行补偿
      if (!success && compensateExprs != null) {
        java.util.List<Integer> stepsToCompensate;
        synchronized (completedSteps) {
          stepsToCompensate = new java.util.ArrayList<>(completedSteps);
        }
        // 逆序补偿（最后完成的先补偿）
        java.util.Collections.reverse(stepsToCompensate);
        for (int idx : stepsToCompensate) {
          Node compensate = compensateExprs[idx];
          if (compensate != null) {
            try {
              Exec.exec(compensate, capturedFrames[idx]);
            } catch (Throwable t) {
              // 补偿失败记录日志但继续执行其他补偿
              logger.log(Level.WARNING,
                  String.format("补偿失败 [workflowId=%s, step=%s]: %s",
                      workflowId, taskNames[idx], t.getMessage()),
                  t);
            }
          }
        }
      }

      // 5. 收集结果（成功时）
      if (success) {
        for (int i = 0; i < taskNames.length; i++) {
          String taskId = nameToId.get(taskNames[i]);
          results[i] = registry.getResult(taskId);
        }
      }
    } finally {
      // 6. 清理所有注册的任务，避免内存泄漏和状态污染
      for (String taskId : nameToId.values()) {
        registry.removeTask(taskId);
      }
    }

    // 7. 失败时抛出异常
    if (!success) {
      if (failure instanceof RuntimeException) {
        throw (RuntimeException) failure;
      }
      throw new RuntimeException("Workflow failed", failure);
    }

    return results;
  }

  private Set<String> resolveDependencyIds(String name, Map<String, String> nameToId) {
    Set<String> depNames = dependencies.get(name);
    if (depNames == null || depNames.isEmpty()) {
      return Collections.emptySet();
    }
    LinkedHashSet<String> ids = new LinkedHashSet<>();
    for (String dep : depNames) {
      String taskId = nameToId.get(dep);
      if (taskId == null) {
        throw new RuntimeException("Unknown workflow dependency: " + dep);
      }
      ids.add(taskId);
    }
    return ids;
  }
}
