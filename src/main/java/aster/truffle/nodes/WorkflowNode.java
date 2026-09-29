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
import java.util.List;
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
  private final List<Set<String>> dependencies;  // step 下标 -> 依赖的 step 名集合
  private final long timeoutMs;

  /**
   * 构造工作流节点
   *
   * @param env 环境对象（用于变量绑定）
   * @param taskExprs 任务表达式数组
   * @param compensateExprs 补偿表达式数组（可为 null，与 taskExprs 一一对应）
   * @param taskNames 任务名称数组（与 taskExprs 一一对应，未命名 step 为 null）
   * @param dependencies 依赖声明（与 taskExprs 一一对应：第 i 项是第 i 个 step 依赖的 step 名集合，
   *                     无依赖为空集）。按下标而非按名字挂载，未命名 step 的依赖才不会丢失
   * @param timeoutMs 工作流全局超时时间（毫秒）
   */
  public WorkflowNode(Env env, Node[] taskExprs, Node[] compensateExprs, String[] taskNames,
                      List<Set<String>> dependencies, long timeoutMs) {
    if (taskExprs == null || taskNames == null || dependencies == null) {
      throw new IllegalArgumentException("taskExprs, taskNames and dependencies cannot be null");
    }
    if (taskExprs.length != taskNames.length || taskExprs.length != dependencies.size()) {
      throw new IllegalArgumentException(
          "taskExprs, taskNames and dependencies must have the same length"
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
    this.dependencies = List.copyOf(dependencies);
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
    // ★步骤级 trace（M2.1b）：workflow 任务体在 executor worker 线程跑决策节点（非 eval 线程，
    // 收不进 collector），且 await/wait 的 inline 调度让 trace 形状随调度路径漂移。故整条 trace
    // 标 NON_REPLAYABLE——绝不产不稳定/部分 traceHash（Codex 复审 P0）。全局关时 PE 折叠 no-op。
    aster.truffle.trace.TraceAccess.markAsyncTainted();
    AsyncTaskRegistry registry = context.getAsyncRegistry();
    // 生成唯一的 workflowId 并获取事件存储（如已配置）
    String workflowId = "wf-" + UUID.randomUUID().toString().substring(0, 8);
    PostgresEventStore eventStore = registry.getEventStore();
    WorkflowScheduler scheduler = new WorkflowScheduler(registry, workflowId, eventStore);

    // 注册、收集结果、清理一律按 step 下标走 idByIndex；nameToId 只登记**有名字**的 step，
    // 仅供依赖解析用。step 名在 Core IR 里并非必填（Loader 对 null step / 缺 name 都会产出
    // null），若以 name 为键，两个未命名 step 会共用 null 键互相覆盖，随后对同一 taskId
    // 注册两次而报 "Task already exists"——错误指向 registry 而非缺 name 的真实原因。
    String[] idByIndex = new String[taskExprs.length];
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
      idByIndex[i] = taskId;

      if (taskNames[i] != null) {
        // 检测重复任务名称，避免静默数据错乱
        if (nameToId.putIfAbsent(taskNames[i], taskId) != null) {
          throw new IllegalArgumentException("Duplicate workflow step name: " + taskNames[i]);
        }
        env.set(taskNames[i], taskId);
      }
      capturedFrames[i] = materializedFrame;
      effectSnapshots[i] = capturedEffects;
    }

    // 2. 注册任务并声明依赖关系
    //
    // ★注册循环必须自带回滚（issue #109 body）。此前它在下面第 3 步的 try/finally
    //   **之外**：循环第 i 步抛异常时（resolveDependencyIds 的 "Unknown workflow
    //   dependency"、或 DependencyGraph.addTask 的循环依赖检测），第 0..i-1 步已注册
    //   的任务永远留在共享 registry —— remainingTasks 已递增且无人递减、无人移除。
    //
    //   后果是**永久毒化该 Context**：泄漏任务中无依赖者会被后续无关 workflow 当作
    //   ready 幽灵执行（副作用在错误时刻触发）；依赖未注册任务者永远不 ready，
    //   于是此后每次都撞 executeUntilComplete 的死锁检测。即一个含循环依赖的坏程序
    //   会打断该 Context 之后所有 workflow 求值。
    java.util.List<String> registeredTaskIds = new java.util.ArrayList<>(taskExprs.length);
    try {
    for (int i = 0; i < taskExprs.length; i++) {
      Node expr = taskExprs[i];
      String stepName = taskNames[i];
      String taskId = idByIndex[i];
      MaterializedFrame materializedFrame = capturedFrames[i];
      Set<String> capturedEffects = effectSnapshots[i];
      final int stepIndex = i;

      Callable<Object> callable = () -> {
        Set<String> previousEffects = context.getAllowedEffects();
        // ★step 体在 executor worker 线程上执行，**既不经过 AsterRootNode 也不经过
        // LambdaRootNode** → 不接线的话分配预算在整条 workflow 路径上等于没上线
        // （实测：把放大逻辑放进 step，1174ms 抛 Java heap space，与修复前同形）。
        // 同时 worker 线程跨 workflow/跨租户复用，不配对 exitFrame 还会让额度只增不清，
        // 一旦触线该 worker 此后**永久拒绝一切 step**——漏防与误杀并存。
        final boolean outermostFrame = aster.truffle.runtime.AllocationBudget.enterFrame();
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
          aster.truffle.runtime.AllocationBudget.exitFrame(outermostFrame);
        }
      };

      Set<String> depIds = resolveDependencyIds(i, nameToId);
      // 使用显式 workflowId 注册，避免并发 workflow 时全局字段被覆盖
      registry.registerTaskWithWorkflowId(taskId, callable, depIds, workflowId);
      // 只记录**注册成功**的 id：registerTaskWithWorkflowId 自身抛出时
      // 该任务未进入 registry，不能进回滚名单（否则 removeTask 会误删同名残留）。
      registeredTaskIds.add(taskId);
    }
    } catch (Throwable registrationFailure) {
      // 回滚本次已注册的部分，再把原异常原样抛出——绝不吞掉。
      for (String registeredId : registeredTaskIds) {
        try {
          registry.removeTask(registeredId);
        } catch (Throwable cleanupFailure) {
          // 清理失败不得掩盖原始失败原因，记录后继续清理其余任务。
          logger.log(Level.WARNING,
              String.format("回滚注册失败 [workflowId=%s, taskId=%s]", workflowId, registeredId),
              cleanupFailure);
        }
      }
      throw registrationFailure;
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
          results[i] = registry.getResult(idByIndex[i]);
        }
      }
    } finally {
      // 6. 清理所有注册的任务，避免内存泄漏和状态污染
      for (String taskId : idByIndex) {
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

  /**
   * 把第 {@code stepIndex} 个 step 声明的依赖名解析为 taskId。
   * 依赖按下标取、依赖目标按名字查：只有具名 step 才能被别人依赖，但任何 step 都可以声明依赖。
   */
  private Set<String> resolveDependencyIds(int stepIndex, Map<String, String> nameToId) {
    LinkedHashSet<String> ids = new LinkedHashSet<>();
    for (String dep : dependencies.get(stepIndex)) {
      String taskId = nameToId.get(dep);
      if (taskId == null) {
        throw new RuntimeException("Unknown workflow dependency: " + dep);
      }
      ids.add(taskId);
    }
    return ids;
  }
}
