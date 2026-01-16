package aster.truffle.runtime;

import aster.core.exceptions.MaxRetriesExceededException;
import io.aster.workflow.DeterminismContext;
import aster.runtime.workflow.WorkflowEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * 异步任务注册表 - 负责任务注册、状态跟踪与调度
 *
 * Phase 2.0 之前：
 * - 依赖 LinkedList 顺序执行，无法并行
 *
 * Phase 2.2：
 * - 基于 CompletableFuture + ExecutorService 的依赖感知并发调度
 * - 维持 TaskState API，保证 Await/Wait 节点向后兼容
 * - 暴露 registerTaskWithDependencies 支撑 Workflow emitter
 */
public final class AsyncTaskRegistry {
  private static final Logger logger = Logger.getLogger(AsyncTaskRegistry.class.getName());
  private static final long DEFAULT_TTL_MILLIS = 60_000L;

  // 任务状态存储：task_id -> TaskState
  private final ConcurrentHashMap<String, TaskState> tasks = new ConcurrentHashMap<>();
  // 调度信息存储：task_id -> TaskInfo
  private final ConcurrentHashMap<String, TaskInfo> taskInfos = new ConcurrentHashMap<>();
  // 依赖图：内部自管理，避免依赖外部 WorkflowScheduler
  private final DependencyGraph dependencyGraph = new DependencyGraph();
  // 重试策略存储
  private final Map<String, RetryPolicy> retryPolicies = new ConcurrentHashMap<>();
  private final Map<String, Integer> attemptCounters = new ConcurrentHashMap<>();
  private static final String WORKFLOW_RETRY_TASK_ID = "__workflow_retry__";
  private final ConcurrentHashMap<String, DelayedTask> pendingRetryTasks = new ConcurrentHashMap<>();
  private final Set<String> workflowRetryTasks = ConcurrentHashMap.newKeySet();
  private volatile String workflowId;
  private PostgresEventStore eventStore;
  // 剩余待完成任务计数
  private final AtomicInteger remainingTasks = new AtomicInteger();
  // 线程池（默认 CPU 核数，可配置），size=1 时即单线程回退模式
  private final ExecutorService executor;
  private final boolean singleThreadMode;
  // graph 同步锁，DependencyGraph 非线程安全
  private final Object graphLock = new Object();
  // 补偿 LIFO 栈（按 workflowId 隔离，避免跨 workflow 误补偿）
  private final Map<String, Deque<String>> compensationStacks = new ConcurrentHashMap<>();
  // 默认超时时间（毫秒），0 表示无限制
  private final long defaultTimeoutMs;
  private final PriorityQueue<DelayedTask> delayQueue = new PriorityQueue<>();
  private final ReentrantLock delayQueueLock = new ReentrantLock();
  private Thread pollThread;
  private volatile boolean running = false;
  private final DeterminismContext determinismContext;
  private volatile boolean replayMode = false;
  // 最近失败任务的 workflowId（用于 executeUntilComplete 异常时定位补偿目标）
  private volatile String lastFailedWorkflowId;

  /**
   * 并发调度需要的任务元数据
   */
  private static final class TaskInfo {
    final String taskId;
    final String workflowId;  // 任务所属的 workflow ID
    final Callable<?> callable;
    final Set<String> dependencies;
    final CompletableFuture<Object> future = new CompletableFuture<>();
    final AtomicBoolean submitted = new AtomicBoolean(false);
    final long timeoutMs;
    final Runnable compensationCallback;
    final int priority;
    // 绑定执行线程的 Future，用于 cancelAll() 真正中断运行中的任务
    final AtomicReference<java.util.concurrent.Future<?>> runningFuture = new AtomicReference<>();

    TaskInfo(String taskId, String workflowId, Callable<?> callable, Set<String> dependencies, long timeoutMs,
             Runnable compensationCallback, int priority) {
      this.taskId = taskId;
      this.workflowId = workflowId;
      this.callable = callable;
      this.dependencies = Collections.unmodifiableSet(new LinkedHashSet<>(dependencies));
      this.timeoutMs = timeoutMs;
      this.compensationCallback = compensationCallback;
      this.priority = priority;
    }
  }

  /**
   * 任务状态封装（保持 Phase 1 API）
   */
  public static final class TaskState {
    final String taskId;
    final AtomicReference<TaskStatus> status;
    volatile Object result;
    volatile Throwable exception;
    final long createdAt;

    TaskState(String taskId) {
      this.taskId = taskId;
      this.status = new AtomicReference<>(TaskStatus.PENDING);
      this.createdAt = System.currentTimeMillis();
    }

    public String getTaskId() {
      return taskId;
    }

    public TaskStatus getStatus() {
      return status.get();
    }

    public Object getResult() {
      return result;
    }

    public Throwable getException() {
      return exception;
    }
  }

  /**
   * 任务状态枚举
   */
  public enum TaskStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
  }

  /**
   * 重试策略配置
   */
  public static class RetryPolicy {
    public final int maxAttempts;
    public final String backoff; // "exponential" or "linear"
    public final long baseDelayMs;

    public RetryPolicy(int maxAttempts, String backoff, long baseDelayMs) {
      this.maxAttempts = maxAttempts;
      this.backoff = backoff;
      this.baseDelayMs = baseDelayMs;
    }
  }

  public AsyncTaskRegistry() {
    this(loadThreadPoolSize(), loadDefaultTimeout(), new DeterminismContext());
  }

  public AsyncTaskRegistry(int threadPoolSize) {
    this(threadPoolSize, loadDefaultTimeout(), new DeterminismContext());
  }

  public AsyncTaskRegistry(int threadPoolSize, DeterminismContext determinismContext) {
    this(threadPoolSize, loadDefaultTimeout(), determinismContext);
  }

  private AsyncTaskRegistry(int threadPoolSize, long defaultTimeoutMs) {
    this(threadPoolSize, defaultTimeoutMs, new DeterminismContext());
  }

  private AsyncTaskRegistry(int threadPoolSize, long defaultTimeoutMs, DeterminismContext determinismContext) {
    int normalizedSize = Math.max(1, threadPoolSize);
    this.executor = Executors.newFixedThreadPool(normalizedSize);
    this.singleThreadMode = normalizedSize == 1;
    this.defaultTimeoutMs = Math.max(0L, defaultTimeoutMs);
    this.determinismContext = Objects.requireNonNull(determinismContext, "determinismContext cannot be null");
  }

  /**
   * 设置 workflowId（用于单 workflow 场景）
   *
   * 注意：此方法仅适用于单 workflow 执行场景。对于并发 workflow 场景，
   * 请使用 {@link #registerTaskWithWorkflowId} 显式传递 workflowId。
   *
   * @param workflowId workflow 唯一标识符
   */
  public void setWorkflowId(String workflowId) {
    this.workflowId = workflowId;
  }

  /**
   * 注入事件存储
   */
  public void setEventStore(PostgresEventStore eventStore) {
    this.eventStore = eventStore;
  }

  /**
   * 获取事件存储（若未设置则返回 null）
   *
   * @return PostgresEventStore 实例或 null
   */
  public PostgresEventStore getEventStore() {
    return eventStore;
  }

  /**
   * 获取当前的 workflowId（若未设置则返回 null）
   *
   * @return workflowId 或 null
   */
  public String getWorkflowId() {
    return workflowId;
  }

  /**
   * 获取当前的 DeterminismContext。
   *
   * @return 确定性上下文
   */
  public DeterminismContext getDeterminismContext() {
    return determinismContext;
  }

  /**
   * 切换重放模式：开启后重试退避从事件日志读取，而非重新计算。
   */
  public void setReplayMode(boolean replayMode) {
    this.replayMode = replayMode;
  }

  /**
   * 注册无依赖任务（兼容 StartNode 用途，单 workflow 场景）
   *
   * 注意：此方法使用全局 workflowId，仅适用于单 workflow 执行场景。
   * 对于并发 workflow 场景，请使用 {@link #registerTaskWithWorkflowId}。
   *
   * @param taskId 任务唯一标识符
   * @param taskBody 任务逻辑
   * @return taskId
   */
  public String registerTask(String taskId, Runnable taskBody) {
    Objects.requireNonNull(taskId, "taskId");
    Objects.requireNonNull(taskBody, "taskBody");

    Callable<Object> callable = () -> {
      taskBody.run();
      return null;
    };
    registerInternal(taskId, callable, Collections.emptySet(), defaultTimeoutMs, null, 0, this.workflowId);
    return taskId;
  }

  /**
   * 新的依赖感知注册接口：Workflow emitter 使用
   */
  public String registerTaskWithDependencies(
      String taskId, Callable<?> callable, Set<String> dependencies) {
    return registerTaskWithDependencies(taskId, callable, dependencies, 0L, null, 0);
  }

  /**
   * 依赖感知注册接口（可指定超时）
   */
  public String registerTaskWithDependencies(
      String taskId, Callable<?> callable, Set<String> dependencies, long timeoutMs) {
    return registerTaskWithDependencies(taskId, callable, dependencies, timeoutMs, null, 0);
  }

  /**
   * 依赖感知注册接口（可指定超时与补偿回调）
   */
  public String registerTaskWithDependencies(
      String taskId, Callable<?> callable, Set<String> dependencies, long timeoutMs,
      Runnable compensationCallback) {
    return registerTaskWithDependencies(taskId, callable, dependencies, timeoutMs, compensationCallback, 0);
  }

  /**
   * 依赖感知注册接口（可指定超时、补偿回调与优先级，单 workflow 场景）
   *
   * 注意：此方法使用全局 workflowId，仅适用于单 workflow 执行场景。
   * 对于并发 workflow 场景，请使用 {@link #registerTaskWithWorkflowId}。
   *
   * @param taskId 任务唯一标识符
   * @param callable 任务逻辑
   * @param dependencies 依赖的任务 ID 集合
   * @param timeoutMs 超时时间（毫秒），0 表示使用默认值
   * @param compensationCallback 补偿回调（可选）
   * @param priority 优先级（数值越小优先级越高）
   * @return taskId
   */
  public String registerTaskWithDependencies(
      String taskId, Callable<?> callable, Set<String> dependencies, long timeoutMs,
      Runnable compensationCallback, int priority) {
    Objects.requireNonNull(taskId, "taskId");
    Objects.requireNonNull(callable, "callable");
    Set<String> deps = (dependencies == null) ? Collections.emptySet() : new LinkedHashSet<>(dependencies);
    long effectiveTimeout = timeoutMs > 0 ? timeoutMs : defaultTimeoutMs;
    registerInternal(taskId, callable, deps, effectiveTimeout, compensationCallback, priority, this.workflowId);
    return taskId;
  }

  /**
   * 依赖感知注册接口（显式指定 workflowId，用于并发 workflow 场景）
   *
   * 使用此方法可避免多 workflow 并发注册时 workflowId 被覆盖的问题。
   *
   * @param taskId 任务唯一标识符
   * @param callable 任务逻辑
   * @param dependencies 依赖的任务 ID 集合
   * @param workflowId 任务所属的 workflow ID（不可为 null）
   * @return taskId
   */
  public String registerTaskWithWorkflowId(
      String taskId, Callable<?> callable, Set<String> dependencies, String workflowId) {
    Objects.requireNonNull(taskId, "taskId");
    Objects.requireNonNull(callable, "callable");
    Objects.requireNonNull(workflowId, "workflowId");
    Set<String> deps = (dependencies == null) ? Collections.emptySet() : new LinkedHashSet<>(dependencies);
    registerInternal(taskId, callable, deps, defaultTimeoutMs, null, 0, workflowId);
    return taskId;
  }

  /**
   * 注册带重试策略的任务（单 workflow 场景）
   *
   * 注意：此方法使用全局 workflowId，仅适用于单 workflow 执行场景。
   * 对于并发 workflow 场景，请使用 {@link #registerTaskWithRetry(String, Supplier, Set, RetryPolicy, String)}。
   *
   * @param taskId 任务唯一标识符
   * @param task 任务逻辑
   * @param dependencies 依赖的任务 ID 集合
   * @param policy 重试策略
   * @return taskId
   */
  public String registerTaskWithRetry(String taskId, Supplier<?> task, Set<String> dependencies,
      RetryPolicy policy) {
    return registerTaskWithRetry(taskId, task, dependencies, policy, this.workflowId);
  }

  /**
   * 注册带重试策略的任务（显式指定 workflowId，推荐用于并发 workflow 场景）
   *
   * @param taskId 任务唯一标识符
   * @param task 任务逻辑
   * @param dependencies 依赖的任务 ID 集合
   * @param policy 重试策略
   * @param workflowId 任务所属的 workflow ID
   * @return taskId
   */
  public String registerTaskWithRetry(String taskId, Supplier<?> task, Set<String> dependencies,
      RetryPolicy policy, String workflowId) {
    Objects.requireNonNull(taskId, "taskId");
    Objects.requireNonNull(task, "task");
    if (policy != null) {
      retryPolicies.put(taskId, policy);
      attemptCounters.putIfAbsent(taskId, 1);
    }
    Callable<Object> callable = task::get;
    Set<String> deps = (dependencies == null) ? Collections.emptySet() : new LinkedHashSet<>(dependencies);
    registerInternal(taskId, callable, deps, defaultTimeoutMs, null, 0, workflowId);
    return taskId;
  }

  /**
   * 返回任务状态（供 Await/Wait Node 轮询）
   */
  public TaskState getTaskState(String taskId) {
    return tasks.get(taskId);
  }

  public TaskStatus getStatus(String taskId) {
    TaskState state = tasks.get(taskId);
    return state != null ? state.status.get() : null;
  }

  public boolean isCompleted(String taskId) {
    TaskStatus status = getStatus(taskId);
    return status == TaskStatus.COMPLETED;
  }

  public boolean isFailed(String taskId) {
    TaskStatus status = getStatus(taskId);
    return status == TaskStatus.FAILED;
  }

  public Object getResult(String taskId) {
    TaskState state = tasks.get(taskId);
    if (state == null) {
      throw new RuntimeException("Task not found: " + taskId);
    }

    TaskStatus status = state.status.get();
    if (status == TaskStatus.COMPLETED) {
      return state.result;
    } else if (status == TaskStatus.FAILED) {
      throw new RuntimeException("Task failed: " + taskId, state.exception);
    } else {
      throw new RuntimeException("Task not completed: " + taskId + " (status: " + status + ")");
    }
  }

  public Throwable getException(String taskId) {
    TaskState state = tasks.get(taskId);
    return state != null ? state.exception : null;
  }

  /**
   * Phase 1 兼容：显式执行一个就绪任务（串行 fallback）
   */
  public void executeNext() {
    String nextTaskId = nextReadyTaskId();
    if (nextTaskId == null) {
      return;
    }
    TaskInfo info = taskInfos.get(nextTaskId);
    if (info == null) {
      return;
    }
    // 单线程模式直接在调用线程执行；多线程模式下也允许显式拉起一个任务
    runTaskInline(info);
  }

  /**
   * 并发调度主入口：按依赖拓扑批量调度所有任务
   */
  public void executeUntilComplete() {
    try {
      while (remainingTasks.get() > 0) {
        List<String> readyTasks = snapshotReadyTasks();
        if (readyTasks.isEmpty()) {
          // 1. 检查失败任务（保持现有逻辑）
          Optional<TaskState> failed =
              tasks.values().stream().filter(state -> state.getStatus() == TaskStatus.FAILED).findFirst();
          if (failed.isPresent()) {
            throw new RuntimeException("Task failed: " + failed.get().taskId, failed.get().exception);
          }

          // 2. 收集详细诊断信息（增强）
          List<String> runningTasks = new ArrayList<>();
          Map<String, Set<String>> pendingTaskDeps = new LinkedHashMap<>();

          for (Map.Entry<String, TaskInfo> entry : taskInfos.entrySet()) {
            String taskId = entry.getKey();
            TaskState state = tasks.get(taskId);
            if (state != null) {
              TaskStatus status = state.getStatus();
              if (status == TaskStatus.RUNNING) {
                runningTasks.add(taskId);
              } else if (status == TaskStatus.PENDING) {
                Set<String> uncompletedDeps = getUncompletedDependencies(entry.getValue().dependencies);
                pendingTaskDeps.put(taskId, uncompletedDeps);
              }
            }
          }

          // 3. 检测循环依赖（增强）
          List<List<String>> cycles = detectCycles();

          // 4. 构建详细错误消息（增强）
          StringBuilder errorMsg = new StringBuilder();
          errorMsg.append("死锁检测：无就绪任务但仍有 ").append(remainingTasks.get()).append(" 个任务待完成\n");

          errorMsg.append("运行中任务：").append(runningTasks).append("\n");

          errorMsg.append("待处理任务及其依赖：\n");
          for (Map.Entry<String, Set<String>> entry : pendingTaskDeps.entrySet()) {
            TaskInfo info = taskInfos.get(entry.getKey());
            int priority = (info != null) ? info.priority : 0;
            errorMsg.append("  - ").append(entry.getKey())
                .append(" (优先级 ").append(priority).append(") 等待: ")
                .append(entry.getValue()).append("\n");
          }

          if (!cycles.isEmpty()) {
            errorMsg.append("检测到循环依赖：\n");
            for (List<String> cycle : cycles) {
              errorMsg.append("  - ");
              for (int i = 0; i < cycle.size(); i++) {
                if (i > 0) {
                  errorMsg.append(" -> ");
                }
                errorMsg.append(cycle.get(i));
              }
              errorMsg.append(" -> ").append(cycle.get(0)).append("\n");
            }
          }

          // 防止 race condition：在抛出异常前再次检查 remainingTasks
          if (remainingTasks.get() == 0) {
            // 最后一个任务刚完成，退出循环
            break;
          }

          // 修复死锁误报：如果仍有任务在运行，等待它们完成后重试
          // 而不是立即抛出死锁异常（运行中的任务可能会解锁新的就绪任务）
          if (!runningTasks.isEmpty()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            continue;
          }

          throw new IllegalStateException(errorMsg.toString());
        }

        List<CompletableFuture<Object>> batchFutures = new ArrayList<>(readyTasks.size());
        for (String taskId : readyTasks) {
          TaskInfo info = taskInfos.get(taskId);
          if (info == null) {
            continue;
          }
          if (canSchedule(taskId)) {
            batchFutures.add(submitTask(info));
          }
        }

        if (batchFutures.isEmpty()) {
          // 所有 ready 节点都处于非 PENDING 状态，说明等待上轮运行结束
          LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
          continue;
        }

        CompletableFuture<Void> barrier =
            CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]));
        try {
          barrier.join();
        } catch (CompletionException ex) {
          throw unwrapCompletion(ex);
        }
      }
    } catch (RuntimeException | Error ex) {
      // 找到失败任务的 workflowId，仅补偿该 workflow
      String failedWorkflowId = findFailedWorkflowId();
      if (failedWorkflowId != null) {
        executeCompensations(failedWorkflowId);
      }
      throw ex;
    }
  }

  /**
   * 获取最近失败任务的 workflowId
   *
   * 使用 lastFailedWorkflowId 字段而非遍历全局 tasks，
   * 避免并发或连续运行多个 workflow 时返回旧 workflow 的失败任务。
   */
  private String findFailedWorkflowId() {
    return lastFailedWorkflowId;
  }

  /**
   * 设置任务执行结果（StartNode/Workflow step 共用）
   */
  public void setResult(String taskId, Object result) {
    TaskState state = tasks.get(taskId);
    if (state != null) {
      state.result = result;
    }
  }

  public void removeTask(String taskId) {
    TaskInfo info = taskInfos.remove(taskId);
    tasks.remove(taskId);

    // 从依赖图移除，避免内存泄漏
    synchronized (graphLock) {
      dependencyGraph.removeTask(taskId);
    }

    // 从补偿栈移除（如果存在）
    if (info != null && info.workflowId != null) {
      Deque<String> stack = compensationStacks.get(info.workflowId);
      if (stack != null) {
        stack.remove(taskId);
        // 如果补偿栈为空，清理整个条目
        if (stack.isEmpty()) {
          compensationStacks.remove(info.workflowId);
        }
      }
    }
  }

  public void gc() {
    long now = System.currentTimeMillis();
    tasks.entrySet().removeIf(entry -> {
      TaskState state = entry.getValue();
      TaskStatus status = state.status.get();
      boolean expired = now - state.createdAt > DEFAULT_TTL_MILLIS;
      if ((status == TaskStatus.COMPLETED || status == TaskStatus.FAILED) && expired) {
        taskInfos.remove(state.taskId);
        return true;
      }
      return false;
    });
  }

  public int getTaskCount() {
    return tasks.size();
  }

  public int getPendingCount() {
    int pending = 0;
    for (TaskState state : tasks.values()) {
      if (state.getStatus() == TaskStatus.PENDING) {
        pending++;
      }
    }
    return pending;
  }

  public boolean isDependencySatisfied(String taskId) {
    TaskInfo info = taskInfos.get(taskId);
    if (info == null || info.dependencies.isEmpty()) {
      return true;
    }

    for (String dep : info.dependencies) {
      if (!isCompleted(dep)) {
        return false;
      }
    }
    return true;
  }

  public void cancelTask(String taskId) {
    TaskState state = tasks.get(taskId);
    TaskInfo info = taskInfos.get(taskId);
    if (state == null || info == null) {
      return;
    }

    // 使用自旋确保正确取消，处理 PENDING 和 RUNNING 状态（与 cancelAll 保持一致）
    while (true) {
      TaskStatus status = state.status.get();
      switch (status) {
        case PENDING:
          if (state.status.compareAndSet(TaskStatus.PENDING, TaskStatus.CANCELLED)) {
            info.future.cancel(true);
            remainingTasks.decrementAndGet();
            synchronized (graphLock) {
              dependencyGraph.markCompleted(taskId);
            }
            cleanupRetryState(taskId);
            return;
          }
          // CAS 失败，继续自旋重试
          continue;
        case RUNNING:
          // 使用绑定的执行器 Future 真正中断线程
          java.util.concurrent.Future<?> executorFuture = info.runningFuture.getAndSet(null);
          if (executorFuture != null) {
            executorFuture.cancel(true); // 发送中断信号到执行线程
          }
          info.future.cancel(true);
          // 不修改状态和计数，让 runTask() 正常完成并处理状态转换
          cleanupRetryState(taskId);
          return;
        case COMPLETED:
        case FAILED:
        case CANCELLED:
          // 已完成或已取消，无需处理
          return;
      }
    }
  }

  public boolean isCancelled(String taskId) {
    TaskStatus status = getStatus(taskId);
    return status == TaskStatus.CANCELLED;
  }

  /**
   * 取消所有未完成的任务
   *
   * 用于超时或外部中断时快速终止所有任务。
   * 使用自旋 CAS 确保正确处理竞态条件。
   */
  public void cancelAll() {
    for (Map.Entry<String, TaskInfo> entry : taskInfos.entrySet()) {
      String taskId = entry.getKey();
      TaskState state = tasks.get(taskId);
      TaskInfo info = entry.getValue();
      if (state == null || info == null) {
        continue;
      }

      // 使用自旋确保正确取消，处理 PENDING 和 RUNNING 状态
      while (true) {
        TaskStatus status = state.status.get();
        switch (status) {
          case PENDING:
            if (state.status.compareAndSet(TaskStatus.PENDING, TaskStatus.CANCELLED)) {
              info.future.cancel(true);
              remainingTasks.decrementAndGet();
              synchronized (graphLock) {
                dependencyGraph.markCompleted(taskId);
              }
              cleanupRetryState(taskId); // 清理 retry 元数据
              break; // 成功取消，退出 switch
            }
            // CAS 失败，继续自旋重试
            continue;
          case RUNNING:
            // 使用绑定的执行器 Future 真正中断线程
            java.util.concurrent.Future<?> executorFuture = info.runningFuture.getAndSet(null);
            if (executorFuture != null) {
              executorFuture.cancel(true); // 发送中断信号到执行线程
            }
            info.future.cancel(true);
            // 不修改状态和计数，让 runTask() 正常完成并处理状态转换
            cleanupRetryState(taskId); // 清理 retry 元数据
            break;
          case COMPLETED:
          case FAILED:
          case CANCELLED:
            // 已完成或已取消，无需处理
            break;
        }
        break; // 退出 while 循环
      }
    }
  }

  /**
   * 等待所有运行中的任务完成或超时
   *
   * 用于 cancelAll() 后确保任务真正停止，防止超时场景下任务继续后台执行。
   *
   * @param timeoutMs 等待超时时间（毫秒）
   * @return true 如果所有任务已停止，false 如果超时
   */
  public boolean awaitQuiescent(long timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;

    for (Map.Entry<String, TaskInfo> entry : taskInfos.entrySet()) {
      TaskInfo info = entry.getValue();
      TaskState state = tasks.get(entry.getKey());

      if (state == null) {
        continue;
      }

      // 跳过已完成的任务
      TaskStatus status = state.status.get();
      if (status == TaskStatus.COMPLETED || status == TaskStatus.FAILED || status == TaskStatus.CANCELLED) {
        continue;
      }

      // 等待运行中的任务完成
      long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0) {
        return false;
      }

      try {
        info.future.get(remaining, TimeUnit.MILLISECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
        return false;
      } catch (java.util.concurrent.ExecutionException | java.util.concurrent.CancellationException e) {
        // 任务已完成（成功或失败），继续检查下一个
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    return true;
  }

  /**
   * 关闭线程池，释放资源
   */
  public void shutdown() {
    stopPolling();
    executor.shutdown();
    try {
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  // ========= 内部工具方法 =========

  private void registerInternal(String taskId, Callable<?> callable, Set<String> deps, long timeoutMs,
      Runnable compensationCallback, int priority, String taskWorkflowId) {
    if (tasks.putIfAbsent(taskId, new TaskState(taskId)) != null) {
      throw new IllegalArgumentException("Task already exists: " + taskId);
    }

    // 使用显式传入的 workflowId，确保多 workflow 并发时不会覆盖
    TaskInfo info = new TaskInfo(taskId, taskWorkflowId, callable, deps, Math.max(0L, timeoutMs), compensationCallback, priority);
    taskInfos.put(taskId, info);
    remainingTasks.incrementAndGet();

    try {
      synchronized (graphLock) {
        dependencyGraph.addTask(taskId, deps, priority);
      }
    } catch (RuntimeException ex) {
      // 回滚已插入的数据，避免 registry 状态污染
      taskInfos.remove(taskId);
      tasks.remove(taskId);
      remainingTasks.decrementAndGet();
      throw ex;
    }
  }

  private List<String> snapshotReadyTasks() {
    synchronized (graphLock) {
      return dependencyGraph.getReadyTasks();
    }
  }

  private String nextReadyTaskId() {
    List<String> ready = snapshotReadyTasks();
    for (String taskId : ready) {
      if (canSchedule(taskId)) {
        return taskId;
      }
    }
    return null;
  }

  private boolean canSchedule(String taskId) {
    TaskState state = tasks.get(taskId);
    return state != null && state.status.get() == TaskStatus.PENDING;
  }

  private CompletableFuture<Object> submitTask(TaskInfo info) {
    if (!info.submitted.compareAndSet(false, true)) {
      return info.future;
    }

    CompletableFuture<Object> trackedFuture = info.future;
    if (info.timeoutMs > 0) {
      trackedFuture = info.future
          .orTimeout(info.timeoutMs, TimeUnit.MILLISECONDS)
          .handle((result, throwable) -> {
            if (throwable == null) {
              return result;
            }
            Throwable actual = throwable instanceof CompletionException
                ? throwable.getCause()
                : throwable;
            if (actual instanceof TimeoutException timeout) {
              TimeoutException enriched = new TimeoutException("任务超时: " + info.taskId);
              enriched.initCause(timeout);
              handleTaskTimeout(info.taskId, enriched);
              throw new CompletionException(enriched);
            }
            throw new CompletionException(actual);
          });
    }

    // 绑定执行线程 Future，使 cancelAll() 可真正中断
    java.util.concurrent.Future<?> executorFuture = executor.submit(() -> {
      try {
        runTask(info);
      } finally {
        info.runningFuture.set(null); // 任务结束后清除引用
      }
    });
    info.runningFuture.set(executorFuture);
    return trackedFuture;
  }

  private void runTaskInline(TaskInfo info) {
    if (!info.submitted.compareAndSet(false, true)) {
      return;
    }
    if (singleThreadMode) {
      runTask(info);
    } else {
      // 在多线程模式下，同步执行仍运行于调用线程，适合旧节点的协作式调用
      runTask(info);
    }
  }

  /**
   * 核心执行逻辑：更新状态、写回依赖图、完成 future
   */
  private void runTask(TaskInfo info) {
    TaskState state = tasks.get(info.taskId);
    if (state == null) {
      info.future.completeExceptionally(new IllegalStateException("Unknown task: " + info.taskId));
      remainingTasks.decrementAndGet();
      return;
    }

    // Check if any dependency has failed before transitioning to RUNNING
    // This prevents race condition where task gets scheduled before cancellation propagates
    if (!info.dependencies.isEmpty()) {
      for (String dep : info.dependencies) {
        if (isFailed(dep)) {
          // Dependency failed, mark this task as cancelled
          if (state.status.compareAndSet(TaskStatus.PENDING, TaskStatus.CANCELLED)) {
            info.future.cancel(false);
            remainingTasks.decrementAndGet();
            synchronized (graphLock) {
              dependencyGraph.markCompleted(info.taskId);
            }
          }
          return;
        }
      }
    }

    if (!state.status.compareAndSet(TaskStatus.PENDING, TaskStatus.RUNNING)) {
      if (state.status.get() == TaskStatus.CANCELLED) {
        info.future.cancel(false);
        remainingTasks.decrementAndGet();
      }
      return;
    }

    boolean retryScheduled = false;
    Throwable failure = null;
    try {
      Object callResult = info.callable.call();
      if (callResult != null && state.result == null) {
        state.result = callResult;
      }
      if (state.status.compareAndSet(TaskStatus.RUNNING, TaskStatus.COMPLETED)) {
        info.future.complete(state.result);
        if (info.compensationCallback != null && info.workflowId != null) {
          compensationStacks
              .computeIfAbsent(info.workflowId, k -> new ConcurrentLinkedDeque<>())
              .push(info.taskId);
        }
        cleanupRetryState(info.taskId);
        synchronized (graphLock) {
          dependencyGraph.markCompleted(info.taskId);
        }
      }
    } catch (Throwable t) {
      failure = t;
      RetryPolicy policy = retryPolicies.get(info.taskId);
      if (policy != null) {
        workflowRetryTasks.add(info.taskId);
        Exception failureException = (t instanceof Exception) ? (Exception) t : new Exception(t);
        try {
          onTaskFailed(info.taskId, failureException, replayMode);
          retryScheduled = true;
        } catch (MaxRetriesExceededException maxEx) {
          failure = maxEx;
          retryPolicies.remove(info.taskId);
          attemptCounters.remove(info.taskId);
          workflowRetryTasks.remove(info.taskId);
          pendingRetryTasks.remove(info.taskId);
        }
      }

      if (retryScheduled) {
        state.exception = t;
        state.status.set(TaskStatus.PENDING);
      } else {
        handleFinalTaskFailure(info, state, failure);
      }
    } finally {
      if (!retryScheduled) {
        // 仅在任务真正结束时递减计数器
        remainingTasks.decrementAndGet();
      }
    }
  }

  private void handleFinalTaskFailure(TaskInfo info, TaskState state, Throwable error) {
    if (state.status.compareAndSet(TaskStatus.RUNNING, TaskStatus.FAILED)) {
      state.exception = error;
      // 记录最近失败的 workflowId，用于 executeUntilComplete 异常时精准补偿
      if (info.workflowId != null) {
        lastFailedWorkflowId = info.workflowId;
      }
      info.future.completeExceptionally(error);
      synchronized (graphLock) {
        dependencyGraph.markCompleted(info.taskId);
      }
      cancelDownstreamTasks(info.taskId);
    }
    cleanupRetryState(info.taskId);
  }

  private void cleanupRetryState(String taskId) {
    retryPolicies.remove(taskId);
    attemptCounters.remove(taskId);
    workflowRetryTasks.remove(taskId);
    if (taskId != null) {
      pendingRetryTasks.remove(taskId);
    }
  }

  /**
   * 取消所有直接或间接依赖于指定任务的下游任务
   */
  private void cancelDownstreamTasks(String failedTaskId) {
    for (Map.Entry<String, TaskInfo> entry : taskInfos.entrySet()) {
      String taskId = entry.getKey();
      TaskInfo info = entry.getValue();

      // 检查此任务是否依赖于失败的任务（直接或间接）
      if (dependsOnFailedTask(taskId, failedTaskId)) {
        cancelTask(taskId);
      }
    }
  }

  /**
   * 检查 taskId 是否直接或间接依赖于 failedTaskId
   */
  private boolean dependsOnFailedTask(String taskId, String failedTaskId) {
    TaskInfo info = taskInfos.get(taskId);
    if (info == null || info.dependencies.isEmpty()) {
      return false;
    }

    // 直接依赖
    if (info.dependencies.contains(failedTaskId)) {
      return true;
    }

    // 间接依赖：递归检查
    for (String dep : info.dependencies) {
      if (dependsOnFailedTask(dep, failedTaskId)) {
        return true;
      }
    }

    return false;
  }

  private RuntimeException unwrapCompletion(CompletionException ex) {
    Throwable cause = ex.getCause();
    if (cause instanceof RuntimeException) {
      return (RuntimeException) cause;
    }
    return new RuntimeException("Async task execution failed", cause);
  }

  /**
   * 处理任务超时：检查重试策略，若可重试则调度重试，否则标记失败并触发补偿
   * 注意：不递减 remainingTasks，因为 runTask 的 finally 块会处理
   */
  private void handleTaskTimeout(String taskId, TimeoutException timeoutException) {
    TaskState state = tasks.get(taskId);
    if (state == null) {
      return;
    }

    TaskInfo info = taskInfos.get(taskId);
    RetryPolicy policy = retryPolicies.get(taskId);

    // 如果配置了重试策略且未达到最大重试次数，尝试重试
    if (policy != null) {
      int attempt = attemptCounters.getOrDefault(taskId, 1);
      if (attempt < policy.maxAttempts) {
        // 重置状态为 PENDING 以允许重新调度
        if (state.status.compareAndSet(TaskStatus.RUNNING, TaskStatus.PENDING)) {
          // 重置 submitted 标记，允许重新提交
          if (info != null) {
            info.submitted.set(false);
          }
          // 复用 onTaskFailed 的重试逻辑
          try {
            onTaskFailed(taskId, new Exception("任务超时: " + taskId, timeoutException), false);
            return; // 重试已调度，不执行补偿
          } catch (MaxRetriesExceededException e) {
            // 达到最大重试次数，继续执行失败逻辑
          } catch (RuntimeException e) {
            // 其他异常，记录日志并继续执行失败逻辑
            logger.log(Level.WARNING, String.format("重试调度失败 [taskId=%s]: %s", taskId, e.getMessage()), e);
          }
        }
      }
    }

    // 无重试策略或达到最大重试次数，标记为失败
    boolean updated =
        state.status.compareAndSet(TaskStatus.RUNNING, TaskStatus.FAILED)
            || state.status.compareAndSet(TaskStatus.PENDING, TaskStatus.FAILED);
    if (!updated) {
      return;
    }

    state.exception = timeoutException;
    if (info != null) {
      // 记录最近失败的 workflowId
      if (info.workflowId != null) {
        lastFailedWorkflowId = info.workflowId;
      }
      info.future.obtrudeException(timeoutException);
    }

    // 标记超时任务为已完成（让依赖图更新）
    synchronized (graphLock) {
      dependencyGraph.markCompleted(taskId);
    }

    cancelDownstreamTasks(taskId);

    // 触发补偿栈（Traditional Saga 模式：超时失败时回滚已完成任务）
    // 仅补偿当前 workflow 的任务，避免跨 workflow 误补偿
    if (info != null && info.workflowId != null) {
      executeCompensations(info.workflowId);
    }

    // 注意：不递减 remainingTasks，runTask 的 finally 块会处理
  }

  /**
   * 执行指定 workflow 的补偿栈中的回调（LIFO 顺序）
   *
   * @param workflowId 要补偿的 workflow ID
   */
  private void executeCompensations(String workflowId) {
    if (workflowId == null) {
      return;
    }
    Deque<String> stack = compensationStacks.get(workflowId);
    if (stack == null) {
      return;
    }
    while (true) {
      String taskId = stack.poll();
      if (taskId == null) {
        break;
      }
      TaskInfo info = taskInfos.get(taskId);
      if (info == null || info.compensationCallback == null) {
        continue;
      }
      try {
        info.compensationCallback.run();
      } catch (Throwable t) {
        logger.log(Level.WARNING,
            String.format("补偿失败 [taskId=%s, workflowId=%s]: %s",
                taskId, info.workflowId, t.getMessage()),
            t);
      }
    }
    // 清理空的补偿栈
    compensationStacks.remove(workflowId);
  }

  /**
   * 查询给定依赖集合中尚未完成的任务
   *
   * @param dependencies 依赖任务 ID 集合
   * @return 状态不为 COMPLETED 的任务 ID 集合（包括 PENDING/RUNNING/FAILED/CANCELLED 或不存在的任务）
   */
  private Set<String> getUncompletedDependencies(Set<String> dependencies) {
    Set<String> uncompleted = new HashSet<>();
    for (String depId : dependencies) {
      TaskState state = tasks.get(depId);
      // null 状态或非 COMPLETED 状态均视为未完成
      if (state == null || state.status.get() != TaskStatus.COMPLETED) {
        uncompleted.add(depId);
      }
    }
    return uncompleted;
  }

  /**
   * 检测任务依赖图中的循环依赖
   * <p>
   * 使用深度优先搜索（DFS）算法遍历依赖图，识别反向边以检测循环。
   * 仅检查未完成的依赖（状态不为 COMPLETED），因为已完成的任务不会导致死锁。
   * 时间复杂度：O(V+E)，V=任务数，E=依赖边数
   *
   * @return 检测到的所有循环列表，每个循环是一个任务 ID 列表
   */
  private List<List<String>> detectCycles() {
    Set<String> visited = new HashSet<>();
    Set<String> recStack = new HashSet<>();
    List<List<String>> cycles = new ArrayList<>();

    for (String taskId : taskInfos.keySet()) {
      if (!visited.contains(taskId)) {
        List<String> path = new ArrayList<>();
        dfsCycleDetect(taskId, visited, recStack, path, cycles);
      }
    }
    return cycles;
  }

  /**
   * DFS 递归方法，检测从指定任务开始的循环依赖
   *
   * @param taskId 当前访问的任务 ID
   * @param visited 已访问节点集合（全局）
   * @param recStack 当前递归路径上的节点集合（用于检测反向边）
   * @param path 当前路径上的节点列表（用于提取循环）
   * @param cycles 存储检测到的循环列表
   * @return 如果检测到循环返回 true，否则返回 false
   */
  private boolean dfsCycleDetect(String taskId, Set<String> visited, Set<String> recStack,
                                 List<String> path, List<List<String>> cycles) {
    visited.add(taskId);
    recStack.add(taskId);
    path.add(taskId);

    TaskInfo info = taskInfos.get(taskId);
    if (info != null && info.dependencies != null) {
      for (String depId : info.dependencies) {
        TaskState depState = tasks.get(depId);
        // 仅检查未完成的依赖，已完成的任务不会导致死锁
        if (depState == null || depState.status.get() != TaskStatus.COMPLETED) {
          if (!visited.contains(depId)) {
            // 递归访问未访问的依赖
            if (dfsCycleDetect(depId, visited, recStack, path, cycles)) {
              return true;
            }
          } else if (recStack.contains(depId)) {
            // 发现反向边，提取循环路径
            int cycleStartIndex = path.indexOf(depId);
            List<String> cycle = new ArrayList<>(path.subList(cycleStartIndex, path.size()));
            cycles.add(cycle);
            return true;
          }
        }
      }
    }

    // 回溯：从路径和递归栈中移除当前节点
    path.remove(path.size() - 1);
    recStack.remove(taskId);
    return false;
  }

  /**
   * 计算 backoff 延迟（确定性版本）
   *
   * 使用 DeterminismContext 确保重放时 jitter 一致
   */
  private long calculateBackoff(int attempt, String strategy, long baseDelayMs, DeterminismContext ctx) {
    long normalizedAttempt = Math.max(1, attempt);
    long backoffBase;
    if ("exponential".equalsIgnoreCase(strategy)) {
      backoffBase = (long) (baseDelayMs * Math.pow(2, normalizedAttempt - 1));
    } else {
      backoffBase = baseDelayMs * normalizedAttempt;
    }
    long jitterBound = Math.max(0L, baseDelayMs / 2);
    // 优先使用传入的 ctx，否则使用实例字段（保证非 null）
    DeterminismContext targetCtx = ctx != null ? ctx : this.determinismContext;
    long jitter = 0L;
    if (jitterBound > 0) {
      long raw = targetCtx.random().nextLong("async-task-backoff");
      jitter = Math.floorMod(raw, jitterBound);
    }
    return backoffBase + jitter;
  }

  private void onTaskFailed(String taskId, Exception exception, boolean isReplay) {
    RetryPolicy policy = retryPolicies.get(taskId);
    if (policy == null) {
      throw new RuntimeException("Task failed: " + taskId, exception);
    }

    // 从 TaskInfo 获取任务专属的 workflowId，避免多 workflow 并发时全局字段被覆盖
    TaskInfo info = taskInfos.get(taskId);
    String taskWorkflowId = (info != null && info.workflowId != null) ? info.workflowId : this.workflowId;

    int attempt = attemptCounters.getOrDefault(taskId, 1);
    if (attempt >= policy.maxAttempts) {
      throw new MaxRetriesExceededException(policy.maxAttempts, exception.getMessage(), exception);
    }

    long delayMs = isReplay
        ? getBackoffFromLog(taskId, attempt, taskWorkflowId)
        : calculateBackoff(attempt, policy.backoff, policy.baseDelayMs, determinismContext);
    String failureReason = exception.getMessage() != null ? exception.getMessage() : exception.getClass().getSimpleName();

    if (eventStore != null && taskWorkflowId != null) {
      try {
        eventStore.appendEvent(
            taskWorkflowId,
            "RETRY_SCHEDULED",
            Map.of("taskId", taskId, "reason", failureReason),
            attempt + 1,
            delayMs,
            failureReason
        );
      } catch (Exception logEx) {
        logger.log(Level.WARNING, String.format("Failed to log retry event for task %s", taskId), logEx);
      }
    }

    if (taskWorkflowId == null) {
      throw new IllegalStateException("workflowId must be set before scheduling retries");
    }

    attemptCounters.put(taskId, attempt + 1);
    scheduleRetry(taskId, taskWorkflowId, delayMs, attempt + 1, failureReason);

    logger.info(String.format("Task %s failed (attempt %d/%d), retrying in %dms",
        taskId, attempt, policy.maxAttempts, delayMs));
  }

  /**
   * 从事件日志恢复重试状态
   *
   * @param events 事件列表（按序列号升序）
   */
  public void restoreRetryState(List<WorkflowEvent> events) {
    if (events == null || events.isEmpty()) {
      return;
    }
    for (WorkflowEvent event : events) {
      if ("RETRY_SCHEDULED".equals(event.getEventType())) {
        Object payload = event.getPayload();
        String taskId = null;
        if (payload instanceof Map<?, ?> map) {
          Object raw = map.get("taskId");
          if (raw != null) {
            taskId = raw.toString();
          }
        }
        if (taskId == null) {
          continue;
        }
        Integer attemptNumber = event.getAttemptNumber();
        if (attemptNumber != null) {
          // 使用 Math::max 确保记录的是"已消耗的最大尝试次数"，避免重试无限循环
          int normalizedAttempt = Math.max(1, attemptNumber - 1);
          int merged = attemptCounters.merge(taskId, normalizedAttempt, Math::max);
          logger.fine(String.format("Restored retry state for task %s: attempt=%d, backoff=%dms",
              taskId, merged, event.getBackoffDelayMs()));
        }
      }
    }
  }

  private long getBackoffFromLog(String taskId, int attempt, String taskWorkflowId) {
    if (eventStore == null || taskWorkflowId == null) {
      throw new IllegalStateException("eventStore 和 workflowId 未设置，无法从日志恢复 backoff");
    }
    List<WorkflowEvent> events = eventStore.getEvents(taskWorkflowId, 0L);
    int expectedAttemptNumber = attempt + 1;
    for (WorkflowEvent event : events) {
      if (!"RETRY_SCHEDULED".equals(event.getEventType())) {
        continue;
      }
      Integer attemptNumber = event.getAttemptNumber();
      if (!Objects.equals(attemptNumber, expectedAttemptNumber)) {
        continue;
      }
      Object payload = event.getPayload();
      if (payload instanceof Map<?, ?> map) {
        Object rawTaskId = map.get("taskId");
        if (rawTaskId != null && taskId.equals(rawTaskId.toString())) {
          Long delay = event.getBackoffDelayMs();
          if (delay != null) {
            return delay;
          }
        }
      }
    }
    throw new IllegalStateException("未找到任务 " + taskId + " 第 " + expectedAttemptNumber + " 次重试的 backoff 记录");
  }

  /**
   * 调度延迟重试任务
   *
   * @param workflowId workflow 唯一标识符
   * @param delayMs 延迟时间（毫秒）
   * @param attemptNumber 重试次数（从1开始）
   * @param failureReason 失败原因
   */
  public void scheduleRetry(String workflowId, long delayMs, int attemptNumber, String failureReason) {
    scheduleRetry(WORKFLOW_RETRY_TASK_ID, workflowId, delayMs, attemptNumber, failureReason);
  }

  public void scheduleRetry(String taskId, String workflowId, long delayMs, int attemptNumber, String failureReason) {
    if (workflowId == null) {
      throw new IllegalStateException("workflowId must be set before scheduling retries");
    }
    long triggerAt = this.determinismContext.clock().now().toEpochMilli() + Math.max(0L, delayMs);

    boolean shouldStartPoller = false;
    DelayedTask task = new DelayedTask(taskId, workflowId, triggerAt, attemptNumber, failureReason);
    delayQueueLock.lock();
    try {
      if (taskId != null && !WORKFLOW_RETRY_TASK_ID.equals(taskId)) {
        pendingRetryTasks.put(taskId, task);
      }
      delayQueue.offer(task);
      if (!running) {
        shouldStartPoller = true;
      }
      logger.fine(String.format("Scheduled retry for workflow %s in %dms (attempt %d)",
          workflowId, delayMs, attemptNumber));
    } finally {
      delayQueueLock.unlock();
    }

    if (shouldStartPoller) {
      startPolling();
    }
  }

  /**
   * 启动延迟任务轮询线程
   *
   * 注意：需在 WorkflowSchedulerService 启动时调用
   */
  public synchronized void startPolling() {
    if (running) {
      return;
    }

    running = true;
    pollThread = new Thread(this::pollDelayedTasks, "workflow-delay-poller");
    pollThread.setDaemon(true);
    pollThread.start();
    logger.info("Started workflow delay task poller");
  }

  /**
   * 停止延迟任务轮询线程
   *
   * 注意：需在 WorkflowSchedulerService 关闭时调用
   */
  public synchronized void stopPolling() {
    running = false;
    if (pollThread != null) {
      pollThread.interrupt();
      try {
        pollThread.join(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        pollThread = null;
      }
    }
    logger.info("Stopped workflow delay task poller");
  }

  /**
   * 后台轮询延迟任务队列，触发到期任务
   */
  private void pollDelayedTasks() {
    while (running) {
      try {
        delayQueueLock.lock();
        try {
          DelayedTask task = delayQueue.peek();
          long now = this.determinismContext.clock().now().toEpochMilli();

          if (task != null && task.triggerAtMs <= now) {
            delayQueue.poll();
            logger.fine(String.format("Triggering delayed retry for workflow %s (attempt %d)",
                task.workflowId, task.attemptNumber));

            resumeTask(task);
          }
        } finally {
          delayQueueLock.unlock();
        }

        // 轮询间隔 100ms
        Thread.sleep(100);

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Error polling delayed tasks", e);
      }
    }
  }

  /**
   * 恢复 workflow 执行（由 timer 触发）
   *
   * 仅调度属于当前 workflowId 的任务，避免跨 workflow 的错误唤醒。
   */
  private void resumeWorkflow(String workflowId, int attemptNumber) {
    logger.info(String.format("Resuming workflow %s (attempt %d)", workflowId, attemptNumber));
    for (String taskId : workflowRetryTasks) {
      TaskInfo info = taskInfos.get(taskId);
      // 仅调度属于当前 workflow 的任务
      if (info != null && workflowId.equals(info.workflowId)) {
        scheduleTask(taskId, null);
      }
    }
  }

  private void resumeTask(DelayedTask delayedTask) {
    String taskId = delayedTask.taskId;
    if (taskId == null || WORKFLOW_RETRY_TASK_ID.equals(taskId)) {
      resumeWorkflow(delayedTask.workflowId, delayedTask.attemptNumber);
      return;
    }
    DelayedTask current = pendingRetryTasks.get(taskId);
    if (current != delayedTask) {
      return;
    }
    scheduleTask(taskId, delayedTask);
  }

  private void scheduleTask(String taskId, DelayedTask sourceDelayedTask) {
    TaskInfo info = taskInfos.get(taskId);
    TaskState state = tasks.get(taskId);
    if (info == null || state == null) {
      return;
    }
    if (state.status.get() != TaskStatus.PENDING) {
      return;
    }
    if (!isDependencySatisfied(taskId)) {
      if (sourceDelayedTask != null) {
        requeueDelayedTask(sourceDelayedTask, 100L);
      } else {
        workflowRetryTasks.add(taskId);
      }
      return;
    }
    info.submitted.set(false);
    if (sourceDelayedTask != null) {
      pendingRetryTasks.remove(taskId, sourceDelayedTask);
    }
    workflowRetryTasks.remove(taskId);
    submitTask(info);
  }

  private void requeueDelayedTask(DelayedTask original, long additionalDelayMs) {
    if (original.taskId == null || WORKFLOW_RETRY_TASK_ID.equals(original.taskId)) {
      return;
    }
    long newTrigger = this.determinismContext.clock().now().toEpochMilli() + additionalDelayMs;
    DelayedTask updated = new DelayedTask(original.taskId, original.workflowId, newTrigger,
        original.attemptNumber, original.failureReason);
    pendingRetryTasks.put(original.taskId, updated);
    delayQueueLock.lock();
    try {
      delayQueue.offer(updated);
    } finally {
      delayQueueLock.unlock();
    }
  }

  /**
   * 读取线程池大小（环境变量 ASTER_THREAD_POOL_SIZE，默认 CPU 核心数）
   */
  private static int loadThreadPoolSize() {
    String env = System.getenv("ASTER_THREAD_POOL_SIZE");
    if (env == null || env.isEmpty()) {
      return Runtime.getRuntime().availableProcessors();
    }
    try {
      int size = Integer.parseInt(env);
      if (size > 0) {
        return size;
      }
      System.err.println("警告：ASTER_THREAD_POOL_SIZE 必须 > 0，使用默认值");
      return Runtime.getRuntime().availableProcessors();
    } catch (NumberFormatException e) {
      System.err.println("警告：ASTER_THREAD_POOL_SIZE 解析失败，使用默认值: " + env);
      return Runtime.getRuntime().availableProcessors();
    }
  }

  /**
   * 读取默认超时时间（环境变量 ASTER_DEFAULT_TIMEOUT_MS，0 表示无限制）
   */
  private static long loadDefaultTimeout() {
    String env = System.getenv("ASTER_DEFAULT_TIMEOUT_MS");
    if (env == null || env.isEmpty()) {
      return 0L;
    }
    try {
      long timeout = Long.parseLong(env);
      if (timeout >= 0) {
        return timeout;
      }
      System.err.println("警告：ASTER_DEFAULT_TIMEOUT_MS 必须 >= 0，使用默认值");
      return 0L;
    } catch (NumberFormatException e) {
      System.err.println("警告：ASTER_DEFAULT_TIMEOUT_MS 解析失败，使用默认值: " + env);
      return 0L;
    }
  }
}
