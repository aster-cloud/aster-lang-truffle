package aster.truffle.runtime;

import io.aster.workflow.DeterminismContext;

/**
 * 工作流调度器 - 仅负责驱动 AsyncTaskRegistry
 *
 * Phase 2.5 目标：
 * - 彻底移除 Scheduler 内部的依赖图副本
 * - 依赖感知调度完全交由 AsyncTaskRegistry 负责
 * - Scheduler 仅负责 fail-fast 包装与向后兼容 API
 */
public final class WorkflowScheduler {
  private final AsyncTaskRegistry registry;
  private final String workflowId;
  private final PostgresEventStore eventStore;
  private final DeterminismContext determinismContext;

  public WorkflowScheduler(AsyncTaskRegistry registry) {
    this(registry, null, null, registry != null ? registry.getDeterminismContext() : new DeterminismContext());
  }

  /**
   * 构造工作流调度器
   *
   * @param registry 异步任务注册表
   * @param workflowId workflow 唯一标识符
   * @param eventStore PostgreSQL 事件存储
   */
  public WorkflowScheduler(AsyncTaskRegistry registry, String workflowId, PostgresEventStore eventStore) {
    this(registry, workflowId, eventStore, registry != null ? registry.getDeterminismContext() : new DeterminismContext());
  }

  /**
   * 构造工作流调度器（重放模式需要注入 DeterminismContext）
   *
   * 注意：传入的 determinismContext 应与 registry 的 DeterminismContext 一致。
   * 如果不一致，将使用 registry 的实例以确保行为一致。
   */
  public WorkflowScheduler(AsyncTaskRegistry registry, String workflowId, PostgresEventStore eventStore,
      DeterminismContext determinismContext) {
    if (registry == null) {
      throw new IllegalArgumentException("registry cannot be null");
    }
    this.registry = registry;
    this.workflowId = workflowId;
    this.eventStore = eventStore;

    // 使用 registry 的 DeterminismContext 以确保一致性
    DeterminismContext registryContext = registry.getDeterminismContext();
    if (determinismContext != null && registryContext != determinismContext) {
      System.getLogger(WorkflowScheduler.class.getName()).log(System.Logger.Level.WARNING,
          "Provided DeterminismContext differs from registry's context. Using registry's context for consistency.");
    }
    this.determinismContext = registryContext;

    if (workflowId != null) {
      registry.setWorkflowId(workflowId);
    }
    if (eventStore != null) {
      registry.setEventStore(eventStore);
    }
  }

  /**
   * 执行所有工作流任务直至完成（依赖 AsyncTaskRegistry 的并发调度）
   *
   * @throws RuntimeException 透传原始异常类型，保留完整堆栈信息
   */
  public void executeUntilComplete() {
    try {
      registry.executeUntilComplete();
    } catch (RuntimeException e) {
      // 直接透传 RuntimeException，保留原始类型
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Workflow execution failed", e);
    }
  }

  /**
   * 执行所有工作流任务直至完成，带全局超时控制
   *
   * @param timeoutMs 全局超时时间（毫秒）
   * @throws RuntimeException 如果工作流执行失败或超时
   */
  public void executeWithTimeout(long timeoutMs) {
    if (timeoutMs <= 0) {
      executeUntilComplete();
      return;
    }

    java.util.concurrent.CompletableFuture<Void> executionFuture =
        java.util.concurrent.CompletableFuture.runAsync(() -> {
          try {
            registry.executeUntilComplete();
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException("Workflow execution failed", e);
          }
        });

    try {
      executionFuture.get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.TimeoutException e) {
      // 超时时取消所有未完成的任务并等待它们真正停止
      registry.cancelAll();
      // 等待任务真正停止，最多再等 5 秒（防止后台任务继续执行）
      boolean quiesced = registry.awaitQuiescent(5000);
      executionFuture.cancel(true);
      String message = "Workflow execution timed out after " + timeoutMs + "ms";
      if (!quiesced) {
        message += " (warning: some tasks may still be running)";
      }
      throw new RuntimeException(message, e);
    } catch (java.util.concurrent.ExecutionException e) {
      // 透传原始异常类型
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new RuntimeException("Workflow execution failed", cause);
    } catch (InterruptedException e) {
      // 中断时也取消所有任务并等待它们停止
      registry.cancelAll();
      boolean quiesced = registry.awaitQuiescent(5000);
      Thread.currentThread().interrupt();
      String message = "Workflow execution interrupted";
      if (!quiesced) {
        message += " (warning: some tasks may still be running)";
      }
      throw new RuntimeException(message, e);
    }
  }

  /**
   * 调度 workflow 重试（带延迟）
   *
   * @param workflowId workflow 唯一标识符
   * @param delayMs 延迟时间（毫秒）
   * @param attemptNumber 重试次数
   * @param failureReason 失败原因
   */
  public void scheduleRetry(String workflowId, long delayMs, int attemptNumber, String failureReason) {
    registry.scheduleRetry(workflowId, delayMs, attemptNumber, failureReason);
  }

  /**
   * 获取关联的任务注册表
   *
   * @return 任务注册表
   */
  public AsyncTaskRegistry getRegistry() {
    return registry;
  }

  public String getWorkflowId() {
    return workflowId;
  }

  public PostgresEventStore getEventStore() {
    return eventStore;
  }

  public DeterminismContext getDeterminismContext() {
    return determinismContext;
  }
}
