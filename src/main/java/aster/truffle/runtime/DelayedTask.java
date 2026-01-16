package aster.truffle.runtime;

/**
 * 延迟任务 - 用于实现重试 exponential backoff 延迟调度
 *
 * 设计要点：
 * 1. 实现 Comparable<DelayedTask> 接口，按 triggerAtMs 升序排序（PriorityQueue 要求）
 * 2. triggerAtMs 使用绝对时间戳（从 DeterminismContext.clock() 获取，支持重放一致性）
 * 3. 不可变对象（所有字段 final），线程安全
 */
public class DelayedTask implements Comparable<DelayedTask> {
  public final String taskId;
  public final String workflowId;
  public final long triggerAtMs;  // 绝对时间戳（毫秒）
  public final int attemptNumber;
  public final String failureReason;

  public DelayedTask(String taskId, String workflowId, long triggerAtMs, int attemptNumber, String failureReason) {
    this.taskId = taskId;
    this.workflowId = workflowId;
    this.triggerAtMs = triggerAtMs;
    this.attemptNumber = attemptNumber;
    this.failureReason = failureReason;
  }

  @Override
  public int compareTo(DelayedTask o) {
    // 按触发时间升序排序（最早触发的在队首）
    return Long.compare(this.triggerAtMs, o.triggerAtMs);
  }

  @Override
  public String toString() {
    return String.format("DelayedTask{taskId=%s, workflowId=%s, triggerAt=%d, attempt=%d, reason=%s}",
        taskId, workflowId, triggerAtMs, attemptNumber, failureReason);
  }
}
