package aster.truffle.nodes;

import aster.truffle.AsterLanguage;
import aster.truffle.AsterContext;
import aster.truffle.runtime.AsyncTaskRegistry;
import aster.truffle.runtime.AsyncTaskRegistry.TaskState;
import aster.truffle.runtime.AsyncTaskRegistry.TaskStatus;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

import com.oracle.truffle.api.dsl.Specialization;

/**
 * Await表达式节点 - 等待异步任务完成并返回结果
 *
 * Phase 1 实现：
 * - 接收 task_id 作为输入
 * - 轮询 AsyncTaskRegistry 直到任务完成
 * - 在轮询过程中调用 executeNext() 调度待执行任务
 * - COMPLETED 时返回结果，FAILED 时抛出异常
 *
 * Phase 2.4 优化：
 * - 改为 abstract class + @Specialization
 * - 保持单一执行路径（异步轮询逻辑）
 */
public abstract class AwaitNode extends AsterExpressionNode {
  @Child private AsterExpressionNode taskIdExpr;

  protected AwaitNode(AsterExpressionNode taskIdExpr) {
    this.taskIdExpr = taskIdExpr;
  }

  public static AwaitNode create(AsterExpressionNode taskIdExpr) {
    return AwaitNodeGen.create(taskIdExpr);
  }

  @Specialization
  protected Object doAwait(VirtualFrame frame) {
    Profiler.inc("await");

    // 获取 task_id
    Object taskIdObj = Exec.exec(taskIdExpr, frame);
    if (!(taskIdObj instanceof String)) {
      throw new RuntimeException("await expects task_id (String), got: " +
          (taskIdObj == null ? "null" : taskIdObj.getClass().getName()));
    }
    String taskId = (String) taskIdObj;

    // 获取 AsyncTaskRegistry
    AsterContext context = AsterLanguage.getContext();
    AsyncTaskRegistry registry = context.getAsyncRegistry();

    return pollUntilTerminal(registry, taskId);
  }

  /**
   * 轮询单个任务直到进入终态（Phase 1 协作式调度）。
   *
   * <p>提取为包级静态方法以便直接回归测试（无需进入 polyglot 上下文）。终态判定：
   * <ul>
   *   <li>COMPLETED → 返回结果</li>
   *   <li>FAILED → 抛 "Async task failed"</li>
   *   <li>CANCELLED → 抛 "Async task cancelled"（#43 HIGH：CANCELLED 既非 COMPLETED
   *       也非 FAILED，下游失败 / cancelAll() / 超时会把任务推进到 CANCELLED；若不在
   *       此处终止，轮询会永久忙等烧核）</li>
   * </ul>
   * PENDING/RUNNING 时调用 {@code executeNext()} 推进调度后继续轮询。
   */
  static Object pollUntilTerminal(AsyncTaskRegistry registry, String taskId) {
    while (true) {
      TaskState state = registry.getTaskState(taskId);

      // 检查任务是否存在
      if (state == null) {
        throw new RuntimeException("Task not found: " + taskId);
      }

      TaskStatus status = state.getStatus();

      // 任务已完成 - 返回结果
      if (status == TaskStatus.COMPLETED) {
        return state.getResult();
      }

      // 任务失败 - 抛出异常
      if (status == TaskStatus.FAILED) {
        Throwable exception = state.getException();
        throw new RuntimeException("Async task failed: " + taskId, exception);
      }

      // 任务被取消 - 终态（#43 HIGH）
      if (status == TaskStatus.CANCELLED) {
        throw new RuntimeException("Async task cancelled: " + taskId);
      }

      // 任务尚未完成 (PENDING 或 RUNNING) - 调度下一个任务并继续等待
      registry.executeNext();

      // 避免忙等，让出 CPU
      Thread.yield();
    }
  }
}
