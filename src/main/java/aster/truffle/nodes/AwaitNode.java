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

    // ★取到结果后 removeTask（issue #103 body），与 WaitNode 同理：
    //   StartNode 注册的任务此前没有任何路径会清理，池化 Context 下按 eval 次数
    //   无界累积（实测同一 Context 连跑 5 次 start+wait → taskCount 1→2→3→4→5）。
    //
    //   清理放在 pollUntilTerminal **之后**而非其内部：该方法是 AwaitNode 与
    //   WaitNode 共用的轮询工具，其语义应是「等到终态并取值」，
    //   删除任务是调用方的生命周期决定，不该由工具方法代做。
    //
    //   ★范围声明：只清理**成功**路径。FAILED/CANCELLED 时 pollUntilTerminal
    //   直接抛出，任务仍留在 registry——那是刻意的：workflow 补偿与错误诊断需要
    //   查它的状态与异常（WorkflowNode 的 finally 会统一清）。
    //   但对**不在 workflow 内**的裸 start/await，失败任务确实仍会残留，
    //   属本次未覆盖的部分，不假装已解决。
    Object result = pollUntilTerminal(registry, taskId);
    registry.removeTask(taskId);
    return result;
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
  /** 轮询让步时长：短睡眠替代 Thread.yield()，既省 CPU 又可被 interrupt 打断。 */
  static final long POLL_PARK_MILLIS = 1L;

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

      // ★让出 CPU 时必须**可中断**（issue #106 body）。
      //   原实现只有 Thread.yield()：它不响应中断，于是
      //   - 被 await 的任务若永远到不了终态，本线程就 100% CPU 自旋；
      //   - executor.shutdownNow() 发出的 interrupt 被完全无视，
      //     awaitTermination(30s) 必然超时 → 非守护线程永久泄漏、阻止 JVM 退出，
      //     disposeContext 被硬拖 30 秒。
      //
      //   改为 Thread.sleep(park) 并显式检查中断：
      //   - sleep 会抛 InterruptedException，shutdownNow 得以真正终止本线程；
      //   - 短睡眠（1ms）比 yield 更省 CPU，且对 workflow 这种毫秒级调度粒度无影响。
      //   捕获后**恢复中断位**再抛，不吞掉中断信号。
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Await interrupted while waiting for task: " + taskId);
      }
      try {
        Thread.sleep(POLL_PARK_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();   // 恢复中断位，交给上层决定
        throw new RuntimeException("Await interrupted while waiting for task: " + taskId, e);
      }
    }
  }
}
