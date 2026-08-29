package aster.truffle.nodes;

import aster.truffle.AsterLanguage;
import aster.truffle.AsterContext;
import aster.truffle.runtime.AsyncTaskRegistry;
import aster.truffle.runtime.AsyncTaskRegistry.TaskState;
import aster.truffle.runtime.AsyncTaskRegistry.TaskStatus;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

/**
 * Wait节点 - 等待多个异步任务完成并返回结果
 *
 * Phase 1 实现：
 * - 接收多个 task_id 变量名作为输入
 * - 从 Env 读取这些变量的值（task_id）
 * - 轮询 AsyncTaskRegistry 直到所有任务完成
 * - 在轮询过程中调用 executeNext() 调度待执行任务
 * - 任何任务 FAILED 时抛出异常
 * - 所有任务完成后返回对应结果（单任务返回单值，多任务返回结果数组）
 *
 * 注意：此节点不使用 @Child 注解，因为它在构造时就确定了要等待的变量名列表。
 * 这些变量名会在运行时通过 Env 查找对应的 task_id 值。
 */
public final class WaitNode extends Node {
  private final Env env;
  private final String[] taskIdNames;

  public WaitNode(Env env, String[] taskIdNames) {
    this.env = env;
    this.taskIdNames = taskIdNames;
  }

  public Object execute(VirtualFrame frame) {
    Profiler.inc("wait");

    // 如果没有任务需要等待，直接返回
    if (taskIdNames == null || taskIdNames.length == 0) {
      return null;
    }

    // 获取 AsyncTaskRegistry
    AsterContext context = AsterLanguage.getContext();
    AsyncTaskRegistry registry = context.getAsyncRegistry();

    // 从 Env 中读取所有 task_id
    // 注意：这些变量应该已经通过 Start 节点设置为 task_id 字符串
    String[] taskIds = new String[taskIdNames.length];
    for (int i = 0; i < taskIdNames.length; i++) {
      Object taskIdObj = (env != null) ? env.get(taskIdNames[i]) : null;
      if (!(taskIdObj instanceof String)) {
        throw new RuntimeException("wait expects task_id (String) for variable '" + taskIdNames[i] +
            "', got: " + (taskIdObj == null ? "null" : taskIdObj.getClass().getName()));
      }
      taskIds[i] = (String) taskIdObj;
    }

    // Phase 1: 轮询等待所有任务进入终态（CANCELLED/FAILED 抛错，全部 COMPLETED 才返回）
    pollUntilAllTerminal(registry, taskIds);

    // ★取到结果后必须 removeTask（issue #103 body）。
    //
    //   StartNode 注册的任务此前**没有任何路径**会清理：removeTask 的唯一调用点是
    //   WorkflowNode 的 finally，gc() 的唯一调用点在测试里。于是 start/await 注册的
    //   任务完成后，tasks / taskInfos / DependencyGraph.nodes / completedNodes
    //   全部随 Context 终身存活。
    //
    //   实测（同一 Context 连跑 5 次 start+wait）：taskCount 1→2→3→4→5，
    //   **每次 eval 线性泄漏一个**，即便任务已被正确 await。
    //   TaskInfo.callable 闭包持有 MaterializedFrame 与结果对象，
    //   宿主池化 Context（aster-api 用 pooled Context）下按 eval 次数无界累积。
    //
    //   ★必须在 getResult 之后再删：removeTask 会清掉 taskInfos/tasks，
    //   先删就取不到结果了。
    //
    //   顺带消除「跨 eval 幽灵执行」：残留的无依赖 PENDING 任务会被后续无关 eval 的
    //   AwaitNode → executeNext 捞起来执行（它不按 eval/workflow 过滤）。
    //
    // 单任务场景直接返回对应结果，多任务保持 taskIdNames 顺序返回结果数组
    if (taskIds.length == 1) {
      Object result = registry.getResult(taskIds[0]);
      registry.removeTask(taskIds[0]);
      if (env != null) {
        env.set(taskIdNames[0], result);
      }
      return result;
    }

    Object[] results = new Object[taskIds.length];
    for (int i = 0; i < taskIds.length; i++) {
      Object result = registry.getResult(taskIds[i]);
      registry.removeTask(taskIds[i]);
      results[i] = result;
      if (env != null) {
        env.set(taskIdNames[i], result);
      }
    }
    return results;
  }

  /**
   * 轮询等待所有任务进入终态（Phase 1 协作式调度）。
   *
   * <p>提取为包级静态方法以便直接回归测试（无需进入 polyglot 上下文）。任一任务
   * FAILED → 抛 "Async task failed"；任一任务 CANCELLED → 抛 "Async task cancelled"
   * （#43 HIGH：CANCELLED 既非 COMPLETED 也非 FAILED，若不在此处终止会永久忙等）。
   * 全部 COMPLETED 才返回；否则 {@code executeNext()} 推进调度后继续轮询。
   */
  /** 轮询让步时长：与 AwaitNode 同源（AwaitNode.POLL_PARK_MILLIS）。 */
  static void pollUntilAllTerminal(AsyncTaskRegistry registry, String[] taskIds) {
    while (true) {
      boolean allCompleted = true;

      for (String taskId : taskIds) {
        TaskState state = registry.getTaskState(taskId);

        // 检查任务是否存在
        if (state == null) {
          throw new RuntimeException("Task not found: " + taskId);
        }

        TaskStatus status = state.getStatus();

        // 任务失败 - 抛出异常
        if (status == TaskStatus.FAILED) {
          Throwable exception = state.getException();
          throw new RuntimeException("Async task failed: " + taskId, exception);
        }

        // 任务被取消 - 终态（#43 HIGH）
        if (status == TaskStatus.CANCELLED) {
          throw new RuntimeException("Async task cancelled: " + taskId);
        }

        // 任务尚未完成
        if (status != TaskStatus.COMPLETED) {
          allCompleted = false;
        }
      }

      // 所有任务都已完成
      if (allCompleted) {
        return;
      }

      // 调度下一个任务并继续等待
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
        throw new RuntimeException("Await interrupted while waiting for task: " + java.util.Arrays.toString(taskIds));
      }
      try {
        Thread.sleep(AwaitNode.POLL_PARK_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();   // 恢复中断位，交给上层决定
        throw new RuntimeException("Await interrupted while waiting for task: " + java.util.Arrays.toString(taskIds), e);
      }
    }
  }
}
