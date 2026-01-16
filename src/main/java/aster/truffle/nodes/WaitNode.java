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

  // For backward compatibility with existing code
  public WaitNode(String[] taskIdNames) {
    this(null, taskIdNames);
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

    // Phase 1: 轮询等待所有任务完成
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

        // 任务尚未完成
        if (status != TaskStatus.COMPLETED) {
          allCompleted = false;
        }
      }

      // 所有任务都已完成
      if (allCompleted) {
        // 单任务场景直接返回对应结果，多任务保持 taskIdNames 顺序返回结果数组
        if (taskIds.length == 1) {
          Object result = registry.getResult(taskIds[0]);
          if (env != null) {
            env.set(taskIdNames[0], result);
          }
          return result;
        }

        Object[] results = new Object[taskIds.length];
        for (int i = 0; i < taskIds.length; i++) {
          Object result = registry.getResult(taskIds[i]);
          results[i] = result;
          if (env != null) {
            env.set(taskIdNames[i], result);
          }
        }
        return results;
      }

      // 调度下一个任务并继续等待
      registry.executeNext();

      // 避免忙等，让出 CPU
      Thread.yield();
    }
  }
}
