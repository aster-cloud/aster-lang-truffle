package aster.truffle.nodes;

import aster.truffle.AsterLanguage;
import aster.truffle.AsterContext;
import aster.truffle.runtime.AsyncTaskRegistry;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.frame.MaterializedFrame;
import com.oracle.truffle.api.nodes.Node;
import java.util.Set;

/**
 * StartNode - 异步任务启动节点
 *
 * Phase 1 实现：
 * - 将子表达式包装为 Runnable 并注册到 AsyncTaskRegistry
 * - 返回 task_id 字符串
 * - 校验 Async effect 权限
 * - 使用 MaterializedFrame 捕获当前 Frame 上下文
 */
public final class StartNode extends Node {
  private final Env env;
  private final String name;
  @Child private Node expr;

  public StartNode(Env env, String name, Node expr) {
    this.env = env;
    this.name = name;
    this.expr = expr;
  }

  public Object execute(VirtualFrame frame) {
    Profiler.inc("start");

    // Effect 校验：start 需要 Async effect
    AsterContext context = AsterLanguage.getContext();
    if (!context.isEffectAllowed("Async")) {
      throw new RuntimeException("start requires Async effect");
    }

    // 捕获 Frame：Truffle Frame 不能直接跨线程传递，需要 materialize
    MaterializedFrame materializedFrame = frame.materialize();

    // 捕获当前 effect 权限：异步任务需要继承父上下文的 effect 权限
    Set<String> capturedEffects = context.getAllowedEffects();

    // 生成 task_id（需要在 lambda 之前，因为 lambda 内部会引用它）
    AsyncTaskRegistry registry = context.getAsyncRegistry();
    String taskId = context.generateTaskId();

    // 包装任务：将子表达式包装为 Runnable
    Runnable task = () -> {
      try {
        // 保存当前 effect 权限
        Set<String> previousEffects = context.getAllowedEffects();
        try {
          // 恢复捕获的 effect 权限，使任务体能够执行需要特定 effect 的操作
          context.setAllowedEffects(capturedEffects);
          // 在 materializedFrame 上下文中执行子表达式并捕获结果
          Object result = Exec.exec(expr, materializedFrame);
          // 存储任务结果
          AsyncTaskRegistry registryInTask = context.getAsyncRegistry();
          registryInTask.setResult(taskId, result);
        } finally {
          // 恢复之前的 effect 权限
          context.setAllowedEffects(previousEffects);
        }
      } catch (Throwable t) {
        // 异常会被 AsyncTaskRegistry.executeNext() 捕获并存储在 TaskState
        throw new RuntimeException("Async task failed", t);
      }
    };

    // 注册任务
    registry.registerTask(taskId, task);

    // 将 task_id 绑定到变量名
    if (name != null) {
      env.set(name, taskId);
    }

    return taskId;
  }
}

