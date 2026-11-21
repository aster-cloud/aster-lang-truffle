package aster.truffle.runtime;

/**
 * guest 调用深度超过 {@link AllocationBudget#MAX_CALL_DEPTH}（与 aster-lang-ts 包格式 CLI 的 {@code maxCallDepth} 同值、同消息）。
 *
 * <p><b>为什么继承 {@link StackOverflowError}</b>：它表达的正是「guest 调用栈超限」，下游必须与真实栈溢出走同一条路：
 * <ul>
 *   <li>节点与 builtin 里大量 {@code catch (RuntimeException)} 会把异常包成「Builtin call failed …」再抛，若是普通异常，
 *       经 {@code List.map} 回调触发的深度超限就会变成 {@code EVAL_ERROR}；{@link Error} 不被这些分支拦截；</li>
 *   <li>Polyglot 把宿主侧的栈溢出报告为 {@code PolyglotException#isResourceExhausted()}，{@code SandboxedExecutor}
 *       据此归为 {@code RESOURCE_EXHAUSTED}——与 Node CLI 的 {@code ResourceExhaustedError} 同码；aster-api 的平台求值同理。</li>
 * </ul>
 * workflow step 与 {@code start} 异步任务在 executor 线程上运行，各自从 0 计数；它们的边界把任何 {@link Throwable}
 * 包成 {@code RuntimeException} 跨线程传回（真实栈溢出在那里同样被包装，属既有行为）。导出的策略包不含 workflow / await
 * （ADR 0059 §5.1，E730–E733 在导出时拒绝），所以 CLI 求值不会经过这两条边界。
 *
 * <p>不收集栈轨迹：抛出点位于上万层 guest 帧之下，填栈既慢又无诊断价值。
 */
public final class CallDepthExceededError extends StackOverflowError {
  private static final long serialVersionUID = 1L;

  public CallDepthExceededError(int limit) {
    super("Maximum call depth (" + limit + ") exceeded");
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }
}
