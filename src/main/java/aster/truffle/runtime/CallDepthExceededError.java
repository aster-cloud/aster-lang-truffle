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
 * 不收集栈轨迹：抛出点位于上万层 guest 帧之下，填栈既慢又无诊断价值。
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
