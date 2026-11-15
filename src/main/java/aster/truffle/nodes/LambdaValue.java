package aster.truffle.nodes;

import aster.truffle.AsterContext;
import aster.truffle.AsterLanguage;
import aster.truffle.purity.PurityAnalyzer;
import aster.truffle.runtime.AllocationBudget;
import aster.truffle.runtime.AsterConfig;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.interop.ArityException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.interop.UnsupportedTypeException;
import java.util.*;

/**
 * Lambda 值，实现 Truffle InteropLibrary 使其可从 Polyglot 调用
 */
@ExportLibrary(InteropLibrary.class)
public final class LambdaValue implements TruffleObject {
  private final List<String> params;
  private final List<String> captureNames;
  private final Object[] capturedValues;
  private final CallTarget callTarget;
  private final java.util.Set<String> requiredEffects;  // Effect 元数据（如 ["io"]）

  /**
   * 创建 LambdaValue (使用 CallTarget + 闭包捕获 + effects)
   *
   * @param params 参数名列表
   * @param captureNames 闭包变量名列表
   * @param capturedValues 闭包变量值数组
   * @param callTarget Lambda 的 CallTarget
   * @param requiredEffects 函数所需 effects（如 ["io"]）
   */
  public LambdaValue(List<String> params, List<String> captureNames, Object[] capturedValues, CallTarget callTarget, java.util.Set<String> requiredEffects) {
    this.params = params == null ? List.of() : params;
    this.captureNames = captureNames == null ? List.of() : captureNames;
    this.capturedValues = capturedValues != null ? capturedValues : new Object[0];
    this.callTarget = callTarget;
    this.requiredEffects = requiredEffects != null ? java.util.Set.copyOf(requiredEffects) : java.util.Set.of();
    // effectsKnown=false：Core IR 的 Lambda 目前不携带 effect 元数据，此处的
    // requiredEffects 只是构造点硬编码的空集，不能据此判定为纯（issue #53）。
    PurityAnalyzer.recordEffects(callTarget, this.requiredEffects, false);
  }

  /**
   * 简化构造器 (使用 CallTarget，无 effect 要求)
   *
   * @param params 参数名列表
   * @param captures 闭包变量映射
   * @param callTarget Lambda 的 CallTarget
   */
  public LambdaValue(List<String> params, Map<String,Object> captures, CallTarget callTarget) {
    this.params = params == null ? List.of() : params;
    this.captureNames = captures != null ? new java.util.ArrayList<>(captures.keySet()) : List.of();
    this.capturedValues = captures != null ? captures.values().toArray() : new Object[0];
    this.callTarget = callTarget;
    this.requiredEffects = java.util.Set.of();  // 默认无 effect 要求
    // effectsKnown=false：Core IR 的 Lambda 目前不携带 effect 元数据，此处的
    // requiredEffects 只是构造点硬编码的空集，不能据此判定为纯（issue #53）。
    PurityAnalyzer.recordEffects(callTarget, this.requiredEffects, false);
  }

  public CallTarget getCallTarget() {
    return callTarget;
  }

  public Object[] getCapturedValues() {
    return capturedValues;
  }

  /**
   * 获取函数所需的 effects 列表（不可变副本）
   */
  public java.util.Set<String> getRequiredEffects() {
    return requiredEffects;
  }

  // ==================== Truffle InteropLibrary 实现 ====================

  /**
   * 告知 Polyglot 此对象是可执行的
   */
  @ExportMessage
  boolean isExecutable() {
    return true;
  }

  /**
   * Polyglot 调用入口 - 执行 Lambda 函数
   *
   * @param args 调用参数数组
   * @return Lambda 执行结果
   */
  @ExportMessage
  Object execute(Object[] args) throws ArityException, UnsupportedTypeException, UnsupportedMessageException {
    // ★宿主调用边界按函数声明安装 effect 权限（#174）。有参入口经 ctx.eval 只取出本函数值，
    // 真正计算发生在宿主随后的 Value.execute(args)——走到这里，而不再经过 AsterRootNode，
    // 此前没有任何地方安装权限，入口声明的 io 因而不生效。guest 内部调用（CallNode、
    // List.map 等内建回调）直接调 CallTarget，不经过本消息，故嵌套调用沿用调用方权限，
    // 被调函数无从给自己授权。
    AsterContext context = AsterLanguage.getContext();
    Set<String> previousEffects = installHostCallEffects(context);
    try {
      // interop execute 是跨边界返回点：入口 lambda 返回 null 时须规整为 guest-null
      // （否则裸 null 在宿主 ToHostValue/asGuestValue 处违反契约或 NPE）。内部 apply()
      // 直调路径不经此，仍保留 raw null 供 List.map/Maybe.map 等下游按 == null 消费。
      return aster.truffle.runtime.interop.InteropValues.toInteropValue(apply(args, null));
    } finally {
      // 恢复调用前的线程权限视图：宿主线程回到调用前状态，不外泄到后续调用（fail-closed）。
      restoreEffects(context, previousEffects);
    }
  }

  /**
   * 按本函数的声明安装权限，返回调用前的权限视图。
   *
   * <p>宿主发起的最外层调用：权限 = 声明的 effects。效应变量（如 {@code E}）按原名放入，
   * 不匹配任何具体效应名，因而不授权任何具体效应（fail-closed，与 AsterRootNode 同一口径）；
   * 内部 lambda 没有效应声明，宿主直接调用时一律无权限。
   *
   * <p>guest 执行中经宿主回调再入（当前线程已在 guest 帧内）：权限 = 声明 ∩ 调用方现有权限，
   * 只能收窄不能放宽——否则 guest 可借宿主回调让一个声明了 io 的函数绕过入口声明。
   */
  @TruffleBoundary
  private Set<String> installHostCallEffects(AsterContext context) {
    Set<String> previous = context.getAllowedEffects();
    Set<String> granted = requiredEffects;
    if (AllocationBudget.isInsideGuestFrame()) {
      Set<String> narrowed = new HashSet<>(requiredEffects);
      narrowed.retainAll(previous);
      granted = narrowed;
    }
    context.setAllowedEffects(granted);
    return previous;
  }

  @TruffleBoundary
  private static void restoreEffects(AsterContext context, Set<String> previous) {
    context.setAllowedEffects(previous);
  }

  // ==================== 内部 API ====================

  /**
   * 调用 Lambda 函数
   *
   * @param args 调用参数数组
   * @param frame 当前 VirtualFrame（可能未使用，保留以兼容旧代码）
   * @return Lambda 执行结果
   */
  public Object apply(Object[] args, VirtualFrame frame) {
    Profiler.inc("lambda_apply");
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: lambda_apply params=" + params + ", captureNames=" + captureNames + ", args.length=" + (args != null ? args.length : 0));
    }

    // 准备参数数组：[param0, param1, ..., paramN, capture0, capture1, ...]
    int paramCount = args != null ? args.length : 0;
    int captureCount = capturedValues.length;
    Object[] callArgs = new Object[paramCount + captureCount];

    // 拷贝调用参数
    if (args != null) {
      System.arraycopy(args, 0, callArgs, 0, paramCount);
    }

    // 附加闭包捕获值
    System.arraycopy(capturedValues, 0, callArgs, paramCount, captureCount);

    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: lambda calling with " + callArgs.length + " args (params=" + paramCount + ", captures=" + captureCount + ")");
    }

    // 调用 Lambda 的 CallTarget
    try {
      Object result = callTarget.call(callArgs);
      if (AsterConfig.DEBUG) {
        System.err.println("DEBUG: lambda CallTarget returned=" + result);
      }
      return result;
    } catch (ReturnNode.ReturnException r) {
      return r.value;
    }
  }
}
