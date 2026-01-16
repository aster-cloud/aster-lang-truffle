package aster.truffle.nodes;

import aster.truffle.purity.PurityAnalyzer;
import aster.truffle.runtime.AsterConfig;
import com.oracle.truffle.api.CallTarget;
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
  private final java.util.Set<String> requiredEffects;  // Effect 元数据（如 ["IO", "Async"]）

  /**
   * 创建 LambdaValue (使用 CallTarget + 闭包捕获 + effects)
   *
   * @param params 参数名列表
   * @param captureNames 闭包变量名列表
   * @param capturedValues 闭包变量值数组
   * @param callTarget Lambda 的 CallTarget
   * @param requiredEffects 函数所需 effects（如 ["IO", "Async"]）
   */
  public LambdaValue(List<String> params, List<String> captureNames, Object[] capturedValues, CallTarget callTarget, java.util.Set<String> requiredEffects) {
    this.params = params == null ? List.of() : params;
    this.captureNames = captureNames == null ? List.of() : captureNames;
    this.capturedValues = capturedValues != null ? capturedValues : new Object[0];
    this.callTarget = callTarget;
    this.requiredEffects = requiredEffects != null ? java.util.Set.copyOf(requiredEffects) : java.util.Set.of();
    PurityAnalyzer.recordEffects(callTarget, this.requiredEffects);
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
    PurityAnalyzer.recordEffects(callTarget, this.requiredEffects);
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
    return apply(args, null);
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
