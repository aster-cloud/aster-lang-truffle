package aster.truffle.nodes;

import aster.truffle.AsterLanguage;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.dsl.Idempotent;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;
import java.util.List;

/**
 * LambdaNode - 在运行时创建 LambdaValue，动态捕获闭包变量
 */
public abstract class LambdaNode extends AsterExpressionNode {
  @CompilationFinal private final AsterLanguage language;
  @CompilationFinal private final Env env;
  @CompilationFinal private final List<String> params;
  @CompilationFinal private final List<String> captureNames;
  @Children private final AsterExpressionNode[] captureExprs;
  @CompilationFinal private final CallTarget callTarget;

  protected LambdaNode(
      AsterLanguage language,
      Env env,
      List<String> params,
      List<String> captureNames,
      AsterExpressionNode[] captureExprs,
      CallTarget callTarget) {
    this.language = language;
    this.env = env;
    this.params = params;
    this.captureNames = captureNames;
    this.captureExprs = captureExprs;
    this.callTarget = callTarget;
  }

  public static LambdaNode create(
      AsterLanguage language,
      Env env,
      List<String> params,
      List<String> captureNames,
      AsterExpressionNode[] captureExprs,
      CallTarget callTarget) {
    return LambdaNodeGen.create(language, env, params, captureNames, captureExprs, callTarget);
  }

  @Specialization(guards = "hasNoCaptures()")
  protected LambdaValue doNoClosure() {
    Profiler.inc("lambda_create");
    return new LambdaValue(params, captureNames, new Object[0], callTarget, java.util.Set.of());
  }

  @Specialization
  @ExplodeLoop
  protected LambdaValue doWithClosure(VirtualFrame frame) {
    Profiler.inc("lambda_create");
    Object[] capturedValues = new Object[captureExprs.length];
    for (int i = 0; i < captureExprs.length; i++) {
      capturedValues[i] = captureExprs[i].executeGeneric(frame);
    }
    return new LambdaValue(params, captureNames, capturedValues, callTarget, java.util.Set.of());
  }

  @Idempotent
  protected boolean hasNoCaptures() {
    return captureExprs.length == 0;
  }

  @Override
  public String toString() {
    return "LambdaNode(params=" + params + ", captures=" + captureNames + ")";
  }
}
