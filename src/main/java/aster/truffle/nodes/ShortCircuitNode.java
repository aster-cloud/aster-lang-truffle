package aster.truffle.nodes;

import aster.truffle.runtime.Builtins;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.profiles.CountingConditionProfile;

/**
 * 逻辑 and/or 的短路求值节点。
 *
 * <p>右操作数只在左操作数不足以决定结果时求值：{@code and} 仅当左为真、{@code or} 仅当左为假。
 * 与 TS 解释器 {@code evalBuiltinOp} 的短路语义一致——规则可依赖左侧守卫保护右侧
 * （如 {@code List.length(xs) equals to 5 and List.get(xs, 4) ...}）。
 *
 * <p>不能并入 {@link BuiltinCallNode}：后者在调用 builtin 前求值全部参数。
 * 真值判定统一走 {@link Builtins#toBool}（含 Verdict 不可当布尔用的检查），结果恒为 Boolean。
 */
public abstract class ShortCircuitNode extends AsterExpressionNode {
  @Child private AsterExpressionNode left;
  @Child private AsterExpressionNode right;
  /** 记录右操作数被求值的频率，供编译器裁剪冷分支。 */
  private final CountingConditionProfile evaluateRightProfile = CountingConditionProfile.create();
  private final String profilerKey;

  protected ShortCircuitNode(AsterExpressionNode left, AsterExpressionNode right, String profilerKey) {
    this.left = left;
    this.right = right;
    this.profilerKey = profilerKey;
  }

  /** 左操作数取值为 {@code leftValue} 时是否还需要求值右操作数。 */
  protected abstract boolean isEvaluateRight(boolean leftValue);

  @Override
  public final Object executeGeneric(VirtualFrame frame) {
    return executeBoolean(frame);
  }

  @Override
  public final boolean executeBoolean(VirtualFrame frame) {
    Profiler.inc(profilerKey);
    boolean leftValue = Builtins.toBool(left.executeGeneric(frame));
    if (evaluateRightProfile.profile(isEvaluateRight(leftValue))) {
      return Builtins.toBool(right.executeGeneric(frame));
    }
    // 未求值右侧时结果即左值：and 为 false，or 为 true
    return leftValue;
  }

  /** {@code a and b}：左为真才求值右侧。 */
  public static final class AndNode extends ShortCircuitNode {
    public AndNode(AsterExpressionNode left, AsterExpressionNode right) {
      super(left, right, "and");
    }

    @Override
    protected boolean isEvaluateRight(boolean leftValue) {
      return leftValue;
    }
  }

  /** {@code a or b}：左为假才求值右侧。 */
  public static final class OrNode extends ShortCircuitNode {
    public OrNode(AsterExpressionNode left, AsterExpressionNode right) {
      super(left, right, "or");
    }

    @Override
    protected boolean isEvaluateRight(boolean leftValue) {
      return !leftValue;
    }
  }
}
