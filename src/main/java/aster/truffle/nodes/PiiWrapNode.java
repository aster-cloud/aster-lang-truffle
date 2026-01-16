package aster.truffle.nodes;

import aster.truffle.core.CoreModel;
import aster.truffle.runtime.PiiSupport;
import com.oracle.truffle.api.frame.VirtualFrame;

/**
 * 根据声明类型对表达式结果做 PII 包装。
 */
public final class PiiWrapNode extends AsterExpressionNode {
  @Child private AsterExpressionNode valueNode;
  private final CoreModel.Type declaredType;

  private PiiWrapNode(AsterExpressionNode valueNode, CoreModel.Type declaredType) {
    this.valueNode = valueNode;
    this.declaredType = declaredType;
  }

  public static PiiWrapNode create(AsterExpressionNode valueNode, CoreModel.Type type) {
    return new PiiWrapNode(valueNode, type);
  }

  @Override
  public Object executeGeneric(VirtualFrame frame) {
    Object raw = Exec.exec(valueNode, frame);
    return PiiSupport.wrapValue(raw, declaredType);
  }
}
