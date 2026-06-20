package aster.truffle.nodes;

import aster.truffle.runtime.AsterConfig;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;

/**
 * 表达式级条件节点（ADR 0019 G2b）：{@code if cond then thenExpr else elseExpr}。
 *
 * <p>与语句级 {@link IfNode} 区别：thenExpr/elseExpr 是 {@link AsterExpressionNode}
 * （求值产出**值**），整个节点求值得到被选中分支的值——可作子表达式 / Return 值 /
 * Let 绑定右侧。else 必需（Core IR IfE 保证 thenE/elseE 都非空），不存在"无值分支"。
 *
 * <p>条件特化沿用 IfNode 模式：布尔条件走快速特化，非布尔走通用 {@code toBool} 回退。
 */
@NodeChild(value = "condNode", type = AsterExpressionNode.class)
public abstract class IfExprNode extends AsterExpressionNode {
  @Child private AsterExpressionNode thenNode;
  @Child private AsterExpressionNode elseNode;

  protected IfExprNode(AsterExpressionNode thenNode, AsterExpressionNode elseNode) {
    this.thenNode = thenNode;
    this.elseNode = elseNode;
  }

  public static IfExprNode create(AsterExpressionNode cond, AsterExpressionNode thenNode,
                                  AsterExpressionNode elseNode) {
    return IfExprNodeGen.create(thenNode, elseNode, cond);
  }

  @Specialization
  protected Object doBooleanCond(VirtualFrame frame, boolean condValue) {
    Profiler.inc("ifExpr");
    return executeBranch(condValue, frame);
  }

  @Specialization(replaces = "doBooleanCond")
  protected Object doGenericCond(VirtualFrame frame, Object condValue) {
    Profiler.inc("ifExpr");
    return executeBranch(Exec.toBool(condValue), frame);
  }

  private Object executeBranch(boolean condValue, VirtualFrame frame) {
    AsterExpressionNode target = condValue ? thenNode : elseNode;
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: ifExpr condition => " + condValue
          + ", branch=" + (condValue ? "then" : "else"));
    }
    return target.executeGeneric(frame);
  }
}
