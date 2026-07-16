package aster.truffle.nodes;

import aster.truffle.runtime.AsterConfig;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

/**
 * 条件分支节点 - 利用 Truffle DSL 针对布尔条件进行特化，并提供通用回退。
 */
@NodeChild(value = "condNode", type = AsterExpressionNode.class)
public abstract class IfNode extends AsterExpressionNode {
  @Child private Node thenNode;
  @Child private Node elseNode;

  protected IfNode(Node thenNode, Node elseNode) {
    this.thenNode = thenNode;
    this.elseNode = elseNode;
  }

  public static IfNode create(AsterExpressionNode cond, Node thenNode, Node elseNode) {
    return IfNodeGen.create(thenNode, elseNode, cond);
  }

  @Specialization
  protected Object doBooleanCond(VirtualFrame frame, boolean condValue) {
    Profiler.inc("if");
    if (AsterConfig.DEBUG) {
      logDebug(Boolean.valueOf(condValue), condValue);
    }
    return executeBranch(condValue, frame);
  }

  @Specialization(replaces = "doBooleanCond")
  protected Object doGenericCond(VirtualFrame frame, Object condValue) {
    Profiler.inc("if");
    boolean boolValue = Exec.toBool(condValue);
    if (AsterConfig.DEBUG) {
      logDebug(condValue, boolValue);
    }
    return executeBranch(boolValue, frame);
  }

  private Object executeBranch(boolean condValue, VirtualFrame frame) {
    // 步骤级 trace（M2.1b）：记条件求值 + 走了哪支。TraceAccess.record 全局关时 PE 折叠为 no-op。
    // depth 传 0（扁平列表模型，children 恒空；maxSteps 为主上限，maxDepth 为保留次级护栏）。
    aster.truffle.trace.TraceAccess.record("if", "if condition", condValue, condValue, 0);
    Node target = condValue ? thenNode : elseNode;
    if (target == null) {
      return null;
    }
    return Exec.exec(target, frame);
  }

  private void logDebug(Object condValue, boolean boolValue) {
    System.err.println("DEBUG: if condition=" + condValue + " => " + boolValue +
        ", thenNode=" + simpleName(thenNode) +
        ", elseNode=" + simpleName(elseNode));
  }

  private static String simpleName(Node node) {
    return node != null ? node.getClass().getSimpleName() : "null";
  }
}
