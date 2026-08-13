package aster.truffle.nodes;

import aster.truffle.runtime.AsterConfig;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.nodes.Node;

/**
 * 条件分支节点 - 利用 Truffle DSL 针对布尔条件进行特化，并提供通用回退。
 */
@NodeChild(value = "condNode", type = AsterExpressionNode.class)
public abstract class IfNode extends AsterExpressionNode {
  @Child private Node thenNode;
  @Child private Node elseNode;
  /**
   * 预拼好的 trace 标签，形如 {@code "if condition @L15"}（行号未知时无后缀）。
   *
   * <p>★步骤级 trace 此前记的是硬编码字面量 {@code "if condition"}，
   * 于是一条策略里**所有** If 在漏斗聚合时被并成一行——
   * 4 个不同条件显示成一个 {@code 4/4 (100%)}，数字正确但毫无意义。
   * 带上行号后各条件自成一组（cloud 的聚合键本就是 stepId+expression）。
   *
   * <p>在构造期拼好而非每次 record 时拼：AST 节点复用，热路径不应做字符串拼接。
   * {@code @CompilationFinal} 让 PE 把它当常量折叠（与本仓 BuiltinCallNode 一致）。
   */
  @CompilationFinal private final String traceLabel;

  protected IfNode(Node thenNode, Node elseNode, int sourceLine) {
    this.thenNode = thenNode;
    this.elseNode = elseNode;
    this.traceLabel = sourceLine > 0 ? "if condition @L" + sourceLine : "if condition";
  }

  public static IfNode create(AsterExpressionNode cond, Node thenNode, Node elseNode, int sourceLine) {
    return IfNodeGen.create(thenNode, elseNode, sourceLine, cond);
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
    aster.truffle.trace.TraceAccess.record("if", traceLabel, condValue, condValue, 0);
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
