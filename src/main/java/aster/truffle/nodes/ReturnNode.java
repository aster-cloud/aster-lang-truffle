package aster.truffle.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ControlFlowException;
import com.oracle.truffle.api.nodes.Node;

public final class ReturnNode extends Node {
  public static final class ReturnException extends ControlFlowException {
    private static final long serialVersionUID = 1L;
    public final transient Object value;
    public ReturnException(Object v) { this.value = v; }
  }
  @Child private Node expr;
  /**
   * return 语句在源码中的行号（1-based；0=未知）。
   *
   * <p>与 {@link IfNode} 同理：此前记的是硬编码 {@code "return value"}，
   * 一条策略里 5 个不同的 Return 在漏斗里被并成一行。
   */
  private final int sourceLine;
  /** 预拼好的 trace 标签，避免热路径拼串。 */
  private final String traceLabel;

  public ReturnNode(Node expr) { this(expr, 0); }

  public ReturnNode(Node expr, int sourceLine) {
    this.expr = expr;
    this.sourceLine = sourceLine;
    this.traceLabel = sourceLine > 0 ? "return value @L" + sourceLine : "return value";
  }
  public Object execute(VirtualFrame frame) {
    Object v = Exec.exec(expr, frame);
    // 步骤级 trace（M2.1b）：记 return 的最终值（matched=true=走到的返回点）。全局关时 PE 折叠 no-op。
    aster.truffle.trace.TraceAccess.record("return", traceLabel, v, true, 0);
    throw new ReturnException(v);
  }
  @Override public String toString() { return "ReturnNode"; }
}
