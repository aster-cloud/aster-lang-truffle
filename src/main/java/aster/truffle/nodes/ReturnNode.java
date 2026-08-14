package aster.truffle.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ControlFlowException;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.nodes.Node;

public final class ReturnNode extends Node {
  public static final class ReturnException extends ControlFlowException {
    private static final long serialVersionUID = 1L;
    public final transient Object value;
    public ReturnException(Object v) { this.value = v; }
  }
  @Child private Node expr;
  /**
   * 预拼好的 trace 标签，形如 {@code "return value @L22"}（行号未知时无后缀）。
   *
   * <p>与 {@link IfNode} 同理：此前记硬编码 {@code "return value"}，
   * 一条策略里多个不同的 Return 在漏斗里被并成一行。
   */
  @CompilationFinal private final String traceLabel;

  public ReturnNode(Node expr, int sourceLine) {
    this.expr = expr;
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
