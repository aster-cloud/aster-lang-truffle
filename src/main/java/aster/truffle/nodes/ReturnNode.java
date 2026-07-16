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
  public ReturnNode(Node expr) { this.expr = expr; }
  public Object execute(VirtualFrame frame) {
    Object v = Exec.exec(expr, frame);
    // 步骤级 trace（M2.1b）：记 return 的最终值（matched=true=走到的返回点）。全局关时 PE 折叠 no-op。
    aster.truffle.trace.TraceAccess.record("return", "return value", v, true, 0);
    throw new ReturnException(v);
  }
  @Override public String toString() { return "ReturnNode"; }
}
