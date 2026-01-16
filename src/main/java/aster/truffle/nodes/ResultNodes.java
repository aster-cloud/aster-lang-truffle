package aster.truffle.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

public final class ResultNodes {
  private ResultNodes() {}

  public static final class OkNode extends AsterExpressionNode {
    @Child private AsterExpressionNode expr;
    public OkNode(AsterExpressionNode expr) { this.expr = expr; }
    @Override
    public Object executeGeneric(VirtualFrame frame) {
      Profiler.inc("ok");
      Object value = Exec.exec(expr, frame);
      return createResult("Ok", value);
    }
  }
  public static final class ErrNode extends AsterExpressionNode {
    @Child private AsterExpressionNode expr;
    public ErrNode(AsterExpressionNode expr) { this.expr = expr; }
    @Override
    public Object executeGeneric(VirtualFrame frame) {
      Profiler.inc("err");
      Object value = Exec.exec(expr, frame);
      return createResult("Err", value);
    }
  }

  public static final class SomeNode extends AsterExpressionNode {
    @Child private AsterExpressionNode expr;
    public SomeNode(AsterExpressionNode expr) { this.expr = expr; }
    @Override
    public Object executeGeneric(VirtualFrame frame) {
      Profiler.inc("some");
      Object value = Exec.exec(expr, frame);
      return createResult("Some", value);
    }
  }
  public static final class NoneNode extends AsterExpressionNode {
    @Override
    public Object executeGeneric(VirtualFrame frame) {
      Profiler.inc("none");
      return createResult("None");
    }
  }

  private static java.util.Map<String,Object> createResult(String type, Object value) {
    java.util.LinkedHashMap<String,Object> map = new java.util.LinkedHashMap<>();
    map.put("_type", type);
    map.put("value", value);
    return map;
  }

  private static java.util.Map<String,Object> createResult(String type) {
    java.util.LinkedHashMap<String,Object> map = new java.util.LinkedHashMap<>();
    map.put("_type", type);
    return map;
  }
}
