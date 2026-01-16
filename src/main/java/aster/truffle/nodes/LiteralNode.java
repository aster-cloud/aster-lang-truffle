package aster.truffle.nodes;

import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.dsl.Idempotent;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;

/**
 * 字面量节点 - 使用 Truffle DSL 进行类型特化，避免通用类型装箱成本。
 */
public abstract class LiteralNode extends AsterExpressionNode {
  private enum ValueKind {
    INT, LONG, DOUBLE, STRING, BOOLEAN, OBJECT
  }

  @CompilationFinal private final Object value;
  @CompilationFinal private final ValueKind kind;

  protected LiteralNode(int value) { this.value = value; this.kind = ValueKind.INT; }
  protected LiteralNode(long value) { this.value = value; this.kind = ValueKind.LONG; }
  protected LiteralNode(double value) { this.value = value; this.kind = ValueKind.DOUBLE; }
  protected LiteralNode(String value) { this.value = value; this.kind = ValueKind.STRING; }
  protected LiteralNode(boolean value) { this.value = value; this.kind = ValueKind.BOOLEAN; }
  protected LiteralNode(Object value) { this.value = value; this.kind = ValueKind.OBJECT; }

  public static LiteralNode create(Object value) {
    if (value instanceof Integer i) {
      return LiteralNodeGen.create(i.intValue());
    } else if (value instanceof Long l) {
      return LiteralNodeGen.create(l.longValue());
    } else if (value instanceof Double d) {
      return LiteralNodeGen.create(d.doubleValue());
    } else if (value instanceof String s) {
      return LiteralNodeGen.create(s);
    } else if (value instanceof Boolean b) {
      return LiteralNodeGen.create(b.booleanValue());
    }
    return LiteralNodeGen.create(value);
  }

  @Specialization(guards = "isInt()")
  protected int doInt() {
    Profiler.inc("literal");
    return ((Integer) value).intValue();
  }

  @Specialization(guards = "isLong()")
  protected long doLong() {
    Profiler.inc("literal");
    return ((Long) value).longValue();
  }

  @Specialization(guards = "isDouble()")
  protected double doDouble() {
    Profiler.inc("literal");
    return ((Double) value).doubleValue();
  }

  @Specialization(guards = "isString()")
  protected String doString() {
    Profiler.inc("literal");
    return (String) value;
  }

  @Specialization(guards = "isBoolean()")
  protected boolean doBoolean() {
    Profiler.inc("literal");
    return ((Boolean) value).booleanValue();
  }

  @Specialization(replaces = {"doInt", "doLong", "doDouble", "doString", "doBoolean"})
  protected Object doGeneric() {
    Profiler.inc("literal");
    return value;
  }

  @Idempotent protected boolean isInt() { return kind == ValueKind.INT; }
  @Idempotent protected boolean isLong() { return kind == ValueKind.LONG; }
  @Idempotent protected boolean isDouble() { return kind == ValueKind.DOUBLE; }
  @Idempotent protected boolean isString() { return kind == ValueKind.STRING; }
  @Idempotent protected boolean isBoolean() { return kind == ValueKind.BOOLEAN; }
}
