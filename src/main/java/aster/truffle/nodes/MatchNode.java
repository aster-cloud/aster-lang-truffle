package aster.truffle.nodes;

import aster.truffle.runtime.AsterConfig;
import aster.truffle.runtime.AsterDataValue;
import aster.truffle.runtime.AsterEnumValue;
import com.oracle.truffle.api.dsl.Idempotent;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

@NodeChild(value = "scrutineeNode", type = AsterExpressionNode.class)
public abstract class MatchNode extends AsterExpressionNode {
  private final Env env;
  @Children private final CaseNode[] cases;

  protected MatchNode(Env env, java.util.List<CaseNode> cases) {
    this.env = env;
    this.cases = cases.toArray(new CaseNode[0]);
  }

  public static MatchNode create(Env env, AsterExpressionNode scrutinee, java.util.List<CaseNode> cases) {
    return MatchNodeGen.create(env, cases, scrutinee);
  }

  @Specialization(guards = "isNull(scrutinee)")
  protected Object matchNull(VirtualFrame frame, Object scrutinee) {
    Profiler.inc("match");
    return executeCases(frame, scrutinee);
  }

  @Specialization(guards = "isMap(scrutinee)")
  protected Object matchMap(VirtualFrame frame, Object scrutinee) {
    Profiler.inc("match");
    return executeCases(frame, scrutinee);
  }

  @Specialization(replaces = {"matchNull", "matchMap"})
  protected Object matchGeneric(VirtualFrame frame, Object scrutinee) {
    Profiler.inc("match");
    return executeCases(frame, scrutinee);
  }

  private Object executeCases(VirtualFrame frame, Object scrutinee) {
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: match scrutinee=" + scrutinee);
    }
    for (CaseNode c : cases) {
      if (c.matchesAndBind(scrutinee, env)) {
        if (AsterConfig.DEBUG) {
          System.err.println("DEBUG: case matched");
        }
        return c.execute(frame);
      }
    }
    return null;
  }

  @Idempotent protected boolean isNull(Object value) {
    return value == null;
  }

  @Idempotent @SuppressWarnings("rawtypes")
  protected boolean isMap(Object value) {
    return value instanceof java.util.Map;
  }

  public static abstract class PatternNode extends Node {
    public abstract boolean matchesAndBind(Object s, Env env);
  }

  public static final class PatNullNode extends PatternNode {
    @Override public boolean matchesAndBind(Object s, Env env) { return s == null; }
  }

  // ctor match: match Data 值或旧 Map，按字段顺序依次绑定
  public static final class PatCtorNode extends PatternNode {
    private final String typeName;
    private final java.util.List<String> bindNames;
    private final java.util.List<PatternNode> args;
    public PatCtorNode(String typeName, java.util.List<String> bindNames) { this(typeName, bindNames, java.util.List.of()); }
    public PatCtorNode(String typeName, java.util.List<String> bindNames, java.util.List<PatternNode> args) { this.typeName = typeName; this.bindNames = bindNames; this.args = (args == null ? java.util.List.of() : args); }
    @Override @SuppressWarnings("unchecked") public boolean matchesAndBind(Object s, Env env) {
      if (s instanceof AsterDataValue dataValue) {
        if (!typeName.equals(dataValue.getTypeName())) return false;
        return matchOrderedFields(dataValue.fieldCount(), idx -> dataValue.fieldValue(idx), env);
      }
      if (!(s instanceof java.util.Map)) return false;
      var m = (java.util.Map<String,Object>) s;
      Object t = m.get("_type");
      if (!(t instanceof String) || !typeName.equals(t)) return false;
      java.util.ArrayList<Object> values = new java.util.ArrayList<>();
      for (var e : m.entrySet()) {
        if ("_type".equals(e.getKey())) continue;
        values.add(e.getValue());
      }
      return matchOrderedFields(values.size(), values::get, env);
    }

    private boolean matchOrderedFields(int fieldCount, java.util.function.IntFunction<Object> valueProvider, Env env) {
      int idx = 0;
      for (int i = 0; i < fieldCount; i++) {
        Object value = valueProvider.apply(i);
        if (idx < args.size()) {
          PatternNode pn = args.get(idx);
          if (pn instanceof PatNameNode patName) {
            if (!(patName.name == null || patName.name.isEmpty() || "_".equals(patName.name))) {
              env.set(patName.name, value);
            }
          } else if (!pn.matchesAndBind(value, env)) {
            return false;
          }
        } else if (idx < (bindNames == null ? 0 : bindNames.size())) {
          String bn = bindNames.get(idx);
          if (bn != null && !bn.isEmpty() && !"_".equals(bn)) env.set(bn, value);
        }
        idx++;
      }
      return true;
    }
  }

  // Name match: match if s equals the variant name or map with _type == name; else non-null catch-all
  public static final class PatNameNode extends PatternNode {
    private final String name;
    public PatNameNode(String name) { this.name = name; }
    @Override @SuppressWarnings("unchecked") public boolean matchesAndBind(Object s, Env env) {
      if (s == null) return false;
      if (s instanceof String) return name.equals(s);
      if (s instanceof AsterEnumValue enumValue) {
        return name.equals(enumValue.getVariantName()) || name.equals(enumValue.getEnumName());
      }
      if (s instanceof AsterDataValue dataValue) {
        return name.equals(dataValue.getTypeName());
      }
      if (s instanceof java.util.Map) {
        var m = (java.util.Map<String,Object>) s;
        Object v = m.get("value");
        if (v instanceof String && name.equals(v)) return true; // enum variant value
        Object t = m.get("_type");
        if (t instanceof String && name.equals(t)) return true; // constructor type fallback
        return false;
      }
      return true; // fallback catch-all on non-null values
    }
  }

  // Int pattern match: 匹配整数字面量
  public static final class PatIntNode extends PatternNode {
    private final int value;
    public PatIntNode(int value) { this.value = value; }
    @Override public boolean matchesAndBind(Object s, Env env) {
      if (s instanceof Integer i) return value == i.intValue();
      if (s instanceof Long l) return value == l.longValue();
      if (s instanceof Double d) return value == d.doubleValue();
      return false;
    }
  }

  public static final class CaseNode extends Node {
    @Child private PatternNode pat;
    @Child private Node body;
    public CaseNode(PatternNode pat, Node body) { this.pat = pat; this.body = body; }
    public boolean matchesAndBind(Object s, Env env) { return pat.matchesAndBind(s, env); }
    public Object execute(VirtualFrame frame) { return Exec.exec(body, frame); }
  }
}
