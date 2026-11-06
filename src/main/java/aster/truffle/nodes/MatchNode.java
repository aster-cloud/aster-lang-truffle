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

  /** 预拼好的 trace 标签，形如 {@code "match no-arm @L15"}。见 {@link IfNode}。 */
  @com.oracle.truffle.api.CompilerDirectives.CompilationFinal private final String traceLabel;

  protected MatchNode(Env env, java.util.List<CaseNode> cases, int sourceLine) {
    this.env = env;
    this.cases = cases.toArray(new CaseNode[0]);
    this.traceLabel = sourceLine > 0 ? "match no-arm @L" + sourceLine : "match no-arm";
  }

  public static MatchNode create(Env env, AsterExpressionNode scrutinee,
                                 java.util.List<CaseNode> cases, int sourceLine) {
    return MatchNodeGen.create(env, cases, sourceLine, scrutinee);
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
      System.err.println("DEBUG: match scrutinee=" + scrutinee + " type=" + (scrutinee == null ? "null" : scrutinee.getClass().getName()) + " cases=" + cases.length);
    }
    for (int i = 0; i < cases.length; i++) {
      CaseNode c = cases[i];
      if (AsterConfig.DEBUG) {
        System.err.println("DEBUG: trying case pat=" + c.pat.getClass().getSimpleName());
      }
      if (c.matchesAndBind(scrutinee, frame, env)) {
        if (AsterConfig.DEBUG) {
          System.err.println("DEBUG: case matched");
        }
        Object result = c.execute(frame);
        // 步骤级 trace（M2.1b）：记命中的 arm 序号 + 其求值结果（matched=true）。全局关时 PE 折叠 no-op。
        // ★用 recordMatchArm（arm 序号拼接在 slow path，关闭态连字符串 concat 都不出现——Codex 复审）。
        aster.truffle.trace.TraceAccess.recordMatchArm(i, result, true, 0);
        return result;
      }
    }
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: no case matched, returning null");
    }
    // 步骤级 trace（M2.1b）：无 arm 命中（matched=false，result=null）——决策路径的关键信息。
    aster.truffle.trace.TraceAccess.record("match", traceLabel, null, false, 0);
    return null;
  }

  @Idempotent protected boolean isNull(Object value) {
    return value == null;
  }

  @Idempotent @SuppressWarnings("rawtypes")
  protected boolean isMap(Object value) {
    return value instanceof java.util.Map;
  }

  /**
   * 模式节点。绑定写入 case 分支所在 frame 的槽位（{@code slot >= 0}，由 Loader 按词法作用域分配，
   * 同名外层变量因此被遮蔽而非覆盖，ADR 0052 §2.1）；没有槽位时退回写 Env。
   */
  public static abstract class PatternNode extends Node {
    public abstract boolean matchesAndBind(Object s, VirtualFrame frame, Env env);

    static void bind(VirtualFrame frame, Env env, int slot, String name, Object value) {
      if (name == null || name.isEmpty() || "_".equals(name)) return;
      if (slot >= 0) {
        frame.setObject(slot, value);
      } else {
        env.set(name, value);
      }
    }
  }

  /**
   * 一个值在 guest 视角下是否为 null。既包括 Java {@code null}（引擎内部产生的
   * null），也包括 interop-null——当宿主经 {@code Value.execute(null)} 注入一个
   * null 参数时，GraalVM 把它包成 {@code isNull()==true} 的 HostObject 而非裸
   * Java null。两者在 {@code When null} 语义下都应视为 null。
   */
  static boolean isGuestNull(Object s) {
    if (s == null) return true;
    return com.oracle.truffle.api.interop.InteropLibrary.getUncached().isNull(s);
  }

  /**
   * 值是否为可空 T? 的裸载荷（aster-lang-ts#277）：非 null，且不是 Some / None / Ok / Err 构造值。
   * data 值、枚举成员等照常算裸载荷（{@code Person?} 位置直接放 Person）。与 TS 解释器 isBareOptionalPayload 一致。
   */
  @SuppressWarnings("rawtypes")
  static boolean isBareOptionalPayload(Object s) {
    if (isGuestNull(s)) return false;
    if (!(s instanceof java.util.Map m)) return true;
    Object tag = m.get("__type");
    return !("Some".equals(tag) || "None".equals(tag) || "Ok".equals(tag) || "Err".equals(tag));
  }

  public static final class PatNullNode extends PatternNode {
    @Override public boolean matchesAndBind(Object s, VirtualFrame frame, Env env) { return isGuestNull(s); }
  }

  // ctor match: match Data 值或旧 Map，按字段顺序依次绑定
  public static final class PatCtorNode extends PatternNode {
    private final String typeName;
    private final java.util.List<String> bindNames;
    /** 与 bindNames 一一对应的绑定槽位，-1 表示写 Env。 */
    private final int[] bindSlots;
    private final java.util.List<PatternNode> args;
    public PatCtorNode(String typeName, java.util.List<String> bindNames) { this(typeName, bindNames, null, java.util.List.of()); }
    public PatCtorNode(String typeName, java.util.List<String> bindNames, int[] bindSlots, java.util.List<PatternNode> args) {
      this.typeName = typeName;
      this.bindNames = bindNames;
      this.bindSlots = bindSlots;
      this.args = (args == null ? java.util.List.of() : args);
    }
    @Override @SuppressWarnings("unchecked") public boolean matchesAndBind(Object s, VirtualFrame frame, Env env) {
      // 可空值 T? 的裸表示（aster-lang-ts#277）：宿主传入或 T → T? 直接传入的值不带 Some/None 标签。
      // 裸的非 null 值按 Some(值) 匹配，null 按 None 匹配；带 __type 标签的值（Some/None/Ok/Err）仍走下方原逻辑。
      if ("Some".equals(typeName) && isBareOptionalPayload(s)) {
        return matchOrderedFields(1, idx -> s, frame, env);
      }
      if ("None".equals(typeName) && args.isEmpty() && (bindNames == null || bindNames.isEmpty()) && isGuestNull(s)) {
        return true;
      }
      if (s instanceof AsterDataValue dataValue) {
        if (!typeName.equals(dataValue.getTypeName())) return false;
        return matchOrderedFields(dataValue.fieldCount(), idx -> dataValue.fieldValue(idx), frame, env);
      }
      if (s instanceof AsterEnumValue enumValue) {
        return typeName.equals(enumValue.getVariantName()) || typeName.equals(enumValue.getEnumName());
      }
      // 枚举变体的字符串表示匹配（如 "Addieren" 匹配 When Addieren）
      if (s instanceof String str) {
        return typeName.equals(str);
      }
      if (!(s instanceof java.util.Map)) return false;
      var m = (java.util.Map<String,Object>) s;
      Object t = m.get("__type");
      if (!(t instanceof String) || !typeName.equals(t)) return false;
      java.util.ArrayList<Object> values = new java.util.ArrayList<>();
      for (var e : m.entrySet()) {
        if ("__type".equals(e.getKey())) continue;
        values.add(e.getValue());
      }
      return matchOrderedFields(values.size(), values::get, frame, env);
    }

    private boolean matchOrderedFields(int fieldCount, java.util.function.IntFunction<Object> valueProvider, VirtualFrame frame, Env env) {
      int idx = 0;
      for (int i = 0; i < fieldCount; i++) {
        Object value = valueProvider.apply(i);
        if (idx < args.size()) {
          PatternNode pn = args.get(idx);
          if (pn instanceof PatNameNode patName) {
            bind(frame, env, patName.slot, patName.name, value);
          } else if (!pn.matchesAndBind(value, frame, env)) {
            return false;
          }
        } else if (idx < (bindNames == null ? 0 : bindNames.size())) {
          String bn = bindNames.get(idx);
          bind(frame, env, bindSlots == null ? -1 : bindSlots[idx], bn, value);
        }
        idx++;
      }
      return true;
    }
  }

  // Name pattern, two meanings distinguished by case (mirrors the TS interpreter):
  //   - ASCII-capitalized name (enum variant / type, e.g. NotFound) → match by name
  //     equality; does NOT bind. Without this, a lowercase catch-all arm could
  //     never be reached when the scrutinee is an enum-as-string, and a
  //     Capitalized arm would not exclude other variants.
  //   - any other name (x / value / 其他 / Ärger) → catch-all binding: matches any
  //     non-null value and binds it. (A `When null` arm is a separate PatNull.)
  public static final class PatNameNode extends PatternNode {
    private final String name;
    /** 绑定槽位，-1 表示写 Env；变体名（大写开头）不绑定。 */
    private final int slot;
    public PatNameNode(String name) { this(name, -1); }
    public PatNameNode(String name, int slot) { this.name = name; this.slot = slot; }
    /** 名字按绑定语义处理（非变体、非 {@code _}）时才需要槽位。 */
    public static boolean binds(String name) {
      return name != null && !name.isEmpty() && !"_".equals(name) && !isVariantName(name);
    }
    /**
     * 只看首字母是否为 ASCII 大写（ADR 0051 裁决 R55），与 TS 解释器 {@code /^[A-Z]/}、R2 判定、parity 归一化一致。
     * 非 ASCII 大写开头（{@code Ärger}）与无大小写文字的枚举成员由 R2 产出无参数 PatCtor，PatName 一律是绑定。
     */
    static boolean isVariantName(String name) {
      return name != null && !name.isEmpty() && name.charAt(0) >= 'A' && name.charAt(0) <= 'Z';
    }
    private boolean isVariant() {
      return isVariantName(name);
    }
    @Override @SuppressWarnings("unchecked") public boolean matchesAndBind(Object s, VirtualFrame frame, Env env) {
      if (AsterConfig.DEBUG) {
        System.err.println("DEBUG: PatNameNode name=" + name + " scrutinee=" + s + " type=" + (s == null ? "null" : s.getClass().getName()));
      }
      if (isVariant()) {
        // `When None` 也匹配可空值的裸 null（aster-lang-ts#277，见 PatCtorNode）
        if ("None".equals(name) && isGuestNull(s)) return true;
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
          Object t = m.get("__type");
          return t instanceof String && name.equals(t); // constructor type fallback
        }
        return false;
      }
      // lowercase: catch-all binding over any non-null value. interop-null (a
      // host-injected null) must NOT match here — it belongs to the `When null`
      // arm, same as a Java null.
      if (isGuestNull(s)) return false;
      bind(frame, env, slot, name, s);
      return true;
    }
  }

  // Int pattern match: 匹配整数字面量
  public static final class PatIntNode extends PatternNode {
    private final int value;
    public PatIntNode(int value) { this.value = value; }
    @Override public boolean matchesAndBind(Object s, VirtualFrame frame, Env env) {
      if (s instanceof Integer i) return value == i.intValue();
      if (s instanceof Long l) return value == l.longValue();
      if (s instanceof Double d) return value == d.doubleValue();
      // Int→Decimal 宽化：Decimal 按精确数值相等匹配（0 / 0.0 / 0.00 均命中 When 0），
      // 用 compareTo 而非 double 转换，避免精度丢失；Text 等非数值仍不匹配。
      if (s instanceof aster.truffle.runtime.interop.AsterDecimalValue dv) return matchesDecimal(dv.decimal());
      if (s instanceof java.math.BigDecimal bd) return matchesDecimal(bd);
      return false;
    }

    @com.oracle.truffle.api.CompilerDirectives.TruffleBoundary
    private boolean matchesDecimal(java.math.BigDecimal d) {
      return d.compareTo(java.math.BigDecimal.valueOf(value)) == 0;
    }
  }

  public static final class CaseNode extends Node {
    @Child private PatternNode pat;
    @Child private Node body;
    public CaseNode(PatternNode pat, Node body) { this.pat = pat; this.body = body; }
    public boolean matchesAndBind(Object s, VirtualFrame frame, Env env) { return pat.matchesAndBind(s, frame, env); }
    public Object execute(VirtualFrame frame) { return Exec.exec(body, frame); }
  }
}
