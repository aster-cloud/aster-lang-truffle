package aster.truffle.trace;

import aster.truffle.runtime.AsterDataValue;
import aster.truffle.runtime.AsterEnumValue;
import aster.truffle.runtime.interop.AsterListValue;
import aster.truffle.runtime.interop.AsterMapValue;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 步骤级决策追踪收集器（M2.1b，ADR 0030 真回放 payload）。
 *
 * <p>每次 eval 请求 capture 时由 {@link aster.truffle.AsterContext} per-thread 挂一个实例，
 * 决策节点（If/IfExpr/Match/Return + 可选 And/Or）经 {@link TraceAccess} 在其上追加**有序**
 * 的 canonical TraceStep。宿主（aster-api）在归还 pooled Context 前 drain 出 plain
 * {@code List<Map<String,Object>>}，构建 {@code DecisionTrace.TraceStep} 填进决策链。
 *
 * <p>★三条铁律（决定 replay payload 能否喂 canonical hash）：
 * <ol>
 *   <li><b>决定性归一</b>：任何 captured 值反向归一到 plain
 *       String/Long/Boolean/BigDecimal/List/Map——绝不放 guest 包装（Enum/Data/List/Map value）、
 *       对象身份、时间戳。Map 键**排序**（AsterMapValue 底层可能是 HashMap，迭代序非确定）。</li>
 *   <li><b>有序 + 单调 sequence</b>：steps 是 {@link ArrayList}（保序）；sequence 由 push 时的
 *       单调计数器给出（从 1 起），不从 map 内容派生。</li>
 *   <li><b>有界不抛</b>：单一 push 入口检查 maxSteps/maxDepth/估算字节，破界**只置 truncated 静默停记**，
 *       **绝不抛异常**——trace 是旁路观测，不能因预算把真实决策计算炸掉。宿主据 truncated 标 NON_REPLAYABLE。
 *       push 已是 {@code @TruffleBoundary} 慢路径，禁用热路径零成本。</li>
 *   <li><b>诚实降级</b>：async/workflow 路径（{@link #markAsyncTainted}）或未知不可归一类型 → 整条
 *       NON_REPLAYABLE（drain 返回空），绝不产调度依赖/含对象身份的不可信 payload。</li>
 * </ol>
 *
 * <p>本类**非** static、**非**线程安全——每个 eval 线程独占一个实例（经 {@link TraceAccess} 的
 * ThreadLocal 隔离），与 effectPermissions 同构。所有 push 走 {@code @TruffleBoundary}（PE 剪枝），不污染上游编译。
 */
public final class TraceCollector {
  /** step 数上限：现无 cap，防病态深递归/大 match 撑爆内存。 */
  private final int maxSteps;
  /** 嵌套深度上限（If/Match 经 children 嵌套）。 */
  private final int maxDepth;
  /** 归一后 result 字符串化的估算字节上限（单值），防单个巨值。 */
  private final int maxValueBytes;

  /** 有序步骤（顶层）。每个元素是 canonical TraceStep map（含 children）。 */
  private final List<Map<String, Object>> steps = new ArrayList<>();
  /** 单调 sequence 计数器（从 1 起），全局跨嵌套唯一。 */
  private int sequence = 0;
  /** 已达上限 → 后续 push 短路为 no-op（避免抛异常后继续记）。 */
  private boolean truncated = false;
  /**
   * ★async 污染（Codex 复审 P0#1/#2）：本次 eval 含 workflow/start 异步节点。异步任务体在
   * executor **worker 线程**跑决策节点（非 eval 线程，收不进本 collector），且 await/wait 的
   * inline 调度会让「同一 async 子表达式有时被捕获有时不」——trace 形状随调度路径漂移，喂 canonical
   * hash 即跨运行不稳定。故一旦本次 eval 走 async 路径，**整条 trace 标 NON_REPLAYABLE**（宿主据此），
   * 绝不产出误导性的部分/不稳定 traceHash（诚实优于假证据）。完整 async 步骤级 trace（per-task
   * collector + 确定性 merge）是独立后续工程，不在 M2.1b 范围。
   */
  private boolean asyncTainted = false;
  /**
   * ★归一降级（Codex 复审）：捕获到无法确定性归一的运行时值（PII 脱敏值 / Lambda / 未知 host
   * object / TruffleObject）——兜底字符串化可能含对象身份或语义丢失。置此标 → 宿主标 NON_REPLAYABLE，
   * 不把不可信值塞进 hash payload。
   */
  private boolean replayableDegraded = false;

  public TraceCollector(int maxSteps, int maxDepth, int maxValueBytes) {
    this.maxSteps = maxSteps;
    this.maxDepth = maxDepth;
    this.maxValueBytes = maxValueBytes;
  }

  /** 默认预算：500 步 / 深度 64 / 单值 64KB。生产策略远低于此，是安全阀非性能需求。 */
  public static TraceCollector withDefaults() {
    return new TraceCollector(500, 64, 64 * 1024);
  }

  /**
   * 追加一个决策步骤（唯一 push 入口，@TruffleBoundary 慢路径）。
   *
   * @param kind       步骤种类（"if"/"if-expr"/"match"/"return"/"and"/"or"），进 expression 描述
   * @param expression 人读描述（如 "if cond" / "match arm[2]"），归一后原样存
   * @param resultValue 该步骤的求值结果（guest 值，push 内部归一）
   * @param matched    是否为最终匹配/走到的分支
   * @param depth      嵌套深度（0=顶层）——超 maxDepth fail-fast
   */
  @TruffleBoundary
  public void push(String kind, String expression, Object resultValue, boolean matched, int depth) {
    // 已不可信（截断/async 污染/归一降级）→ 早短路，省 async-tainted eval 的无用 normalize/append
    // （反正 drain 会丢弃）。Codex 复审 follow-up。
    if (truncated || asyncTainted || replayableDegraded) {
      return;
    }
    // ★破界**不抛异常**：trace 是旁路观测，绝不能因 trace 预算把真实决策计算炸掉。
    // 只置 truncated + 静默停止后续记录——决策照常返回，宿主据 truncated 标 NON_REPLAYABLE。
    if (steps.size() >= maxSteps || depth >= maxDepth) {
      truncated = true;
      return;
    }
    Object normalizedResult = normalize(resultValue);
    if (exceedsValueBytes(normalizedResult)) {
      // 单值过大同样只截断不抛——丢弃这一步，保住决策与已有步骤。
      truncated = true;
      return;
    }

    Map<String, Object> step = new LinkedHashMap<>(5);
    step.put("sequence", ++sequence);
    step.put("expression", expression == null ? kind : expression);
    step.put("result", normalizedResult);
    step.put("matched", matched);
    // children 目前不由引擎侧构建嵌套（If/Match 分支体的子步骤经 sequence 平铺 + depth 表达），
    // 保留字段与宿主 TraceStep.children 契约对齐——空列表，非 null（宿主契约要求非 null shape）。
    step.put("children", new ArrayList<Map<String, Object>>());
    steps.add(step);
  }

  /**
   * 标记本次 eval 走了 async/workflow 路径 → 整条 trace NON_REPLAYABLE（见 {@link #asyncTainted}）。
   * 由 WorkflowNode/StartNode 经 {@link TraceAccess#markAsyncTainted()} gated 调用。
   */
  public void markAsyncTainted() {
    this.asyncTainted = true;
  }

  /**
   * drain 出 step 列表（宿主消费）。已 async 污染或归一降级 → 返回**空列表**（不产不可信步骤，
   * 宿主据 {@link #isReplayable()} 标 NON_REPLAYABLE）。否则深拷贝成不可变快照。
   */
  @TruffleBoundary
  public List<Map<String, Object>> drain() {
    if (asyncTainted || replayableDegraded) {
      // 不可信：不吐部分/降级步骤进 hash payload。宿主见 isReplayable()=false 标 NON_REPLAYABLE。
      return List.of();
    }
    return deepImmutable(steps);
  }

  /** 本次 trace 是否可回放（无截断/无 async 污染/无归一降级）。宿主据此标 replayability。 */
  public boolean isReplayable() {
    return !truncated && !asyncTainted && !replayableDegraded;
  }

  public boolean isTruncated() {
    return truncated;
  }

  public boolean isAsyncTainted() {
    return asyncTainted;
  }

  public int size() {
    return steps.size();
  }

  /** 深度不可变化：每个 step map + 其 result 内嵌集合 + children 全 immutable，防 drain 后被改。 */
  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> deepImmutable(List<Map<String, Object>> src) {
    List<Map<String, Object>> out = new ArrayList<>(src.size());
    for (Map<String, Object> step : src) {
      Map<String, Object> copy = new LinkedHashMap<>(step.size());
      for (Map.Entry<String, Object> e : step.entrySet()) {
        copy.put(e.getKey(), deepImmutableValue(e.getValue()));
      }
      out.add(java.util.Collections.unmodifiableMap(copy));
    }
    return java.util.Collections.unmodifiableList(out);
  }

  @SuppressWarnings("unchecked")
  private static Object deepImmutableValue(Object v) {
    if (v instanceof Map<?, ?> m) {
      Map<String, Object> copy = new LinkedHashMap<>(m.size());
      for (Map.Entry<?, ?> e : m.entrySet()) {
        copy.put(String.valueOf(e.getKey()), deepImmutableValue(e.getValue()));
      }
      return java.util.Collections.unmodifiableMap(copy);
    }
    if (v instanceof List<?> list) {
      List<Object> copy = new ArrayList<>(list.size());
      for (Object e : list) {
        copy.add(deepImmutableValue(e));
      }
      return java.util.Collections.unmodifiableList(copy);
    }
    return v; // String/Long/Boolean/BigDecimal/null 已不可变。
  }

  // ===== §norm 决定性归一 =====

  /**
   * 把任意 guest/运行时值反向归一到 plain String/Long/Boolean/BigDecimal/List/Map。
   *
   * <p>★与 {@code AsterInteropAdapter.adapt} 的差异：adapt 只包 List/Map，Enum/Data **不经 adapt**
   * 直接透传——故这里**单独**处理 Enum(→变体限定名 String)/Data(→按字段名摊成有序 Map)，不能复用 adapt 反向。
   */
  private Object normalize(Object value) {
    if (value == null) {
      return null;
    }
    // interop-null（宿主注入的 null 包装）视同 null。
    if (com.oracle.truffle.api.interop.InteropLibrary.getUncached().isNull(value)) {
      return null;
    }
    if (value instanceof String || value instanceof Boolean) {
      return value;
    }
    // 整数统一 Long（跨引擎 canonical 铁律：只 safe integer，不吐 double）。
    if (value instanceof Integer i) {
      return i.longValue();
    }
    if (value instanceof Long) {
      return value;
    }
    // 小数：BigDecimal 保留（宿主 lift 非整数→canonical String）；double 转 BigDecimal 值语义。
    if (value instanceof BigDecimal) {
      return value;
    }
    if (value instanceof Double d) {
      return BigDecimal.valueOf(d);
    }
    if (value instanceof Float f) {
      return BigDecimal.valueOf(f.doubleValue());
    }
    // Decimal 一等公民（ADR 0025）：解包精确 BigDecimal（与 input/output 小数路径一致；
    // 宿主 liftDecimals 会把它 lift 成 canonical string）。★Codex 复审补：此前漏 → 会走兜底。
    if (value instanceof aster.truffle.runtime.interop.AsterDecimalValue decimalValue) {
      return decimalValue.decimal();
    }
    // Enum：变体限定名（enum.variant），有 args 时摊成 {variant, args:[...]} 有序 map。
    if (value instanceof AsterEnumValue enumValue) {
      Object[] args = enumValue.getArgs();
      if (args.length == 0) {
        return enumValue.getQualifiedName();
      }
      Map<String, Object> m = new LinkedHashMap<>(2);
      m.put("variant", enumValue.getQualifiedName());
      m.put("args", normalizeList(java.util.Arrays.asList(args)));
      return m;
    }
    // Data：{_type, 字段名:值...} 按声明字段顺序（AsterDataValue fieldName(i) 有序）。
    if (value instanceof AsterDataValue dataValue) {
      Map<String, Object> m = new LinkedHashMap<>(dataValue.fieldCount() + 1);
      m.put("_type", dataValue.getTypeName());
      for (int i = 0; i < dataValue.fieldCount(); i++) {
        m.put(dataValue.fieldName(i), normalize(dataValue.fieldValue(i)));
      }
      return m;
    }
    if (value instanceof AsterListValue listValue) {
      return normalizeList(listValue.elements());
    }
    if (value instanceof AsterMapValue mapValue) {
      return normalizeMap(mapValue.entries());
    }
    // 裸集合（引擎内部产生的 LinkedHashMap/List）。
    if (value instanceof List<?> list) {
      return normalizeList(list);
    }
    if (value instanceof Map<?, ?> map) {
      return normalizeMap(map);
    }
    // ★未知类型（Codex 复审）：PII 脱敏值 / Lambda / 未知 TruffleObject / host object——**不**兜底
    // 字符串化（可能含对象身份 System.identityHashCode 或语义丢失，塞进 hash payload =不可信）。
    // 置归一降级标 → 宿主标 NON_REPLAYABLE；step 里存类型占位符（人读 trace 可见但不进可信回放）。
    replayableDegraded = true;
    return "<unrepresentable:" + value.getClass().getSimpleName() + ">";
  }

  private List<Object> normalizeList(List<?> list) {
    List<Object> out = new ArrayList<>(list.size());
    for (Object e : list) {
      out.add(normalize(e));
    }
    return out;
  }

  /**
   * 归一 Map：**键排序**（底层可能 HashMap，迭代序非确定）→ TreeMap 保证 canonical 键序。
   * 键强制字符串化（Aster map 键恒为 String，但防御性处理）。
   */
  private Object normalizeMap(Map<?, ?> map) {
    // 用 TreeMap 排序键：canonical hash 要求确定性键序，不依赖插入序。
    Map<String, Object> sorted = new TreeMap<>();
    for (Map.Entry<?, ?> e : map.entrySet()) {
      Object key = e.getKey();
      // ★非 String 键（未来 host Map / 非 Aster Map 进 trace）→ String.valueOf 可能含对象身份，
      // 标降级（Codex 复审 follow-up）。Aster map 键恒为 String，此为防御性护栏。
      if (!(key instanceof String)) {
        replayableDegraded = true;
      }
      sorted.put(String.valueOf(key), normalize(e.getValue()));
    }
    // 回填成 LinkedHashMap 保持排序后的序（TreeMap 迭代已是键序）。
    return new LinkedHashMap<>(sorted);
  }

  /**
   * 估算单个归一值字节是否超 maxValueBytes（防单个巨值撑爆 payload）。
   * ★Codex 复审：递归**累计**估算并**短路**超限，不先构造整串再判断（大 Map/List 会先撑内存）。
   * 返回 true=超限（调用方截断）。近似估算（安全阀语义，非精确字节）。
   */
  private boolean exceedsValueBytes(Object normalized) {
    return estimateBytes(normalized, 0) > maxValueBytes;
  }

  /** 递归累计估算字节，一旦超 maxValueBytes 立即返回（短路，不继续遍历）。 */
  private int estimateBytes(Object v, int acc) {
    if (acc > maxValueBytes) {
      return acc; // 已超，短路。
    }
    if (v == null) {
      return acc + 4; // "null"
    }
    if (v instanceof Map<?, ?> m) {
      int total = acc + 2;
      for (Map.Entry<?, ?> e : m.entrySet()) {
        total = estimateBytes(String.valueOf(e.getKey()), total);
        total = estimateBytes(e.getValue(), total);
        if (total > maxValueBytes) {
          return total;
        }
      }
      return total;
    }
    if (v instanceof List<?> list) {
      int total = acc + 2;
      for (Object e : list) {
        total = estimateBytes(e, total);
        if (total > maxValueBytes) {
          return total;
        }
      }
      return total;
    }
    // 标量：String/Long/Boolean/BigDecimal——toString 长度近似。
    return acc + String.valueOf(v).length();
  }
}
