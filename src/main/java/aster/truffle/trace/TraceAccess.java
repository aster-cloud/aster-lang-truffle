package aster.truffle.trace;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;

import java.util.List;
import java.util.Map;

/**
 * 步骤级决策追踪的静态门 + per-thread collector 通道（M2.1b，ADR 0030）。仿
 * {@link aster.truffle.nodes.Profiler} 的 PE 纪律，同时充当**宿主↔引擎的同线程通道**。
 *
 * <p><b>为何 static ThreadLocal 而非挂 AsterContext</b>：宿主（aster-api）只持有 polyglot
 * {@code Context}（{@code ctx.eval(...)}），拿不到引擎侧 {@code AsterContext} 实例。而 polyglot
 * {@code eval} 是**同步同线程**——宿主 arm、引擎节点 record、宿主 drain 都在同一线程。故用本类的
 * static ThreadLocal 做通道：宿主经 {@link #armCurrentThread}/{@link #drainCurrentThread} 无需引擎
 * 内部实例即可对齐 collector，引擎节点经 {@link #record} 读同一 ThreadLocal。
 *
 * <p><b>PE 纪律</b>（两级门控）：
 * <ul>
 *   <li>①{@link #ENABLED} 标 {@code @CompilationFinal}：**全局**关时 PE 折叠掉 {@link #record} 整个体，
 *       ~70 处 @Specialization 里的调用在编译产物中消失，零 PE 影响（Profiler 文档的教训）。</li>
 *   <li>②per-eval：即使全局开，本线程未 arm（ThreadLocal null）时 {@link #recordSlow} 短路 no-op。</li>
 * </ul>
 * 默认 {@link #ENABLED}=false（生产默认关）；宿主在**进程启动**时若开启 replay 特性调 {@link #setEnabled}
 * 一次，per-request 隔离靠 per-thread collector 的有无。
 */
public final class TraceAccess {
  /**
   * 全局开关。{@code @CompilationFinal} → PE 关闭时把 record() 折叠成常量 no-op。
   * system property {@code aster.trace.enabled} 初始化（默认 false）。
   */
  @CompilationFinal
  private static boolean ENABLED = Boolean.getBoolean("aster.trace.enabled");

  /** 当前线程的收集器（宿主 arm、引擎 record、宿主 drain 同线程共享）。未 arm=null。 */
  private static final ThreadLocal<TraceCollector> CURRENT = new ThreadLocal<>();

  private TraceAccess() {}

  // ===== 引擎节点侧（热路径）=====

  /**
   * 决策节点热路径调用。ENABLED=false 时 PE 折叠成 no-op（编译产物无残留）。
   *
   * @param kind        步骤种类（"if"/"if-expr"/"match"/"return"/"and"/"or"）
   * @param expression  人读描述
   * @param resultValue 步骤求值结果（recordSlow 内归一）
   * @param matched     是否走到/匹配的分支
   * @param depth       嵌套深度（0=顶层）
   */
  public static void record(String kind, String expression, Object resultValue, boolean matched, int depth) {
    if (!ENABLED) {
      return; // PE-friendly fast path
    }
    recordSlow(kind, expression, resultValue, matched, depth);
  }

  /** 慢路径：取当前线程 collector 并 push。{@code @TruffleBoundary} 让 PE 剪枝；未 arm→短路。 */
  @TruffleBoundary
  private static void recordSlow(String kind, String expression, Object resultValue, boolean matched, int depth) {
    TraceCollector collector = CURRENT.get();
    if (collector == null) {
      return;
    }
    collector.push(kind, expression, resultValue, matched, depth);
  }

  /**
   * 命中的 match arm 记步骤（★Codex 复审：arm 序号拼接移到 slow path，关闭态连 concat 都不出现）。
   * ENABLED=false 时 PE 折叠为 no-op。
   */
  public static void recordMatchArm(int armIndex, Object resultValue, boolean matched, int depth) {
    if (!ENABLED) {
      return;
    }
    recordMatchArmSlow(armIndex, resultValue, matched, depth);
  }

  @TruffleBoundary
  private static void recordMatchArmSlow(int armIndex, Object resultValue, boolean matched, int depth) {
    TraceCollector collector = CURRENT.get();
    if (collector == null) {
      return;
    }
    collector.push("match", "match arm[" + armIndex + "]", resultValue, matched, depth);
  }

  /**
   * 标记当前线程 collector 走了 async/workflow 路径 → 整条 trace NON_REPLAYABLE。
   * 由 WorkflowNode/StartNode gated 调用（ENABLED=false 时 PE 折叠 no-op）。见
   * {@link TraceCollector#markAsyncTainted()}。
   */
  public static void markAsyncTainted() {
    if (!ENABLED) {
      return;
    }
    markAsyncTaintedSlow();
  }

  @TruffleBoundary
  private static void markAsyncTaintedSlow() {
    TraceCollector collector = CURRENT.get();
    if (collector != null) {
      collector.markAsyncTainted();
    }
  }

  // ===== 宿主侧（同线程 arm / drain）=====

  /**
   * 宿主在 {@code ctx.eval(...)} **之前**为当前线程 arm 一个收集器（仅请求 replayCapture 时）。
   * 覆盖式设置（同线程串行 eval，不需保存 previous——drain 时 disarm）。
   */
  @TruffleBoundary
  public static void armCurrentThread(TraceCollector collector) {
    CURRENT.set(collector);
  }

  /**
   * 宿主在 eval **之后**（归还 pooled Context 前）drain：返回有序 plain step 列表并**清除** ThreadLocal
   * （防残留跨顺序 eval 泄漏——fail-closed）。未 arm→返回空列表。第二个返回值经 {@link DrainResult}。
   */
  @TruffleBoundary
  public static DrainResult drainCurrentThread() {
    TraceCollector collector = CURRENT.get();
    CURRENT.remove(); // 无论如何清除，防池线程残留。
    if (collector == null) {
      return new DrainResult(List.of(), false, false, true);
    }
    // 注意：collector.drain() 在 async 污染/归一降级时返回空列表（不吐不可信步骤）。
    return new DrainResult(
        collector.drain(),
        collector.isTruncated(),
        collector.isAsyncTainted(),
        collector.isReplayable());
  }

  /**
   * drain 结果：有序步骤 + 各不可回放信号。宿主据 {@link #replayable}=false 标 NON_REPLAYABLE
   * （不产误导性 traceHash）。truncated/asyncTainted 供审计/诊断区分原因。
   */
  public record DrainResult(
      List<Map<String, Object>> steps,
      boolean truncated,
      boolean asyncTainted,
      boolean replayable) {}

  // ===== 全局开关 =====

  /**
   * 运行时切换全局 trace 状态。宿主进程启动时若开 replay 特性调一次；测试用。
   * {@code transferToInterpreterAndInvalidate} 触发 deopt，下次编译按新值折叠。
   */
  public static void setEnabled(boolean value) {
    if (ENABLED != value) {
      CompilerDirectives.transferToInterpreterAndInvalidate();
      ENABLED = value;
    }
  }

  public static boolean isEnabled() {
    return ENABLED;
  }
}
