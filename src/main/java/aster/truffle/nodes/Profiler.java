package aster.truffle.nodes;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Truffle-friendly profiler.
 *
 * <p>R30+ audit P1：原实现把 {@code ConcurrentHashMap.merge()} 嵌在每个
 * Truffle 节点的热路径，partial evaluation 走到 inc() 直接看到一团 PE-hostile
 * 字节码 —— PE 要么放弃专门化要么生成大量 deopt 路径，整体影响 ~70 处
 * @Specialization 的编译质量。
 *
 * <p>修复策略：
 * <ul>
 *   <li>{@link #ENABLED} 标 {@code @CompilationFinal}：profile 关闭时
 *       PE 会折叠掉整个 inc() 体，编译产物里完全消失。</li>
 *   <li>profile 开启时把 ConcurrentHashMap 写入移到 {@code @TruffleBoundary}
 *       方法 incSlow()，告诉 Truffle "别 PE，转 interpreter 调用"。</li>
 * </ul>
 *
 * <p>由 system property {@code aster.profiler.enabled} 控制；默认关闭。
 * 开发期开启 + 生产期关闭是预期使用方式。
 */
public final class Profiler {
  private static final Map<String, Long> COUNTERS = new ConcurrentHashMap<>();

  /**
   * profile 全局开关。{@code @CompilationFinal} 允许 PE 把它当常量折叠：
   * 关闭时整个 inc() 体在编译产物中消失。改值需要 deopt（{@code Profiler.setEnabled}）。
   */
  @CompilationFinal
  private static boolean ENABLED = Boolean.getBoolean("aster.profiler.enabled");

  private Profiler() {}

  public static void inc(String key) {
    if (!ENABLED) {
      // PE-friendly fast path：profile 关闭时编译产物里没有任何残留。
      return;
    }
    incSlow(key);
  }

  /**
   * 慢路径：ConcurrentHashMap 写入。{@code @TruffleBoundary} 告诉 PE
   * 在这里"剪枝"，慢路径以 interpreter 模式调用，不污染上游编译。
   */
  @TruffleBoundary
  private static void incSlow(String key) {
    COUNTERS.merge(key, 1L, Long::sum);
    COUNTERS.merge("total", 1L, Long::sum);
  }

  @TruffleBoundary
  public static String dump() {
    var sb = new StringBuilder();
    sb.append("Truffle profile (counts):\n");
    COUNTERS.forEach((k,v) -> sb.append(k).append(": ").append(v).append('\n'));
    return sb.toString();
  }

  /**
   * 获取所有计数器的副本（用于测试和分析）
   * Phase 3C P0-2: 添加 Profiler 数据收集 API
   */
  @TruffleBoundary
  public static Map<String, Long> getCounters() {
    return new java.util.HashMap<>(COUNTERS);
  }

  /**
   * 重置所有计数器（用于测试隔离）
   * Phase 3C P0-2: 添加 Profiler 数据收集 API
   */
  @TruffleBoundary
  public static void reset() {
    COUNTERS.clear();
  }

  /**
   * 运行时切换 profile 状态。生产代码不应调用；仅供测试与开发期使用。
   * 写入后通过 {@link CompilerDirectives#transferToInterpreterAndInvalidate}
   * 触发 deopt，下一次编译会重新折叠新值。
   */
  public static void setEnabled(boolean value) {
    if (ENABLED != value) {
      CompilerDirectives.transferToInterpreterAndInvalidate();
      ENABLED = value;
    }
  }
}
