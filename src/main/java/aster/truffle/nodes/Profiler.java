package aster.truffle.nodes;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Profiler {
  private static final Map<String, Long> COUNTERS = new ConcurrentHashMap<>();
  private static final boolean ENABLED = Boolean.getBoolean("aster.profiler.enabled");
  private Profiler() {}
  public static void inc(String key) {
    if (!ENABLED) {
      return;
    }
    COUNTERS.merge(key, 1L, Long::sum);
    COUNTERS.merge("total", 1L, Long::sum);
  }
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
  public static Map<String, Long> getCounters() {
    return new java.util.HashMap<>(COUNTERS);
  }

  /**
   * 重置所有计数器（用于测试隔离）
   * Phase 3C P0-2: 添加 Profiler 数据收集 API
   */
  public static void reset() {
    COUNTERS.clear();
  }
}
