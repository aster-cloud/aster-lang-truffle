package aster.truffle.purity;

import com.oracle.truffle.api.CallTarget;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 纯度分析器：根据 effect 元数据判定函数是否为纯函数。
 *
 * <p>运行时在创建 LambdaValue 时调用 {@link #recordEffects} 注册纯度信息。</p>
 */
public final class PurityAnalyzer {
  private static final Map<CallTarget, Boolean> CACHE = new ConcurrentHashMap<>();

  private PurityAnalyzer() {}

  /**
   * 注册 CallTarget 与 effect 元数据。
   *
   * @param target 对应的 CallTarget
   * @param requiredEffects 函数声明的 effect 列表（null 或空列表视为纯函数）
   */
  public static void recordEffects(CallTarget target, Set<String> requiredEffects) {
    if (target == null) {
      return;
    }
    boolean pure = requiredEffects == null || requiredEffects.isEmpty();
    CACHE.put(target, pure);
  }

  /**
   * 判定目标函数是否为纯函数。
   *
   * @param target 需要检测的 CallTarget
   * @return true 表示纯函数，可参与并行化；false 表示含副作用或未知，需走顺序路径
   */
  public static boolean isPure(CallTarget target) {
    if (target == null) {
      return false;
    }
    Boolean cached = CACHE.get(target);
    return cached != null && cached;
  }
}
