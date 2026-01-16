package aster.truffle.runtime;

import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;

/**
 * Aster Truffle 运行时配置
 *
 * 集中管理所有环境变量配置，在类加载时读取一次，避免热路径中的 System.getenv 调用。
 * 使用 @CompilationFinal 标记，Truffle 可以将这些字段视为编译时常量进行优化。
 */
public final class AsterConfig {
  private AsterConfig() {}

  /**
   * 调试模式开关
   * 环境变量：ASTER_TRUFFLE_DEBUG
   * 启用时会在各个执行节点打印调试信息
   */
  @CompilationFinal
  public static final boolean DEBUG = System.getenv("ASTER_TRUFFLE_DEBUG") != null;

  /**
   * 性能分析模式开关
   * 环境变量：ASTER_TRUFFLE_PROFILE
   * 启用时会在程序结束时打印性能统计
   */
  @CompilationFinal
  public static final boolean PROFILE = System.getenv("ASTER_TRUFFLE_PROFILE") != null;

  /**
   * 默认入口函数名
   * 环境变量：ASTER_TRUFFLE_FUNC
   * 如果未指定，默认为 "main"
   */
  @CompilationFinal
  public static final String DEFAULT_FUNCTION = getEnvOrDefault("ASTER_TRUFFLE_FUNC", "main");

  /**
   * 辅助方法：读取环境变量或返回默认值
   */
  private static String getEnvOrDefault(String key, String defaultValue) {
    String value = System.getenv(key);
    return value != null ? value : defaultValue;
  }
}
