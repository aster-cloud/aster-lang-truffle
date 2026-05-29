package aster.truffle.runtime;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;

/**
 * R30+ audit P1 follow-up：DEBUG println 集中到一处。
 *
 * <p>原来散落在 ~13 个 node 类里的写法：
 * <pre>
 *   if (AsterConfig.DEBUG) {
 *     System.err.println("DEBUG: ..." + something);
 *   }
 * </pre>
 *
 * <p>{@link AsterConfig#DEBUG} 标 {@code @CompilationFinal}，PE 在 DEBUG=false
 * 时能把整个 if 块折叠掉。但 String 拼接 + System.err 写本身是 PE-hostile
 * 操作，万一未来 DEBUG 改成 dynamic 或者 PE budget 紧张，散落各处的写法
 * 难以一次性 boundary 化。
 *
 * <p>这里把 println 收到唯一一处 {@code @TruffleBoundary} 方法，调用方只需
 * {@code if (Debug.ENABLED) Debug.log(...)} —— Debug.ENABLED 委托给同一份
 * {@code @CompilationFinal}，fold 行为不变，但 println 的 PE-cut 边界统一。
 *
 * <p>{@link #log(String, Object...)} 走 {@code String.format} —— 调用方应当
 * 用 {@code "%s"} 占位符，避免在判定外做 string concat。
 */
public final class Debug {

  private Debug() {}

  /** 委托给 AsterConfig.DEBUG，调用方读这个常量即可。 */
  public static final boolean ENABLED = AsterConfig.DEBUG;

  /** 调试日志慢路径。boundary 保证 PE 在调用点剪枝。 */
  @TruffleBoundary
  public static void log(String fmt, Object... args) {
    if (!ENABLED) return;
    System.err.println("DEBUG: " + String.format(fmt, args));
  }
}
