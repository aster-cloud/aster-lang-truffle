package aster.truffle;

import aster.truffle.runtime.AsterConfig;
import aster.truffle.runtime.AsyncTaskRegistry;
import aster.truffle.runtime.Builtins;
import com.oracle.truffle.api.TruffleLanguage;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Aster 语言运行时上下文。
 *
 * 负责封装 Truffle 环境、内建函数注册表与配置快照，提供线程安全的访问接口。
 */
public final class AsterContext {
  private final TruffleLanguage.Env env;
  private final AtomicReference<Builtins> builtinsRef = new AtomicReference<>();
  private final AtomicReference<Set<String>> effectPermissions = new AtomicReference<>(Set.of());
  private final AtomicReference<AsyncTaskRegistry> asyncRegistry = new AtomicReference<>();
  private final AtomicLong taskIdGenerator = new AtomicLong(0);
  private final ConfigView configView;
  private final Class<AsterConfig> configClass;

  public AsterContext(TruffleLanguage.Env env) {
    this.env = Objects.requireNonNull(env, "env");
    // 预先捕捉静态配置，避免执行过程中反复读取环境变量
    this.configView = new ConfigView(AsterConfig.DEBUG, AsterConfig.PROFILE, AsterConfig.DEFAULT_FUNCTION);
    this.configClass = AsterConfig.class;
  }

  public TruffleLanguage.Env getEnv() {
    return env;
  }

  /**
   * 延迟初始化内建函数注册表。
   * 目前注册表为静态实现，但仍通过原子引用保证未来扩展时的线程安全。
   */
  public Builtins getBuiltins() {
    Builtins current = builtinsRef.get();
    if (current != null) {
      return current;
    }
    Builtins created = new Builtins();
    return builtinsRef.compareAndSet(null, created) ? created : builtinsRef.get();
  }

  /**
   * 返回配置快照，便于在节点中读取调试/性能等开关。
   */
  public ConfigView getConfig() {
    return configView;
  }

  /**
   * 设置当前允许的 effect 列表。
   * 用于在函数调用前限制可执行的副作用操作。
   *
   * @param effects 允许的 effect 名称集合（如 "IO", "Async", "CPU"）
   */
  public void setAllowedEffects(Set<String> effects) {
    effectPermissions.set(Set.copyOf(effects));
  }

  /**
   * 检查指定 effect 是否在当前允许列表中。
   *
   * @param effect effect 名称（如 "IO", "Async", "CPU"）
   * @return true 如果允许，false 否则
   */
  public boolean isEffectAllowed(String effect) {
    return effectPermissions.get().contains(effect);
  }

  /**
   * 获取当前允许的所有 effects（只读副本）。
   *
   * @return 当前允许的 effect 集合的不可变副本
   */
  public Set<String> getAllowedEffects() {
    return Set.copyOf(effectPermissions.get());
  }

  /**
   * 延迟初始化异步任务注册表。
   * 使用与 builtinsRef 相同的模式，确保线程安全的单例创建。
   */
  public AsyncTaskRegistry getAsyncRegistry() {
    AsyncTaskRegistry current = asyncRegistry.get();
    if (current != null) {
      return current;
    }
    AsyncTaskRegistry created = new AsyncTaskRegistry();
    return asyncRegistry.compareAndSet(null, created) ? created : asyncRegistry.get();
  }

  /**
   * 生成唯一的任务 ID。
   * 使用 AtomicLong 递增序列，格式为 "task-<seq>"。
   *
   * @return 任务 ID（如 "task-1", "task-2", ...）
   */
  public String generateTaskId() {
    return "task-" + taskIdGenerator.incrementAndGet();
  }

  /**
   * 暴露原始配置类引用，方便后续迁移至非静态配置时保留向后兼容。
   */
  public Class<AsterConfig> getConfigClass() {
    return configClass;
  }

  /**
   * 配置快照，仅包含运行时常用的几个开关。
   */
  public static final class ConfigView {
    private final boolean debugEnabled;
    private final boolean profileEnabled;
    private final String defaultFunction;

    private ConfigView(boolean debugEnabled, boolean profileEnabled, String defaultFunction) {
      this.debugEnabled = debugEnabled;
      this.profileEnabled = profileEnabled;
      this.defaultFunction = defaultFunction;
    }

    public boolean isDebugEnabled() {
      return debugEnabled;
    }

    public boolean isProfileEnabled() {
      return profileEnabled;
    }

    public String getDefaultFunction() {
      return defaultFunction;
    }
  }
}
