package aster.truffle.nodes;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 变量作用域链。
 *
 * <p><b>线程安全</b>：{@code vars} 必须是并发安全容器——{@link StartNode} 会把子表达式
 * 包成 Runnable 提交给 {@code AsyncTaskRegistry} 的固定线程池（默认大小 = CPU 核心数），
 * 而该子表达式树里的 {@link SetNodeEnv} / {@link LetNodeEnv} 持有的正是<b>与 eval 线程
 * 同一个</b> {@code Env} 实例。叠加 {@link #set} 的写冒泡语义（变量已存在于父作用域时
 * 写会向上落到父 Env），多个并发任务的写会汇聚到同一张表上。
 *
 * <p>此前这里是裸 {@link java.util.HashMap}。并发写 HashMap 不是「偶尔丢一次更新」
 * 的良性竞态：扩容期间会破坏内部结构。实测（{@code EnvConcurrencyTest}）一个自始至终
 * 存在的键在并发扩容期间被读成 {@code null}——对合规决策引擎而言，
 * 这意味着<b>判定依据静默消失而无人知晓</b>。
 *
 * <p>{@link ConcurrentHashMap} 不接受 null 值，而 {@code Loader} 会以 null 预登记
 * 函数名占位，故内部用 {@link #NULL} 哨兵表示「存在且值为 null」，
 * 以保持 {@code get} 对「不存在」与「存在但为 null」的原有返回行为完全不变。
 */
public final class Env {
  /** 「存在且值为 null」的哨兵——ConcurrentHashMap 不接受 null 值。 */
  private static final Object NULL = new Object();

  private final Env parent;
  private final Map<String, Object> vars = new ConcurrentHashMap<>();

  public Env() {
    this(null);
  }

  private Env(Env parent) {
    this.parent = parent;
  }

  /**
   * 创建子环境，读取可向父环境回溯，写入仅影响当前作用域。
   */
  public Env createChild() { return new Env(this); }

  public Object get(String name) {
    Object v = vars.get(name);
    if (v != null) {
      return v == NULL ? null : v;
    }
    return parent != null ? parent.get(name) : null;
  }

  public void set(String name, Object v) {
    Object boxed = v == null ? NULL : v;
    // containsKey + put 之间无需原子性：两者都是对同一个键的操作，
    // 竞争的写方本就是"最后写入者胜"，与原语义一致；这里要防的是
    // 底层表结构被并发破坏，而非给赋值加事务。
    if (vars.containsKey(name)) {
      vars.put(name, boxed);
      return;
    }
    if (parent != null && parent.contains(name)) {
      parent.set(name, v);
      return;
    }
    vars.put(name, boxed);
  }

  public boolean contains(String name) {
    if (vars.containsKey(name)) return true;
    return parent != null && parent.contains(name);
  }

  public Set<String> getAllKeys() {
    LinkedHashSet<String> keys = new LinkedHashSet<>();
    collectKeys(keys);
    return Collections.unmodifiableSet(keys);
  }

  private void collectKeys(Set<String> keys) {
    if (parent != null) parent.collectKeys(keys);
    keys.addAll(vars.keySet());
  }
}
