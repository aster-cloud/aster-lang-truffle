package aster.truffle.nodes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * {@link Env} 的跨线程安全性（2026-08-17 审计）。
 *
 * <p>{@code Env} 的 {@code vars} 是裸 {@link java.util.HashMap}，无任何同步。
 * 而它<b>确实会被工作线程写</b>：{@link StartNode} 把子表达式包成 Runnable 提交给
 * {@code AsyncTaskRegistry} 的固定线程池（默认大小 = CPU 核心数），
 * 该子表达式树里的 {@link SetNodeEnv} / {@link LetNodeEnv} 持有的正是
 * <b>与 eval 线程同一个</b> {@code Env} 实例。
 *
 * <p>更关键的是 {@code Env.set} 的语义：变量已存在于<b>父</b>作用域时写会
 * 向上冒泡到父 Env。于是多个并发任务各自的子作用域写同一个继承变量时，
 * 全部落到<b>同一个</b> HashMap 上。
 *
 * <p>HashMap 并发写不是「偶尔丢一次更新」这种良性竞态——扩容期间可能
 * 破坏内部结构，历史上在 JDK 7 能导致 {@code get} 死循环，JDK 8 之后
 * 表现为丢数据 / 结构损坏 / 抛 {@code ConcurrentModificationException}。
 * 对合规决策引擎而言，「静默丢一次赋值」意味着<b>判定依据被改变而无人知晓</b>。
 */
class EnvConcurrencyTest {

  /** 并发写同一继承变量：暴露裸 HashMap 无同步。 */
  @Test
  void concurrentWritesToInheritedVariableDoNotLoseUpdates() throws Exception {
    final int threads = 8;
    final int writesPerThread = 2000;

    // 父作用域先声明变量 → 子作用域的写会冒泡到父，全部落在同一个 HashMap 上，
    // 这正是并发 start 任务共享 Env 时的形态。
    Env parent = new Env();
    for (int i = 0; i < threads; i++) {
      parent.set("shared" + i, 0);
    }

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger done = new AtomicInteger();

    try {
      for (int t = 0; t < threads; t++) {
        final int id = t;
        pool.submit(() -> {
          Env child = parent.createChild();
          try {
            start.await();
            for (int i = 0; i < writesPerThread; i++) {
              // 每个线程只写"自己"的键：没有逻辑上的写-写冲突，
              // 唯一的共享点是底层那一个 HashMap。
              child.set("shared" + id, i);
            }
          } catch (Throwable e) {
            failure.compareAndSet(null, e);
          } finally {
            done.incrementAndGet();
          }
        });
      }
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "并发写未在超时内结束");
    } finally {
      pool.shutdownNow();
    }

    assertEquals(threads, done.get(), "所有线程都应跑完");
    assertEquals(null, failure.get(), "并发写不应抛异常，实际：" + failure.get());

    // 每个键都必须存在且是该线程写入的最后一个值。
    // HashMap 并发扩容丢条目时，这里会读到 null。
    for (int i = 0; i < threads; i++) {
      assertEquals(writesPerThread - 1, parent.get("shared" + i),
          "键 shared" + i + " 的最终值丢失或被破坏——Env 的 HashMap 并发写损坏");
    }
  }

  /**
   * null 值语义必须与改用 ConcurrentHashMap 之前<b>完全一致</b>。
   *
   * <p>ConcurrentHashMap 不接受 null 值，内部改用哨兵存储。此处锁死可观察行为：
   * {@code Loader} 会以 null 预登记函数名占位（Loader.java:126），
   * 若哨兵泄漏到外部或 contains/get 语义变化，会静默破坏名字解析。
   */
  @Test
  void nullValueSemanticsUnchanged() {
    Env env = new Env();
    env.set("declared", null);

    assertEquals(null, env.get("declared"), "存入 null 应读回 null，而不是哨兵对象");
    assertTrue(env.contains("declared"), "存入 null 的键必须视为存在（Loader 靠这个预登记函数名）");
    assertTrue(env.getAllKeys().contains("declared"), "存入 null 的键必须出现在 getAllKeys 中");

    assertEquals(null, env.get("neverSet"), "从未赋值的键读回 null");
    assertTrue(!env.contains("neverSet"), "从未赋值的键不应视为存在");

    // null → 非 null → null 往返
    env.set("declared", 42);
    assertEquals(42, env.get("declared"));
    env.set("declared", null);
    assertEquals(null, env.get("declared"), "改回 null 后仍应读回 null");
    assertTrue(env.contains("declared"), "改回 null 后键仍存在");

    // 父作用域存 null，子作用域读取要能回溯到父。
    Env child = env.createChild();
    assertEquals(null, child.get("declared"));
    assertTrue(child.contains("declared"), "子作用域应能看到父作用域里值为 null 的键");

    // ★写冒泡：子作用域写一个只存在于父的键，必须落到父上（原语义），
    //   哨兵不得让 contains 判断失效导致写变成子作用域新建。
    child.set("declared", 7);
    assertEquals(7, env.get("declared"), "子作用域的写必须冒泡到父作用域");
  }

  /** 一写一读：读侧不得看到破损结构（null / 异常）。 */
  @Test
  void concurrentReadDuringWriteSeesConsistentState() throws Exception {
    Env parent = new Env();
    parent.set("known", 1);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    CountDownLatch start = new CountDownLatch(1);
    final int rounds = 20000;

    try {
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < rounds; i++) {
            // ★必须直接写 parent：写子作用域里的新键只会落在 child 自己的 map 上，
            //   压不到读侧那张表，会得到一个恒绿的假测试。
            //   新键持续增长 → 触发 HashMap 扩容，扩容期结构最脆弱。
            parent.set("k" + i, i);
          }
        } catch (Throwable e) {
          failure.compareAndSet(null, e);
        }
      });
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < rounds; i++) {
            // 读一个自始至终存在的键：任何时刻都不应读到 null。
            if (parent.get("known") == null) {
              failure.compareAndSet(null,
                  new AssertionError("并发扩容期间读到 null——Env 结构被破坏"));
              return;
            }
          }
        } catch (Throwable e) {
          failure.compareAndSet(null, e);
        }
      });
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "读写未在超时内结束");
    } finally {
      pool.shutdownNow();
    }

    assertEquals(null, failure.get(), "并发读写不应观察到破损状态，实际：" + failure.get());
  }
}
