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
    final int threads = 4;
    final int writesPerThread = 3000;

    // ★这条用例的原始写法是**结构性假绿**（2026-08-18 对抗性审查实测）：
    //   它在并发段开始前就把全部键 set 好，之后每个线程只写**已存在**的键。
    //   键集恒定 ⇒ HashMap 从不扩容 ⇒ 结构损坏窗口根本不存在；
    //   put 已存在的键只改 Node.value（单次引用写，JMM 下无撕裂）。
    //   实证：把 Env 还原成 HashMap 后该用例 30/30 全绿——它检不出自己声称要检的东西。
    //
    //   改为**持续新增键**（触发扩容）+ 每个线程直写 parent（压到同一张表）
    //   + 读侧同时校验全部已写键与 getAllKeys()。
    //   审查者实测该形态在 HashMap 上 6/6 变红、CHM 上 6/6 全绿。
    Env parent = new Env();
    Env child = parent.createChild();
    // 一个自始至终存在的稳定键：结构被破坏时它会读成 null。
    parent.set("stable", "kept");

    ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
    CountDownLatch start = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger done = new AtomicInteger();

    try {
      for (int t = 0; t < threads; t++) {
        final int id = t;
        pool.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < writesPerThread; i++) {
              // 直写 parent 且键持续新增 → 反复触发 HashMap 扩容。
              parent.set("p" + id + "_" + i, i);
            }
          } catch (Throwable e) {
            failure.compareAndSet(null, e);
          } finally {
            done.incrementAndGet();
          }
        });
      }
      // 读侧：与写并发地遍历链（getAllKeys 会递归 collectKeys），
      // HashMap 并发扩容时这里会抛 ConcurrentModificationException。
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < writesPerThread; i++) {
            if (child.get("stable") == null) {
              failure.compareAndSet(null,
                  new AssertionError("并发扩容期间稳定键读到 null——Env 结构被破坏"));
              return;
            }
            child.getAllKeys();
          }
        } catch (Throwable e) {
          failure.compareAndSet(null, e);
        } finally {
          done.incrementAndGet();
        }
      });
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS), "并发读写未在超时内结束");
    } finally {
      pool.shutdownNow();
    }

    assertEquals(threads + 1, done.get(), "所有线程都应跑完");
    assertEquals(null, failure.get(), "并发读写不应抛异常，实际：" + failure.get());

    // 全部写入的键都必须还在：HashMap 并发扩容丢条目时这里会读到 null。
    for (int t = 0; t < threads; t++) {
      for (int i = 0; i < writesPerThread; i++) {
        assertEquals(i, parent.get("p" + t + "_" + i),
            "键 p" + t + "_" + i + " 丢失或被破坏——Env 底层表并发写损坏");
      }
    }
    assertEquals(writesPerThread * threads + 1, parent.getAllKeys().size(),
        "键总数不符——有条目在并发扩容中丢失");
    for (int i = 0; i < threads; i++) {
      assertEquals("kept", parent.get("stable"),
          "稳定键必须始终可读");
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

  /**
   * 子作用域存 null 必须<b>遮蔽</b>父作用域的值（哨兵语义的第二半）。
   *
   * <p>★对抗性审查（2026-08-18）用变异测试找出的**存活变异体**：把 {@code get} 的
   * {@code if (v != null) return v == NULL ? null : v;} 改成
   * {@code if (v != null && v != NULL) return v;}（遇哨兵继续回溯父作用域），
   * 全量 <b>363 个测试全绿</b>——整个套件对这条语义零覆盖。
   *
   * <p>哨兵引入的正是这类风险：原来子作用域里存的 Java {@code null} 天然遮蔽父值；
   * 换成哨兵后若解包逻辑写错，子作用域的「显式置空」会静默回退成父作用域的旧值。
   * 对决策引擎而言这等于<b>一个本该被清空的变量又变回有值</b>。
   */
  @Test
  void childNullShadowsParentValue() {
    Env parent = new Env();
    parent.set("v", 1);

    Env child = parent.createChild();
    assertEquals(1, child.get("v"), "未遮蔽前子作用域应看到父值");

    // 让 child 自己拥有该键并置为 null：Env.set 的冒泡语义会写到 parent
    // （因为 parent 已有该键），故这里用一个 child 独有的键验证遮蔽语义。
    Env fresh = parent.createChild();
    fresh.set("onlyChild", null);
    assertEquals(null, fresh.get("onlyChild"), "子作用域存 null 应读回 null");
    assertTrue(fresh.contains("onlyChild"), "存 null 的键在子作用域应视为存在");
    assertTrue(!parent.contains("onlyChild"), "子作用域新建的键不得泄漏到父作用域");

    // ★关键构造：**外层必须持有该键的非 null 值**，中间层再存 null。
    //   否则「遇哨兵继续回溯」这个变异回溯到外层也读不到值，同样返回 null，
    //   两种实现给出相同答案 → 用例无鉴别力（我第一版就是这么写的，实测变异存活）。
    //
    //   注意 Env.set 的冒泡语义：若父链已有该键，子作用域的 set 会写到父上。
    //   所以中间层要"自己持有"该键必须绕过 set —— 用 createChild 后先在
    //   parent 建键、再让 mid 通过**直接写自己的 map**是不可能的（无此 API）。
    //   故改用两条独立链路验证：
    //     链路①（下方 shadowRoot）：root 有值 → child 存 null → child 读必须是 null。
    //       child.set 会冒泡写到 root，所以这里验证的是"冒泡后 root 存了哨兵，
    //       读回来必须是 null 而不是回溯失败"。
    Env shadowRoot = new Env();
    shadowRoot.set("s", 42);
    Env shadowChild = shadowRoot.createChild();
    shadowChild.set("s", null);        // 冒泡到 root，root 存哨兵
    assertEquals(null, shadowChild.get("s"),
        "置 null 后必须读回 null——哨兵不得被当成\"未命中\"而继续回溯到旧值");
    assertEquals(null, shadowRoot.get("s"),
        "root 自身读同一个哨兵也必须是 null");
    assertTrue(shadowChild.contains("s"), "置 null 后该键仍应视为存在");

    // 链路②：更强的一条——让**父链上层有非 null 值、下层有哨兵**，
    //   这样"遇哨兵回溯"会读到上层的旧值，与正确行为（null）可区分。
    //   构造法：先让 leaf 自己拥有该键（父链都没有 → set 落在 leaf 自己的 map），
    //   再往 root 写同名键（此时 leaf 已有，不影响）。
    Env root2 = new Env();
    Env leaf2 = root2.createChild();
    leaf2.set("t", null);              // 父链无此键 → 哨兵落在 leaf2 自己的 map
    root2.set("t", 7);                 // root2 独立持有非 null 值
    assertEquals(null, leaf2.get("t"),
        "leaf 自身的哨兵必须遮蔽 root 的值 7——遇哨兵继续回溯就会错读成 7");
    assertEquals(7, root2.get("t"), "root 自身的值不受影响");
  }
}
