package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * {@code Builtins.REGISTRY} 的线程安全与 null 安全（issue aster-lang-truffle#107 正文）。
 *
 * <p>★真实缺陷：REGISTRY 原为普通 {@code HashMap}，而 {@code register()} 是
 * {@code public static}，确有静态块之外的运行时调用先例。任何运行时 register 与其他线程的
 * {@code Builtins.call} 并发，即是对非线程安全 HashMap 的写读竞态——
 * 轻则丢条目，重则结构损坏（HashMap 并发扩容可致读操作死循环）。
 *
 * <p>本仓正好具备触发条件：workflow 的 step 体在 executor worker 线程上执行，
 * 那里会调 {@code Builtins.call}；而 {@code register} 可被任意线程调用。
 */
class BuiltinsRegistryConcurrencyTest {

  @Test
  void concurrentRegisterAndCallDoNotLoseEntries() throws Exception {
    // ★并发写读：注册线程持续 register，查询线程持续 exists/get。
    //   HashMap 下这会丢条目或结构损坏；ConcurrentHashMap 下必须每条都能查到。
    int n = 200;
    ExecutorService pool = Executors.newFixedThreadPool(8);
    var errors = new ConcurrentLinkedQueue<String>();
    var start = new CountDownLatch(1);
    var done = new CountDownLatch(2);
    try {
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < n; i++) {
            final int v = i;
            Builtins.register("__concProbe" + i, new Builtins.BuiltinDef(args -> v));
          }
        } catch (Throwable t) {
          errors.add("writer: " + t);
        } finally {
          done.countDown();
        }
      });
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < n * 5; i++) {
            // 并发读既有条目——不得抛异常，也不得死循环
            Builtins.has("List.length");
          }
        } catch (Throwable t) {
          errors.add("reader: " + t);
        } finally {
          done.countDown();
        }
      });
      start.countDown();
      assertTrue(done.await(30, TimeUnit.SECONDS), "并发读写超时（HashMap 结构损坏可致死循环）");
      assertTrue(errors.isEmpty(), "并发读写不得抛异常: " + errors);

      // 全部注册的条目都必须查得到——HashMap 并发写会丢条目
      for (int i = 0; i < n; i++) {
        assertTrue(Builtins.has("__concProbe" + i), "条目 __concProbe" + i + " 丢失");
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void nullNameLookupReturnsNullInsteadOfThrowing() {
    // ★换用 ConcurrentHashMap 后必须补 null 安全（issue #107 的连带风险）：
    //   canonicalName(null) 返回 null，而 HashMap.get(null) 老实返回 null、
    //   ConcurrentHashMap.get(null) **抛 NPE**。
    //   不补的话，「换个 Map 实现」这个看似无害的改动会把「查不到」变成「崩溃」。
    assertDoesNotThrow(() -> {
      assertFalse(Builtins.has(null), "null 名字应视为不存在，而不是抛 NPE");
    });
    assertDoesNotThrow(() -> assertNull(Builtins.defOf(null), "null 名字应返回 null"));
  }

  @Test
  void existingBuiltinsStillResolve() {
    // ★反向护栏：没有这条，把 lookup 写成「恒返回 null」也能让上面两条变绿。
    assertTrue(Builtins.has("List.length"), "既有 builtin 必须仍可解析");
    assertTrue(Builtins.has("plus"), "别名（plus → add）必须仍可解析");
  }
}
