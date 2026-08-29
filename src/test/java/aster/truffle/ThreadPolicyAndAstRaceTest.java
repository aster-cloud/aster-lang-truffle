package aster.truffle;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * 多线程访问策略与共享 AST 的并发改写（issue #104 正文）。
 *
 * <p>★缺陷有两半：
 *
 * <ol>
 *   <li>{@code AsterLanguage} 未覆写 {@code isThreadAccessAllowed}。而 workflow 的 step 体
 *       在 executor worker 线程上执行 Truffle 节点树，{@code TruffleLanguage} 的**默认**
 *       实现只允许单线程访问——不覆写时是否放行取决于运行时「单 context 快速路径」等
 *       实现细节，属**未定义行为**而非契约保证。</li>
 *   <li>{@code BuiltinCallNode.getParallelListMapNode()} 是**无锁** check-then-insert。
 *       两个 step 并发调 {@code List.map} 会双双看到 null、双双 {@code insert()}——
 *       Truffle 的 {@code insert()} 改写 {@code @Child} 字段与父指针，
 *       并发改写共享 AST 属未定义行为。</li>
 * </ol>
 *
 * <p>★如实标注：竞态窗口窄，本测试是**压力型**而非确定性复现——它不能证明竞态已不存在，
 * 只能在回归时提高撞见的概率。真正的保证来自代码结构（{@code getLock()} + double-check
 * 使双重 insert 从**可能**变为**不可能**），而非本测试变绿。
 */
class ThreadPolicyAndAstRaceTest {

  private String fixture(String name) throws Exception {
    try (InputStream in = getClass().getClassLoader().getResourceAsStream("thread-safety/" + name)) {
      assertNotNull(in, "fixture 缺失: " + name);
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  @Test
  void languageDeclaresMultiThreadAccess() {
    // ★这条是**确定性**的：直接断言策略已声明，不依赖时序。
    //   没有它，把 isThreadAccessAllowed 删掉只会让下面的压力测试偶尔变红。
    var lang = new AsterLanguage();
    assertTrue(
        invokeThreadAccessAllowed(lang, Thread.currentThread(), true),
        "单线程场景必须放行");
    assertTrue(
        invokeThreadAccessAllowed(lang, Thread.currentThread(), false),
        "★多线程场景必须显式放行——workflow step 在 worker 线程上执行 Truffle 节点树，"
            + "不声明就是依赖运行时未定义行为");
  }

  private boolean invokeThreadAccessAllowed(AsterLanguage lang, Thread t, boolean singleThreaded) {
    try {
      var m = com.oracle.truffle.api.TruffleLanguage.class
          .getDeclaredMethod("isThreadAccessAllowed", Thread.class, boolean.class);
      m.setAccessible(true);
      return (boolean) m.invoke(lang, t, singleThreaded);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("无法反射调用 isThreadAccessAllowed", e);
    }
  }

  @Test
  void concurrentStepsSharingAstDoNotCorrupt() throws Exception {
    // 压力型：4 个 step 并发跑同一段含 List.map 的 AST，反复多轮。
    // 每轮都要求结果正确（0）——AST 被写坏时表现为异常或错值。
    String src = fixture("concurrent-list-map.json");
    for (int round = 0; round < 20; round++) {
      final int r = round;
      try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
        Value v = assertDoesNotThrow(() -> {
          Value res = ctx.eval(Source.newBuilder("aster", src, "race" + r).build());
          return res.canExecute() ? res.execute() : res;
        }, "第 " + r + " 轮：并发 step 共享 AST 不得崩");
        assertEquals(0, v.asInt(), "第 " + r + " 轮结果应为 0");
      }
    }
  }

  @Test
  void multipleContextsRunWorkflowsConcurrently() throws Exception {
    // ★多 Context 并存：issue 提到「宿主同 JVM 池化多个 Context 时」是风险场景。
    //   实测这条在修复前也通过——如实记录，不假装它证明了什么额外的东西；
    //   它的价值是**回归护栏**：确保线程策略声明没有反而打断多 Context 场景。
    String src = fixture("concurrent-list-map.json");
    int n = 4;
    var pool = java.util.concurrent.Executors.newFixedThreadPool(n);
    var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();
    var latch = new java.util.concurrent.CountDownLatch(n);
    try {
      for (int i = 0; i < n; i++) {
        final int id = i;
        pool.submit(() -> {
          try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value res = ctx.eval(Source.newBuilder("aster", src, "multi" + id).build());
            assertEquals(0, (res.canExecute() ? res.execute() : res).asInt());
          } catch (Throwable t) {
            errors.add(t);
          } finally {
            latch.countDown();
          }
        });
      }
      assertTrue(latch.await(180, java.util.concurrent.TimeUnit.SECONDS), "并发 Context 超时");
      assertTrue(errors.isEmpty(), "并发 Context 出错: " + errors);
    } finally {
      pool.shutdownNow();
    }
  }
}
