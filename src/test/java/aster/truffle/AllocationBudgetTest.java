package aster.truffle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import aster.truffle.runtime.Builtins;
import java.io.InputStream;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

/**
 * 累计分配预算（{@link aster.truffle.runtime.AllocationBudget}）的行为锁定。
 *
 * <p><b>本测试守的是什么</b>：单个 builtin 各自有上限（{@code List.range} 1e6、
 * {@code List.combinations} C(n,k)≤5000），但**组合起来**此前没有任何约束——
 * {@code List.concat} 反复翻倍可用不到 200 字节源码把规模推到约 1.3 亿元素。
 *
 * <p>修复前实测：本引擎 1403ms 抛 {@code Java heap space}（宿主 5 秒看门狗
 * **来不及**，OOM 比超时快）；TS 侧 128M 元素吃 2.8GB 堆且返回 success。
 *
 * <p>fixture 由 TS 编译器产出，与 aster-lang-ts 的同名用例**喂同一份 IR**，
 * 保证两引擎在同一输入上给出同一判定（parity）。
 */
class AllocationBudgetTest {

  private static Source load(String fixture) throws Exception {
    String json;
    try (InputStream in =
        AllocationBudgetTest.class.getClassLoader()
            .getResourceAsStream("allocbudget/" + fixture)) {
      assertNotNull(in, "fixture 缺失: " + fixture);
      json = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
    return Source.newBuilder("aster", json, fixture).build();
  }

  private static Value run(Context ctx, String fixture) throws Exception {
    Value r = ctx.eval(load(fixture));
    return r.canExecute() ? r.execute(0) : r;
  }

  /**
   * 放大攻击必须被拒绝，且是**域内错误**而非 OOM。
   *
   * <p>断言消息内容而不只是「抛了异常」：修复前它也抛异常（{@code Java heap space}），
   * 只断言「抛了」会让这个测试在缺陷回归后**依然是绿的**。
   */
  @Test
  void concatAmplificationIsRejectedBeforeOom() throws Exception {
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      PolyglotException ex =
          assertThrows(PolyglotException.class, () -> run(ctx, "attack.json"));
      String msg = String.valueOf(ex.getMessage());
      assertTrue(
          msg.contains("分配预算耗尽"),
          "应因分配预算被拒（而非 OOM/其他错误）。实际: " + msg);
      assertTrue(
          !msg.contains("Java heap space"),
          "不应退化为堆溢出——那说明预算没在物化之前生效。实际: " + msg);
    }
  }

  /**
   * ★攻击藏在 lambda 回调里也必须被拦住。
   *
   * <p>预算的重置点在 {@code LambdaRootNode}（有参入口的真实执行路径），而该节点
   * 同时承担**每一次内部 lambda 调用**。若在那里无条件重置，{@code List.map} 的
   * 每次回调都会把额度清零 → 守护形同虚设，且**前两个测试依然全绿**
   * （它们的攻击不经过 lambda）。故必须单独钉住这一形态。
   *
   * <p>本用例：20 次回调，每次造 2e6 元素（累计 4e7 ≫ 1e7 预算）。
   */
  @Test
  void amplificationInsideLambdaCallbackIsAlsoRejected() throws Exception {
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      PolyglotException ex =
          assertThrows(PolyglotException.class, () -> run(ctx, "lambda_attack.json"));
      String msg = String.valueOf(ex.getMessage());
      assertTrue(
          msg.contains("分配预算耗尽"),
          "lambda 回调内的放大必须同样受预算约束——否则重置点把守护清空了。实际: " + msg);
      assertTrue(
          !msg.contains("Java heap space"),
          "不应退化为堆溢出——那说明预算没在物化之前生效。实际: " + msg);
    }
  }

  /**
   * ★{@code Text.concat} 字符串翻倍必须同样被拦住。
   *
   * <p>这条向量与集合向量**同构但类型不同**：返回的是 {@code String}，
   * 若计量只认 Collection/Map 就扣 0 通过。实测修复前 509ms 物化 1.34 亿字符
   * （UTF-16 约 268MB）并**返回 success**；25 次翻倍则 292ms 直接
   * {@code Java heap space}——同样快过宿主 5 秒看门狗。
   *
   * <p>另一层意义是 **parity**：TS 侧先补上了字符串计量，Java 侧未补时同一份 IR
   * 一边拒绝一边 OOM，直接违反「两引擎逐字节一致」。
   *
   * <p>还有一个结构性教训：{@code Text.concat} 在 {@code BuiltinCallNode} 有
   * **内联快速路径**，直接返回拼接结果而**不经过** {@code Builtins.call}——
   * 「所有 builtin 都经过 call()」这个假设是错的，内联特化绕开了它。
   */
  @Test
  void textConcatAmplificationIsRejected() throws Exception {
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      PolyglotException ex =
          assertThrows(PolyglotException.class, () -> run(ctx, "text_attack.json"));
      String msg = String.valueOf(ex.getMessage());
      assertTrue(
          msg.contains("分配预算耗尽"),
          "字符串放大必须被预算拦住（内联路径也要记账）。实际: " + msg);
      assertTrue(
          !msg.contains("Java heap space"),
          "不应退化为堆溢出。实际: " + msg);
    }
  }

  /**
   * ★worker 线程复用下，额度必须按帧重置，否则该线程被**永久毒化**。
   *
   * <p>workflow step / async task 的体经 {@code Exec.exec} 在 executor worker 线程上执行，
   * **既不经过 AsterRootNode 也不经过 LambdaRootNode**。若那两处不接 enterFrame/exitFrame：
   * <ul>
   *   <li>漏防：整条 async/workflow 路径上预算等于没上线；</li>
   *   <li>误杀：额度只增不清，一旦触线 {@code charge} 会把计数钉死在 {@code MAX+1}，
   *       该 worker 此后**拒绝一切 step**——而线程池跨 workflow、跨租户复用，
   *       等于一个租户的一次超限拖垮其他租户的后续请求。</li>
   * </ul>
   *
   * <p>本用例直接在单线程 executor 上复现该语义（比构造完整 workflow IR 更稳，
   * 且锁住的正是 enterFrame/exitFrame 这对语义本身）：4 个各自合法的 6e6，
   * 接线后全过；不接线则第 2 个起全被误拒（实测 spent 恒为 10000001）。
   */
  @Test
  void budgetResetsPerFrameOnReusedWorkerThread() throws Exception {
    java.util.concurrent.ExecutorService pool =
        java.util.concurrent.Executors.newSingleThreadExecutor();
    try {
      for (int i = 1; i <= 4; i++) {
        final int step = i;
        String outcome = pool.submit(() -> {
          boolean outermost = aster.truffle.runtime.AllocationBudget.enterFrame();
          try {
            aster.truffle.runtime.AllocationBudget.charge(6_000_000L);
            return "OK";
          } catch (RuntimeException e) {
            return "REJECTED:" + aster.truffle.runtime.AllocationBudget.spent();
          } finally {
            aster.truffle.runtime.AllocationBudget.exitFrame(outermost);
          }
        }).get();
        assertEquals(
            "OK",
            outcome,
            "第 " + step + " 个 step（各自 6e6 合法）应通过——额度未按帧重置会毒化整个 worker 线程");
      }
    } finally {
      pool.shutdown();
    }
  }

  /**
   * ★{@code List.map} 链必须被计量——锁住 {@code BuiltinCallNode} 的**内联**记账点。
   *
   * <p>为什么单列：{@code List.map}/{@code List.append}/{@code List.filter} 都有内联
   * 快速路径，直接返回结果、不经过 {@code Builtins.call}，各自补了
   * {@code chargeForResult}。但在本用例加入之前，把这 3 处记账**全部删掉**，
   * 其余 5 个用例**依然全绿**——因为它们的攻击向量只经过 {@code List.range}/
   * {@code List.concat}（走通用 {@code call}）和 {@code Text.concat}（唯一被锁住的内联点）。
   *
   * <p>即这 3 处内联记账此前是「删了不报红」的无保护状态，未来重构极易被当作
   * 冗余代码清掉。本用例用 12 次链式 map（1e6 × 12 = 1.2e7 &gt; 1e7 预算）把它钉死：
   * 实测删掉记账后本用例 SUCCESS（1237ms），恢复后 REJECTED（1144ms）。
   */
  @Test
  void listMapChainIsMeteredViaInlinedPath() throws Exception {
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      PolyglotException ex =
          assertThrows(PolyglotException.class, () -> run(ctx, "map_chain.json"));
      assertTrue(
          String.valueOf(ex.getMessage()).contains("分配预算耗尽"),
          "List.map 的内联路径必须记账——否则链式 map 可无限累积。实际: " + ex.getMessage());
    }
  }

  /**
   * ★guest 列表载体 {@code AsterListValue} 必须被计量。
   *
   * <p>它 {@code implements TruffleObject} 而**不是** {@code Collection}，
   * 也不是 {@code Map}——两个 {@code instanceof} 都为 false（本用例头两行断言即证）。
   * 而姊妹类 {@code AsterMapValue} 恰好 {@code implements Map}，所以一直被计量，
   * 两个载体口径此前不一致，只有列表那侧漏了。
   *
   * <p>不是理论缺口：{@code LambdaRootNode} 对每个返回值调
   * {@code AsterInteropAdapter.adapt}，那里 {@code new ArrayList<>(list.size())}
   * **真实拷贝一份**——即每次用户 Rule 返回列表都物化一份副本，此前零记账。
   *
   * <p>用直调而非端到端 fixture：端到端场景里 {@code List.range} 等叶子调用
   * 已走通用 {@code Builtins.call} 被计量，会**掩盖**本分支的缺失
   * （实测：关掉本分支，端到端用例照样 REJECTED，变异存活）。直调才有鉴别力。
   */
  @Test
  void guestListCarrierIsMetered() throws Exception {
    Object guestList = new aster.truffle.runtime.interop.AsterListValue(
        new java.util.ArrayList<>(java.util.Collections.nCopies(1_000, (Object) 1)));
    assertTrue(
        !(guestList instanceof java.util.Collection),
        "前提：AsterListValue 不是 Collection——若哪天它变成了，本用例的理由需重写");
    assertTrue(
        !(guestList instanceof java.util.Map),
        "前提：AsterListValue 也不是 Map");

    long before = aster.truffle.runtime.AllocationBudget.spent();
    Builtins.chargeForResult(guestList);
    assertEquals(
        1_000L,
        aster.truffle.runtime.AllocationBudget.spent() - before,
        "guest 列表载体必须按元素数扣减——否则 Rule 返回列表的 adapt 拷贝全部逃出预算");
  }

  /**
   * ★嵌套容器不得靠「顶层小」逃过计量（groupBy 复用绕过）。
   *
   * <p>这是「只计顶层」那版设计的 Critical 漏洞：
   * {@code List.groupBy(a, one)} 把 1e6 元素全归进**同一组**，返回的 Map
   * 顶层 {@code size=1}——只扣 1 点额度，实际物化 1e6。重复 20 次，
   * 840 字节源码即可物化 2000 万元素而只扣 20 点。
   *
   * <p>实测（修复前，本引擎无步数上限故可直接打穿）：
   * {@code SUCCESS, 1972ms, heapDelta=210MB}，预算形同虚设。
   *
   * <p>错在哪：原注释推理「内层元素来自已被计过的入参列表，故不会漏计」——
   * 入参只被扣过**一次**，但可被**重新物化任意多次**。修法是有界深度递归计量。
   */
  @Test
  void nestedContainerReuseIsMetered() throws Exception {
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      PolyglotException ex =
          assertThrows(PolyglotException.class, () -> run(ctx, "groupby_reuse.json"));
      String msg = String.valueOf(ex.getMessage());
      assertTrue(
          msg.contains("分配预算耗尽"),
          "groupBy 反复重新物化入参必须被计量——只计顶层会让 1e6 元素只扣 1 点。实际: " + msg);
      assertTrue(
          !msg.contains("Java heap space"),
          "不应退化为堆溢出。实际: " + msg);
    }
  }

  /** 预算内的大批量运算不得被误拒（防守护过严把合法策略打死）。 */
  @Test
  void withinBudgetStillSucceeds() throws Exception {
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      assertEquals(2_000_000, run(ctx, "legit.json").asInt());
    }
  }

  /**
   * ★跨执行不得残留：同一 Context 连续两次执行同一段「单次 4e6」的程序，
   * 两次都必须成功。
   *
   * <p>预算是 ThreadLocal，而 GraalVM 单线程策略**拒并发但允许串行交接**，
   * 池线程跨执行存活。若顶层 eval 边界不 reset，第一次烧掉的 4e6 会被第二次继承，
   * 累计 8e6 仍在 1e7 内、第三次就炸——表现为**完全合法的策略随机失败**，
   * 且失败与否取决于该线程此前跑过什么，是最难查的一类缺陷。
   */
  @Test
  void budgetDoesNotLeakAcrossSequentialEvals() throws Exception {
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      for (int i = 1; i <= 3; i++) {
        assertEquals(
            4_000_000,
            run(ctx, "repeat.json").asInt(),
            "第 " + i + " 次执行应与第 1 次完全一致——预算未按 eval 边界重置");
      }
    }
  }
}
