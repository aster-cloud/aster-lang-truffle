package aster.truffle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Map.* 首参类型守卫的跨引擎一致性（aster-lang-ts#132）。
 *
 * <p>全部 fixture 由 TS 编译器产出，喂给本引擎执行——即验证「同一份 IR 两引擎同样拒绝」，
 * 而非各自写一段等价源码（那样验不出 IR 层面的分叉）。
 *
 * <p>修复前 TS 侧 {@code asGuestMap} 把任意值静默兜底成空 Map，于是
 * {@code Maybe.withDefault(Map.get(42,"k"),0)} 得 0——一个**看起来完全合理**的决策值，
 * 而本引擎一直抛 {@code 操作 Map.get 期望类型 Map}。危害高于 Maybe.map 那条：
 * {@code Map.get} 是策略规则读字段的主路径。
 *
 * <p>★断言不止"抛异常"，还核对错误信息指向对应的 Map.* ——否则任何早期失败
 * （fixture 损坏、解析失败）都能让 assertThrows 假通过。反事实已验证：
 * 换成合法 Map 的 fixture 会让测试正确转红。
 */
class MapGuardParityTest {

  private static String loadIr(String name) throws Exception {
    try (InputStream in =
        MapGuardParityTest.class.getClassLoader().getResourceAsStream("map-guard/" + name)) {
      assertTrue(in != null, "fixture 缺失: " + name);
      return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
  }

  /**
   * 七个 Map.* 对 Int 首参一律拒绝。
   *
   * <p>初版只覆盖 Map.get 一个，而 PR 声称的是七个——覆盖面对不上声称本身就是缺陷，
   * 故改为参数化覆盖全部七个。
   */
  @ParameterizedTest(name = "{0} 对非 Map 首参抛错")
  @CsvSource({
    "Map.get, mg-get.json",
    "Map.contains, mg-contains.json",
    "Map.size, mg-size.json",
    "Map.put, mg-put.json",
    "Map.remove, mg-remove.json",
    "Map.keys, mg-keys.json",
    "Map.values, mg-values.json",
  })
  void nonMapFirstArgIsRejected(String op, String fixture) throws Exception {
    String json = loadIr(fixture);
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      // 注意：该类 fixture 的模块体在 eval 阶段就会求值到 Map.*，异常从 ctx.eval
      // 直接抛出，而非等到 execute——故整段包进 assertThrows，不预设它在哪一步炸。
      PolyglotException ex =
          assertThrows(
              PolyglotException.class,
              () -> {
                Value r = ctx.eval(Source.newBuilder("aster", json, fixture).build());
                Value v = r.canExecute() ? r.execute() : r;
                v.as(Object.class);
              },
              op + " 首参非 Map 时本引擎必须抛错（与 TS 侧守卫一致）");
      assertTrue(
          ex.getMessage().contains(op),
          "错误信息应指向 " + op + "，实际: " + ex.getMessage());
    }
  }

  /**
   * ★残留分叉锁（aster-lang-ts#134）：{@code None} 首参本引擎**不抛**。
   *
   * <p>本引擎的 None 是 {@code LinkedHashMap{_type:"None"}}，它本身就是
   * {@code java.util.Map}，于是 {@code Map.size} 的 {@code instanceof Map} 命中，
   * 把 {@code _type} 当成一个键数进去 → 返回 1。而 TS 侧 None 是裸 null，被守卫拒掉。
   *
   * <p>本用例**不是**在断言"正确行为"，而是钉住当前的不一致：将来任一侧改动时
   * 必须显式面对它，而不是让分叉在某次重构里无声消失或反向扩大。
   */
  @Test
  void residualDivergence_noneIsAcceptedHere() throws Exception {
    String json = loadIr("mg-none.json");
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value r = ctx.eval(Source.newBuilder("aster", json, "mg-none.json").build());
      Value v = r.canExecute() ? r.execute() : r;
      assertEquals(
          1,
          v.asInt(),
          "本引擎把 None 当成含 _type 一个键的 Map（TS 侧则拒绝）——见 aster-lang-ts#134");
    }
  }
}
