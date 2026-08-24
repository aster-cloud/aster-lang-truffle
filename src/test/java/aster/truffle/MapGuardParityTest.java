package aster.truffle;

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
   * Maybe/Result 变体不是 Map（aster-lang-ts#134）。
   *
   * <p>这些变体的运行期表示**本身就是 {@code java.util.Map}**（见 {@code maybeNone()}），
   * 此前会被 {@code Map.*} 的 {@code instanceof Map} 收下、把 {@code _type}/{@code value}
   * 当成键来数：{@code Map.size(None)} 返回 1、{@code Map.size(Some(1))} 返回 2——
   * **静默错答案**，不报错却给出看起来完全合理的数字。
   *
   * <p>二维矩阵实测（6 个 Map.* × 6 种输入 = 36 格）：修复前 None 那 6 格两引擎分叉
   * （TS 拒、此处放行）、Some/Ok/Err 那 18 格两引擎"一致地错"。两侧同步收紧后
   * 36 格逐格一致。本用例锁住其中的 None 一格。
   */
  @Test
  void maybeVariantIsRejected() throws Exception {
    String json = loadIr("mg-none.json");
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      PolyglotException ex =
          assertThrows(
              PolyglotException.class,
              () -> {
                Value r = ctx.eval(Source.newBuilder("aster", json, "mg-none.json").build());
                Value v = r.canExecute() ? r.execute() : r;
                v.as(Object.class);
              },
              "None 不是 Map，Map.size 必须拒绝（此前返回 1）");
      assertTrue(
          ex.getMessage().contains("Map.size"),
          "错误信息应指向 Map.size，实际: " + ex.getMessage());
    }
  }
}
