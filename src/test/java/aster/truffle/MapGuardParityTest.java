package aster.truffle;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.net.URL;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

/**
 * Map.* 首参类型守卫的跨引擎一致性（aster-lang-ts#132）。
 *
 * <p>fixture 由 TS 编译器产出：{@code Maybe.withDefault(Map.get(42, "k"), 0)}——
 * 首参不是 Map。修复前 TS 侧 {@code asGuestMap} 把任意值静默兜底成空 Map，
 * 于是返回兜底值 0（一个**看起来完全合理**的决策值），而本引擎一直抛
 * {@code 操作 Map.get 期望类型 Map}。
 *
 * <p>危害高于 Maybe.map 那条：{@code Map.get} 是策略规则读字段的主路径，
 * {@code Maybe.withDefault(Map.get(applicant,"creditScore"), 0)} 会让同一条规则
 * 在两引擎上一边出决策、一边拒绝执行。
 *
 * <p>★断言不止"抛异常"，还核对错误信息指向 Map.get——否则任何早期失败
 * （fixture 损坏、解析失败）都能让 assertThrows 假通过。
 */
class MapGuardParityTest {

  @Test
  void nonMapFirstArgIsRejected() throws Exception {
    URL url = getClass().getClassLoader().getResource("stdlib-hof/map-guard.json");
    assertTrue(url != null, "fixture 缺失");
    String json;
    try (InputStream in = url.openStream()) {
      json = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      // 注意：该 fixture 的模块体在 eval 阶段就会求值到 Map.get，异常从 ctx.eval
      // 直接抛出，而非等到 execute——故整段包进 assertThrows，不预设它在哪一步炸。
      PolyglotException ex =
          assertThrows(
              PolyglotException.class,
              () -> {
                Value r = ctx.eval(Source.newBuilder("aster", json, "map-guard.json").build());
                Value v = r.canExecute() ? r.execute() : r;
                v.as(Object.class);
              },
              "首参非 Map 时本引擎必须抛错（与 TS 侧守卫一致）");
      assertTrue(
          ex.getMessage().contains("Map.get"),
          "错误信息应指向 Map.get，实际: " + ex.getMessage());
    }
  }
}
