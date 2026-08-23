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
 * Maybe.map 首参类型守卫的跨引擎一致性（aster-lang-ts#128）。
 *
 * <p>fixture 由 TS 编译器产出（`Maybe.map(42, double)`——首参不是 Maybe）。
 * 修复前 TS 静默返回 None（外层 withDefault 得 0），JVM 抛错：同一段规则两引擎
 * 给出不同结果，且 TS 那侧是**静默错答案**。补上守卫后两边都拒绝。
 */
class MaybeMapGuardParityTest {

  @Test
  void nonMaybeFirstArgIsRejected() throws Exception {
    URL url = getClass().getClassLoader().getResource("stdlib-hof/maybe-map-guard.json");
    assertTrue(url != null, "fixture 缺失");
    String json;
    try (InputStream in = url.openStream()) {
      json = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value r = ctx.eval(Source.newBuilder("aster", json, "maybe-map-guard.json").build());
      PolyglotException ex =
          assertThrows(
              PolyglotException.class,
              () -> {
                Value v = r.canExecute() ? r.execute(0) : r;
                v.as(Object.class);
              },
              "首参非 Maybe 时本引擎必须抛错（与 TS 侧守卫一致）");
      assertTrue(
          ex.getMessage().contains("Maybe.map"),
          "错误信息应指向 Maybe.map，实际: " + ex.getMessage());
    }
  }
}
