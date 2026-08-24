package aster.truffle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * {@code Maybe.isNone} 语义的跨引擎一致性（aster-lang-ts#131）。
 *
 * <p>TS 侧此前把 isNone 实现成「**不是 Some**」（{@code x?.__type !== 'Some'}），
 * 于是 {@code Maybe.isNone(42)} / {@code Maybe.isNone(Ok(1))} 返回 true；
 * 本引擎写的是 {@code "None".equals(m.get("__type"))}——**是** None 才为真。
 * 同一条规则两引擎走不同分支 → 跨引擎决策翻转，且 TS 那侧不报错（静默错答案）。
 *
 * <p>TS 侧已改为正向判定后，本测试用**由 TS 编译器产出的同一份 IR** 验证
 * 两引擎逐格一致。fixture 覆盖 36 格矩阵里与 isNone 相关的 6 格。
 *
 * <p>期望值以本引擎的既有语义为准（它是对的）：只有 None 为 1，其余皆 0。
 */
class IsNoneParityTest {

  @ParameterizedTest(name = "Maybe.isNone({0}) → {2}")
  @CsvSource({
    "None,     isn-none.json, 1",
    "Some(1),  isn-some.json, 0",
    "Ok(1),    isn-ok.json,   0",
    "Err(1),   isn-err.json,  0",
    "42,       isn-int.json,  0",
    "'\"a\"',  isn-text.json, 0",
  })
  void isNoneMatchesAcrossEngines(String label, String fixture, int expected) throws Exception {
    String json;
    try (InputStream in =
        getClass().getClassLoader().getResourceAsStream("isnone/" + fixture)) {
      assertTrue(in != null, "fixture 缺失: " + fixture);
      json = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value r = ctx.eval(Source.newBuilder("aster", json, fixture).build());
      Value v = r.canExecute() ? r.execute() : r;
      assertEquals(
          expected,
          v.asInt(),
          "Maybe.isNone(" + label + ") 两引擎应一致——isNone 语义是「是 None」而非「不是 Some」");
    }
  }
}
