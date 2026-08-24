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
 * Maybe/Result 变体标签的跨引擎一致性（aster-lang-ts#137）。
 *
 * <p>本引擎此前产出 {@code {_type: Ok}}、TS 侧产出 {@code {__type: "Ok"}}——
 * **同一条规则的返回值在两引擎上形状不同**。等价性语料的 cases 全部以
 * {@code __type} 为准（39 处），故本引擎对齐过来。
 *
 * <p>危害不只是"不好看"：宿主拿到的 JSON 一边是 {@code {"_type":"Ok"}}、
 * 一边是 {@code {"__type":"Ok"}}，等价性比对按值比较会判为不等；而这条分叉
 * 已经实际卡住了语料补全（{@code g3-apply-call} 的 {@code wrap}、
 * {@code stdlib_collections} 的 {@code head} 都因两侧表示不同而无法定基线）。
 *
 * <p>fixture 由 TS 编译器产出，期望值即 TS 侧的标签——即断言"同一份 IR 在两引擎上
 * 产出同样的标签"，而非各写一份等价源码（那样验不出形状差异）。
 */
class VariantTagParityTest {

  @ParameterizedTest(name = "{0} 的标签为 __type")
  @CsvSource({
    "Ok of 7,   tagp-ok.json,   Ok",
    "Err of 7,  tagp-err.json,  Err",
    "Some of 7, tagp-some.json, Some",
  })
  void variantTagIsDoubleUnderscore(String label, String fixture, String variant)
      throws Exception {
    String json;
    try (InputStream in =
        getClass().getClassLoader().getResourceAsStream("tagparity/" + fixture)) {
      assertTrue(in != null, "fixture 缺失: " + fixture);
      json = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value r = ctx.eval(Source.newBuilder("aster", json, fixture).build());
      Value v = r.canExecute() ? r.execute() : r;
      // 变体经 polyglot 边界后是 PolyglotMap，用 Map 视图读取（getHashValue 不适用）。
      java.util.Map<?, ?> m = v.as(java.util.Map.class);
      assertTrue(
          m.containsKey("__type"),
          label + " 的标签应为 __type（与 TS 一致），实际键集: " + m.keySet());
      assertEquals(variant, String.valueOf(m.get("__type")), label + " 的变体名");
      assertEquals(7, ((Number) m.get("value")).intValue(), label + " 的载荷应为 7");
    }
  }
}
