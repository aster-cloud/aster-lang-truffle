package aster.truffle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * 宿主传入的 Maybe/Result 变体能被正确识别（aster-lang-ts#138）。
 *
 * <p>宿主经 polyglot 传进来的 map，到达 builtin 时是
 * {@code com.oracle.truffle.host.HostObject} 包装，裸 {@code instanceof Map} 恒为 false。
 * 修复前 {@code Maybe.withDefault({__type:"Some",value:7}, 0)} 会**静默返回兜底值 0**
 * ——不报错，给出一个看起来完全合理的决策值，而 TS 侧同样输入返回 7。
 *
 * <p>这是**真实调用形态**：宿主通过 /evaluate 传 context 时，Maybe/Result
 * 就是以 JSON 对象形式进来的。
 *
 * <p>★这一层极难自查：{@code Builtins.typeName} 走 InteropLibrary 能穿透、
 * 直接 {@code Builtins.call} 传的是裸 Java 对象、{@code Value.as(Object.class)}
 * 会自动解包——三种常用观察手段都显示"值是对的"。只有在 {@code BuiltinCallNode}
 * 实际调用点插桩才看得到 HostObject。故本测试**必须走完整的 ctx.eval + execute 路径**，
 * 不能用 Builtins.call 直调来"验证"。
 */
class HostVariantParityTest {

  private static Map<String, Object> variant(String tag, Object value) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("__type", tag);
    m.put("value", value);
    return m;
  }

  private static String loadIr(String name) throws Exception {
    try (InputStream in =
        HostVariantParityTest.class.getClassLoader().getResourceAsStream("hostvariant/" + name)) {
      assertTrue(in != null, "fixture 缺失: " + name);
      return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
  }

  /** 期望值取自 TS 引擎对同一份 IR、同一入参的输出。 */
  @ParameterizedTest(name = "{0} 接受宿主传入的变体")
  @CsvSource({
    "Maybe.isSome,   hv-isSome.json,     1",
    "Maybe.unwrap,   hv-unwrap.json,     7",
    "Result.isOk,    hv-resultIsOk.json, 1",
  })
  void hostVariantIsRecognized(String label, String fixture, int expected) throws Exception {
    String json = loadIr(fixture);
    Object arg = label.startsWith("Result") ? variant("Ok", 7) : variant("Some", 7);
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value f = ctx.eval(Source.newBuilder("aster", json, fixture).build());
      assertEquals(
          expected,
          f.execute(arg).asInt(),
          label + " 应识别宿主传入的变体（修复前会静默走兜底/返回 false）");
    }
  }

  @Test
  void withDefaultReturnsPayloadNotFallback() throws Exception {
    String json = loadIr("hv-withDefault.json");
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value f = ctx.eval(Source.newBuilder("aster", json, "hv-withDefault.json").build());
      // ★这条是本 issue 的原始症状：修复前返回 0（兜底值）而非 7（载荷）。
      assertEquals(
          7,
          f.execute(variant("Some", 7), 0).asInt(),
          "Maybe.withDefault 应取出 Some 的载荷，而不是静默返回兜底值");
    }
  }

  /**
   * 反向一致性：{@code Map.*} 必须**拒绝**同一个宿主变体（#134）。
   *
   * <p>两处判定共用 {@code asVariantMap} 的穿透逻辑。若只给 Maybe.* 加穿透，
   * 同一个值会被两处判成不同结论——Map.size 当它是普通 map 收下（回到 #134
   * 修掉的静默错答案）、Maybe.map 当它是变体接受。本用例锁住"两头一致"。
   */
  @Test
  void mapStillRejectsHostVariant() throws Exception {
    String json = loadIr("hv-isSome.json").replace("Maybe.isSome", "Map.size");
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      PolyglotException ex =
          assertThrows(
              PolyglotException.class,
              () -> {
                Value f = ctx.eval(Source.newBuilder("aster", json, "map-reject.json").build());
                f.execute(variant("Some", 7)).as(Object.class);
              },
              "Map.* 必须拒绝变体形状的宿主输入（与 Maybe.* 的接受互为反面）");
      assertTrue(
          ex.getMessage().contains("Map.size"),
          "错误信息应指向 Map.size，实际: " + ex.getMessage());
    }
  }
}
