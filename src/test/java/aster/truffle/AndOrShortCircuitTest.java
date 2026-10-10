package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * and/or 短路求值：右操作数仅在左操作数不足以决定结果时求值（与 TS 解释器一致）。
 *
 * <p>右操作数统一是 {@code List.get([1,2,3,4], 4) == 14}——一旦被求值就抛越界，
 * 因此「不抛」即证明未求值，「抛越界」即证明确实求值了。经 Loader 走完整 Core IR 路径，
 * 覆盖中缀与前缀两种写法（二者在 Core IR 中都是 {@code Call{Name "and"|"or"}}）。
 */
class AndOrShortCircuitTest {

  private static final String XS = """
      {"kind":"ListLit","elements":[
        {"kind":"Int","value":1},{"kind":"Int","value":2},
        {"kind":"Int","value":3},{"kind":"Int","value":4}]}""";

  /** 求值即越界的右操作数。 */
  private static final String THROWING_RIGHT =
      "{\"kind\":\"Call\",\"target\":{\"kind\":\"Name\",\"name\":\"==\"},\"args\":["
          + "{\"kind\":\"Call\",\"target\":{\"kind\":\"Name\",\"name\":\"List.get\"},\"args\":["
          + XS + ",{\"kind\":\"Int\",\"value\":4}]},{\"kind\":\"Int\",\"value\":14}]}";

  private static String bool(boolean v) {
    return "{\"kind\":\"Bool\",\"value\":" + v + "}";
  }

  private static String call(String op, String left, String right) {
    return "{\"kind\":\"Call\",\"target\":{\"kind\":\"Name\",\"name\":\"" + op + "\"},\"args\":["
        + left + "," + right + "]}";
  }

  private static Value run(String expr) throws Exception {
    String json = "{\"name\":\"sc\",\"decls\":[{\"kind\":\"Func\",\"name\":\"main\",\"params\":[],"
        + "\"ret\":{\"kind\":\"TypeName\",\"name\":\"Bool\"},\"effects\":[],"
        + "\"body\":{\"kind\":\"Block\",\"statements\":[{\"kind\":\"Return\",\"expr\":" + expr + "}]}}]}";
    Context context = Context.newBuilder("aster").allowAllAccess(true).build();
    try (context) {
      Value v = context.eval(Source.newBuilder("aster", json, "sc.json").build());
      return Value.asValue(v.asBoolean());
    }
  }

  private static void assertEvaluatesRight(String expr) {
    PolyglotException ex = assertThrows(PolyglotException.class, () -> run(expr));
    assertTrue(ex.getMessage().contains("out of bounds"), ex.getMessage());
  }

  @Test
  void andWithFalseLeftSkipsRight() throws Exception {
    assertEquals(false, run(call("and", bool(false), THROWING_RIGHT)).asBoolean());
  }

  @Test
  void andWithTrueLeftEvaluatesRight() {
    assertEvaluatesRight(call("and", bool(true), THROWING_RIGHT));
  }

  @Test
  void orWithTrueLeftSkipsRight() throws Exception {
    assertEquals(true, run(call("or", bool(true), THROWING_RIGHT)).asBoolean());
  }

  @Test
  void orWithFalseLeftEvaluatesRight() {
    assertEvaluatesRight(call("or", bool(false), THROWING_RIGHT));
  }

  @Test
  void rightValueDecidesResultWhenEvaluated() throws Exception {
    assertEquals(true, run(call("and", bool(true), bool(true))).asBoolean());
    assertEquals(false, run(call("and", bool(true), bool(false))).asBoolean());
    assertEquals(true, run(call("or", bool(false), bool(true))).asBoolean());
    assertEquals(false, run(call("or", bool(false), bool(false))).asBoolean());
  }

  @Test
  void pokerIsWheelGuardProtectsIndex() throws Exception {
    // isWheel 形态：长度守卫在左，越界下标在右；长度为 4 时必须返回 false 而不是抛错
    String guard = call("==",
        "{\"kind\":\"Call\",\"target\":{\"kind\":\"Name\",\"name\":\"List.length\"},\"args\":[" + XS + "]}",
        "{\"kind\":\"Int\",\"value\":5}");
    assertEquals(false, run(call("and", guard, THROWING_RIGHT)).asBoolean());
  }
}
