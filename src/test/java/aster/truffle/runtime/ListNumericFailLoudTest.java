package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import aster.truffle.nodes.LambdaValue;
import com.oracle.truffle.api.CallTarget;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * List.sum/min/max/sort/sortBy/minBy/maxBy 对非数值元素必须响亮失败，且字符串接受集与
 * aster-lang-ts {@code toNum}（{@code NUMERIC_LITERAL_RE}）逐条一致——镜像 TS 侧
 * {@code test/unit/list-numeric-fail-loud.test.ts}（aster-lang-ts#199 truffle 侧）。
 *
 * <p>★修复前：List.sum 走 {@code toLong}，"1.5" 直接裸抛 {@code NumberFormatException}，
 *   而 TS 同输入得 1.5；toDouble 走 {@code Double.parseDouble}，"1d"/"0x1p3"/"NaN"/
 *   "Infinity" 被静默接受，TS 侧全部拒绝。两侧一边响亮失败一边静默给答案，
 *   同一规则在两个引擎上算出不同结果。
 */
class ListNumericFailLoudTest {
  private static List<Object> list(Object... xs) { return Arrays.asList(xs); }
  private static Object call(String name, Object... args) { return Builtins.call(name, args); }

  /** 恒等键函数，对应 TS 测试里的 {@code Rule id given x, produce: Return x.}。 */
  private static LambdaValue identity() {
    CallTarget target = new CallTarget() {
      @Override public Object call(Object... args) { return args[0]; }
    };
    return new LambdaValue(List.of("x"), Map.of(), target);
  }

  private static void assertFailsLoud(String label, org.junit.jupiter.api.function.Executable exec) {
    // 必须是 BuiltinException（domain error），而不是裸 NumberFormatException 之类的 JDK 异常
    Builtins.BuiltinException ex = assertThrows(Builtins.BuiltinException.class, exec,
        "★" + label + " 静默给了答案——静默错答案比报错危险");
    // 消息走 ErrorMessages.typeExpectedGot（"Expected X, got Y"）：字符串元素报期望 Number；
    // Bool 之类非文本元素在 List.sum 里仍由 numericAdd 的整数路径报期望 Int，同为类型错误。
    assertTrue(ex.getMessage().contains("Expected"),
        label + " 错误消息应是类型不匹配，实际: " + ex.getMessage());
  }

  @Test
  void nonNumericElementsFailLoud() {
    assertFailsLoud("List.sum", () -> call("List.sum", list("10", "2o")));
    assertFailsLoud("List.min", () -> call("List.min", list("x", "y")));
    assertFailsLoud("List.max", () -> call("List.max", list("x", "y")));
    assertFailsLoud("List.sort", () -> call("List.sort", list("1", "b")));
    assertFailsLoud("List.sum 空串", () -> call("List.sum", list("")));
    assertFailsLoud("List.sum 字符串 NaN", () -> call("List.sum", list("NaN")));
    assertFailsLoud("List.sum 字符串 Infinity", () -> call("List.sum", list("Infinity")));
    assertFailsLoud("List.sum 字符串 -Infinity", () -> call("List.sum", list("-Infinity")));
    assertFailsLoud("List.sum 十六进制", () -> call("List.sum", list("0x10")));
    assertFailsLoud("List.sum 布尔", () -> call("List.sum", list(true)));
  }

  @Test
  void keyFunctionReturningNonNumericFailsLoud() {
    assertFailsLoud("List.sortBy", () -> call("List.sortBy", list("1", "b"), identity()));
    assertFailsLoud("List.minBy", () -> call("List.minBy", list("x", "y"), identity()));
    assertFailsLoud("List.maxBy", () -> call("List.maxBy", list("x", "y"), identity()));
  }

  /** toDouble 收紧到 TS 接受集：Double.parseDouble 额外吃掉的文本全部拒绝。 */
  @Test
  void javaOnlyNumericSpellingsRejected() {
    for (String bad : new String[]{"1d", "1f", "1D", "1F", "0x1p3", "2o", " ", "1_000", "1e", ".", "+"}) {
      assertFailsLoud("List.max [\"" + bad + "\"]", () -> call("List.max", list(bad, "0")));
      assertFailsLoud("\"4\" / \"" + bad + "\"", () -> call("/", "4", bad));
    }
  }

  /** 反向保险：合法数值（含可解析字符串、前后空白、小数、科学计数）不得被误伤。 */
  @Test
  void numericListsStillWork() {
    assertEquals(21, ((Number) call("List.sum", list(3, 8, 1, 9))).intValue());
    assertEquals(9, ((Number) call("List.max", list(3, 8, 1, 9))).intValue());
    assertEquals(1, ((Number) call("List.min", list(3, 8, 1, 9))).intValue());
    assertEquals(List.of(1, 3, 8), call("List.sort", list(3, 8, 1)));
    assertEquals(8, ((Number) call("List.maxBy", list(3, 8, 1), identity())).intValue());
  }

  @Test
  void parsableNumericStringsAccepted() {
    assertEquals(23.5, ((Number) call("List.sum", list("10", " 2 ", "1.5", "1e1"))).doubleValue());
    assertEquals("10", call("List.max", list("10", "9")));
    assertEquals(List.of("1", "2", "10"), call("List.sort", list("10", "2", "1")));
    assertEquals(2.0, ((Number) call("/", "4", " 2 ")).doubleValue());
    assertEquals(-0.5, ((Number) call("List.sum", list("-.5", "+0", "0."))).doubleValue());
  }

  /** 整数文本求和的输出类型不变：此前经 toLong 得 Int 12，改走 toDouble 后不得变成 12.0。 */
  @Test
  void integerStringsKeepIntegralResultType() {
    Object r = call("List.sum", list("10", "2"));
    assertInstanceOf(Long.class, r, "整数文本求和必须仍是整数类型，实际: " + r.getClass());
    assertEquals(12L, r);
    // 混入 Int 元素亦然；小数文本才提升到 Double
    assertInstanceOf(Long.class, call("List.sum", list(1, "2", "3e0")));
    assertInstanceOf(Double.class, call("List.sum", list("1.5", "0.5")));
    assertEquals(2.0, ((Number) call("List.sum", list("1.5", "0.5"))).doubleValue());
  }

  /** 真实的 double NaN/Infinity（不是字符串）来自算术，不是坏输入，原样放行。 */
  @Test
  void realNonFiniteDoublesPassThrough() {
    assertTrue(Double.isNaN(((Number) call("List.sum", list(1, Double.NaN))).doubleValue()));
    assertTrue(Double.isInfinite(((Number) call("List.sum", list(1.0, Double.POSITIVE_INFINITY))).doubleValue()));
    assertEquals(Double.POSITIVE_INFINITY, call("List.max", list(1, Double.POSITIVE_INFINITY)));
    assertEquals(Double.NEGATIVE_INFINITY, call("List.min", list(1, Double.NEGATIVE_INFINITY)));
  }
}
