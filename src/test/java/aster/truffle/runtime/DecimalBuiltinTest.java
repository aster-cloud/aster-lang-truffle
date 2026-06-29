package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import aster.truffle.runtime.interop.AsterDecimalValue;
import org.junit.jupiter.api.Test;

/**
 * Decimal 一等公民（ADR 0025）：m 后缀字面量 → guest AsterDecimalValue（包装 BigDecimal）运行时
 * 值 + 精确加减乘 + compareTo 比较 + 禁 Double 混算 + 禁 `/`//`%`。fixture 与 ts
 * decimal-literals.test.ts 同源——canonical 字符串必须与 TS decimal.js 逐位一致（双引擎 parity
 * 契约；序列化走 CoreIrEvalCli.valueToJson 的 AsterDecimalValue.isString()→toPlainString 路径，
 * 与 TS toJSON 字符串对齐）。
 *
 * 注：这里直接调 Builtins.call 测算术/比较内核（运算符 lower 到 add/sub/mul/eq…）。
 * 字面量 → AsterDecimalValue 的 LiteralNode 路径由 tier1-parity 端到端 corpus 验证。
 */
public class DecimalBuiltinTest {

  /** 构造 guest Decimal 值（生产路径运行时表示）。 */
  private static AsterDecimalValue d(String s) { return AsterDecimalValue.of(new BigDecimal(s)); }

  /** 解包结果为 canonical 十进制字符串（结果是 AsterDecimalValue）。 */
  private static String canon(Object o) {
    return ((AsterDecimalValue) o).canonicalString();
  }

  @Test void exactAdd() {
    // 0.1 + 0.2 = 0.3 精确（无二进制浮点误差，这是 Decimal 存在的理由）
    assertEquals("0.3", canon(Builtins.call("add", new Object[]{d("0.1"), d("0.2")})));
    assertEquals("99.99", canon(Builtins.call("sub", new Object[]{d("100.01"), d("0.02")})));
    assertEquals("2000.75", canon(Builtins.call("add", new Object[]{d("1200.50"), d("800.25")})));
  }

  @Test void exactMul() {
    // 1.20 × 1.080 = 1.296（scale 增长，精确保留）
    assertEquals("1.296", canon(Builtins.call("mul", new Object[]{d("1.20"), d("1.080")})));
    // price × taxRate: 100.00 × 1.08 = 108（去尾零 canonical）
    assertEquals("108", canon(Builtins.call("mul", new Object[]{d("100.00"), d("1.08")})));
  }

  @Test void intLongExactPromotion() {
    // Int/Long → Decimal 精确提升允许：1m + 2(Int) = 3
    assertEquals("3", canon(Builtins.call("add", new Object[]{d("1"), 2})));
    // 5(Int) × 1.5m = 7.5
    assertEquals("7.5", canon(Builtins.call("mul", new Object[]{5, d("1.5")})));
    // Long 同样精确提升
    assertEquals("3", canon(Builtins.call("add", new Object[]{d("1"), 2L})));
  }

  @Test void comparisonValueSemantics() {
    // 1.0m equals 1.00m → true（compareTo 值语义，绕开 BigDecimal.equals 的 scale 敏感）
    assertEquals(Boolean.TRUE, Builtins.call("eq", new Object[]{d("1.0"), d("1.00")}));
    assertEquals(Boolean.TRUE, Builtins.call("gt", new Object[]{d("1.01"), d("1.001")}));
    assertEquals(Boolean.TRUE, Builtins.call("gte", new Object[]{d("100.00"), d("50")}));
    // 0.1m + 0.2m equals 0.3m → true（精确）
    Object sum = Builtins.call("add", new Object[]{d("0.1"), d("0.2")});
    assertEquals(Boolean.TRUE, Builtins.call("eq", new Object[]{sum, d("0.3")}));
  }

  @Test void banDoubleMixing() {
    // Double ↔ Decimal 混算禁（ADR 0025：Double 是二进制浮点，混算引入误差）
    assertThrows(Exception.class, () -> Builtins.call("add", new Object[]{d("1.08"), 2.5}));
    assertThrows(Exception.class, () -> Builtins.call("mul", new Object[]{2.5, d("3")}));
    // 比较也禁混算
    assertThrows(Exception.class, () -> Builtins.call("eq", new Object[]{d("1.0"), 1.0}));
  }

  @Test void banDivisionAndModulo() {
    // `/`//`%` 对 Decimal 禁用（除法需显式 Decimal.divide(scale, mode)，M2）
    assertThrows(Exception.class, () -> Builtins.call("div", new Object[]{d("6"), d("2")}));
    assertThrows(Exception.class, () -> Builtins.call("intdiv", new Object[]{d("6"), d("2")}));
    assertThrows(Exception.class, () -> Builtins.call("mod", new Object[]{d("6"), d("4")}));
  }

  @Test void canonicalSerialization() {
    // 序列化 canonical：去尾零 / 去前导零（前导零由 lexer canonicalizeDecimal 处理，
    // 这里验运行时归一）/ 零归 "0"。与 TS decimal.js toJSON 字符串对齐。
    assertEquals("1", canon(d("1.00")));
    assertEquals("1.23", canon(d("001.2300")));
    assertEquals("0", canon(d("0.000")));
    assertEquals("10", canon(d("10")));
    // 加法结果的 canonical（含整数化）
    assertEquals("3", canon(Builtins.call("add", new Object[]{d("1.5"), d("1.5")})));
  }

  // === M2: Decimal.round / Decimal.divide（fixture 与 ts decimal-round-divide.test.ts 同源） ===

  @Test void roundModes() {
    assertEquals("2.35", canon(Builtins.call("Decimal.round", new Object[]{d("2.345"), 2, "HALF_UP"})));
    assertEquals("2.34", canon(Builtins.call("Decimal.round", new Object[]{d("2.345"), 2, "HALF_EVEN"}))); // 向偶
    assertEquals("2.36", canon(Builtins.call("Decimal.round", new Object[]{d("2.355"), 2, "HALF_EVEN"}))); // 向偶
    assertEquals("2", canon(Builtins.call("Decimal.round", new Object[]{d("2.5"), 0, "HALF_EVEN"})));       // 银行家
    assertEquals("4", canon(Builtins.call("Decimal.round", new Object[]{d("3.5"), 0, "HALF_EVEN"})));       // 银行家
    assertEquals("2", canon(Builtins.call("Decimal.round", new Object[]{d("2.999"), 0, "DOWN"})));          // 截断
    assertEquals("2.5", canon(Builtins.call("Decimal.round", new Object[]{d("2.50"), 2, "HALF_UP"})));      // canonical 去尾零
  }

  @Test void divideModes() {
    assertEquals("3.3333", canon(Builtins.call("Decimal.divide", new Object[]{d("10"), d("3"), 4, "HALF_UP"})));
    assertEquals("0.67", canon(Builtins.call("Decimal.divide", new Object[]{d("2"), d("3"), 2, "HALF_UP"})));
    assertEquals("0.12", canon(Builtins.call("Decimal.divide", new Object[]{d("1"), d("8"), 2, "DOWN"})));
    assertEquals("0.33", canon(Builtins.call("Decimal.divide", new Object[]{d("1"), d("3"), 2, "HALF_EVEN"})));
    assertEquals("25", canon(Builtins.call("Decimal.divide", new Object[]{d("100"), d("4"), 2, "HALF_UP"}))); // 整除 canonical
    // 合规: 年息 7.25% / 12 期 sc6 HALF_EVEN = 0.006042
    assertEquals("0.006042", canon(Builtins.call("Decimal.divide", new Object[]{d("0.0725"), d("12"), 6, "HALF_EVEN"})));
  }

  @Test void roundDivideErrors() {
    assertThrows(Exception.class, () -> Builtins.call("Decimal.divide", new Object[]{d("1"), d("0"), 2, "HALF_UP"}));   // 除零
    assertThrows(Exception.class, () -> Builtins.call("Decimal.round", new Object[]{d("1.5"), 2, "CEILING"}));          // 未知 mode
    assertThrows(Exception.class, () -> Builtins.call("Decimal.round", new Object[]{d("1.5"), 19, "HALF_UP"}));         // scale 越界
  }
}
