package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import aster.truffle.runtime.interop.AsterDecimalValue;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/**
 * {@code Decimal.round} / {@code Decimal.divide} 的 scale 校验（2026-08-17 审计修复）。
 *
 * <p>此前 {@code decimalScale} 用 {@code toInt(scale)}，而 {@code toInt} 走
 * {@code Number.intValue()}：{@code 2.7} 被<b>静默截断</b>成 {@code 2}。于是
 * {@code Decimal.round(x, 2.7, ...)} 在 Java 上安静地按 2 位舍入，在 TS 上直接报错
 * （实测 TS：{@code Decimal: scale must be an integer in [0, 18], got 2.7.}）。
 *
 * <p>讽刺之处：Java 自己那句错误消息就写着 "scale must be an integer"——
 * 而这条约束<b>并未真正被强制</b>，只在越界时才触发。
 *
 * <p>对合规引擎而言，舍入精度写错应当<b>响亮地失败</b>，而不是被悄悄改写成另一个精度：
 * 一个本该按 4 位计息的规则被静默降成 2 位，金额会真的算错。
 */
class DecimalScaleValidationTest {

  private static Object dec(String s) {
    return AsterDecimalValue.of(new BigDecimal(s));
  }

  private static Object round(Object scale) {
    return Builtins.call("Decimal.round", new Object[]{dec("1.23456"), scale, "HALF_EVEN"});
  }

  @Test
  void fractionalScaleIsRejected() {
    // ★核心回归：此前静默返回 1.23。
    Builtins.BuiltinException ex = assertThrows(Builtins.BuiltinException.class, () -> round(2.7),
        "小数 scale 必须报错，而不是被截断成 2");
    assertTrue(ex.getMessage().contains("must be an integer"),
        "错误消息应说明必须是整数，实际：" + ex.getMessage());
    assertTrue(ex.getMessage().contains("2.7"),
        "错误消息应回显原始值 2.7（而非截断后的 2），实际：" + ex.getMessage());
  }

  @Test
  void integerValuedDoubleIsAccepted() {
    // 2.0 在数学上就是整数，TS 的 Number.isInteger(2.0) 为真 → 必须接受。
    // 这条防止"把所有 Double 一律拒掉"式的过度修复。
    assertEquals("1.23", String.valueOf(round(2.0)), "整数值的 Double（2.0）必须被接受");
  }

  @Test
  void integerScaleStillWorks() {
    // ★同等重要的一半：正常路径不得被破坏。
    assertEquals("1.23", String.valueOf(round(2)));
    assertEquals("1.2346", String.valueOf(round(4)));
    assertEquals("1", String.valueOf(round(0)), "scale=0 是合法边界");
  }

  @Test
  void numericStringScaleStillWorks() {
    // 此前 toInt 走 Integer.parseInt 接受数字字符串；TS 侧 Number("2") 同样接受。
    // 修复不得顺手掐掉这条既有路径。
    assertEquals("1.23", String.valueOf(round("2")));
  }

  @Test
  void outOfRangeScaleStillRejected() {
    // 越界校验必须保留。
    assertThrows(Builtins.BuiltinException.class, () -> round(-1), "负 scale 必须被拒");
    assertThrows(Builtins.BuiltinException.class, () -> round(19), "超过 18 必须被拒");
    assertEquals("1.23456", String.valueOf(round(18)), "18 是合法上界");
  }

  @Test
  void nonNumericScaleIsRejected() {
    assertThrows(Builtins.BuiltinException.class, () -> round("abc"), "非数字字符串必须被拒");
    assertThrows(Builtins.BuiltinException.class, () -> round(java.util.List.of(2)), "列表必须被拒");
  }

  @Test
  void divideUsesSameValidation() {
    // Decimal.divide 与 round 共用 decimalScale，必须同样严格。
    assertThrows(Builtins.BuiltinException.class,
        () -> Builtins.call("Decimal.divide", new Object[]{dec("1"), dec("3"), 2.7, "HALF_EVEN"}),
        "Decimal.divide 的小数 scale 同样必须被拒");
    assertEquals("0.33",
        String.valueOf(Builtins.call("Decimal.divide",
            new Object[]{dec("1"), dec("3"), 2, "HALF_EVEN"})),
        "整数 scale 的除法路径不得被破坏");
  }
}
