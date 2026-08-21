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
  void errorMessageRendersIntegerValuedDoubleWithoutTrailingZero() {
    // ★对抗性审查（2026-08-18）实测出的假绿：把 formatScale 的整数 Double 分支
    //   改成死代码后全量 742 测试仍全绿 —— 因为唯一断言消息内容的用例用的是 2.7，
    //   而 2.7 走的是不经过该分支的 String.valueOf 路径。
    //   TS 侧 JSON.stringify(19) 得 "19"，Java 若输出 "19.0" 即消息分叉。
    Builtins.BuiltinException ex = assertThrows(Builtins.BuiltinException.class,
        () -> round(19.0), "19.0 超出 [0,18] 必须被拒");
    assertTrue(ex.getMessage().contains("got 19."),
        "整数值 Double 应渲染成 19（与 TS JSON.stringify 一致），实际：" + ex.getMessage());
    assertTrue(!ex.getMessage().contains("19.0"),
        "不得出现 Java 的 19.0 形式，实际：" + ex.getMessage());

    Builtins.BuiltinException neg = assertThrows(Builtins.BuiltinException.class,
        () -> round(-1.0));
    assertTrue(neg.getMessage().contains("got -1."),
        "负整数值 Double 同样应去掉 .0，实际：" + neg.getMessage());
  }

  @Test
  void javaTypeSuffixStringsAreRejected() {
    // ★issue #74：Double.parseDouble 会吃掉 Java 的类型后缀，
    //   此前 "2d" / "2f" 被**静默接受为 2**，而 TS 的 Number("2d") 得 NaN → 拒绝。
    //   与本方法「响亮失败」的立意相悖，也是一处双引擎分叉。
    assertThrows(Builtins.BuiltinException.class, () -> round("2d"), "Java 类型后缀 d 必须被拒");
    assertThrows(Builtins.BuiltinException.class, () -> round("2f"), "Java 类型后缀 f 必须被拒");
    assertThrows(Builtins.BuiltinException.class, () -> round("2D"));
    assertThrows(Builtins.BuiltinException.class, () -> round("0x1p3"), "十六进制浮点必须被拒");

    // ★反向断言：TS 的 Number() 接受的形态不得被误伤
    assertEquals("1.23", String.valueOf(round("2")));
    assertEquals("1.23", String.valueOf(round(" 2 ")), "首尾空白 TS 侧同样接受");
    assertEquals("1.23", String.valueOf(round("+2")), "正号 TS 侧同样接受");
    assertEquals("1.23", String.valueOf(round("2.0")), "整数值小数形式同样接受");
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

  /**
   * scale 的数字校验正则必须**线性**匹配，不得因病态输入灾难性回溯（ReDoS）。
   *
   * <p>原先写法 {@code (\d+\.?\d*|\.\d+)} 的两个分支对 "000…0" 存在歧义，
   * 遇到「大量 0 + 一个非法字符」会指数级回溯——实测 2 万个 '0' 需 **101 秒**
   * （TS 侧同一模式被 CodeQL 判 high: js/polynomial-redos）。
   * scale 可由宿主传入，属**不可控输入**，必须线性。
   *
   * <p>本用例给 2 万字符的病态输入设 5 秒上限：修好后实测 ~7ms，
   * 回退到旧正则则会远超上限而失败。
   */
  @Test
  void pathologicalScaleStringRejectedQuickly() {
    String pathological = "0".repeat(20_000) + "!";
    long start = System.nanoTime();
    assertThrows(Builtins.BuiltinException.class, () -> round(pathological),
        "病态输入仍应被拒（行为不变）");
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    org.junit.jupiter.api.Assertions.assertTrue(elapsedMs < 5_000,
        "★scale 正则必须线性匹配：2 万字符耗时 " + elapsedMs + "ms（上限 5000ms）。"
            + "超限说明正则又退回了会灾难性回溯的写法。");
  }
}
