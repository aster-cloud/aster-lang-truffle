package aster.truffle.nodes;

import aster.truffle.runtime.interop.AsterDecimalValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 整数模式须按精确数值相等匹配 Decimal（Int→Decimal 宽化）。 */
public class PatIntDecimalTest {

  private static boolean matches(int pat, Object v) {
    // 整数模式不读写 frame/env，传 null 即可
    return new MatchNode.PatIntNode(pat).matchesAndBind(v, null, null);
  }

  private static AsterDecimalValue dec(String s) { return AsterDecimalValue.of(new BigDecimal(s)); }

  @Test
  void decimalZeroMatchesWhenZero() {
    assertTrue(matches(0, dec("0")));
    assertTrue(matches(0, dec("0.0")));
    assertTrue(matches(0, dec("0.00")));
    assertTrue(matches(0, new BigDecimal("0.00")));
  }

  @Test
  void decimalIntegralValuesMatch() {
    assertTrue(matches(1, dec("1.000")));
    assertTrue(matches(-42, dec("-42")));
    assertTrue(matches(100, dec("1E+2")));
  }

  @Test
  void nonIntegralOrDifferentDecimalDoesNotMatch() {
    assertFalse(matches(1, dec("1.5")));
    assertFalse(matches(0, dec("0.0000000000000000000001")));
    assertFalse(matches(2, dec("1")));
  }

  @Test
  void nonNumericDoesNotMatch() {
    assertFalse(matches(0, "0"));
    assertFalse(matches(0, Boolean.FALSE));
    assertFalse(matches(0, null));
  }

  @Test
  void existingNumericTypesStillMatch() {
    assertTrue(matches(3, 3));
    assertTrue(matches(3, 3L));
    assertTrue(matches(3, 3.0d));
    assertFalse(matches(3, 3.5d));
  }
}
