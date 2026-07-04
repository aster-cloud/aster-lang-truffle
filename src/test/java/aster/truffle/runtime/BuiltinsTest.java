package aster.truffle.runtime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #14 回归测试：Builtins 数值与查找语义。
 */
public class BuiltinsTest {

  @Test
  public void intdivKeepsPrecisionAbove2Pow53() {
    // 2^53 + 1 无法在 double 中精确表示；走 double 路径会先丢失精度。
    long a = 9007199254740993L; // 2^53 + 1
    Object result = Builtins.call("intdiv", new Object[]{a, 2L});
    // long 整除：(2^53 + 1) / 2 = 4503599627370496（向零截断）
    assertEquals(4503599627370496L, ((Number) result).longValue());
  }

  @Test
  public void intdivLargeOperandsExact() {
    long a = 1_000_000_000_000_000_007L; // 远超 2^53
    long b = 3L;
    Object result = Builtins.call("intdiv", new Object[]{a, b});
    assertEquals(a / b, ((Number) result).longValue());
  }

  @Test
  public void intdivTruncatesTowardZero() {
    assertEquals(-3L, ((Number) Builtins.call("intdiv", new Object[]{-7L, 2L})).longValue());
    assertEquals(-3L, ((Number) Builtins.call("intdiv", new Object[]{-7, 2})).longValue());
  }

  @Test
  public void intdivStillWorksWithFractionalOperand() {
    // 任一操作数为浮点时仍走 double 路径。
    Object result = Builtins.call("intdiv", new Object[]{7.5, 2});
    assertEquals(3L, ((Number) result).longValue());
  }

  @Test
  public void intdivByZeroThrows() {
    assertThrows(Builtins.BuiltinException.class,
        () -> Builtins.call("intdiv", new Object[]{10L, 0L}));
  }

  @Test
  public void unknownBuiltinReturnsNull() {
    // #14: Builtins.call 对未知名称返回 null（与「合法返回 null」区分由调用点负责）。
    assertNull(Builtins.call("no.such.builtin", new Object[]{}));
  }

  // ── #43 回归 ──────────────────────────────────────────────────────────────

  @Test
  public void modByZeroThrowsGuestError() {
    // #43：mod 除数为 0 抛 guest BuiltinException（"division by zero"），
    // 不再返回静默 NaN 或抛裸 ArithmeticException。
    Builtins.BuiltinException ex = assertThrows(Builtins.BuiltinException.class,
        () -> Builtins.call("mod", new Object[]{10L, 0L}));
    assertTrue(ex.getMessage().toLowerCase().contains("division by zero"),
        "mod-by-zero should surface a 'division by zero' guest error, got: " + ex.getMessage());
  }

  @Test
  public void modByZeroThrowsForIntOperands() {
    // int 字面量除数同样命中零守卫（提升到 long 路径）。
    assertThrows(Builtins.BuiltinException.class,
        () -> Builtins.call("mod", new Object[]{7, 0}));
  }

  @Test
  public void modIntegerPromotedToLongNoTruncation() {
    // #43：整数取模提升到 long；原 32 位 toInt 截断会把大操作数算错。
    long a = 10_000_000_000L; // > 2^31，无法用 int 表示
    Object result = Builtins.call("mod", new Object[]{a, 7L});
    assertEquals(a % 7L, ((Number) result).longValue());
  }

  @Test
  public void addPromotedToLongNoOverflowWrap() {
    // #43：int add 溢出回绕修复——提升到 long。Integer.MAX_VALUE + 1 应为 2^31，非 -2^31。
    Object result = Builtins.call("add", new Object[]{Integer.MAX_VALUE, 1});
    assertEquals((long) Integer.MAX_VALUE + 1L, ((Number) result).longValue());
  }

  @Test
  public void mulPromotedToLongNoOverflowWrap() {
    // #43：int mul 溢出回绕修复。100000 * 100000 = 10^10，超出 int。
    Object result = Builtins.call("mul", new Object[]{100000, 100000});
    assertEquals(10_000_000_000L, ((Number) result).longValue());
  }

  @Test
  public void toUpperIsLocaleRootStable() {
    // #43：Text.toUpper 用 Locale.ROOT，与默认 locale（尤其 tr_TR 的 i→İ）无关，
    // 保证确定性与 TS parity。无论当前默认 locale 如何，'i' → 'I'。
    java.util.Locale previous = java.util.Locale.getDefault();
    try {
      java.util.Locale.setDefault(new java.util.Locale("tr", "TR"));
      assertEquals("TITLE", Builtins.call("Text.toUpper", new Object[]{"title"}));
      assertEquals("I", Builtins.call("Text.toUpper", new Object[]{"i"}));
    } finally {
      java.util.Locale.setDefault(previous);
    }
  }

  @Test
  public void toLowerIsLocaleRootStable() {
    // #43：Text.toLower 用 Locale.ROOT，tr_TR 默认 locale 下 'I' 仍降为 'i'（非 'ı'）。
    java.util.Locale previous = java.util.Locale.getDefault();
    try {
      java.util.Locale.setDefault(new java.util.Locale("tr", "TR"));
      assertEquals("title", Builtins.call("Text.toLower", new Object[]{"TITLE"}));
      assertEquals("i", Builtins.call("Text.toLower", new Object[]{"I"}));
    } finally {
      java.util.Locale.setDefault(previous);
    }
  }
}
