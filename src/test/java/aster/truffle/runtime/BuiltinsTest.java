package aster.truffle.runtime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
