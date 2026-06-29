package aster.truffle.runtime.interop;

import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

import java.math.BigDecimal;

/**
 * Decimal 一等公民（ADR 0025）的 guest 运行时值：包装精确 {@link BigDecimal}。
 *
 * <p>为何要包装而非裸 BigDecimal：Truffle 沙箱（无 host access）下，guest 值必须是
 * 基本类型或 {@link TruffleObject}；裸 BigDecimal 是 host 对象，流到 interop 边界会抛
 * {@code ClassCastException: BigDecimal cannot be cast to TruffleObject}。
 *
 * <p>序列化对齐：暴露 {@link #isString()}=true + {@link #asString()}=canonical 十进制串
 * （toPlainString，去尾零）。这样 CoreIrEvalCli.valueToJson 走 isString 分支输出 JSON
 * **字符串**（如 "107.9"），与 TS decimal.js 的 JSON.stringify（toJSON→字符串）逐位一致
 * ——避免裸 BigDecimal 被当 number 输出导致精度丢失 + 双引擎分歧。NOT 暴露 isNumber()，
 * 正是为了走字符串路径。算术由 Builtins 解包 BigDecimal 后做精确运算再重新包装。
 */
@ExportLibrary(InteropLibrary.class)
public final class AsterDecimalValue implements TruffleObject {
  private final BigDecimal value;

  private AsterDecimalValue(BigDecimal value) {
    this.value = value;
  }

  /** 用 canonical 化（去尾零、零归 ZERO）后的 BigDecimal 构造，保证值语义稳定。 */
  public static AsterDecimalValue of(BigDecimal raw) {
    BigDecimal stripped = raw.stripTrailingZeros();
    if (stripped.signum() == 0) stripped = BigDecimal.ZERO;
    return new AsterDecimalValue(stripped);
  }

  /** 暴露底层精确值，供算术/比较 builtin 消费。 */
  public BigDecimal decimal() {
    return value;
  }

  /** canonical 十进制字符串（无指数），序列化与 TS decimal.js 对齐。 */
  public String canonicalString() {
    return value.toPlainString();
  }

  @ExportMessage
  boolean isString() {
    return true;
  }

  @ExportMessage
  String asString() {
    return value.toPlainString();
  }

  @Override
  public String toString() {
    return value.toPlainString();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof AsterDecimalValue other && value.compareTo(other.value) == 0;
  }

  @Override
  public int hashCode() {
    return value.stripTrailingZeros().hashCode();
  }
}
