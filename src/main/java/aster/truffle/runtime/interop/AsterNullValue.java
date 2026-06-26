package aster.truffle.runtime.interop;

import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

/**
 * Guest 视角的 null 单例。
 *
 * <p>引擎内部用 Java {@code null} 表示空值（{@code Err null}、{@code Some null}、
 * 列表里的 null 元素等），但 Truffle 的 InteropLibrary 契约规定：凡是被声明可读的
 * 成员/数组元素，{@code readMember}/{@code readArrayElement} 的返回值都不得是裸 Java
 * {@code null}——必须是合法的 interop 值。直接把内部 Java null 透传到 Polyglot 边界会
 * 触发 {@code AssertUtils.validInteropReturn} 的后置断言失败
 * （"Post-condition contract violation … and return value null"）。
 *
 * <p>因此在 interop 读取边界（{@link AsterMapValue#readMember}、
 * {@link AsterListValue#readArrayElement}）把 Java null 包成本单例。它导出
 * {@code isNull()==true}，宿主侧 {@code Value.isNull()} 据此还原为 Java null，
 * 完成 null 的往返且不破坏契约。{@code When null} 匹配语义不变——
 * {@code MatchNode.isGuestNull} 本就把 {@code isNull()==true} 的对象视为 null。
 *
 * <p>仅作用于 interop 边界：{@code AsterMapValue} 的底层 {@code entries} 与
 * {@code AsterListValue} 的 {@code elements} 仍存原始 Java null，故所有按 {@code Map}/
 * {@code List} 直接消费的 builtins 行为不变。
 */
@ExportLibrary(InteropLibrary.class)
public final class AsterNullValue implements TruffleObject {
  public static final AsterNullValue INSTANCE = new AsterNullValue();

  private AsterNullValue() {}

  @ExportMessage
  boolean isNull() {
    return true;
  }

  @Override
  public String toString() {
    return "null";
  }
}
