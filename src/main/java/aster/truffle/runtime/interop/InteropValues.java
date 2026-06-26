package aster.truffle.runtime.interop;

/**
 * interop 返回值的统一规整工具。
 *
 * <p>Truffle 的 InteropLibrary 契约规定：凡声明为可读的成员/数组元素，
 * {@code readMember}/{@code readArrayElement} 等的返回值都不得是裸 Java {@code null}
 * （否则在启用断言时触发 {@code AssertUtils.validInteropReturn} 后置违例）。所有
 * interop 读取导出点都应经此把内部 Java {@code null} 规整为 {@link AsterNullValue}
 * 单例（{@code isNull()==true}），宿主侧 {@code Value.isNull()} 据此还原 Java null。
 *
 * <p>单一收口点：新增的可空 interop 返回路径只需复用本方法，避免在各
 * {@code @ExportMessage} 里散落重复的 null 判定（消灭特殊情况而非到处打补丁）。
 */
public final class InteropValues {
  private InteropValues() {}

  /**
   * 把内部值规整为合法 interop 返回值：Java {@code null} → guest-null 单例，其余原样。
   */
  public static Object toInteropValue(Object value) {
    return value == null ? AsterNullValue.INSTANCE : value;
  }
}
