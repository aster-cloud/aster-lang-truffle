package aster.truffle.runtime.interop;

import com.oracle.truffle.api.interop.InvalidArrayIndexException;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

import java.util.List;

@ExportLibrary(InteropLibrary.class)
public final class AsterListValue implements TruffleObject {
  private final List<Object> elements;

  public AsterListValue(List<Object> elements) {
    this.elements = elements;
  }

  /** 暴露底层列表，供 List.* builtins 直接消费（避免逐元素 interop 读取）。 */
  public List<Object> elements() {
    return elements;
  }

  @ExportMessage
  boolean hasArrayElements() {
    return true;
  }

  @ExportMessage
  long getArraySize() {
    return elements.size();
  }

  @ExportMessage
  boolean isArrayElementReadable(long index) {
    return index >= 0 && index < elements.size();
  }

  @ExportMessage
  Object readArrayElement(long index) throws InvalidArrayIndexException {
    if (!isArrayElementReadable(index)) {
      throw InvalidArrayIndexException.create(index);
    }
    Object element = elements.get((int) index);
    // interop 契约：可读数组元素的返回值不得为裸 Java null（否则后置断言失败）。
    // 内部 null 元素包成 guest-null 单例，宿主据 isNull() 还原。
    return element == null ? AsterNullValue.INSTANCE : element;
  }

  @ExportMessage
  boolean isArrayElementModifiable(long index) {
    return false;
  }

  @ExportMessage
  boolean isArrayElementInsertable(long index) {
    return false;
  }

  @ExportMessage
  boolean isArrayElementRemovable(long index) {
    return false;
  }

  @ExportMessage
  void writeArrayElement(long index, Object value) throws UnsupportedMessageException {
    throw UnsupportedMessageException.create();
  }

  @ExportMessage
  void removeArrayElement(long index) throws UnsupportedMessageException {
    throw UnsupportedMessageException.create();
  }

  @ExportMessage
  Object getIterator() throws UnsupportedMessageException {
    throw UnsupportedMessageException.create();
  }

  @ExportMessage
  boolean hasIterator() {
    return false;
  }

  @Override
  public String toString() {
    return elements.toString();
  }
}
