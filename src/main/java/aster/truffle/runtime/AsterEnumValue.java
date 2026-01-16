package aster.truffle.runtime;

import aster.truffle.runtime.interop.AsterListValue;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Enum 运行时值，保留所属枚举名与变体名，并兼容旧版 `_enum`/`value` 字段访问方式。
 *
 * 目前 Core IR Enum 不携带参数，但预留 args 数组便于未来扩展。
 */
@ExportLibrary(InteropLibrary.class)
public final class AsterEnumValue implements TruffleObject {
  private static final String MEMBER_ENUM = "_enum";
  private static final String MEMBER_VALUE = "value";
  private static final String MEMBER_VARIANT = "variant";
  private static final String MEMBER_ARGS = "args";

  private final String enumName;
  private final String variantName;
  private final Object[] args;

  public AsterEnumValue(String enumName, String variantName) {
    this(enumName, variantName, null);
  }

  public AsterEnumValue(String enumName, String variantName, Object[] args) {
    this.enumName = enumName;
    this.variantName = variantName;
    this.args = args == null ? new Object[0] : Arrays.copyOf(args, args.length);
  }

  public String getEnumName() {
    return enumName;
  }

  public String getVariantName() {
    return variantName;
  }

  public Object[] getArgs() {
    return Arrays.copyOf(args, args.length);
  }

  public String getQualifiedName() {
    return enumName + "." + variantName;
  }

  @ExportMessage
  boolean hasMembers() {
    return true;
  }

  @ExportMessage
  Object getMembers(boolean includeInternal) {
    List<Object> members = new ArrayList<>(4);
    members.add(MEMBER_ENUM);
    members.add(MEMBER_VALUE);
    members.add(MEMBER_VARIANT);
    members.add(MEMBER_ARGS);
    return new AsterListValue(members);
  }

  @ExportMessage
  boolean isMemberReadable(String member) {
    return MEMBER_ENUM.equals(member)
        || MEMBER_VALUE.equals(member)
        || MEMBER_VARIANT.equals(member)
        || MEMBER_ARGS.equals(member);
  }

  @ExportMessage
  Object readMember(String member) throws UnknownIdentifierException {
    if (MEMBER_ENUM.equals(member)) {
      return enumName;
    }
    if (MEMBER_VALUE.equals(member) || MEMBER_VARIANT.equals(member)) {
      return variantName;
    }
    if (MEMBER_ARGS.equals(member)) {
      List<Object> elements = new ArrayList<>(args.length);
      elements.addAll(Arrays.asList(args));
      return new AsterListValue(elements);
    }
    throw UnknownIdentifierException.create(member);
  }

  @Override
  public String toString() {
    if (args.length == 0) {
      return getQualifiedName();
    }
    return getQualifiedName() + Arrays.toString(args);
  }
}
