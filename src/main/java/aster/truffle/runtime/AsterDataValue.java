package aster.truffle.runtime;

import aster.truffle.core.CoreModel;
import aster.truffle.runtime.interop.AsterListValue;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 运行时 Data 值对象，提供稳定的字段顺序与类型元数据。
 *
 * 设计要点：
 * - 通过字段索引缓存实现 O(1) 读取，避免 Map 包装带来的装箱与 rehash
 * - 保留 CoreModel.Data 定义，方便调试或未来的反射化特性
 * - 与原有 `_type` 语义兼容，便于旧版模式匹配与 Polyglot 调用
 */
@ExportLibrary(InteropLibrary.class)
public final class AsterDataValue implements TruffleObject {
  private static final String META_TYPE = "_type";

  private final String typeName;
  private final String[] fieldNames;
  private final Object[] fieldValues;
  private final Map<String, Integer> indexByName;
  private final CoreModel.Data definition;

  public AsterDataValue(String typeName, String[] fieldNames, Object[] fieldValues, CoreModel.Data definition) {
    if (fieldNames == null || fieldValues == null) {
      throw new IllegalArgumentException("Data 值构造参数不能为空");
    }
    if (fieldNames.length != fieldValues.length) {
      throw new IllegalArgumentException("字段名与字段值数量不一致: " + Arrays.toString(fieldNames));
    }
    this.typeName = typeName;
    this.fieldNames = Arrays.copyOf(fieldNames, fieldNames.length);
    this.fieldValues = Arrays.copyOf(fieldValues, fieldValues.length);
    this.definition = definition;
    Map<String, Integer> index = new LinkedHashMap<>();
    for (int i = 0; i < this.fieldNames.length; i++) {
      index.put(this.fieldNames[i], i);
    }
    this.indexByName = Collections.unmodifiableMap(index);
  }

  public String getTypeName() {
    return typeName;
  }

  public int fieldCount() {
    return fieldNames.length;
  }

  public String fieldName(int index) {
    return fieldNames[index];
  }

  public Object fieldValue(int index) {
    return fieldValues[index];
  }

  public boolean hasField(String name) {
    return indexByName.containsKey(name);
  }

  public Object getField(String name) {
    Integer idx = indexByName.get(name);
    return idx == null ? null : fieldValues[idx];
  }

  public CoreModel.Data getDefinition() {
    return definition;
  }

  public List<String> getFieldNames() {
    return List.copyOf(Arrays.asList(fieldNames));
  }

  @ExportMessage
  boolean hasMembers() {
    return true;
  }

  @ExportMessage
  Object getMembers(boolean includeInternal) {
    ArrayList<Object> members = new ArrayList<>(fieldNames.length + 1);
    members.add(META_TYPE);
    members.addAll(Arrays.asList(fieldNames));
    return new AsterListValue(members);
  }

  @ExportMessage
  boolean isMemberReadable(String member) {
    return META_TYPE.equals(member) || indexByName.containsKey(member);
  }

  @ExportMessage
  Object readMember(String member) throws UnknownIdentifierException {
    if (META_TYPE.equals(member)) {
      return typeName;
    }
    Integer idx = indexByName.get(member);
    if (idx == null) {
      throw UnknownIdentifierException.create(member);
    }
    return fieldValues[idx];
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(typeName == null ? "<anonymous>" : typeName).append('{');
    for (int i = 0; i < fieldNames.length; i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(fieldNames[i]).append('=').append(fieldValues[i]);
    }
    return builder.append('}').toString();
  }
}
