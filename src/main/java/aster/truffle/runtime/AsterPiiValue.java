package aster.truffle.runtime;

import aster.truffle.core.CoreModel;
import aster.truffle.runtime.interop.AsterListValue;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * 运行时 PII 封装对象，用于携带数据本身与敏感元数据。
 *
 * 设计要点：
 * - 默认类型擦除：业务代码仍以普通值使用，仅在日志/审计时读取元信息
 * - Interop 支持：可供 polyglot 客户端读取 value/tags/sensitivity 字段
 * - 标签合并：当同一值被不同 PII 声明标记时合并全部标签
 */
@ExportLibrary(InteropLibrary.class)
public final class AsterPiiValue implements TruffleObject {
  private static final String MEMBER_VALUE = "value";
  private static final String MEMBER_TAGS = "tags";
  private static final String MEMBER_SENSITIVITY = "sensitivity";

  private final Object innerValue;
  private final List<String> tags;
  private final String sensitivity;

  public AsterPiiValue(Object value, CoreModel.PiiType type) {
    this(value, extractTags(type), type != null ? normalizeSensitivity(type.sensitivity) : null);
  }

  public AsterPiiValue(Object value, List<String> tags, String sensitivity) {
    this.innerValue = value;
    this.tags = tags == null ? List.of() : List.copyOf(tags);
    this.sensitivity = sensitivity;
  }

  public Object getInnerValue() {
    return innerValue;
  }

  public List<String> getTags() {
    return tags;
  }

  public String getSensitivity() {
    return sensitivity;
  }

  /**
   * 合并新的 PiiType 元信息，返回包含全部标签/敏感级别的新对象。
   */
  public AsterPiiValue enrich(CoreModel.PiiType type) {
    if (type == null) {
      return this;
    }
    List<String> extraTags = extractTags(type);
    String normalized = normalizeSensitivity(type.sensitivity);
    boolean sameTags = tags.containsAll(extraTags) && extraTags.containsAll(tags);
    boolean sameSensitivity = Objects.equals(sensitivity, normalized);
    if (sameTags && sameSensitivity) {
      return this;
    }
    Set<String> merged = new LinkedHashSet<>(tags);
    merged.addAll(extraTags);
    String finalSensitivity = selectSensitivity(normalized, sensitivity);
    return new AsterPiiValue(innerValue, List.copyOf(merged), finalSensitivity);
  }

  /**
   * 统一的脱敏文本表示，供日志和 Text.redact 使用。
   */
  public String redact() {
    if (tags.isEmpty()) {
      return "<PII>";
    }
    return "<PII:" + String.join(",", tags) + ">";
  }

  @ExportMessage
  boolean hasMembers() {
    return true;
  }

  @ExportMessage
  Object getMembers(boolean includeInternal) {
    return new AsterListValue(List.of(MEMBER_VALUE, MEMBER_TAGS, MEMBER_SENSITIVITY));
  }

  @ExportMessage
  boolean isMemberReadable(String member) {
    return MEMBER_VALUE.equals(member) || MEMBER_TAGS.equals(member) || MEMBER_SENSITIVITY.equals(member);
  }

  @ExportMessage
  Object readMember(String member) throws UnknownIdentifierException {
    if (MEMBER_VALUE.equals(member)) {
      return innerValue;
    }
    if (MEMBER_TAGS.equals(member)) {
      java.util.List<Object> tagObjects = new ArrayList<>(tags.size());
      tagObjects.addAll(tags);
      return new AsterListValue(tagObjects);
    }
    if (MEMBER_SENSITIVITY.equals(member)) {
      return sensitivity;
    }
    throw UnknownIdentifierException.create(member);
  }

  @Override
  public String toString() {
    if (tags.isEmpty()) {
      return "<PII>";
    }
    return "<PII:" + String.join(",", tags) + ">";
  }

  public static Object unwrap(Object value) {
    if (value instanceof AsterPiiValue pii) {
      return pii.getInnerValue();
    }
    return value;
  }

  private static List<String> extractTags(CoreModel.PiiType type) {
    if (type == null || type.category == null || type.category.isBlank()) {
      return List.of();
    }
    return List.of(type.category);
  }

  private static String normalizeSensitivity(String raw) {
    if (raw == null) {
      return null;
    }
    return raw.trim().toUpperCase(Locale.ROOT);
  }

  private static String selectSensitivity(String candidate, String current) {
    if (candidate == null) {
      return current;
    }
    if (current == null) {
      return candidate;
    }
    // 采用字母序更高的敏感级别，避免降低标签
    return candidate.compareTo(current) >= 0 ? candidate : current;
  }
}
