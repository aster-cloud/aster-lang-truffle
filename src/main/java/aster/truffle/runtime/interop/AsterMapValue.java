package aster.truffle.runtime.interop;

import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Guest 互操作映射。除了导出 InteropLibrary（供 Polyglot 边界按成员访问），还
 * 直接实现 {@code Map<String,Object>}（委托给底层 entries）——这样所有既有的
 * {@code instanceof Map<?,?>} 消费点（MatchNode 结构/枚举匹配、Maybe/Result/Option
 * 全套 builtins 读取 {@code _type}）无需改动即可识别它，避免在 30+ 处分支里各加
 * 一遍对包装类型的特判（消灭特殊情况，而非到处打补丁）。
 */
@ExportLibrary(InteropLibrary.class)
public final class AsterMapValue implements TruffleObject, Map<String, Object> {
  private final Map<String, Object> entries;

  public AsterMapValue(Map<String, Object> entries) {
    this.entries = entries;
  }

  /** 暴露底层映射，供 Map.* builtins 直接消费。 */
  public Map<String, Object> entries() {
    return entries;
  }

  // --- Map<String,Object> 委托 ---
  @Override public int size() { return entries.size(); }
  @Override public boolean isEmpty() { return entries.isEmpty(); }
  @Override public boolean containsKey(Object key) { return entries.containsKey(key); }
  @Override public boolean containsValue(Object value) { return entries.containsValue(value); }
  @Override public Object get(Object key) { return entries.get(key); }
  @Override public Object put(String key, Object value) { return entries.put(key, value); }
  @Override public Object remove(Object key) { return entries.remove(key); }
  @Override public void putAll(Map<? extends String, ?> m) { entries.putAll(m); }
  @Override public void clear() { entries.clear(); }
  @Override public Set<String> keySet() { return entries.keySet(); }
  @Override public Collection<Object> values() { return entries.values(); }
  @Override public Set<Map.Entry<String, Object>> entrySet() { return entries.entrySet(); }

  @ExportMessage
  boolean hasMembers() {
    return true;
  }

  @ExportMessage
  Object getMembers(boolean includeInternal) {
    List<Object> keys = new ArrayList<>(entries.size());
    keys.addAll(entries.keySet());
    return new AsterListValue(keys);
  }

  @ExportMessage
  boolean isMemberReadable(String member) {
    return entries.containsKey(member);
  }

  @ExportMessage
  Object readMember(String member) throws UnknownIdentifierException {
    if (!entries.containsKey(member)) {
      throw UnknownIdentifierException.create(member);
    }
    // interop 契约：可读成员的返回值不得为裸 Java null（否则后置断言失败）。
    // 内部 null（如 Err null 的 value）经 toInteropValue 规整为 guest-null，宿主据
    // isNull() 还原。统一收口见 InteropValues。
    return InteropValues.toInteropValue(entries.get(member));
  }

  @ExportMessage
  boolean isMemberModifiable(String member) {
    return false;
  }

  @ExportMessage
  boolean isMemberInsertable(String member) {
    return false;
  }

  @ExportMessage
  boolean isMemberRemovable(String member) {
    return false;
  }

  @ExportMessage
  void writeMember(String member, Object value) throws UnsupportedMessageException {
    throw UnsupportedMessageException.create();
  }

  @ExportMessage
  void removeMember(String member) throws UnsupportedMessageException {
    throw UnsupportedMessageException.create();
  }

  @Override
  public String toString() {
    return entries.toString();
  }
}
