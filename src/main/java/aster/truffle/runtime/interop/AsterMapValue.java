package aster.truffle.runtime.interop;

import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ExportLibrary(InteropLibrary.class)
public final class AsterMapValue implements TruffleObject {
  private final Map<String, Object> entries;

  AsterMapValue(Map<String, Object> entries) {
    this.entries = entries;
  }

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
    return entries.get(member);
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
