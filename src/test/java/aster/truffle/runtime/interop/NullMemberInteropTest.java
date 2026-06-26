package aster.truffle.runtime.interop;

import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 回归测试：interop 读取边界对内部 Java {@code null} 的处理。
 *
 * <p>背景（生产 bug）：{@code Err null}/{@code Some null} 这类值产出
 * {@code {_type:"Err", value:null}}，经 {@link AsterMapValue} 跨 Polyglot 边界回宿主时，
 * 宿主 {@code TrufflePolicyRuntime.convertValue} 遍历成员调用 {@code readMember("value")}。
 * 若该方法返回裸 Java {@code null}，Truffle 的 {@code AssertUtils.validInteropReturn}
 * 后置断言会抛 "Post-condition contract violation for receiver {_type=Err, value=null} …
 * and return value null"，导致策略执行整体失败。
 *
 * <p>修复：{@code readMember}/{@code readArrayElement} 把内部 null 包成
 * {@link AsterNullValue} 单例（{@code isNull()==true}），宿主侧 {@code Value.isNull()}
 * 据此还原为 Java null，完成往返且不破坏契约。
 */
class NullMemberInteropTest {

  private final InteropLibrary interop = InteropLibrary.getUncached();

  @Test
  @DisplayName("AsterMapValue.readMember 对 null 成员返回 guest-null 而非裸 null（契约）")
  void mapNullMemberReturnsGuestNull() throws Exception {
    // 复刻 Err null 的形状：value 成员存在但为 null。
    Map<String, Object> entries = new LinkedHashMap<>();
    entries.put("_type", "Err");
    entries.put("value", null);
    AsterMapValue map = new AsterMapValue(entries);

    // 成员被声明可读……
    assertTrue(interop.isMemberReadable(map, "value"));

    // ……读取必须返回非裸-null 的合法 interop 值，且经断言版 InteropLibrary 不抛契约违例。
    Object read = interop.readMember(map, "value");
    assertNotNull(read, "null 成员的 readMember 不得返回裸 Java null（违反 interop 契约）");
    assertSame(AsterNullValue.INSTANCE, read);
    assertTrue(interop.isNull(read), "返回值应是 guest-null（isNull()==true），宿主据此还原 Java null");

    // 非 null 成员保持原值不变。
    assertSame("Err", interop.readMember(map, "_type"));

    // 不存在的成员仍抛 UnknownIdentifierException（既有契约不变）。
    assertThrows(UnknownIdentifierException.class, () -> interop.readMember(map, "missing"));
  }

  @Test
  @DisplayName("AsterListValue.readArrayElement 对 null 元素返回 guest-null（契约）")
  void listNullElementReturnsGuestNull() throws Exception {
    // 含 null 元素的列表（如 [1, null, 3]）。
    AsterListValue list = new AsterListValue(Arrays.asList((Object) 1, null, 3));

    assertTrue(interop.isArrayElementReadable(list, 1));
    Object read = interop.readArrayElement(list, 1);
    assertNotNull(read, "null 元素的 readArrayElement 不得返回裸 Java null");
    assertSame(AsterNullValue.INSTANCE, read);
    assertTrue(interop.isNull(read));

    // 非 null 元素保持原值。
    assertSame(1, interop.readArrayElement(list, 0));
    assertSame(3, interop.readArrayElement(list, 2));
  }

  @Test
  @DisplayName("guest-null 单例导出 isNull()==true 且不被误判为有成员/数组")
  void guestNullSingletonContract() {
    assertTrue(interop.isNull(AsterNullValue.INSTANCE));
    assertFalse(interop.hasMembers(AsterNullValue.INSTANCE));
    assertFalse(interop.hasArrayElements(AsterNullValue.INSTANCE));
  }

  @Test
  @DisplayName("底层 Map/List 仍存原始 Java null —— builtins 直接消费路径不变")
  void backingStorageKeepsRawNull() {
    Map<String, Object> entries = new LinkedHashMap<>();
    entries.put("_type", "Err");
    entries.put("value", null);
    AsterMapValue map = new AsterMapValue(entries);

    // entries()/Map.get 仍是 raw null（30+ 处 instanceof Map 消费点行为不变）。
    assertTrue(map.containsKey("value"));
    assertSame(null, map.get("value"));
    assertSame(null, map.entries().get("value"));

    AsterListValue list = new AsterListValue(Arrays.asList((Object) 1, null, 3));
    assertSame(null, list.elements().get(1));
  }

  @Test
  @DisplayName("AsterDataValue.readMember 对 null 字段值返回 guest-null（契约）")
  void dataNullFieldReturnsGuestNull() throws Exception {
    // Construct 字段表达式可求值为 null → 字段值为 Java null。
    aster.truffle.runtime.AsterDataValue data = new aster.truffle.runtime.AsterDataValue(
        "User", new String[]{"name", "age"}, new Object[]{"Alice", null}, null);

    assertTrue(interop.isMemberReadable(data, "age"));
    Object read = interop.readMember(data, "age");
    assertNotNull(read, "null 字段的 readMember 不得返回裸 Java null");
    assertSame(aster.truffle.runtime.interop.AsterNullValue.INSTANCE, read);
    assertTrue(interop.isNull(read));
    // 非 null 字段保持原值。
    assertSame("Alice", interop.readMember(data, "name"));
  }

  @Test
  @DisplayName("AsterPiiValue.readMember 对 null value/sensitivity 返回 guest-null（契约）")
  void piiNullMembersReturnGuestNull() throws Exception {
    // value 与 sensitivity 均允许为 null。
    aster.truffle.runtime.AsterPiiValue pii =
        new aster.truffle.runtime.AsterPiiValue(null, java.util.List.of("email"), null);

    Object val = interop.readMember(pii, "value");
    assertNotNull(val, "null PII value 的 readMember 不得返回裸 Java null");
    assertTrue(interop.isNull(val));

    Object sens = interop.readMember(pii, "sensitivity");
    assertNotNull(sens, "null sensitivity 的 readMember 不得返回裸 Java null");
    assertTrue(interop.isNull(sens));
  }

  @Test
  @DisplayName("入口函数返回 null：跨宿主边界还原为 isNull()，不 NPE/契约违例")
  void entryFunctionReturningNullRoundTrips() {
    // 入口函数直接 Return null（NullE）→ AsterRootNode 经 toInteropValue 规整后
    // asGuestValue，避免裸 null 触发 NPE。宿主拿到的 Value 应 isNull()==true。
    String program = """
        {
          "name": "test.entry.null",
          "decls": [{
            "kind": "Func",
            "name": "evaluate",
            "params": [],
            "ret": {"kind": "TypeName", "name": "Any"},
            "body": {"statements": [{"kind": "Return", "expr": {"kind": "Null"}}]}
          }]
        }
        """;
    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", program, "entry-null.json").build();
      // 零参入口：eval 直接返回结果值（非可调用函数）。修复前 asGuestValue(裸 null)
      // 会在此 NPE；修复后 eval 顺利返回一个 isNull() 的 guest 值。
      Value result = context.eval(source);
      assertTrue(result.isNull(), "入口返回 null 应在宿主侧表现为 isNull()==true");
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }
}
