package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import aster.truffle.runtime.interop.AsterListValue;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 集合 builtin 对 guest 列表（{@link AsterListValue}）的接受一致性（2026-08-17 审计修复）。
 *
 * <p>{@code Builtins} 里存在<b>两套</b>列表取值方式：
 *
 * <ul>
 *   <li>{@code asList(args[0])} —— 兼容原生 {@code java.util.List} 与 guest
 *       {@code AsterListValue}，并 unwrap PII；{@code contains/length/map/reduce} 等用它</li>
 *   <li>裸 {@code args[0] instanceof List<?>} —— 只认原生 List；
 *       {@code append/concat/slice/filter} 曾用它</li>
 * </ul>
 *
 * <p>于是同一个 guest 列表在一半 builtin 上能用、另一半上抛类型错误，且错误消息
 * <b>自相矛盾</b>：「期望类型 List，实际为 AsterListValue」——一个列表类型因为不是列表而被拒。
 *
 * <p>这不是理论问题：{@code AsterEnumValue.readMember("args")} 与 {@code getMembers}
 * 都返回 {@code AsterListValue}，即 CNL 里对枚举值做成员访问就能拿到 guest 列表。
 */
class GuestListAcceptanceTest {

  private static Object guest(Object... xs) {
    List<Object> l = new ArrayList<>();
    for (Object x : xs) {
      l.add(x);
    }
    return new AsterListValue(l);
  }

  @Test
  void concatAcceptsGuestLists() {
    assertEquals(List.of(1, 2, 3),
        Builtins.call("List.concat", new Object[]{guest(1, 2), guest(3)}),
        "List.concat 必须接受 guest 列表");
  }

  @Test
  void concatAcceptsMixedNativeAndGuest() {
    // 混合形态最容易漏：一侧原生一侧 guest。
    assertEquals(List.of(1, 2, 3),
        Builtins.call("List.concat", new Object[]{List.of(1, 2), guest(3)}));
    assertEquals(List.of(1, 2, 3),
        Builtins.call("List.concat", new Object[]{guest(1, 2), List.of(3)}));
  }

  @Test
  void sliceAcceptsGuestList() {
    assertEquals(List.of(1, 2),
        Builtins.call("List.slice", new Object[]{guest(1, 2, 3), 0, 2}),
        "List.slice 必须接受 guest 列表");
  }

  @Test
  void appendAcceptsGuestList() {
    assertEquals(List.of(1, 2, 3),
        Builtins.call("List.append", new Object[]{guest(1, 2), 3}),
        "List.append 必须接受 guest 列表");
  }

  @Test
  void nativeListsStillWork() {
    // ★同等重要的一半：改用 asList 不得破坏原生 List 路径。
    assertEquals(List.of(1, 2, 3),
        Builtins.call("List.concat", new Object[]{List.of(1, 2), List.of(3)}));
    assertEquals(List.of(1, 2),
        Builtins.call("List.slice", new Object[]{List.of(1, 2, 3), 0, 2}));
    assertEquals(List.of(1, 2, 3),
        Builtins.call("List.append", new Object[]{List.of(1, 2), 3}));
  }

  @Test
  void nonListStillRejected() {
    // ★另一半反向断言：不能为了"接受 guest 列表"就把什么都放行。
    //   否则 asList 返回 null 的分支没人守，类型错误会静默变成 NPE 或错误结果。
    assertThrows(Exception.class,
        () -> Builtins.call("List.concat", new Object[]{"not a list", List.of(1)}),
        "非列表必须仍被拒绝");
    assertThrows(Exception.class,
        () -> Builtins.call("List.concat", new Object[]{List.of(1), 42}),
        "第二参非列表同样必须被拒绝");
    assertThrows(Exception.class,
        () -> Builtins.call("List.slice", new Object[]{"not a list", 0, 1}));
    assertThrows(Exception.class,
        () -> Builtins.call("List.append", new Object[]{42, 1}));
  }

  @Test
  void purityPreserved() {
    // 集合 builtin 是纯函数（拷贝语义）。换取值方式不得让它变成原地写。
    List<Object> src = new ArrayList<>(List.of(1, 2));
    Object appended = Builtins.call("List.append", new Object[]{src, 3});
    assertEquals(List.of(1, 2), src, "List.append 不得修改入参");
    assertEquals(List.of(1, 2, 3), appended);

    List<Object> a = new ArrayList<>(List.of(1));
    Builtins.call("List.concat", new Object[]{a, List.of(2)});
    assertEquals(List.of(1), a, "List.concat 不得修改入参");
  }

  @Test
  void guestListAgreesWithNativeAcrossBuiltins() {
    // 同一份数据，guest 与原生两种载体在所有这些 builtin 上结果必须一致。
    Object g = guest(3, 1, 2);
    List<Object> n = new ArrayList<>(List.of(3, 1, 2));
    for (String op : new String[]{"List.length", "List.distinct", "List.sum"}) {
      assertEquals(Builtins.call(op, new Object[]{n}), Builtins.call(op, new Object[]{g}),
          op + " 在 guest 与原生列表上结果必须一致");
    }
    assertTrue((Boolean) Builtins.call("List.contains", new Object[]{g, 1}));
  }
}
