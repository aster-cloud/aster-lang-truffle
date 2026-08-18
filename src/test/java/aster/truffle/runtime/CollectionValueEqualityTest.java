package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import aster.truffle.runtime.interop.AsterListValue;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * 集合语义的<b>值相等</b>（2026-08-17 审计修复）。
 *
 * <p>此前 Java 侧用 {@code Objects.equals} / {@code List.contains}（即 Java 的
 * {@code equals}），而 {@code AsterListValue} / {@code AsterMapValue} /
 * {@code AsterDataValue} <b>都没有覆写 equals</b>，落回引用相等。于是同样的值，
 * 换个载体结论就不同：
 *
 * <pre>
 *   List.contains([[1, 2]], [1, 2])   // 原生嵌套 → true；guest 嵌套 → false
 *   List.distinct([[1], [1]])         // TS → [[1]]；Java（guest 载体）→ [[1], [1]]
 * </pre>
 *
 * <p>TS 侧一直是深度结构相等（{@code interpreter.ts} 的 {@code valueEquals}，
 * 数组逐元素 / 对象逐键 / Decimal 按值），实测：嵌套列表 contains → {@code true}，
 * distinct → {@code [[1]]}。故归一到 TS。
 *
 * <p>对合规引擎而言「同一个值去重不掉 / 查不到」会直接改变判定结果，且不报错。
 */
class CollectionValueEqualityTest {

  private static Object guest(Object... xs) {
    List<Object> l = new ArrayList<>();
    for (Object x : xs) {
      l.add(x);
    }
    return new AsterListValue(l);
  }

  private static Map<String, Object> map(String k, Object v) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put(k, v);
    return m;
  }

  @Test
  void containsFindsEqualNestedGuestList() {
    // ★核心回归：guest 载体的同值元素必须命中。
    assertEquals(Boolean.TRUE,
        Builtins.call("List.contains", new Object[]{List.of(guest(1, 2)), guest(1, 2)}),
        "嵌套 guest 列表的同值元素必须被 contains 命中");
  }

  @Test
  void containsIsCarrierAgnostic() {
    // 同一份值，四种载体组合结论必须一致（都为 true）——这正是此前分叉的地方。
    assertEquals(Boolean.TRUE,
        Builtins.call("List.contains", new Object[]{List.of(List.of(1, 2)), List.of(1, 2)}));
    assertEquals(Boolean.TRUE,
        Builtins.call("List.contains", new Object[]{List.of(guest(1, 2)), List.of(1, 2)}),
        "存 guest 查原生必须命中");
    assertEquals(Boolean.TRUE,
        Builtins.call("List.contains", new Object[]{List.of(List.of(1, 2)), guest(1, 2)}),
        "存原生查 guest 必须命中");
  }

  @Test
  void distinctDedupesEqualNestedLists() {
    // TS 实测为 [[1]]。
    Object r = Builtins.call("List.distinct", new Object[]{List.of(guest(1), guest(1))});
    assertEquals(1, ((List<?>) r).size(), "同值嵌套列表必须去重，实际=" + r);
  }

  @Test
  void distinctDedupesAcrossCarriers() {
    Object r = Builtins.call("List.distinct", new Object[]{List.of(List.of(1), guest(1))});
    assertEquals(1, ((List<?>) r).size(), "原生与 guest 同值必须视为重复，实际=" + r);
  }

  @Test
  void unequalValuesStillDistinct() {
    // ★同等重要的一半：反向断言。否则「valueEquals 一律返回 true」也能让上面全绿——
    //   那会把所有元素去重成一个，是灾难性的假修复。
    assertEquals(Boolean.FALSE,
        Builtins.call("List.contains", new Object[]{List.of(guest(1, 2)), guest(1, 3)}),
        "值不同必须不命中");
    assertEquals(Boolean.FALSE,
        Builtins.call("List.contains", new Object[]{List.of(guest(1, 2)), guest(1)}),
        "长度不同必须不命中");
    Object r = Builtins.call("List.distinct", new Object[]{List.of(guest(1), guest(2))});
    assertEquals(2, ((List<?>) r).size(), "不同值不得被去重，实际=" + r);
  }

  @Test
  void scalarEqualitySemanticsUnchanged() {
    // 标量路径必须与改动前完全一致。
    assertEquals(Boolean.TRUE, Builtins.call("List.contains", new Object[]{List.of(1, 2, 3), 2}));
    assertEquals(Boolean.FALSE, Builtins.call("List.contains", new Object[]{List.of(1, 2, 3), 9}));
    assertEquals(Boolean.TRUE, Builtins.call("List.contains", new Object[]{List.of("a", "b"), "a"}));
    assertEquals(List.of(2, 5, 9),
        Builtins.call("List.distinct", new Object[]{List.of(2, 2, 5, 2, 9)}),
        "标量去重行为不得改变");
  }

  @Test
  void valueEqualsHandlesMapsAndMixedTypes() {
    assertTrue(Builtins.valueEquals(map("a", 1), map("a", 1)), "同值 Map 应相等");
    assertFalse(Builtins.valueEquals(map("a", 1), map("a", 2)), "值不同的 Map 不应相等");
    assertFalse(Builtins.valueEquals(map("a", 1), map("b", 1)), "键不同的 Map 不应相等");

    // 类型不同不得相等（防"结构相等"退化成宽松比较）
    assertFalse(Builtins.valueEquals(List.of(1), map("0", 1)), "列表与 Map 不应相等");
    assertFalse(Builtins.valueEquals(List.of(1), 1), "列表与标量不应相等");
    assertFalse(Builtins.valueEquals(null, List.of()), "null 与空列表不应相等");
    assertTrue(Builtins.valueEquals(null, null), "null 与 null 相等");
  }

  @Test
  void deepNestingRecurses() {
    // 递归而非只比一层。
    assertTrue(Builtins.valueEquals(guest(guest(1, guest(2))), guest(guest(1, guest(2)))),
        "深层嵌套同值应相等");
    assertFalse(Builtins.valueEquals(guest(guest(1, guest(2))), guest(guest(1, guest(3)))),
        "深层嵌套值不同应不等");
  }
}
