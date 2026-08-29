package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import aster.truffle.runtime.interop.AsterListValue;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * {@code ==} / {@code !=} 运算符必须使用结构相等（tier1 eval 门 struct_list_equality 实测分叉）。
 *
 * <p>★真实缺陷：{@code eq}/{@code ne} builtin 用 {@code Objects.equals}，而
 * {@code AsterDataValue} / {@code AsterListValue} / {@code AsterMapValue}
 * <b>都没覆写 equals</b>，落回**引用相等**——字段全同的两个结构体判 {@code false}，
 * 而 TS 侧是结构相等判 {@code true}。
 *
 * <p>实测（tier1-parity eval 门，样本 {@code struct_list_equality}）：
 *
 * <pre>
 *   Let p1 be Point with x set to 1, y set to 2.
 *   Let p2 be Point with x set to 1, y set to 2.
 *   Return p1 is equal to p2.        // ts=true / java=false
 * </pre>
 *
 * <p>同一条规则跨引擎决策翻转且不报错——对合规引擎是最危险的一类缺陷。
 *
 * <p>★本仓自身也不自洽：{@code List.contains} / {@code List.distinct} 一直走结构相等的
 * {@code valueEquals}（见 {@link CollectionValueEqualityTest}），只有 {@code ==} / {@code !=}
 * 走 {@code Objects.equals}。故归一到 {@code valueEquals}。
 *
 * <p>★为什么这个缺陷长期没被发现：守门样本 {@code struct_list_equality} 从来不在
 * {@code tier1-parity/manifest.json} 里，而三个严格门都只遍历 manifest——
 * 样本不在其中就是**整体跳过**，跳过看起来跟通过一模一样
 * （aster-lang-test#118；该仓已补两道守卫禁止再次出现）。
 */
class EqOperatorStructuralEqualityTest {

  private static Object eq(Object a, Object b) {
    try {
      return Builtins.call("eq", new Object[] {a, b});
    } catch (Exception e) {
      throw new AssertionError("eq 抛异常: " + e, e);
    }
  }

  private static Object ne(Object a, Object b) {
    try {
      return Builtins.call("ne", new Object[] {a, b});
    } catch (Exception e) {
      throw new AssertionError("ne 抛异常: " + e, e);
    }
  }

  private static AsterDataValue point(int x, int y) {
    return new AsterDataValue(
        "Point", new String[] {"x", "y"}, new Object[] {x, y}, null);
  }

  private static Object list(Object... xs) {
    List<Object> l = new ArrayList<>();
    for (Object x : xs) {
      l.add(x);
    }
    return new AsterListValue(l);
  }

  @Test
  void structsWithIdenticalFieldsAreEqual() {
    // ★核心回归：这正是 tier1 eval 门报出 ts=true/java=false 的那一条。
    assertEquals(Boolean.TRUE, eq(point(1, 2), point(1, 2)),
        "字段全同的两个结构体必须相等（结构相等，非引用相等）");
  }

  @Test
  void structsWithDifferentFieldsAreNotEqual() {
    // ★反向护栏：没有这条，把 eq 写成「恒返回 true」也能让上一条变绿。
    assertEquals(Boolean.FALSE, eq(point(1, 2), point(1, 3)),
        "字段不同的结构体必须不等");
  }

  @Test
  void neIsStrictComplementOfEq() {
    // ★eq 与 ne 必须走同一套语义，否则会出现 a==b 与 a!=b 同时为 true 的荒谬结果。
    //   修复前 ne 也用 Objects.equals，两边一起错所以互补关系"看起来"成立——
    //   故这条要和上面的正向断言配合才有意义。
    assertEquals(Boolean.FALSE, ne(point(1, 2), point(1, 2)),
        "相等的结构体 != 必须为 false");
    assertEquals(Boolean.TRUE, ne(point(1, 2), point(1, 3)),
        "不等的结构体 != 必须为 true");
  }

  @Test
  void guestListsWithIdenticalElementsAreEqual() {
    // AsterListValue 同样没覆写 equals——issue 正文里 `List.range(1,n) is equal to
    // List.range(1,m)` 那半个分叉走的就是这条路径。
    assertEquals(Boolean.TRUE, eq(list(1, 2, 3), list(1, 2, 3)),
        "元素相同的 guest 列表必须相等");
    assertEquals(Boolean.FALSE, eq(list(1, 2, 3), list(1, 2, 4)),
        "元素不同的 guest 列表必须不等");
  }

  @Test
  void nestedStructsCompareRecursively() {
    // 嵌套：结构体里装结构体，必须逐层递归而非到第一层就退化成引用比较。
    AsterDataValue outerA =
        new AsterDataValue("Box", new String[] {"p"}, new Object[] {point(1, 2)}, null);
    AsterDataValue outerB =
        new AsterDataValue("Box", new String[] {"p"}, new Object[] {point(1, 2)}, null);
    AsterDataValue outerC =
        new AsterDataValue("Box", new String[] {"p"}, new Object[] {point(9, 9)}, null);

    assertEquals(Boolean.TRUE, eq(outerA, outerB), "嵌套结构体须逐层递归比较");
    assertEquals(Boolean.FALSE, eq(outerA, outerC), "内层不同则整体不等");
  }

  @Test
  void sameReferenceStillEqual() {
    // 边界：同一引用当然相等——确认归一到 valueEquals 没把这条基本情形弄坏。
    AsterDataValue p = point(5, 6);
    assertEquals(Boolean.TRUE, eq(p, p));
  }

  @Test
  void primitiveAndNumericEqualityUnchanged() {
    // ★护栏：数值/字符串/布尔的既有行为不得被本次改动波及
    //   （eq 里数值与 Decimal 分支在 valueEquals 之前，本次未动，这条钉住它）。
    assertEquals(Boolean.TRUE, eq(1, 1));
    assertEquals(Boolean.TRUE, eq(1, 1.0), "Int 与 Double 数值相等仍按数值比较");
    assertEquals(Boolean.FALSE, eq(1, 2));
    assertEquals(Boolean.TRUE, eq("abc", "abc"));
    assertEquals(Boolean.FALSE, eq("abc", "abd"));
    assertEquals(Boolean.TRUE, eq(true, true));
    assertEquals(Boolean.FALSE, eq(true, false));
  }

  @Test
  void nullComparisonUnchanged() {
    // 边界：null 参与比较不得抛异常。
    assertEquals(Boolean.TRUE, eq(null, null));
    assertEquals(Boolean.FALSE, eq(null, point(1, 2)));
    assertEquals(Boolean.FALSE, eq(point(1, 2), null));
  }
}
