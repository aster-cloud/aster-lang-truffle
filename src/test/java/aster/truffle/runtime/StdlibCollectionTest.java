package aster.truffle.runtime;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// ADR 0024 受控 stdlib：通用集合 builtin 的纯值参子集（不需 LambdaValue）直接单测。
// fn 参版本（sortBy/minBy/maxBy/groupBy/count）由端到端 Core IR eval + ts 镜像覆盖。
class StdlibCollectionTest {
  private static List<Object> list(Object... xs) { return java.util.Arrays.asList(xs); }

  @Test void sum() { assertEquals(21, ((Number) Builtins.call("List.sum", new Object[]{list(3,8,1,9)})).intValue()); }
  @Test void sumEmpty() { assertEquals(0, ((Number) Builtins.call("List.sum", new Object[]{list()})).intValue()); }
  @Test void sumDouble() { assertEquals(6.5, ((Number) Builtins.call("List.sum", new Object[]{list(3, 3.5)})).doubleValue()); }
  @Test void min() { assertEquals(1, ((Number) Builtins.call("List.min", new Object[]{list(3,8,1,9)})).intValue()); }
  @Test void max() { assertEquals(9, ((Number) Builtins.call("List.max", new Object[]{list(3,8,1,9)})).intValue()); }
  @Test void minEmptyThrows() { assertThrows(Exception.class, () -> Builtins.call("List.min", new Object[]{list()})); }
  @Test void distinct() {
    Object r = Builtins.call("List.distinct", new Object[]{list(2,2,5,2,9)});
    assertEquals(List.of(2,5,9), r);
  }
  @Test void range() {
    Object r = Builtins.call("List.range", new Object[]{2, 7});
    assertEquals(List.of(2,3,4,5,6), r);
  }
  @Test void rangeEmpty() {
    assertEquals(List.of(), Builtins.call("List.range", new Object[]{5, 5}));
  }
  // List.combinations(list, k)：确定性递增索引字典序；与 TS interpreter 逐位一致。
  @Test void combinationsCount() {
    assertEquals(6, ((List<?>) Builtins.call("List.combinations", new Object[]{list(10,20,30,40), 2})).size());
    assertEquals(21, ((List<?>) Builtins.call("List.combinations", new Object[]{list(2,3,4,5,6,7,8), 5})).size());
  }
  @Test void combinationsOrder() {
    // [10,20,30] choose 2 → [[10,20],[10,30],[20,30]]（递增索引序）
    assertEquals(List.of(List.of(10,20), List.of(10,30), List.of(20,30)),
        Builtins.call("List.combinations", new Object[]{list(10,20,30), 2}));
  }
  @Test void combinationsKZero() {
    assertEquals(List.of(List.of()), Builtins.call("List.combinations", new Object[]{list(1,2,3), 0}));
  }
  @Test void combinationsKGreaterThanN() {
    assertEquals(List.of(), Builtins.call("List.combinations", new Object[]{list(1,2), 5}));
  }
  @Test void combinationsTooManyThrows() {
    // C(40,20) 远超 5000 上限 → 抛错（DoS 防护）
    assertThrows(Exception.class, () ->
        Builtins.call("List.combinations", new Object[]{Builtins.call("List.range", new Object[]{0, 40}), 20}));
  }
  @Test void sortAscending() {
    Object r = Builtins.call("List.sort", new Object[]{list(9,1,5,3)});
    assertEquals(List.of(1,3,5,9), r);
  }
  @Test void sortStable() {
    // 等键保持原序（稳定）——这里全不同，至少验升序正确。
    Object r = Builtins.call("List.sort", new Object[]{list(3,1,2)});
    assertEquals(List.of(1,2,3), r);
  }
}
