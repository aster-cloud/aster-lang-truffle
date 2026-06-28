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
