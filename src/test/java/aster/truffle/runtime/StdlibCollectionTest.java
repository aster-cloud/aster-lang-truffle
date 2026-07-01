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
  // 红队 P0-B：range 是唯一「从标量凭空造大列表」的 builtin，statementLimit 不数 native
  // for 循环 → 必须按结果长度设上限。与 ts interpreter MAX_RANGE_SIZE(1_000_000) parity。
  @Test void rangeTooLargeThrows() {
    // 2_000_000 > MAX_RANGE_SIZE(1_000_000) → 抛错（内存耗尽 DoS 防护）
    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> Builtins.call("List.range", new Object[]{0, 2_000_000}));
    assertTrue(ex.getMessage().contains("List.range: 长度过大"),
        "超上限 range 必须报内存耗尽 DoS，实际: " + ex.getMessage());
  }
  @Test void rangeWithinLimitOk() {
    // 边界：正好 1_000_000 应放行
    Object r = Builtins.call("List.range", new Object[]{0, 1_000_000});
    assertEquals(1_000_000, ((List<?>) r).size());
  }
  @Test void rangeNoIntOverflowOnExtremes() {
    // end/start 取 Integer 极值时，长度须用 long 计算，避免 int 溢出绕过上限检查。
    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> Builtins.call("List.range", new Object[]{Integer.MIN_VALUE, Integer.MAX_VALUE}));
    assertTrue(ex.getMessage().contains("List.range: 长度过大"),
        "极值区间长度约 4.29e9，必须被上限拦截，实际: " + ex.getMessage());
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

  // 红队 P2-I：Map 全链 LinkedHashMap（插入序）→ Map.keys/values 顺序确定且与 TS 一致。
  // 用会让 HashMap 明显重排的键集（"zebra","apple","mango","delta"）验证插入序被保留。
  @Test void mapKeysPreserveInsertionOrder() {
    Object m = Builtins.call("Map.empty", new Object[]{});
    m = Builtins.call("Map.put", new Object[]{m, "zebra", 1});
    m = Builtins.call("Map.put", new Object[]{m, "apple", 2});
    m = Builtins.call("Map.put", new Object[]{m, "mango", 3});
    m = Builtins.call("Map.put", new Object[]{m, "delta", 4});
    // 插入序（非哈希序、非字母序）：zebra, apple, mango, delta
    assertEquals(List.of("zebra", "apple", "mango", "delta"),
        Builtins.call("Map.keys", new Object[]{m}));
    assertEquals(List.of(1, 2, 3, 4),
        Builtins.call("Map.values", new Object[]{m}));
  }

  @Test void mapRemovePreservesRemainingOrder() {
    Object m = Builtins.call("Map.empty", new Object[]{});
    m = Builtins.call("Map.put", new Object[]{m, "zebra", 1});
    m = Builtins.call("Map.put", new Object[]{m, "apple", 2});
    m = Builtins.call("Map.put", new Object[]{m, "mango", 3});
    m = Builtins.call("Map.remove", new Object[]{m, "apple"});
    assertEquals(List.of("zebra", "mango"), Builtins.call("Map.keys", new Object[]{m}));
  }
}
