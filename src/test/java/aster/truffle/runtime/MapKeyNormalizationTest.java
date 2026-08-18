package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Map 键归一化的四处一致性（2026-08-17 审计修复）。
 *
 * <p>此前 {@code Map.get} / {@code Map.contains} 用 {@code String.valueOf(key)} 归一，
 * 而 {@code Map.put} / {@code Map.remove} 存的是<b>原始对象</b>。于是非文本键上：
 *
 * <pre>
 *   Let m be Map.put(Map.empty(), 1, "one").
 *   Return Map.get(m, 1).
 * </pre>
 *
 * <p>TS 返回 {@code "one"}，Java 返回 {@code null}——键存成 {@code Integer 1}，
 * 查的却是 {@code "1"}。即<b>刚写进去就读不出来</b>，且不报错，Map 看上去凭空丢数据。
 *
 * <p>TS 侧在 get/put/remove/contains 四处<b>一致地</b>做 {@code String(k)}
 * （{@code interpreter.ts} 的 GuestMap 底层就是字符串键），故归一到 TS 的行为。
 */
class MapKeyNormalizationTest {

  private static Object empty() {
    return Builtins.call("Map.empty", new Object[]{});
  }

  private static Object put(Object m, Object k, Object v) {
    return Builtins.call("Map.put", new Object[]{m, k, v});
  }

  private static Object get(Object m, Object k) {
    return Builtins.call("Map.get", new Object[]{m, k});
  }

  private static Object contains(Object m, Object k) {
    return Builtins.call("Map.contains", new Object[]{m, k});
  }

  @Test
  void integerKeyRoundTrips() {
    // ★核心回归：put 后立刻 get 必须拿得回来。
    Object m = put(empty(), 1, "one");
    assertEquals("one", get(m, 1), "整数键 put 后 get 必须读回值，而不是 null");
    assertEquals(Boolean.TRUE, contains(m, 1), "整数键 put 后 contains 必须为真");
  }

  @Test
  void textKeyStillWorks() {
    // ★同等重要的一半：修复不得破坏本来就正常的文本键路径。
    Object m = put(empty(), "name", "alice");
    assertEquals("alice", get(m, "name"));
    assertEquals(Boolean.TRUE, contains(m, "name"));
  }

  @Test
  void integerAndTextKeyAreTheSameSlot() {
    // 归一到字符串后，1 与 "1" 是同一个键——这与 TS 的 String(k) 行为一致。
    // 显式锁死，避免将来有人"顺手"改成保留原始类型键而不自知已经分叉。
    Object m = put(empty(), 1, "first");
    assertEquals("first", get(m, "1"), "整数键 1 与文本键 \"1\" 必须落到同一槽位（与 TS 一致）");

    Object m2 = put(m, "1", "second");
    assertEquals("second", get(m2, 1), "用 \"1\" 覆写后，用 1 读取应看到新值");
    assertEquals(1, Builtins.call("Map.size", new Object[]{m2}),
        "1 与 \"1\" 是同一个键，size 应为 1 而不是 2");
  }

  @Test
  void removeUsesSameNormalization() {
    // remove 若不归一，会删不掉 put 进去的键——静默无效。
    Object m = put(empty(), 1, "one");
    Object removed = Builtins.call("Map.remove", new Object[]{m, 1});
    assertEquals(null, get(removed, 1), "整数键必须能被 remove 删掉");
    assertEquals(Boolean.FALSE, contains(removed, 1));
    assertEquals(0, Builtins.call("Map.size", new Object[]{removed}));
  }

  @Test
  void keysAreNormalizedStrings() {
    // Map.keys 直接返回底层键集合；归一后应全是字符串，与 TS 的 Object.keys 一致。
    Object m = put(put(empty(), 1, "a"), "b", "c");
    Object keys = Builtins.call("Map.keys", new Object[]{m});
    assertEquals(List.of("1", "b"), keys,
        "Map.keys 应返回归一后的字符串键（保插入序），与 TS Object.keys 一致");
  }

  @Test
  void integerValuedFloatKeyIsSameSlotAsInteger() {
    // ★对抗性审查（2026-08-18）发现：此前 mapKey 只做 String.valueOf，
    //   Java 的 String.valueOf(1.0)="1.0" 而 JS 的 String(1.0)="1"。
    //   于是 put(1.0) 存键 "1.0"、get(1) 查 "1" → null ——
    //   本次修复声称要消灭的「刚写进去就读不出来」在浮点键上原样残留。
    Object m = put(empty(), 1.0, "d");
    assertEquals(List.of("1"), Builtins.call("Map.keys", new Object[]{m}),
        "整数值浮点键必须归一成 \"1\"（与 TS String(1.0) 一致）");
    assertEquals("d", get(m, 1), "put(1.0) 后 get(1) 必须读得到");
    assertEquals("d", get(m, 1.0), "put(1.0) 后 get(1.0) 同样读得到");

    // 反方向：put 整数、get 浮点
    Object m2 = put(empty(), 1, "i");
    assertEquals("i", get(m2, 1.0), "put(1) 后 get(1.0) 必须读得到");

    // 同一槽位：1 与 1.0 覆写彼此
    Object m3 = put(put(empty(), 1, "first"), 1.0, "second");
    assertEquals(1, Builtins.call("Map.size", new Object[]{m3}),
        "1 与 1.0 是同一个键，size 应为 1");
    assertEquals("second", get(m3, 1));
  }

  @Test
  void largeIntegerValuedDoubleMatchesJsString() {
    // ★复评发现（F-1）：此前阈值拍成 1e15，比 JS 保守 6 个数量级 ——
    //   实测 mapKey(1e15) 得 "1.0E15"，而 JS String(1e15)="1000000000000000"，
    //   于是 [1e15, 2^53] 区间**仍与 TS 分叉**（与本 PR 上一轮被退回的理由同型，
    //   只是分叉点右移）。且该阈值当时完全无测试：改成 1e9 也全绿。
    //   上界改为 JS 的 MAX_SAFE_INTEGER（2^53），此处把边界钉死。
    assertEquals("1000000000000000", Builtins.mapKey(1e15),
        "1e15 必须与 JS String(1e15) 一致");
    assertEquals("9007199254740992", Builtins.mapKey(9007199254740992.0),
        "2^53（MAX_SAFE_INTEGER）是上界，含端点");
    assertEquals("-1000000000000000", Builtins.mapKey(-1e15), "负数同样归一");
    assertEquals("-9007199254740992", Builtins.mapKey(-9007199254740992.0), "负向边界同样含端点");

    // ★如实记录**残余分叉**（不掩盖）：超过 2^53 后 double 无法精确表示整数，
    //   降 long 会失真，故保守回落 String.valueOf。此时与 JS 仍有格式差异：
    //     1e16  →  Java "1.0E16"    JS "10000000000000000"
    //   JS 直到 1e21 才转科学计数法。该区间的对齐需要先决定「超安全整数的键
    //   如何表示」这一语义问题（两侧都无法精确表示该值），已另开 issue，
    //   不在本 PR 内假装覆盖。此处把**当前真实行为**钉住，防止无声漂移。
    assertEquals("1.0E16", Builtins.mapKey(1e16),
        "超 2^53 当前回落 String.valueOf —— 与 JS 的 \"10000000000000000\" 仍分叉（已记 issue）");
  }

  @Test
  void floatKeyIsNormalizedToo() {
    // ★复评发现（F-2）：新增的两条测试只用 Double 字面量，
    //   删掉整个 Float 分支全绿 —— 代码行为正确但没被钉住。
    assertEquals("1", Builtins.mapKey(Float.valueOf(1.0f)), "Float 整数值同样归一");
    assertEquals("2.5", Builtins.mapKey(Float.valueOf(2.5f)), "Float 非整数值保留小数形式");

    Object m = put(empty(), Float.valueOf(1.0f), "f");
    assertEquals("f", get(m, 1), "Float 键与整数键必须落到同一槽位");
  }

  @Test
  void nonIntegerFloatKeyKeepsDecimalForm() {
    // ★反向断言：不得把所有浮点一律截成整数。2.5 必须保持 "2.5"（TS String(2.5)="2.5"）。
    Object m = put(empty(), 2.5, "half");
    assertEquals(List.of("2.5"), Builtins.call("Map.keys", new Object[]{m}),
        "非整数值浮点键必须保留小数形式");
    assertEquals("half", get(m, 2.5));
    assertEquals(null, get(m, 2), "2.5 与 2 不得塌陷成同一个键");
  }

  @Test
  void booleanAndDoubleKeysRoundTrip() {
    // 其它非文本键类型同样必须往返。
    Object m = put(empty(), true, "yes");
    assertEquals("yes", get(m, true), "布尔键必须往返");

    Object m2 = put(empty(), 2.5, "half");
    assertEquals("half", get(m2, 2.5), "浮点键必须往返");
  }

  @Test
  void putDoesNotMutateSourceMap() {
    // Map builtin 是纯函数（拷贝语义）。归一化改动不得顺手把它改成原地写。
    Object m = put(empty(), 1, "one");
    Object m2 = put(m, 2, "two");
    assertEquals(null, get(m, 2), "put 必须返回新 Map，不得修改入参");
    assertEquals("two", get(m2, 2));
    assertEquals(1, Builtins.call("Map.size", new Object[]{m}));
    assertEquals(2, Builtins.call("Map.size", new Object[]{m2}));
  }

  @Test
  @SuppressWarnings("unchecked")
  void insertionOrderPreserved() {
    // 红队 P2-I 已锁：Map 全链用 LinkedHashMap 保插入序（决策可回放）。
    // 归一化不得破坏它。
    Object m = put(put(put(empty(), 3, "c"), 1, "a"), 2, "b");
    assertEquals(List.of("3", "1", "2"), Builtins.call("Map.keys", new Object[]{m}),
        "插入序必须保持（可回放性依赖它）");
    assertTrue(((Map<Object, Object>) m) instanceof java.util.LinkedHashMap);
    assertFalse(((Map<Object, Object>) m).isEmpty());
  }
}
