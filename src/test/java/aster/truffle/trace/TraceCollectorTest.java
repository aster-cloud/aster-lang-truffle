package aster.truffle.trace;

import aster.truffle.runtime.AsterDataValue;
import aster.truffle.runtime.AsterEnumValue;
import aster.truffle.runtime.interop.AsterListValue;
import aster.truffle.runtime.interop.AsterMapValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TraceCollector 单元测试（M2.1b，ADR 0030）——纯 Java，不经 Truffle machinery。
 *
 * <p>覆盖三条铁律：①决定性归一（guest 值→plain，Map 键排序，整数→Long，小数→BigDecimal）
 * ②有序 + 单调 sequence ③有界 fail-fast（截断不抛，决策不受影响）。这些是 replay payload 能否
 * 喂 canonical hash 的地基——错一处即跨引擎 hash 分歧。
 */
class TraceCollectorTest {

  // ===== §有序 + 单调 sequence =====

  @Test
  void sequenceIsMonotonicFromOne() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("if", "a", true, true, 0);
    c.push("match", "b", 1L, false, 0);
    c.push("return", "c", "x", true, 0);
    List<Map<String, Object>> steps = c.drain();
    assertEquals(3, steps.size());
    assertEquals(1, steps.get(0).get("sequence"));
    assertEquals(2, steps.get(1).get("sequence"));
    assertEquals(3, steps.get(2).get("sequence"));
  }

  @Test
  void stepPreservesInsertionOrder() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("if", "first", true, true, 0);
    c.push("if", "second", false, false, 0);
    List<Map<String, Object>> steps = c.drain();
    assertEquals("first", steps.get(0).get("expression"));
    assertEquals("second", steps.get(1).get("expression"));
  }

  @Test
  void stepHasAllContractFieldsIncludingNonNullChildren() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("match", "arm[0]", "R", true, 0);
    Map<String, Object> step = c.drain().get(0);
    assertEquals(1, step.get("sequence"));
    assertEquals("arm[0]", step.get("expression"));
    assertEquals("R", step.get("result"));
    assertEquals(true, step.get("matched"));
    // children 契约：非 null（即使空），与宿主 DecisionTrace.TraceStep.children 对齐。
    assertInstanceOf(List.class, step.get("children"));
    assertTrue(((List<?>) step.get("children")).isEmpty());
  }

  @Test
  void nullExpressionFallsBackToKind() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("return", null, 1L, true, 0);
    assertEquals("return", c.drain().get(0).get("expression"));
  }

  // ===== §决定性归一 =====

  @Test
  void integerNormalizedToLong() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "e", 42, true, 0); // 传 Integer
    Object result = c.drain().get(0).get("result");
    assertInstanceOf(Long.class, result);
    assertEquals(42L, result);
  }

  @Test
  void doubleNormalizedToBigDecimal() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "e", 1.5d, true, 0);
    Object result = c.drain().get(0).get("result");
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(0, new BigDecimal("1.5").compareTo((BigDecimal) result));
  }

  @Test
  void bigDecimalPreserved() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "e", new BigDecimal("1.50"), true, 0);
    assertInstanceOf(BigDecimal.class, c.drain().get(0).get("result"));
  }

  @Test
  void stringAndBooleanPassThrough() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "s", "hello", true, 0);
    c.push("k", "b", Boolean.TRUE, true, 0);
    assertEquals("hello", c.drain().get(0).get("result"));
    assertEquals(Boolean.TRUE, c.drain().get(1).get("result"));
  }

  @Test
  void nullResultStaysNull() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("match", "no-arm", null, false, 0);
    assertNull(c.drain().get(0).get("result"));
  }

  @Test
  void enumWithoutArgsNormalizedToQualifiedName() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "e", new AsterEnumValue("Decision", "Approved"), true, 0);
    assertEquals("Decision.Approved", c.drain().get(0).get("result"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void dataValueNormalizedToOrderedMapWithType() {
    AsterDataValue data = new AsterDataValue(
        "Applicant",
        new String[] {"age", "score"},
        new Object[] {30, new BigDecimal("0.75")},
        null);
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "e", data, true, 0);
    Object result = c.drain().get(0).get("result");
    assertInstanceOf(Map.class, result);
    Map<String, Object> m = (Map<String, Object>) result;
    // 字段顺序保留声明序 + _type 头；嵌套整数归一 Long。
    assertEquals(Arrays.asList("_type", "age", "score"), new ArrayList<>(m.keySet()));
    assertEquals("Applicant", m.get("_type"));
    assertEquals(30L, m.get("age"));
    assertInstanceOf(BigDecimal.class, m.get("score"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void listValueNormalizedRecursively() {
    AsterListValue list = new AsterListValue(new ArrayList<>(Arrays.asList(1, 2, "x")));
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "e", list, true, 0);
    Object result = c.drain().get(0).get("result");
    assertInstanceOf(List.class, result);
    assertEquals(Arrays.asList(1L, 2L, "x"), result);
  }

  @Test
  @SuppressWarnings("unchecked")
  void mapKeysSortedForDeterminism() {
    // 底层 HashMap（迭代序非确定）→ 归一后键必须排序，否则跨引擎/跨运行 hash 分歧。
    Map<String, Object> unordered = new HashMap<>();
    unordered.put("zebra", 1);
    unordered.put("alpha", 2);
    unordered.put("mango", 3);
    AsterMapValue mapValue = new AsterMapValue(unordered);
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "e", mapValue, true, 0);
    Map<String, Object> result = (Map<String, Object>) c.drain().get(0).get("result");
    assertEquals(Arrays.asList("alpha", "mango", "zebra"), new ArrayList<>(result.keySet()));
    assertEquals(2L, result.get("alpha"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void nestedMapKeysAlsoSorted() {
    Map<String, Object> inner = new HashMap<>();
    inner.put("y", 1);
    inner.put("x", 2);
    Map<String, Object> outer = new LinkedHashMap<>();
    outer.put("b", new AsterMapValue(inner));
    outer.put("a", 9);
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "e", new AsterMapValue(outer), true, 0);
    Map<String, Object> result = (Map<String, Object>) c.drain().get(0).get("result");
    assertEquals(Arrays.asList("a", "b"), new ArrayList<>(result.keySet()));
    Map<String, Object> nested = (Map<String, Object>) result.get("b");
    assertEquals(Arrays.asList("x", "y"), new ArrayList<>(nested.keySet()));
  }

  @Test
  void twoRunsWithSameInputProduceIdenticalSteps() {
    // 决定性：同输入两次 → 逐字段一致（喂 canonical hash 的前提）。
    List<Map<String, Object>> a = runScenario();
    List<Map<String, Object>> b = runScenario();
    assertEquals(a, b);
  }

  private List<Map<String, Object>> runScenario() {
    Map<String, Object> m = new HashMap<>();
    m.put("k2", 2);
    m.put("k1", 1);
    TraceCollector c = TraceCollector.withDefaults();
    c.push("if", "cond", true, true, 0);
    c.push("match", "arm[1]", new AsterMapValue(m), true, 0);
    c.push("return", "final", new BigDecimal("3.14"), true, 0);
    return c.drain();
  }

  // ===== §有界 fail-fast（截断不抛）=====

  @Test
  void maxStepsTruncatesWithoutThrowing() {
    TraceCollector c = new TraceCollector(2, 64, 64 * 1024);
    c.push("if", "1", true, true, 0);
    c.push("if", "2", true, true, 0);
    // 第 3 次超 maxSteps=2：不抛，静默截断。
    c.push("if", "3", true, true, 0);
    assertTrue(c.isTruncated());
    assertEquals(2, c.drain().size(), "超限步骤被丢弃，已有步骤保留");
  }

  @Test
  void maxDepthTruncatesWithoutThrowing() {
    TraceCollector c = new TraceCollector(500, 4, 64 * 1024);
    c.push("if", "deep", true, true, 4); // depth>=maxDepth
    assertTrue(c.isTruncated());
    assertEquals(0, c.drain().size());
  }

  @Test
  void oversizedValueTruncatesWithoutThrowing() {
    TraceCollector c = new TraceCollector(500, 64, 8); // maxValueBytes=8
    c.push("k", "big", "0123456789ABCDEF", true, 0); // 16 chars > 8
    assertTrue(c.isTruncated());
    assertEquals(0, c.drain().size());
  }

  @Test
  void pushAfterTruncationIsSilentNoop() {
    TraceCollector c = new TraceCollector(1, 64, 64 * 1024);
    c.push("if", "1", true, true, 0);
    c.push("if", "2", true, true, 0); // 触发截断
    c.push("if", "3", true, true, 0); // 已截断，静默丢
    assertTrue(c.isTruncated());
    assertEquals(1, c.drain().size());
  }

  @Test
  void drainReturnsImmutableCopy() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("if", "1", true, true, 0);
    List<Map<String, Object>> steps = c.drain();
    assertEquals(1, steps.size());
    // drain 后继续 push 不影响已 drain 的快照（List.copyOf）。
    c.push("if", "2", true, true, 0);
    assertEquals(1, steps.size());
    assertEquals(2, c.drain().size());
  }

  @Test
  void freshCollectorIsNotTruncatedAndEmpty() {
    TraceCollector c = TraceCollector.withDefaults();
    assertFalse(c.isTruncated());
    assertFalse(c.isAsyncTainted());
    assertTrue(c.isReplayable());
    assertEquals(0, c.size());
    assertTrue(c.drain().isEmpty());
  }

  // ===== §async 污染 → NON_REPLAYABLE（Codex 复审 P0）=====

  @Test
  void asyncTaintMarksNonReplayableAndDrainsEmpty() {
    TraceCollector c = TraceCollector.withDefaults();
    c.push("if", "a", true, true, 0);
    c.push("return", "b", 1L, true, 0);
    c.markAsyncTainted();
    assertTrue(c.isAsyncTainted());
    assertFalse(c.isReplayable(), "async 污染 → 不可回放");
    // drain 返回空——不吐调度依赖的部分步骤进 hash payload（诚实优于假证据）。
    assertTrue(c.drain().isEmpty(), "async 污染 drain 空，不产不稳定 traceHash");
  }

  @Test
  void truncationAloneMarksNonReplayable() {
    TraceCollector c = new TraceCollector(1, 64, 64 * 1024);
    c.push("if", "1", true, true, 0);
    c.push("if", "2", true, true, 0); // 截断
    assertFalse(c.isReplayable());
    assertFalse(c.isAsyncTainted());
  }

  // ===== §未知类型 → 归一降级 → NON_REPLAYABLE（Codex 复审）=====

  @Test
  void unknownTypeMarksDegradedAndNonReplayable() {
    TraceCollector c = TraceCollector.withDefaults();
    // 传一个引擎不认识的运行时对象（模拟 Lambda/host object/未知 TruffleObject）。
    Object opaque = new Object() {
      @Override public String toString() { return "opaque@" + System.identityHashCode(this); }
    };
    c.push("return", "e", opaque, true, 0);
    assertFalse(c.isReplayable(), "未知类型 → 归一降级 → 不可回放");
    // drain 空——不把含对象身份的兜底串塞进 hash payload。
    assertTrue(c.drain().isEmpty());
  }

  @Test
  void nonStringMapKeyMarksDegraded() {
    // 防御性：裸 Map 非 String 键（未来 host Map 进 trace）→ 降级 NON_REPLAYABLE，不放对象身份进 hash。
    Map<Object, Object> weird = new HashMap<>();
    weird.put(42, "int-key");
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "e", weird, true, 0); // 裸 Map<?,?> 走 normalizeMap 分支
    assertFalse(c.isReplayable());
    assertTrue(c.drain().isEmpty());
  }

  @Test
  void knownTypesDoNotDegrade() {
    // 所有已知类型都不触发降级（回归：确保降级只对真正未知类型）。
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "1", 42, true, 0);
    c.push("k", "2", "s", true, 0);
    c.push("k", "3", true, true, 0);
    c.push("k", "4", new BigDecimal("1.5"), true, 0);
    c.push("k", "5", new AsterEnumValue("E", "V"), true, 0);
    c.push("k", "6", new AsterListValue(new ArrayList<>(List.of(1, 2))), true, 0);
    assertTrue(c.isReplayable());
    assertEquals(6, c.drain().size());
  }

  @Test
  void decimalValueUnwrappedToBigDecimal() {
    // AsterDecimalValue（ADR 0025）→ 精确 BigDecimal（与 input/output 小数路径一致，非兜底降级）。
    aster.truffle.runtime.interop.AsterDecimalValue dv =
        aster.truffle.runtime.interop.AsterDecimalValue.of(new BigDecimal("107.90"));
    TraceCollector c = TraceCollector.withDefaults();
    c.push("k", "e", dv, true, 0);
    assertTrue(c.isReplayable());
    Object result = c.drain().get(0).get("result");
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(0, new BigDecimal("107.9").compareTo((BigDecimal) result));
  }

  // ===== §drain 深度不可变（Codex 复审：修"不可变副本"表述不准）=====

  @Test
  @SuppressWarnings("unchecked")
  void drainIsDeeplyImmutable() {
    Map<String, Object> m = new HashMap<>();
    m.put("k", 1);
    TraceCollector c = TraceCollector.withDefaults();
    c.push("match", "arm", new AsterMapValue(m), true, 0);
    Map<String, Object> step = c.drain().get(0);
    // 顶层 step map 不可改。
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class, () -> step.put("x", 1));
    // 内嵌 result map 不可改。
    Map<String, Object> resultMap = (Map<String, Object>) step.get("result");
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class, () -> resultMap.put("y", 1));
    // children 列表不可改。
    List<Object> children = (List<Object>) step.get("children");
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class, () -> children.add("z"));
  }

  // ===== §有界字节估算短路（Codex 复审：不先构造整串）=====

  @Test
  void largeNestedStructureTruncatesViaByteEstimate() {
    // 深嵌套/大集合触发字节估算截断（短路，不先 String.valueOf 整个）。
    TraceCollector c = new TraceCollector(500, 64, 32); // maxValueBytes=32
    List<Object> big = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      big.add("element-" + i);
    }
    c.push("k", "big-list", new AsterListValue(big), true, 0);
    assertTrue(c.isTruncated());
    assertEquals(0, c.drain().size());
  }
}
