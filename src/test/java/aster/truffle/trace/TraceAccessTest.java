package aster.truffle.trace;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TraceAccess 门 + per-thread 通道测试（M2.1b）。
 *
 * <p>验证两级门控与 per-eval 隔离：①全局关时 record no-op ②全局开但未 arm 时 no-op（不崩）
 * ③arm 后 record 落到 collector ④drain 清 ThreadLocal（防跨顺序 eval 残留=fail-closed）。
 * ★这些是「关闭时行为同今 + 开启时不泄漏」的核心保证。
 */
class TraceAccessTest {

  @AfterEach
  void tearDown() {
    // 清理全局状态，避免测试间污染（setEnabled 是进程级）。
    TraceAccess.drainCurrentThread();
    TraceAccess.setEnabled(false);
  }

  @Test
  void disabledRecordIsNoopEvenWhenArmed() {
    TraceAccess.setEnabled(false);
    TraceCollector c = TraceCollector.withDefaults();
    TraceAccess.armCurrentThread(c);
    TraceAccess.record("if", "e", true, true, 0);
    // 全局关：record 折叠 no-op，collector 不动。
    assertEquals(0, c.size());
    TraceAccess.DrainResult drained = TraceAccess.drainCurrentThread();
    assertTrue(drained.steps().isEmpty());
  }

  @Test
  void enabledButNotArmedIsNoopAndDoesNotThrow() {
    TraceAccess.setEnabled(true);
    // 未 arm（无 collector）→ recordSlow 短路，不 NPE。
    TraceAccess.record("if", "e", true, true, 0);
    TraceAccess.DrainResult drained = TraceAccess.drainCurrentThread();
    assertTrue(drained.steps().isEmpty());
    assertFalse(drained.truncated());
  }

  @Test
  void enabledAndArmedRecordsToCollector() {
    TraceAccess.setEnabled(true);
    TraceAccess.armCurrentThread(TraceCollector.withDefaults());
    TraceAccess.record("if", "cond", true, true, 0);
    TraceAccess.record("return", "final", 42, true, 0);
    TraceAccess.DrainResult drained = TraceAccess.drainCurrentThread();
    List<Map<String, Object>> steps = drained.steps();
    assertEquals(2, steps.size());
    assertEquals("cond", steps.get(0).get("expression"));
    assertEquals(42L, steps.get(1).get("result")); // 归一 Long
    assertFalse(drained.truncated());
  }

  @Test
  void drainClearsThreadLocalPreventingLeak() {
    TraceAccess.setEnabled(true);
    TraceAccess.armCurrentThread(TraceCollector.withDefaults());
    TraceAccess.record("if", "a", true, true, 0);
    TraceAccess.drainCurrentThread(); // 清 ThreadLocal

    // 第二次「eval」未重新 arm：record 应短路（前次 collector 已被 drain 清除，不泄漏）。
    TraceAccess.record("if", "b", true, true, 0);
    TraceAccess.DrainResult second = TraceAccess.drainCurrentThread();
    assertTrue(second.steps().isEmpty(), "drain 后 ThreadLocal 已清，后续 eval 不继承前次 collector");
  }

  @Test
  void drainReportsTruncation() {
    TraceAccess.setEnabled(true);
    TraceAccess.armCurrentThread(new TraceCollector(1, 64, 64 * 1024));
    TraceAccess.record("if", "1", true, true, 0);
    TraceAccess.record("if", "2", true, true, 0); // 超 maxSteps=1 → 截断
    TraceAccess.DrainResult drained = TraceAccess.drainCurrentThread();
    assertTrue(drained.truncated());
    assertEquals(1, drained.steps().size());
  }

  @Test
  void reArmReplacesCollector() {
    TraceAccess.setEnabled(true);
    TraceAccess.armCurrentThread(TraceCollector.withDefaults());
    TraceAccess.record("if", "first-eval", true, true, 0);
    // 新一轮 eval：重新 arm 覆盖，旧 collector 丢弃。
    TraceAccess.armCurrentThread(TraceCollector.withDefaults());
    TraceAccess.record("if", "second-eval", true, true, 0);
    TraceAccess.DrainResult drained = TraceAccess.drainCurrentThread();
    assertEquals(1, drained.steps().size());
    assertEquals("second-eval", drained.steps().get(0).get("expression"));
  }

  @Test
  void recordMatchArmFormatsIndexInSlowPath() {
    TraceAccess.setEnabled(true);
    TraceAccess.armCurrentThread(TraceCollector.withDefaults());
    TraceAccess.recordMatchArm(2, "R", true, 0);
    TraceAccess.DrainResult drained = TraceAccess.drainCurrentThread();
    assertEquals("match arm[2]", drained.steps().get(0).get("expression"));
  }

  @Test
  void recordMatchArmIsNoopWhenDisabled() {
    TraceAccess.setEnabled(false);
    TraceCollector c = TraceCollector.withDefaults();
    TraceAccess.armCurrentThread(c);
    TraceAccess.recordMatchArm(0, "R", true, 0);
    assertEquals(0, c.size());
  }

  @Test
  void markAsyncTaintedPropagatesNonReplayableThroughDrain() {
    TraceAccess.setEnabled(true);
    TraceAccess.armCurrentThread(TraceCollector.withDefaults());
    TraceAccess.record("if", "a", true, true, 0);
    TraceAccess.markAsyncTainted(); // 模拟 workflow/start 触点
    TraceAccess.DrainResult drained = TraceAccess.drainCurrentThread();
    assertTrue(drained.asyncTainted());
    assertFalse(drained.replayable());
    assertTrue(drained.steps().isEmpty(), "async 污染 → drain 空");
  }

  @Test
  void markAsyncTaintedIsNoopWhenDisabled() {
    TraceAccess.setEnabled(false);
    TraceCollector c = TraceCollector.withDefaults();
    TraceAccess.armCurrentThread(c);
    TraceAccess.markAsyncTainted();
    assertFalse(c.isAsyncTainted(), "全局关时 markAsyncTainted 折叠 no-op");
  }

  @Test
  void cleanEvalReportsReplayable() {
    TraceAccess.setEnabled(true);
    TraceAccess.armCurrentThread(TraceCollector.withDefaults());
    TraceAccess.record("if", "a", true, true, 0);
    TraceAccess.record("return", "b", 1, true, 0);
    TraceAccess.DrainResult drained = TraceAccess.drainCurrentThread();
    assertTrue(drained.replayable());
    assertFalse(drained.truncated());
    assertFalse(drained.asyncTainted());
    assertEquals(2, drained.steps().size());
  }
}
