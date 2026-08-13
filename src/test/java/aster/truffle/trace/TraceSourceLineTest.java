package aster.truffle.trace;

import aster.truffle.nodes.ReturnNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 步骤级 trace 必须带**源码行号**，使条件漏斗能区分同类型的不同节点。
 *
 * <h2>被修复的问题</h2>
 *
 * <p>此前 {@code IfNode}/{@code ReturnNode} 记的是硬编码字面量
 * （{@code "if condition"} / {@code "return value"}）。cloud 的漏斗聚合键是
 * {@code stepId + expression}，于是一条策略里**所有 If 被并成一行**——
 * 用户看到的是单条 {@code if condition  4/4 (100%)}，
 * 数字正确却毫无意义：那是 4 个**不同条件**的和。
 *
 * <p>根因链（三层都缺）：
 * <ol>
 *   <li>Core IR 的 {@code If.origin} 一直有值（lowering 已填）</li>
 *   <li>但 truffle 侧的 {@code CoreModel} 是独立的反序列化镜像，
 *       <b>没有 origin 字段</b>；而 mapper 配了
 *       {@code FAIL_ON_UNKNOWN_PROPERTIES=false} → 行号被<b>静默丢弃</b></li>
 *   <li>Loader 构造节点时自然也传不了行号</li>
 * </ol>
 */
class TraceSourceLineTest {

  @AfterEach
  void tearDown() {
    TraceAccess.drainCurrentThread();
    TraceAccess.setEnabled(false);
  }

  /** 带行号时标签必须区分开；不带行号时回落到原字面量（向后兼容）。 */
  @Test
  void returnNodeLabelCarriesSourceLine() {
    assertEquals("return value @L22", labelOf(new ReturnNode(null, 22)));
    assertEquals("return value", labelOf(new ReturnNode(null, 0)),
        "★行号未知时必须回落到原字面量，不能出现 `@L0`");
  }

  /** ★同一类型的不同行必须产生**不同**标签——这正是漏斗能分组的前提。 */
  @Test
  void differentLinesProduceDifferentLabels() {
    assertNotEquals(labelOf(new ReturnNode(null, 15)), labelOf(new ReturnNode(null, 17)),
        "★两个不同行的 Return 若标签相同，漏斗仍会把它们并成一行");
  }

  private static String labelOf(ReturnNode node) {
    try {
      var f = ReturnNode.class.getDeclaredField("traceLabel");
      f.setAccessible(true);
      return (String) f.get(node);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("traceLabel 字段缺失——标签生成逻辑被改动了", e);
    }
  }
}
