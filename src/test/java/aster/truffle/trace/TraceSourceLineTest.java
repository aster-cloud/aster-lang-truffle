package aster.truffle.trace;

import org.graalvm.polyglot.Context;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 步骤级 trace 必须带**源码行号**，使条件漏斗能区分同类型的不同节点。
 *
 * <h2>被修复的问题</h2>
 *
 * <p>{@code IfNode}/{@code ReturnNode} 此前记硬编码字面量
 * （{@code "if condition"} / {@code "return value"}）。cloud 的漏斗聚合键是
 * {@code stepId + expression}，于是一条策略里**所有 If 被并成一行**——
 * 用户看到单条 {@code if condition 4/4 (100%)}，那其实是 4 个**不同条件**的和。
 *
 * <p>根因链：Core IR 的 {@code If.origin} 一直有值，但 truffle 侧的
 * {@code CoreModel} 是独立反序列化镜像且<b>没有 origin 字段</b>，
 * 而 mapper 配了 {@code FAIL_ON_UNKNOWN_PROPERTIES=false} → 行号被<b>静默丢弃</b>。
 *
 * <h2>★为什么必须走真实 polyglot 执行</h2>
 *
 * <p>本测试的第一版是直接 {@code new ReturnNode(expr, 22)} 再反射读私有字段
 * {@code traceLabel}。那是**假绿**：{@code Loader} 里有<b>两处</b>构造节点的分支，
 * 把**活跃**那处改回不传行号后，真实 trace 退化成全部同名
 * （实测 {@code if condition} ×4），而该版测试<b>依旧全绿</b>——
 * 它压根没有经过 Loader。
 *
 * <p>现改为喂真实 Core IR JSON 给 polyglot Context 执行，断言 drain 出来的
 * trace 标签。这样任何一处 Loader 漏改都会被抓住。
 */
class TraceSourceLineTest {

  @AfterEach
  void tearDown() {
    TraceAccess.drainCurrentThread();
    TraceAccess.setEnabled(false);
  }

  /**
   * ★端到端：两个位于不同行的 If + 一个 Return，标签必须各带自己的行号。
   *
   * <p>这条同时覆盖 {@code Loader} 的两处构造分支中**实际被执行**的那处——
   * 漏改任意一处都会让断言失败。
   */
  @Test
  void traceLabelsCarryDistinctSourceLines() {
    List<String> labels = runAndCollectLabels();

    assertEquals(
        List.of("if condition @L7", "if condition @L11", "return value @L12"),
        labels,
        "★标签必须逐条带上各自的源码行号；若退化成 `if condition` 说明 "
            + "Loader 没把 origin 传进节点（本仓踩过：两处构造分支只改了一处）");
  }

  /** 同类型节点的标签必须**互不相同**——这正是漏斗能分组的前提。 */
  @Test
  void sameKindDifferentLinesDoNotCollapse() {
    List<String> ifLabels = runAndCollectLabels().stream()
        .filter(l -> l.startsWith("if condition"))
        .collect(Collectors.toList());

    assertEquals(2, ifLabels.size(), "本用例构造了 2 个 If");
    assertEquals(2, ifLabels.stream().distinct().count(),
        "★两个不同行的 If 若标签相同，漏斗仍会把它们并成一行（正是被修复的 bug）");
  }

  /**
   * ★{@code Loader} 的**另一条**构造分支（{@code buildScope}）同样要带行号。
   *
   * <p>独立审查实测：只还原 {@code buildBlock} 那处（307/309），
   * 作者原测试与全量 637 条**全部保持绿色**，而真实 trace 已退化成
   * {@code [if condition, if condition, return value]}——正是本 PR 要修的 bug。
   * 两条分支必须各有覆盖，任一处被还原都要转红。
   *
   * <p>（另：四个无调用方的旧重载已删除，漏改现在会**编译失败**——
   * 比测试更早、更硬的一道门。）
   */
  @Test
  void scopeBranchAlsoCarriesSourceLines() {
    List<String> labels = runLabels(scopeProgram());

    assertTrue(labels.contains("if condition @L31"),
        "★Scope 分支内的 If 必须带行号，实际=" + labels);
    assertTrue(labels.contains("return value @L32"),
        "★Scope 分支内的 Return 必须带行号，实际=" + labels);
  }

  /** 执行默认（两个 If + Return）程序。 */
  private static List<String> runAndCollectLabels() {
    return runLabels(twoIfsProgram());
  }

  /** 执行一段真实 Core IR，返回 drain 出来的 trace 标签列表。 */
  private static List<String> runLabels(String program) {
    TraceAccess.setEnabled(true);
    TraceCollector collector = new TraceCollector(200, 20, 4096);
    TraceAccess.armCurrentThread(collector);
    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      context.eval("aster", program);
    }
    List<Map<String, Object>> steps = TraceAccess.drainCurrentThread().steps();
    assertFalse(steps.isEmpty(), "★没有记录到任何 trace 步骤——arm/record 链路本身断了");
    return steps.stream()
        .map(s -> String.valueOf(s.get("expression")))
        .collect(Collectors.toList());
  }

  /**
   * 两个 If（第 7、11 行）+ 一个 Return（第 12 行）的 Core IR。
   *
   * <p>行号由 {@code origin} 显式给出——本测试验的是「行号有没有被传下去」，
   * 不是「lowering 算得对不对」（后者由 core 侧测试负责）。
   */
  private static String twoIfsProgram() {
    return "{\"name\":\"test.trace.lines\",\"decls\":[{"
        + "\"kind\":\"Func\",\"name\":\"main\",\"params\":[],\"body\":{\"statements\":["
        + ifStmt(false, 7)
        + "," + ifStmt(true, 11)
        + ",{\"kind\":\"Return\",\"expr\":{\"kind\":\"Int\",\"value\":1},"
        + origin(12) + "}"
        + "]}}]}";
  }

  /** 一个把 If/Return 包在 Scope 里的程序——走 Loader 的 buildScope 分支。 */
  private static String scopeProgram() {
    return "{\"name\":\"test.trace.scope\",\"decls\":[{"
        + "\"kind\":\"Func\",\"name\":\"main\",\"params\":[],\"body\":{\"statements\":["
        + "{\"kind\":\"Scope\",\"statements\":["
        + ifStmt(true, 31)
        + ",{\"kind\":\"Return\",\"expr\":{\"kind\":\"Int\",\"value\":1},"
        + origin(32) + "}"
        + "]}"
        + "]}}]}";
  }

  private static String ifStmt(boolean cond, int line) {
    return "{\"kind\":\"If\",\"cond\":{\"kind\":\"Bool\",\"value\":" + cond + "},"
        + "\"thenBlock\":{\"statements\":[]},\"elseBlock\":null," + origin(line) + "}";
  }

  private static String origin(int line) {
    return "\"origin\":{\"file\":null,"
        + "\"start\":{\"line\":" + line + ",\"col\":3},"
        + "\"end\":{\"line\":" + line + ",\"col\":30}}";
  }
}
