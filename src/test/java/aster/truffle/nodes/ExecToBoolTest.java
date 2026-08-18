package aster.truffle.nodes;

import aster.truffle.runtime.AsterPiiValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code Exec.toBool} 的真值判定回归（2026-08-17 审计）。
 *
 * <p>该方法原先是 {@code Builtins.toBool} 之外的**第二份实现**，两者在三处不一致，
 * 且三处都是真实缺陷。更糟的是它让 Truffle **内部**就不自洽：
 * {@code If}/{@code IfExpr} 走 Exec 那份，而 {@code and}/{@code or}/{@code not}
 * 走 Builtins 那份——同一个值在同一段程序里可能被判成不同真值。
 *
 * <p>现已收敛为委托。本类逐条锁住三个缺陷不再复发。
 */
class ExecToBoolTest {

  /**
   * ★最严重的一条：PII 包装的 {@code false} 曾被判为 <b>true</b>。
   *
   * <p>旧实现最后一行是 {@code return o != null}，而 {@code AsterPiiValue} 是个
   * 非 null 的包装对象——于是「被标记为敏感数据的 false」在 {@code If} 条件里
   * 恒为真，<b>静默反转控制流</b>：本该走「拒绝」分支的决策会走「通过」分支。
   *
   * <p>对一个用于合规决策的引擎，这是最坏的一类缺陷——不报错、不留痕、结果相反。
   */
  @Test
  void piiWrappedFalseIsFalse_notTruthyWrapper() {
    Object wrappedFalse = new AsterPiiValue(false, List.of("pii"), "high");
    assertFalse(Exec.toBool(wrappedFalse),
        "PII 包装的 false 必须判为 false——旧实现返回 o != null 恒真，会静默反转控制流");

    Object wrappedTrue = new AsterPiiValue(true, List.of("pii"), "high");
    assertTrue(Exec.toBool(wrappedTrue), "PII 包装的 true 仍应为 true");
  }

  @Test
  void piiWrappedZeroAndEmptyStringAreFalse() {
    // unwrap 之后还要按内层类型判定，不能只处理 boolean
    assertFalse(Exec.toBool(new AsterPiiValue(0, List.of("pii"), "high")),
        "PII 包装的 0 应为 false");
    assertFalse(Exec.toBool(new AsterPiiValue("", List.of("pii"), "high")),
        "PII 包装的空串应为 false");
    assertTrue(Exec.toBool(new AsterPiiValue("x", List.of("pii"), "high")),
        "PII 包装的非空串应为 true");
  }

  /**
   * ★字符串 {@code "false"} 必须判为 <b>true</b>（因为非空）。
   *
   * <p>旧实现特判了 {@code "true"}/{@code "false"} 字面量，而 TS 的
   * {@code isTruthy} 只看长度——同一段 CNL 双引擎结论相反。
   * 以 TS 为准（它是三引擎里语义的事实基准，且与 JS 直觉一致）。
   */
  @Test
  void stringFalseIsTruthy_matchingTsSemantics() {
    assertTrue(Exec.toBool("false"),
        "非空字符串 \"false\" 应判为 true——TS 的 isTruthy 只看长度，双引擎须一致");
    assertTrue(Exec.toBool("FALSE"));
    assertTrue(Exec.toBool(" "), "空格是非空串");
    assertFalse(Exec.toBool(""), "只有空串才是 false");
  }

  /**
   * ★locale 无关：旧实现用无参 {@code toLowerCase()}，在土耳其 locale 下
   * {@code "I"→"ı"}，判定结果随 JVM 默认 locale 漂移。
   *
   * <p>同仓 {@code Text.toUpper}/{@code toLower} 早已因同一问题显式加了
   * {@code Locale.ROOT}（见 #43）——这里通过删除字符串特判从根上消除了该依赖。
   */
  @Test
  void isLocaleIndependent() {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(new Locale("tr", "TR"));
      assertTrue(Exec.toBool("TRUE"));
      assertTrue(Exec.toBool("true"));
      assertTrue(Exec.toBool("false"));
      assertFalse(Exec.toBool(""));
    } finally {
      Locale.setDefault(previous);
    }
  }

  /** 基础类型语义须与 TS 的 isTruthy 完全一致。 */
  @Test
  void primitiveSemanticsMatchTs() {
    assertFalse(Exec.toBool(null), "null → false");
    assertTrue(Exec.toBool(true));
    assertFalse(Exec.toBool(false));
    assertFalse(Exec.toBool(0), "0 → false");
    assertFalse(Exec.toBool(0.0));
    assertTrue(Exec.toBool(1));
    assertTrue(Exec.toBool(-1), "非零即真，包括负数");
    assertTrue(Exec.toBool(new Object()), "其余对象 → true");
  }

  /**
   * ★Truffle 内部一致性：{@code If} 与 {@code and/or/not} 必须用同一套真值判定。
   *
   * <p>此前两者分别走 Exec 与 Builtins 的不同实现，同一个值可能被判成不同真值。
   */
  @Test
  void execAndBuiltinsAgreeOnEveryValue() {
    Object[] samples = {
        null, true, false, 0, 1, -1, 0.0, 2.5, "", " ", "false", "true", "x",
        new AsterPiiValue(false, List.of("pii"), "high"),
        new AsterPiiValue(true, List.of("pii"), "high"),
        new AsterPiiValue("", List.of("pii"), "high"),
        new Object()
    };
    for (Object s : samples) {
      assertEquals(aster.truffle.runtime.Builtins.toBool(s), Exec.toBool(s),
          "Exec 与 Builtins 的真值判定必须一致，样本=" + s);
    }
  }
}
