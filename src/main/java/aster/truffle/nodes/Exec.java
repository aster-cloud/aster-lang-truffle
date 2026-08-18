package aster.truffle.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

public final class Exec {
  private Exec() {}

  public static Object exec(Node n, VirtualFrame f) {
    Profiler.inc("exec");
    if (n instanceof AsterExpressionNode expr) return expr.executeGeneric(f);
    if (n instanceof ReturnNode rn) return rn.execute(f);
    if (n instanceof LetNodeEnv lne) return lne.execute(f);
    if (n instanceof SetNodeEnv sne) return sne.execute(f);
    // IfNode, MatchNode, BlockNode 已迁移到 AsterExpressionNode，由第一个分支处理
    if (n instanceof StartNode sn) return sn.execute(f);
    if (n instanceof WaitNode wn) return wn.execute(f);
    if (n instanceof WorkflowNode wf) return wf.execute(f);
    return null;
  }

  /**
   * 真值判定 —— 委托给 {@link aster.truffle.runtime.Builtins#toBool}。
   *
   * <p>★2026-08-17 审计：本方法原先是**第二份实现**，与 Builtins 那份在三处不一致，
   * 且三处都是真实缺陷：
   * <ol>
   *   <li><b>不 unwrap PII</b> —— PII 包装的 {@code false} 落到 {@code o != null}
   *       恒为真，<b>静默反转控制流</b>（把「拒绝」判成「通过」）。这是最严重的一条。</li>
   *   <li><b>字符串 {@code "false"} 判为假</b> —— TS 的 isTruthy 对任何非空串判真，
   *       同一段 CNL 双引擎结论相反。</li>
   *   <li><b>{@code toLowerCase()} 未指定 Locale</b> —— 土耳其 locale 下 I→ı，
   *       破坏确定性（同仓 Text.toUpper/toLower 早已因此显式加了 Locale.ROOT）。</li>
   * </ol>
   *
   * <p>更糟的是它让 Truffle <b>内部</b>就不自洽：{@code If}/{@code IfExpr} 走本份，
   * 而 {@code and}/{@code or}/{@code not} 走 Builtins 那份。
   *
   * <p>故不再维护第二份实现——消除重复副本才是消除分歧，逐条对齐只是加分支。
   */
  public static boolean toBool(Object o) {
    return aster.truffle.runtime.Builtins.toBool(o);
  }
}
