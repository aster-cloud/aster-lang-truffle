package aster.truffle.purity;

import com.oracle.truffle.api.CallTarget;
import java.util.Set;

/**
 * 纯度分析器：判定函数是否为纯函数，用于决定 {@code List.map} / {@code List.filter}
 * 能否走并行路径。
 *
 * <p>纯度在这里是**安全判定**而非优化提示：判错成"纯"会让含副作用的 lambda 被丢进
 * ForkJoinPool，绕过副作用与线程约束。故采用保守语义——<b>只有被证明为纯才算纯，
 * 未知一律按不纯处理</b>。这与本类原有 javadoc 的声明一致
 * （"false 表示含副作用<b>或未知</b>"），此前的实现却与之相悖。
 *
 * <h2>本次修正（issue #53 / #54）</h2>
 *
 * <p>旧实现是一个 {@code static Map<CallTarget, Boolean>} 缓存，两个缺陷互相掩盖：
 *
 * <ul>
 *   <li><b>#53 判定恒真</b>：把"effect 集合为空"等同于"纯"。而唯一的 LambdaValue
 *       构造点 {@code LambdaNode} 硬编码传 {@code Set.of()}，且
 *       {@code CoreModel.Lambda} <b>根本没有 effects 字段</b>——这份元数据当前
 *       无从获得。于是每个内联 lambda 都被判为纯，并行化闸门实际永远敞开。</li>
 *   <li><b>#54 内存泄漏</b>：该 Map 只 put 不 evict，而每次 parse 产生新
 *       CallTarget、每次构造 LambdaValue 都 put 一次，长运行的 pooled Context
 *       会无界累积。</li>
 * </ul>
 *
 * <p>既然当前**没有任何调用方能提供可信的 effect 元数据**，正确做法不是给缓存加淘汰
 * 策略（那只是让错误判定泄漏得慢一点），而是**不再记录未经证实的纯度**。缓存随之
 * 失去存在意义，一并删除——泄漏（#54）与误判（#53）同时消失。
 *
 * <p><b>取舍</b>：内联 lambda 因此走顺序路径，牺牲部分吞吐换正确性。并行路径此前
 * 属于"未经验证就放行"的潜在错线程风险、而非已测得的收益，故先保正确。待 Core IR
 * 能携带 lambda effect 元数据后，由 {@link #recordEffects} 传
 * {@code effectsKnown=true} 精确放行即可，无需再改调用点结构。
 */
public final class PurityAnalyzer {

  private PurityAnalyzer() {}

  /**
   * 注册 CallTarget 的 effect 元数据。
   *
   * <p>当前**所有**调用点的 {@code effectsKnown} 均为 false（Core IR 尚不携带
   * lambda effect），故本方法目前是 no-op。保留签名是为了让日后元数据可用时，
   * 只需在构造点改传 true，而不必再动这里的判定逻辑。
   *
   * @param target 对应的 CallTarget
   * @param requiredEffects 函数声明的 effect 列表
   * @param effectsKnown effect 元数据是否真实可信。<b>空集合 + false 表示"未知"，
   *                     不是"已知无副作用"</b>——两者的区别正是 #53 的根因
   */
  public static void recordEffects(CallTarget target, Set<String> requiredEffects, boolean effectsKnown) {
    if (target == null || !effectsKnown) {
      return;
    }
    // 元数据可信且声明无 effect —— 未来接入点。当前无调用方走到这里。
    // 需要时在此登记纯度（建议挂到 RootNode 上，使生命周期随 CallTarget 回收，
    // 既不重蹈 #54 的全局表泄漏，也对 native-image 的 build-time 静态状态友好）。
  }

  /**
   * 判定目标函数是否为纯函数。
   *
   * @param target 需要检测的 CallTarget
   * @return true 仅当该 target 被证明为纯；未知一律 false（保守拒绝并行）
   */
  public static boolean isPure(CallTarget target) {
    // 目前没有可信的纯度来源，一律按不纯处理。见类注释的取舍说明。
    return false;
  }
}
