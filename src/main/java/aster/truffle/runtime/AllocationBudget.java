package aster.truffle.runtime;

/**
 * 单次执行的累计分配预算（防「小标量 → 巨内存」DoS）。
 *
 * <p><b>为什么需要它</b>：此前每个会造列表的 builtin 各自设上限
 * （{@code List.range} 1e6、{@code List.combinations} C(n,k)≤5000），
 * 单看都对，但**组合起来没有任何约束**：
 *
 * <pre>
 *   Let v0 be List.range(0, 1000000).
 *   Let v1 be List.concat(v0, v0).   -- 每行翻倍
 *   ... 重复 8 次 ...
 * </pre>
 *
 * 不到 200 字节源码即可把规模推到**约 1.3 亿元素**（再往上两引擎各自先撞到自身硬限：
 * 本引擎 OOM，TS 撞 V8 的 {@code Array.concat} 2^27 上限）。实测（同一份 Core IR 喂两引擎）：
 * 本引擎 1403ms 抛 {@code Java heap space}，TS 侧 128M 元素吃掉 2.8GB 堆
 * 且**返回 success**。
 *
 * <p><b>为什么既有三道防护都没拦住</b>：
 * <ul>
 *   <li>{@code MAX_STEPS}/{@code statementLimit} 只数解释器步进，
 *       {@code concat} 在它们眼里是 1 步，底层却是几千万次数组拷贝；</li>
 *   <li>宿主侧 5 秒 wall-clock 看门狗**来不及**——OOM 发生在 1.4 秒。
 *       wall-clock 只对「慢」攻击有效，对「快而肥」的攻击从不参与。</li>
 * </ul>
 *
 * <p><b>为什么是累计预算而不是给 concat 加上限</b>：逐个 builtin 打补丁是打地鼠
 * （{@code map}/{@code filter}/{@code groupBy} 同样能放大）。本类把
 * 「本次执行一共允许物化多少元素」收敛成**单一防线**，所有造集合的 builtin 共同扣减。
 *
 * <p>★<b>但记账点不止一处</b>：主入口是 {@link Builtins#call}，然而
 * {@code BuiltinCallNode} 的**内联快速路径**（Truffle {@code @Specialization}）
 * 直接返回结果、**不经过** {@code call}，也必须各自记账。规模可预知的特化在结果物化前
 * 调准入计量，其余路径调 {@link Builtins#chargeForResult}。新增会物化集合/字符串的
 * 内联特化时**必须一并补记账**
 * ——「进了 call 就自动计量」这个直觉是错的，本机制第一次漏掉 {@code Text.concat}
 * 正是栽在这里。
 *
 * <p><b>确定性铁律</b>：上限是**固定常量**，不跟随堆大小/GC 状态/机器内存。
 * 跟随运行时状态会让同一段规则在不同机器给出不同结果（甚至一次成功一次失败），
 * 与「两引擎逐字节一致 + 可回放」这条第一约束直接冲突。理由同
 * {@code VALUE_EQUALS_MAX_DEPTH} 当初拒绝「跟随 JVM 栈深」。
 *
 * <p><b>线程模型</b>：ThreadLocal，与 {@code AsterContext.effectPermissions} 同构。
 * 额度按**最外层 guest 帧**重置（{@link #enterFrame}/{@link #exitFrame}），
 * 而非按 eval 边界——因为有参入口的 {@code ctx.eval} 只取出 lambda，真正计算走
 * {@code LambdaRootNode}，不再经过 {@code AsterRootNode}。两个根节点都接了这套
 * 深度计数：无参入口最外层在 AsterRootNode，有参入口最外层在 LambdaRootNode。
 * 不重置就是跨执行污染——GraalVM 单线程策略拒并发但**允许串行交接**，
 * 池线程跨执行存活，残留额度会让完全合法的规则随机失败。
 */
public final class AllocationBudget {

  /**
   * 单次执行允许物化的元素总数上限。
   *
   * <p>取值依据：合法策略的典型规模远低于此（语料库中最大的集合运算是
   * poker best-5-of-7 的 21 组合 × 5 元素）。1e7 给正当的大批量运算留足余量，
   * 同时把单次执行的列表内存占用限制在**百 MB 量级**——实测 32M 元素约 398MB，
   * 故 1e7 元素在最坏情况下约 120MB，远低于触发 OOM 的水位。
   *
   * <p>与 {@code aster-lang-ts} interpreter 的 {@code MAX_ALLOCATION_BUDGET}
   * **必须同值**，否则同一段规则在两引擎一个通过一个拒绝 = parity 分叉。
   */
  public static final long MAX_ALLOCATION_BUDGET = 10_000_000L;

  private static final ThreadLocal<long[]> SPENT = ThreadLocal.withInitial(() -> new long[1]);

  private AllocationBudget() {}

  /**
   * 记账并在超限时抛错。
   *
   * @param count 本次物化的元素个数
   * @throws Builtins.BuiltinException 累计超过 {@link #MAX_ALLOCATION_BUDGET}
   */
  public static void charge(long count) throws Builtins.BuiltinException {
    if (count <= 0) return;
    long[] cell = SPENT.get();
    // 先判溢出再累加：count 本身可能已是天文数字（如 range 的长度计算）。
    if (count > MAX_ALLOCATION_BUDGET || cell[0] > MAX_ALLOCATION_BUDGET - count) {
      cell[0] = MAX_ALLOCATION_BUDGET + 1; // 标记为已耗尽，避免后续调用又“有余额”
      throw new Builtins.BuiltinException(
          "分配预算耗尽：单次执行累计物化元素数超过上限 " + MAX_ALLOCATION_BUDGET
              + "，拒绝以防内存耗尽 DoS");
    }
    cell[0] += count;
  }

  /** 当前已消耗额度（测试/诊断用）。 */
  public static long spent() {
    return SPENT.get()[0];
  }

  /** 清零（仅由 {@link #enterFrame}/{@link #exitFrame} 在最外层帧调用）。 */
  private static void reset() {
    SPENT.get()[0] = 0L;
  }

  /**
   * 当前线程的 guest 执行嵌套深度。
   *
   * <p>★**为什么需要它**：有参入口的 {@code ctx.eval} 只是**取出** lambda
   * （{@code AsterRootNode} 跑完即返回 LambdaValue），真正的计算发生在宿主随后的
   * {@code Value.execute(args)}，那条路径走 {@code LambdaRootNode} 而**不再经过**
   * {@code AsterRootNode}。若只在 AsterRootNode 重置，有参入口的预算就永远停在
   * eval 时的残值——实测正是如此（budgetDoesNotLeakAcrossSequentialEvals 首版失败）。
   *
   * <p>但 {@code LambdaRootNode} 同时也承担**每一次内部 lambda 调用**
   * （{@code List.map} 的回调等），在那里无条件重置会把预算清零、守护彻底失效。
   * 故用深度计数区分「最外层 guest 帧」与「内部调用」：只有 0→1 那一次才重置。
   */
  private static final ThreadLocal<int[]> DEPTH = ThreadLocal.withInitial(() -> new int[2]);

  /** {@link #DEPTH} 的两个槽位：全部 guest 根帧，以及其中的调用帧（函数 / lambda 体）。 */
  private static final int FRAMES = 0;
  private static final int CALLS = 1;

  /**
   * guest 调用深度上限：同一线程上同时处在执行中的调用帧（函数体、lambda 体，含入口本身）最多这么多层。
   *
   * <p>与 aster-lang-ts 包格式 CLI 的 {@code CLI_MAX_CALL_DEPTH}（ADR 0059 修订 1 A4）<b>必须同值</b>，计数口径也相同
   * （ts {@code invokeFunc} / {@code applyClosure}）：入口本身算第 1 层，每进入一次函数体或 lambda 体（含经
   * {@code List.map} 等 builtin 回调的）加 1；程序外壳 {@code AsterRootNode}、workflow / start 帧不计入。
   * 此前 Truffle 没有确定的上限，结果取决于线程栈大小与帧布局（jar 缺省栈约 1 000 层、原生缺省栈约 460 层、
   * Node 恒为 10 000），同一策略在不同发行上可能一个成功一个失败。确定性铁律同 {@link #MAX_ALLOCATION_BUDGET}：
   * 固定常量，不跟随栈大小。真实栈仍可能在上限之前耗尽（单帧很大的函数），那仍是栈溢出、同样报资源耗尽；
   * CLI 的求值线程为此显式给足栈（{@code SandboxedExecutor#EVAL_THREAD_STACK_BYTES}）。
   */
  public static final int MAX_CALL_DEPTH = 10_000;

  /**
   * 进入一个不计调用深度的 guest 根帧（程序外壳、workflow / start 步骤）；若是最外层（深度 0→1）则清零预算。
   *
   * @return 是否为最外层帧——调用方须把该值传给 {@link #exitFrame(boolean)}
   */
  public static boolean enterFrame() {
    return enter(DEPTH.get());
  }

  /**
   * 进入一个调用帧（函数体 / lambda 体），计入 {@link #MAX_CALL_DEPTH}。已达上限时抛 {@link CallDepthExceededError}，
   * 且两个计数都不变（调用方的 try 尚未进入，不会配对 exit）。
   *
   * @return 是否为最外层帧——调用方须把该值传给 {@link #exitCallFrame(boolean)}
   */
  public static boolean enterCallFrame() {
    int[] d = DEPTH.get();
    if (d[CALLS] >= MAX_CALL_DEPTH) {
      throw new CallDepthExceededError(MAX_CALL_DEPTH);
    }
    d[CALLS]++;
    return enter(d);
  }

  private static boolean enter(int[] d) {
    boolean outermost = d[FRAMES] == 0;
    d[FRAMES]++;
    if (outermost) {
      reset();
    }
    return outermost;
  }

  /**
   * 当前线程是否正处在某个 guest 根帧之内（深度 &gt; 0）。
   *
   * <p>供宿主调用边界区分「宿主发起的最外层调用」与「guest 执行中经宿主回调再入」：
   * 后者不得用被调函数自身的声明放宽调用方已有的 effect 权限（见 {@code LambdaValue#execute}）。
   */
  public static boolean isInsideGuestFrame() {
    return DEPTH.get()[FRAMES] > 0;
  }

  /** 退出 {@link #enterCallFrame} 进入的调用帧。 */
  public static void exitCallFrame(boolean outermost) {
    int[] d = DEPTH.get();
    if (d[CALLS] > 0) {
      d[CALLS]--;
    }
    exit(d, outermost);
  }

  /** 退出 {@link #enterFrame} 进入的 guest 根帧；最外层退出时清理 ThreadLocal，避免池线程长期持有。 */
  public static void exitFrame(boolean outermost) {
    exit(DEPTH.get(), outermost);
  }

  private static void exit(int[] d, boolean outermost) {
    if (d[FRAMES] > 0) {
      d[FRAMES]--;
    }
    if (outermost) {
      // 最外层退出即本次宿主调用结束：清零额度与两个深度，池线程复用时从干净状态开始。
      reset();
      d[FRAMES] = 0;
      d[CALLS] = 0;
    }
  }
}
