package aster.truffle;

import aster.truffle.nodes.AsterRootNode;
import aster.truffle.runtime.AsterConfig;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.source.Source;

/**
 * Aster 语言 Truffle 实现
 * <p>
 * 支持两种输入模式：
 * <ul>
 *   <li><b>Core IR JSON</b>：预编译的中间表示，直接加载执行</li>
 *   <li><b>CNL 源码</b>：自然语言策略代码，动态编译后执行</li>
 * </ul>
 * <p>
 * <b>多语言 CNL 支持</b>：
 * <ul>
 *   <li>通过 MIME type 指定：{@code application/x-aster-cnl-zh} 表示中文</li>
 *   <li>通过文件扩展名：{@code .cnl.zh} 或 {@code .cnl.cn} 表示中文</li>
 *   <li>自动检测：根据源码内容识别语言</li>
 * </ul>
 */
@TruffleLanguage.Registration(id = "aster", name = "Aster", version = "1.0.2")
public final class AsterLanguage extends TruffleLanguage<AsterContext> {

  /** 中文 CNL MIME type */
  public static final String MIME_CNL_ZH = "application/x-aster-cnl-zh";
  /** 英文 CNL MIME type */
  public static final String MIME_CNL_EN = "application/x-aster-cnl-en";
  /** 通用 CNL MIME type（自动检测语言） */
  public static final String MIME_CNL = "application/x-aster-cnl";
  /** Core IR JSON MIME type */
  public static final String MIME_JSON = "application/json";

  /**
   * 缓存的 ContextReference —— GraalVM 推荐的当前上下文获取方式。
   * {@code create()} 对同一语言类保证返回同一引用，故作静态常量持有。
   */
  private static final ContextReference<AsterContext> CONTEXT_REF =
      ContextReference.create(AsterLanguage.class);

  /**
   * 获取当前线程的 AsterContext (通过 TruffleLanguage API)。
   * <p>传 {@code null} 节点：官方支持的用法（无 PE-constant 节点可用时）。调用方均为
   * Node，未来若需 PE 优化可改传 {@code this}，但规则引擎单次 eval 短，收益微小，
   * 故保持无参便捷形态。
   */
  public static AsterContext getContext() {
    return CONTEXT_REF.get(null);
  }

  @Override
  protected AsterContext createContext(Env env) {
    return new AsterContext(env);
  }

  /**
   * 声明多线程访问策略（issue #104 正文）。
   *
   * <p>★为什么必须显式覆写：workflow 的 step 体在 {@code AsyncTaskRegistry} 的
   * executor worker 线程上执行 Truffle 节点树（{@code Exec.exec(expr, materializedFrame)}），
   * 而 {@code TruffleLanguage} 的**默认**实现只允许单线程访问——除非语言显式声明支持。
   * 不覆写时，Truffle 是否放行取决于运行时的「单 context 快速路径」等实现细节，
   * 属于「侥幸可用」而非「契约保证」。
   *
   * <p>本语言确实需要多线程：workflow 并行 step、{@code List.map} 的并行化路径
   * （{@code ParallelListMapNode}）都会在非 eval 线程上跑 guest 代码。故显式返回 true，
   * 把「支持多线程」变成**声明的契约**——运行时据此走多线程安全路径，
   * 而不是依赖未定义行为。
   *
   * <p>★这不是「打开一个开关就安全了」：声明多线程后，共享 AST 的并发改写必须自行加锁。
   * 与本次改动配套的是 {@code BuiltinCallNode.getParallelListMapNode()} 的
   * double-checked + {@code getLock()}——那处此前是无锁 check-then-insert，
   * 两个 step 并发调 {@code List.map} 会双双 {@code insert()} 改写同一个 {@code @Child}。
   *
   * <p>范围声明：本方法只声明策略、消除「未定义行为」这一层。issue 正文提到的
   * 「worker 线程未经 polyglot API enter context」本身（{@code TruffleContext.enter()}）
   * 未在此处理——实测当前 {@code AsterLanguage.getContext()} 在 worker 线程上可正常返回，
   * 且四个并发 Context 各自跑 workflow 均成功，未复现该失败。
   */
  @Override
  protected boolean isThreadAccessAllowed(Thread thread, boolean singleThreaded) {
    return true;
  }

  /**
   * 释放 context 持有的原生资源。
   *
   * <p>{@link AsterContext#getAsyncRegistry()} 会懒建一个 {@code AsyncTaskRegistry}，
   * 其中的主执行器是**非守护**固定线程池（大小 = CPU 核数）。此前全仓没有任何地方调用
   * {@code shutdown()}，也没有 disposeContext 钩子——池化宿主按请求创建 context 时，
   * 每个 context 都会漏 N 个非守护线程，最终线程耗尽，且非守护线程会阻止 JVM 退出。
   *
   * <p>Truffle 保证本方法在 context 关闭时调用，是释放这类资源的规定位置。
   */
  @Override
  protected void disposeContext(AsterContext context) {
    context.shutdownAsyncRegistry();
  }

  @Override
  protected CallTarget parse(ParsingRequest request) throws Exception {
    Source source = request.getSource();
    String content = source.getCharacters().toString();

    // 确定输入类型和语言
    String jsonContent;
    if (CnlCompiler.isJsonInput(content)) {
      // Core IR JSON，直接使用
      jsonContent = content;
    } else {
      // CNL 源码，需要编译
      String langId = detectLanguage(source);
      jsonContent = CnlCompiler.compile(content, langId);
    }

    Loader loader = new Loader(this);
    String funcName = AsterConfig.DEFAULT_FUNCTION;

    Loader.Program program = loader.buildProgram(jsonContent, funcName, null);
    AsterRootNode rootNode = new AsterRootNode(this, program.root, program.env, program.params, program.effects);
    return rootNode.getCallTarget();
  }

  /**
   * 从 Source 元数据检测 CNL 语言
   * <p>
   * 优先级：MIME type → 文件扩展名 → 自动检测
   *
   * @param source Truffle Source
   * @return 语言标识（zh-CN, en-US 等），null 表示自动检测
   */
  private String detectLanguage(Source source) {
    // 1. 检查 MIME type
    String mimeType = source.getMimeType();
    if (mimeType != null) {
      if (mimeType.contains("-zh") || mimeType.contains("-cn")) {
        return "zh-CN";
      }
      if (mimeType.contains("-en")) {
        return "en-US";
      }
    }

    // 2. 检查文件扩展名
    String name = source.getName();
    if (name != null) {
      String lower = name.toLowerCase();
      if (lower.endsWith(".cnl.zh") || lower.endsWith(".cnl.cn") ||
          lower.endsWith(".zh.cnl") || lower.endsWith(".cn.cnl")) {
        return "zh-CN";
      }
      if (lower.endsWith(".cnl.en") || lower.endsWith(".en.cnl")) {
        return "en-US";
      }
    }

    // 3. 返回 null，让 CnlCompiler 自动检测
    return null;
  }
}
