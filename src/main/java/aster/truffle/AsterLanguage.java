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
@TruffleLanguage.Registration(id = "aster", name = "Aster", version = "0.1")
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
   * 获取当前线程的 AsterContext (通过 TruffleLanguage API)
   */
  public static AsterContext getContext() {
    return getCurrentContext(AsterLanguage.class);
  }

  @Override
  protected AsterContext createContext(Env env) {
    return new AsterContext(env);
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
