package aster.truffle;

import aster.core.ast.Module;
import aster.core.canonicalizer.Canonicalizer;
import aster.core.ir.CoreModel;
import aster.core.lexicon.EnUsLexicon;
import aster.core.lexicon.Lexicon;
import aster.core.lexicon.LexiconRegistry;
import aster.core.lexicon.ZhCnLexicon;
import aster.core.lowering.CoreLowering;
import aster.core.parser.AstBuilder;
import aster.core.parser.AsterCustomLexer;
import aster.core.parser.AsterParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * CNL 动态编译器
 * <p>
 * 在 Truffle 运行时将 CNL 源码编译为 Core IR JSON。
 * <p>
 * 编译管道：
 * <pre>
 * CNL 源码 → Canonicalizer（规范化） → ANTLR Lexer → ANTLR Parser
 *          → AstBuilder（构建 AST） → CoreLowering（降级）→ Core IR JSON
 * </pre>
 * <p>
 * <b>多语言支持</b>：
 * <ul>
 *   <li>通过 {@code langId} 参数选择词法表（en-US, zh-CN 等）</li>
 *   <li>Canonicalizer 使用 Lexicon 处理多语言关键字规范化</li>
 *   <li>支持自动检测（基于源码内容）</li>
 *   <li>Unicode 标识符（如中文变量名）通过 {@code Character.isLetter()} 支持</li>
 * </ul>
 */
public final class CnlCompiler {

    private static final Logger LOGGER = Logger.getLogger(CnlCompiler.class.getName());

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private CnlCompiler() {
        // 工具类，禁止实例化
    }

    /**
     * 将 CNL 源码编译为 Core IR JSON
     *
     * @param source CNL 源码
     * @param langId 语言标识（en-US, zh-CN 等），为 null 时自动检测
     * @return Core IR JSON 字符串
     * @throws CompilationException 编译失败时抛出
     */
    public static String compile(String source, String langId) throws CompilationException {
        Lexicon lexicon = resolveLexicon(langId, source);
        return compileWithLexicon(source, lexicon);
    }

    /**
     * 使用指定词法表编译 CNL 源码
     *
     * @param source  CNL 源码
     * @param lexicon 词法表
     * @return Core IR JSON 字符串
     * @throws CompilationException 编译失败时抛出
     */
    public static String compileWithLexicon(String source, Lexicon lexicon) throws CompilationException {
        try {
            // 1. 规范化（使用 Lexicon 处理多语言关键字）
            Canonicalizer canonicalizer = new Canonicalizer(lexicon);
            String canonicalized = canonicalizer.canonicalize(source);

            // 2. ANTLR 词法分析
            CharStream charStream = CharStreams.fromString(canonicalized);
            AsterCustomLexer antlrLexer = new AsterCustomLexer(charStream);
            CommonTokenStream tokens = new CommonTokenStream(antlrLexer);

            // 3. ANTLR 语法分析
            AsterParser parser = new AsterParser(tokens);

            // 收集解析错误
            StringBuilder parseErrors = new StringBuilder();
            parser.removeErrorListeners();
            parser.addErrorListener(new org.antlr.v4.runtime.BaseErrorListener() {
                @Override
                public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                                        Object offendingSymbol,
                                        int line, int charPositionInLine,
                                        String msg,
                                        org.antlr.v4.runtime.RecognitionException e) {
                    parseErrors.append(String.format("语法错误 (行 %d:%d): %s\n", line, charPositionInLine, msg));
                }
            });

            AsterParser.ModuleContext moduleCtx = parser.module();

            // 检查解析错误
            if (parseErrors.length() > 0) {
                throw new CompilationException("CNL 语法解析失败:\n" + parseErrors);
            }

            // 4. 构建 AST
            AstBuilder builder = new AstBuilder();
            Module ast = builder.visitModule(moduleCtx);

            // 5. 降级到 Core IR
            CoreLowering lowering = new CoreLowering();
            CoreModel.Module coreModule = lowering.lowerModule(ast);

            // 6. 序列化为 JSON
            return MAPPER.writeValueAsString(coreModule);

        } catch (CompilationException e) {
            throw e;
        } catch (IOException e) {
            throw new CompilationException("JSON 序列化失败: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new CompilationException("CNL 编译失败: " + e.getMessage(), e);
        }
    }

    /**
     * 判断输入是否为 Core IR JSON（而非 CNL 源码）
     * <p>
     * 简单启发式：以 '{' 开头视为 JSON
     *
     * @param input 输入字符串
     * @return 如果是 JSON 返回 true
     */
    public static boolean isJsonInput(String input) {
        if (input == null || input.isBlank()) {
            return false;
        }
        String trimmed = input.stripLeading();
        return trimmed.startsWith("{");
    }

    /**
     * 解析语言标识为词法表
     * <p>
     * 如果 langId 为 null，尝试自动检测源码语言。
     * 如果 langId 未知且回退到英文，会重新检测源码以避免误判。
     *
     * @param langId 语言标识
     * @param source CNL 源码（用于自动检测）
     * @return 词法表
     */
    public static Lexicon resolveLexicon(String langId, String source) {
        // 如果指定了语言，尝试解析
        if (langId != null && !langId.isBlank()) {
            String normalizedId = normalizeLocaleId(langId);
            Lexicon resolved = resolveLexiconById(langId);

            // 如果解析结果是英文，但 langId 不是英文相关的 ID，
            // 则可能是未知 langId 回退到英文，需要重新检测源码
            if (resolved == EnUsLexicon.INSTANCE
                && !isEnglishLangId(normalizedId)
                && source != null
                && detectChineseCnl(source)) {
                LOGGER.log(Level.INFO,
                        "语言标识 ''{0}'' 未知，但源码检测为中文，使用中文词法表",
                        langId);
                return ZhCnLexicon.INSTANCE;
            }

            return resolved;
        }

        // 自动检测：检查源码中是否包含中文关键字
        if (source != null && detectChineseCnl(source)) {
            return ZhCnLexicon.INSTANCE;
        }

        // 默认英文
        return EnUsLexicon.INSTANCE;
    }

    /**
     * 检查归一化后的 langId 是否为英文相关 ID
     * <p>
     * 支持的变体：
     * <ul>
     *   <li>BCP47 格式：en, en-us, en-gb, en-au 等</li>
     *   <li>名称：english</li>
     *   <li>中文名称：英文</li>
     * </ul>
     */
    private static boolean isEnglishLangId(String normalizedId) {
        // 支持所有 en 前缀的 BCP47 语言标签
        if (normalizedId.equals("en") || normalizedId.startsWith("en-")) {
            return true;
        }
        return normalizedId.equals("english")
            || normalizedId.equals("\u82f1\u6587");  // 英文
    }

    /**
     * 根据语言标识获取词法表
     * <p>
     * 支持大小写不敏感查找，会尝试多种格式变体。
     * <p>
     * <b>兼容规则</b>：
     * <ul>
     *   <li>大小写不敏感：zh-CN = zh-cn = ZH-CN</li>
     *   <li>下划线/连字符互通：zh_CN = zh-CN</li>
     *   <li>组合兼容：FOO_BAR = foo-bar = foo_bar</li>
     * </ul>
     *
     * @param langId 语言标识
     * @return 词法表
     */
    private static Lexicon resolveLexiconById(String langId) {
        // 归一化 ID：小写 + 下划线转连字符
        String normalizedId = normalizeLocaleId(langId);

        // 首先尝试直接匹配常用标识
        Lexicon directMatch = switch (normalizedId) {
            // 中文 BCP47 变体（简体中文优先）
            case "zh", "zh-cn", "zh-hans", "zh-sg", "chinese" -> ZhCnLexicon.INSTANCE;
            // 繁体中文变体（暂时也使用简体词法表，将来可扩展）
            case "zh-tw", "zh-hk", "zh-hant" -> ZhCnLexicon.INSTANCE;
            // 英文 BCP47 变体
            case "en", "en-us", "en-gb", "en-au", "en-ca", "english" -> EnUsLexicon.INSTANCE;
            // 中文别名（已归一化为小写）
            case "\u4e2d\u6587" -> ZhCnLexicon.INSTANCE;  // 中文
            case "\u82f1\u6587" -> EnUsLexicon.INSTANCE;  // 英文
            default -> null;
        };

        if (directMatch != null) {
            return directMatch;
        }

        // 使用 Locale.forLanguageTag 解析复杂 BCP47 标签（如 zh-Hans-CN, zh-Hant-TW）
        // 注意：forLanguageTag 需要连字符格式，使用已归一化的 normalizedId
        java.util.Locale locale = java.util.Locale.forLanguageTag(normalizedId);
        String language = locale.getLanguage().toLowerCase(java.util.Locale.ROOT);

        // 基于语言和脚本判断
        if ("zh".equals(language)) {
            // 中文：简体脚本 (hans) 或繁体脚本 (hant)
            // 暂时都使用简体词法表
            return ZhCnLexicon.INSTANCE;
        }
        if ("en".equals(language)) {
            return EnUsLexicon.INSTANCE;
        }

        // 尝试从注册表查找
        LexiconRegistry registry = LexiconRegistry.getInstance();

        // 尝试归一化后的 ID
        if (registry.has(normalizedId)) {
            return registry.getOrThrow(normalizedId);
        }

        // 尝试对注册表中的 ID 进行归一化匹配
        for (String registeredId : registry.list()) {
            if (normalizeLocaleId(registeredId).equals(normalizedId)) {
                return registry.getOrThrow(registeredId);
            }
        }

        // 未知语言 ID，记录警告并回退到英文
        LOGGER.log(Level.WARNING,
                "未知的语言标识 ''{0}''，回退到英文词法表 (en-US)。" +
                "可用的语言标识: {1}",
                new Object[]{langId, registry.list()});
        return EnUsLexicon.INSTANCE;
    }

    /**
     * 归一化 locale ID
     * <p>
     * 转换为小写，并将下划线替换为连字符
     *
     * @param id 原始 ID
     * @return 归一化后的 ID
     */
    private static String normalizeLocaleId(String id) {
        return id.trim().toLowerCase(java.util.Locale.ROOT).replace('_', '-');
    }

    /**
     * 检测源码是否为中文 CNL
     * <p>
     * 启发式规则：仅检查中文 CNL 独有的结构化标记（如 【模块】、【定义】）。
     * <p>
     * <b>设计考量</b>：
     * <ul>
     *   <li>不检查常见中文关键字（如 若、返回），因为它们可能出现在中文变量名中</li>
     *   <li>结构化标记（【...】）是中文 CNL 的唯一特征，不会被用作标识符</li>
     *   <li>如果用户需要使用中文 CNL 但没有结构化标记，应显式指定 langId="zh-CN"</li>
     * </ul>
     * <p>
     * 为避免字符串字面量或注释中的标记误触发，
     * 先剥离引号内的内容和注释再检测。
     *
     * @param source 源码
     * @return 如果检测到中文 CNL 返回 true
     */
    private static boolean detectChineseCnl(String source) {
        // 剥离字符串字面量和所有注释，避免误触发
        // 使用状态机方式同时处理行注释和块注释，正确处理互相嵌套的情况
        String stripped = stripStringLiterals(source);
        stripped = stripAllComments(stripped);

        // 仅检查中文模块声明标记（这些标记非常独特，是中文 CNL 的唯一特征）
        // 不检查常见关键字（如 若、返回），避免中文标识符被误判为中文 CNL
        return stripped.contains("【模块】") || stripped.contains("【定义】") ||
               stripped.contains("【流程】") || stripped.contains("【步骤】") ||
               stripped.contains("【类型】") || stripped.contains("【输入】") ||
               stripped.contains("【输出】");
    }

    /**
     * 剥离所有注释（行注释和块注释）
     * <p>
     * 使用状态机方式同时处理行注释和块注释，正确处理嵌套情况：
     * <ul>
     *   <li>块注释内的 {@code //} 被视为块注释的一部分，不触发行注释</li>
     *   <li>行注释内的 {@code /*} 被视为行注释的一部分，不触发块注释</li>
     * </ul>
     * <p>
     * <b>注意</b>：此方法假设字符串已被 {@link #stripStringLiterals} 剥离，
     * 因此不需要跟踪字符串上下文。
     *
     * @param source 源码（已剥离字符串字面量）
     * @return 剥离所有注释后的源码
     */
    private static String stripAllComments(String source) {
        StringBuilder result = new StringBuilder();
        int len = source.length();
        int i = 0;

        while (i < len) {
            // 检查块注释开始 /*
            if (i + 1 < len && source.charAt(i) == '/' && source.charAt(i + 1) == '*') {
                // 跳过块注释内容，直到找到 */
                i += 2;
                while (i + 1 < len) {
                    if (source.charAt(i) == '*' && source.charAt(i + 1) == '/') {
                        i += 2;
                        break;
                    }
                    i++;
                }
                // 如果没找到 */，i 会停在 len
                if (i >= len) break;
                continue;
            }

            // 检查行注释开始 // 或 #
            if ((i + 1 < len && source.charAt(i) == '/' && source.charAt(i + 1) == '/')
                || source.charAt(i) == '#') {
                // 跳过行注释内容，直到换行符（支持 \n、\r、\r\n）
                while (i < len) {
                    char c = source.charAt(i);
                    if (c == '\n' || c == '\r') {
                        break;
                    }
                    i++;
                }
                // 保留换行符，处理 \r\n 组合
                if (i < len) {
                    char c = source.charAt(i);
                    if (c == '\r') {
                        result.append('\n');  // 统一转换为 \n
                        i++;
                        // 跳过可能的 \n（\r\n 组合）
                        if (i < len && source.charAt(i) == '\n') {
                            i++;
                        }
                    } else {
                        result.append('\n');
                        i++;
                    }
                }
                continue;
            }

            // 普通字符，保留
            result.append(source.charAt(i));
            i++;
        }

        return result.toString();
    }

    /**
     * 剥离字符串字面量
     * <p>
     * 将各种引号内的内容替换为空，避免字符串内容干扰语言检测。
     * 支持：
     * - ASCII 双引号 "..."
     * - ASCII 单引号 '...'（字符常量或部分语言的字符串）
     * - 中文直角引号 「...」
     * - 智能双引号 "..."
     * - 智能单引号 '...'
     *
     * @param source 源码
     * @return 剥离字符串后的源码
     */
    private static String stripStringLiterals(String source) {
        // 使用 (?s) 启用 DOTALL 模式，让 . 匹配换行符，正确处理跨行字符串
        return source
                .replaceAll("(?s)\"(?:[^\"\\\\]|\\\\.)*\"", "")  // ASCII 双引号 "..."（支持跨行和转义）
                .replaceAll("(?s)'(?:[^'\\\\]|\\\\.)*'", "")     // ASCII 单引号 '...'（支持转义）
                .replaceAll("(?s)「[^」]*」", "")                 // 中文直角引号 「...」
                .replaceAll("(?s)\u201C[^\u201D]*\u201D", "")     // 智能双引号 "..."
                .replaceAll("(?s)\u2018[^\u2019]*\u2019", "");    // 智能单引号 '...'
    }

    /**
     * CNL 编译异常
     */
    public static class CompilationException extends Exception {
        public CompilationException(String message) {
            super(message);
        }

        public CompilationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
