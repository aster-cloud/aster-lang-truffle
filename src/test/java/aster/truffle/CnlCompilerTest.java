package aster.truffle;

import aster.core.lexicon.EnUsLexicon;
import aster.core.lexicon.ZhCnLexicon;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CnlCompiler 多语言测试
 * <p>
 * 验证 CNL 编译器对不同语言输入的处理能力。
 */
class CnlCompilerTest {

    // ============================================================
    // 语言检测测试
    // ============================================================

    @Test
    void testDetectChineseCnl_ModuleDecl() {
        // 新语法：使用"模块 "关键词（带尾部空格）
        String source = "模块 测试模块。";
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(ZhCnLexicon.ID, lexicon.getId());
    }

    @Test
    void testDetectChineseCnl_DefineKeyword() {
        // 新语法：使用"定义 "关键词
        String source = "定义 User 包含 name。";
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(ZhCnLexicon.ID, lexicon.getId());
    }

    @Test
    void testDetectChineseCnl_RuleKeyword() {
        // 新语法：使用"规则 "关键词
        String source = "规则 main:";
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(ZhCnLexicon.ID, lexicon.getId());
    }

    @Test
    void testDetectChineseCnl_Keywords() {
        // 仅含中文关键字但无结构化标记（【...】），应回退到英文
        // 用户需要显式指定 langId="zh-CN" 以启用中文模式
        // 这样设计是为了避免英文源码中的中文标识符被误判
        String source = "若 条件 返回 真。";
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(EnUsLexicon.ID, lexicon.getId(),
                "无结构化标记时应回退到英文，需显式指定 langId");
    }

    @Test
    void testDetectChineseCnl_ChineseIdentifiersInEnglish() {
        // 英文模块使用中文标识符不应触发中文检测
        String source = "let 若干 = 10 and 返回值 = 20.";
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(EnUsLexicon.ID, lexicon.getId(),
                "中文标识符不应触发中文模式检测");
    }

    @Test
    void testDetectEnglishCnl_Default() {
        String source = "Module test.";
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(EnUsLexicon.ID, lexicon.getId());
    }

    @Test
    void testResolveLexicon_ExplicitZhCN() {
        var lexicon = CnlCompiler.resolveLexicon("zh-CN", "any source");
        assertEquals(ZhCnLexicon.ID, lexicon.getId());
    }

    @Test
    void testResolveLexicon_ExplicitEnUS() {
        var lexicon = CnlCompiler.resolveLexicon("en-US", "any source");
        assertEquals(EnUsLexicon.ID, lexicon.getId());
    }

    @Test
    void testResolveLexicon_ChineseAliases() {
        assertEquals(ZhCnLexicon.ID, CnlCompiler.resolveLexicon("zh_cn", null).getId());
        assertEquals(ZhCnLexicon.ID, CnlCompiler.resolveLexicon("chinese", null).getId());
        assertEquals(ZhCnLexicon.ID, CnlCompiler.resolveLexicon("中文", null).getId());
    }

    @Test
    void testResolveLexicon_EnglishAliases() {
        assertEquals(EnUsLexicon.ID, CnlCompiler.resolveLexicon("en_us", null).getId());
        assertEquals(EnUsLexicon.ID, CnlCompiler.resolveLexicon("english", null).getId());
        assertEquals(EnUsLexicon.ID, CnlCompiler.resolveLexicon("英文", null).getId());
    }

    // ============================================================
    // JSON 检测测试
    // ============================================================

    @Test
    void testIsJsonInput_ValidJson() {
        assertTrue(CnlCompiler.isJsonInput("{\"name\":\"test\"}"));
        assertTrue(CnlCompiler.isJsonInput("  { \"key\": \"value\" }"));
    }

    @Test
    void testIsJsonInput_CnlSource() {
        assertFalse(CnlCompiler.isJsonInput("Module test."));
        assertFalse(CnlCompiler.isJsonInput("模块 测试。"));
    }

    @Test
    void testIsJsonInput_EmptyOrBlank() {
        assertFalse(CnlCompiler.isJsonInput(null));
        assertFalse(CnlCompiler.isJsonInput(""));
        assertFalse(CnlCompiler.isJsonInput("   "));
    }

    // ============================================================
    // 编译测试（英文 CNL）
    // ============================================================

    @Test
    void testCompile_SimpleEnglishModule() throws CnlCompiler.CompilationException {
        String source = """
            Module test.simple.

            Rule main:
              Return 42.
            """;

        String json = CnlCompiler.compile(source, "en-US");

        assertNotNull(json);
        assertTrue(json.contains("\"name\""));
        assertTrue(json.contains("test.simple"));
    }

    @Test
    void testCompile_EnglishWithFunction() throws CnlCompiler.CompilationException {
        String source = """
            Module test.func.

            Rule add given x: Int and y: Int:
              Return x + y.

            Rule main:
              Return add(1, 2).
            """;

        String json = CnlCompiler.compile(source, "en-US");

        assertNotNull(json);
        assertTrue(json.contains("add"));
        assertTrue(json.contains("main"));
    }

    // ============================================================
    // 编译测试（中文 CNL）
    // 注意：当前 ANTLR 词法器只支持 ASCII 标识符，中文标识符暂不支持
    // 中文 CNL 仅翻译关键字，标识符仍需使用 ASCII
    // ============================================================

    @Test
    void testCompile_SimpleChineseModule() throws CnlCompiler.CompilationException {
        String source = """
            模块 test.simple.

            规则 main:
              返回 42.
            """;

        String json = CnlCompiler.compile(source, "zh-CN");

        assertNotNull(json);
        assertTrue(json.contains("\"name\""));
    }

    @Test
    void testCompile_ChineseWithCondition() throws CnlCompiler.CompilationException {
        String source = """
            模块 test.condition.

            规则 check 给定 value: Int:
              如果 value > 0:
                返回 真.
              否则:
                返回 假.
            """;

        String json = CnlCompiler.compile(source, "zh-CN");

        assertNotNull(json);
        assertTrue(json.contains("\"kind\""));
    }

    // ============================================================
    // 错误处理测试
    // ============================================================

    @Test
    void testCompile_InvalidSyntax() {
        String source = "这不是有效的 CNL 语法 随便写的内容";

        assertThrows(CnlCompiler.CompilationException.class, () -> {
            CnlCompiler.compile(source, "zh-CN");
        });
    }

    // ============================================================
    // 自动语言检测编译测试
    // ============================================================

    @Test
    void testCompile_AutoDetectChinese() throws CnlCompiler.CompilationException {
        String source = """
            模块 auto.detect.

            规则 main:
              返回 100.
            """;

        // langId 为 null，应自动检测为中文
        String json = CnlCompiler.compile(source, null);

        assertNotNull(json);
    }

    @Test
    void testCompile_AutoDetectEnglish() throws CnlCompiler.CompilationException {
        String source = """
            Module auto.detect.

            Rule main:
              Return 200.
            """;

        // langId 为 null，应自动检测为英文
        String json = CnlCompiler.compile(source, null);

        assertNotNull(json);
    }

    // ============================================================
    // Locale ID 兼容性测试
    // ============================================================

    @Test
    void testResolveLexicon_CaseInsensitive() {
        // 大小写不敏感
        assertEquals(ZhCnLexicon.ID, CnlCompiler.resolveLexicon("ZH-CN", null).getId());
        assertEquals(ZhCnLexicon.ID, CnlCompiler.resolveLexicon("Zh-Cn", null).getId());
        assertEquals(EnUsLexicon.ID, CnlCompiler.resolveLexicon("EN-US", null).getId());
        assertEquals(EnUsLexicon.ID, CnlCompiler.resolveLexicon("En-Us", null).getId());
    }

    @Test
    void testResolveLexicon_UnderscoreHyphenInterop() {
        // 下划线/连字符互通
        assertEquals(ZhCnLexicon.ID, CnlCompiler.resolveLexicon("ZH_CN", null).getId());
        assertEquals(EnUsLexicon.ID, CnlCompiler.resolveLexicon("EN_US", null).getId());
    }

    @Test
    void testResolveLexicon_CombinedCompatibility() {
        // 组合兼容：大小写 + 下划线
        assertEquals(ZhCnLexicon.ID, CnlCompiler.resolveLexicon("ZH_cn", null).getId());
        assertEquals(ZhCnLexicon.ID, CnlCompiler.resolveLexicon("zh_CN", null).getId());
        assertEquals(EnUsLexicon.ID, CnlCompiler.resolveLexicon("EN_us", null).getId());
        assertEquals(EnUsLexicon.ID, CnlCompiler.resolveLexicon("en_US", null).getId());
    }

    @Test
    void testResolveLexicon_UnknownFallsBackToEnglish() {
        // 未知 locale 回退到英文
        assertEquals(EnUsLexicon.ID, CnlCompiler.resolveLexicon("xx-XX", null).getId());
        assertEquals(EnUsLexicon.ID, CnlCompiler.resolveLexicon("unknown", null).getId());
    }

    // ============================================================
    // 边界情况测试（注释和字符串剥离）
    // ============================================================

    @Test
    void testDetectChineseCnl_BlockCommentInLineComment() {
        // 行注释中包含 /* 不应导致误删代码
        String source = """
            // TODO: /* fix this later */
            模块 test.module.
            """;
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(ZhCnLexicon.ID, lexicon.getId(),
                "行注释中的 /* 不应干扰语言检测");
    }

    @Test
    void testDetectChineseCnl_KeywordInSingleQuote() {
        // 单引号字符串中的中文关键词不应触发中文检测
        String source = "let marker = '模块 ' and x = 10.";
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(EnUsLexicon.ID, lexicon.getId(),
                "单引号内的关键词不应触发中文模式");
    }

    @Test
    void testDetectChineseCnl_KeywordInDoubleQuote() {
        // 双引号字符串中的中文关键词不应触发中文检测
        String source = "let marker = \"模块 \" and x = 10.";
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(EnUsLexicon.ID, lexicon.getId(),
                "双引号内的关键词不应触发中文模式");
    }

    @Test
    void testDetectChineseCnl_KeywordInBlockComment() {
        // 块注释中的中文关键词不应触发中文检测
        String source = """
            /* 这是块注释 模块 测试 */
            Module test.
            """;
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(EnUsLexicon.ID, lexicon.getId(),
                "块注释内的关键词不应触发中文模式");
    }

    @Test
    void testDetectChineseCnl_KeywordInLineComment() {
        // 行注释中的中文关键词不应触发中文检测
        String source = """
            // 模块 这是行注释
            Module test.
            """;
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(EnUsLexicon.ID, lexicon.getId(),
                "行注释内的关键词不应触发中文模式");
    }

    @Test
    void testResolveLexicon_UnknownLangIdWithChineseSource() {
        // 未知 langId 但源码包含中文关键词，应自动回退到中文
        String source = "模块 test.module.";
        var lexicon = CnlCompiler.resolveLexicon("xx-YY", source);
        assertEquals(ZhCnLexicon.ID, lexicon.getId(),
                "未知 langId 但源码含中文关键词时应回退到中文");
    }

    @Test
    void testResolveLexicon_UnknownLangIdWithEnglishSource() {
        // 未知 langId 且源码为英文，应使用英文
        String source = "Module test.";
        var lexicon = CnlCompiler.resolveLexicon("xx-YY", source);
        assertEquals(EnUsLexicon.ID, lexicon.getId(),
                "未知 langId 且源码为英文时应使用英文");
    }

    @Test
    void testDetectChineseCnl_BlockCommentWithLineCommentInside() {
        // 块注释中包含 // 不应破坏块注释的闭合
        String source = """
            /* 模块 // TODO: investigate */
            Module english.
            """;
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(EnUsLexicon.ID, lexicon.getId(),
                "块注释中的 // 不应破坏块注释闭合，关键词应被正确剥离");
    }

    @Test
    void testDetectChineseCnl_BlockCommentWithHashInside() {
        // 块注释中包含 # 不应破坏块注释的闭合
        String source = """
            /* 模块 # TODO: fix */
            Module english.
            """;
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(EnUsLexicon.ID, lexicon.getId(),
                "块注释中的 # 不应破坏块注释闭合");
    }

    @Test
    void testDetectChineseCnl_LineCommentWithBlockCommentMarker() {
        // 行注释中包含 /* 不应触发块注释
        String source = """
            // 这是 /* 行注释
            模块 test.module.
            """;
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(ZhCnLexicon.ID, lexicon.getId(),
                "行注释中的 /* 不应触发块注释");
    }

    @Test
    void testDetectChineseCnl_MultilineBlockComment() {
        // 多行块注释应被完整剥离
        String source = """
            /*
             * 模块
             * 定义
             */
            Module english.
            """;
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(EnUsLexicon.ID, lexicon.getId(),
                "多行块注释应被完整剥离");
    }

    @Test
    void testDetectChineseCnl_HashCommentWithBlockCommentMarker() {
        // # 行注释中包含 /* 不应触发块注释
        String source = """
            # 这是 /* 行注释
            模块 test.module.
            """;
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(ZhCnLexicon.ID, lexicon.getId(),
                "# 行注释中的 /* 不应触发块注释");
    }

    @Test
    void testDetectChineseCnl_CROnlyLineEnding() {
        // CR-only 行结尾（\r）应被正确处理
        String source = "// comment\r模块 test.";
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(ZhCnLexicon.ID, lexicon.getId(),
                "CR-only 行结尾应被正确处理");
    }

    @Test
    void testDetectChineseCnl_CRLFLineEnding() {
        // CRLF 行结尾（\r\n）应被正确处理
        String source = "// comment\r\n模块 test.";
        var lexicon = CnlCompiler.resolveLexicon(null, source);
        assertEquals(ZhCnLexicon.ID, lexicon.getId(),
                "CRLF 行结尾应被正确处理");
    }
}
