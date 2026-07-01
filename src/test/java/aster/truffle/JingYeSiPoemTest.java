package aster.truffle;

import aster.core.canonicalizer.Canonicalizer;
import aster.core.identifier.DomainVocabulary;
import aster.core.identifier.IdentifierIndex;
import aster.core.ir.CoreModel;
import aster.core.lexicon.DynamicLexicon;
import aster.core.lexicon.Lexicon;
import aster.core.lowering.CoreLowering;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 《静夜思》— 李白：把整首诗按**原词序**当作 Aster Lang 源码。诗句领字做关键词别名
 * （ADR 0022），末句「思故乡」用**字面量宏**（IdentifierKind.LITERAL）展开成 "静夜思"。
 * 本测试证明生产 Java/Truffle 引擎（/evaluate-source 同款路径）编译执行这首诗，
 * 运行输出诗名「静夜思」——与 aster-lang-ts 引擎逐字一致（双引擎）。
 *
 * <p>别名：床前→Module / 疑是→Rule / 举头→produce / 低头→Return。
 * <p>字面量宏：思故乡 → 内容「静夜思」（canonicalize 展开成字符串字面量）。
 * <p>原诗即源码：床前 明月光。疑是 地上霜，举头 望明月：低头 思故乡。
 */
class JingYeSiPoemTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** 取 zh-CN builtin JSON（SPI 资源），注入别名段构造静夜思方言。 */
    private static Lexicon jingYeSiLexicon() throws Exception {
        try (InputStream in = JingYeSiPoemTest.class.getResourceAsStream("/lexicons/zh-CN.json")) {
            assertNotNull(in, "zh-CN.json 应在 classpath（aster-lang-locales-zh SPI 包）");
            ObjectNode root = (ObjectNode) MAPPER.readTree(in);
            ObjectNode aliases = root.putObject("aliases");
            aliases.set("MODULE_DECL", arr("床前"));
            aliases.set("FUNC_TO", arr("疑是"));
            aliases.set("FUNC_PRODUCE", arr("举头"));
            aliases.set("RETURN", arr("低头"));
            root.put("id", "jingyesi");
            root.put("name", "静夜思");
            return DynamicLexicon.fromJsonString(MAPPER.writeValueAsString(root));
        }
    }

    /** 字面量宏词汇表：思故乡 → 内容「静夜思」。 */
    private static IdentifierIndex jingYeSiVocabIndex() {
        DomainVocabulary vocab = DomainVocabulary.builder("jingyesi", "静夜思", "jingyesi")
            .addLiteral("静夜思", "思故乡")
            .build();
        return IdentifierIndex.build(vocab);
    }

    private static ArrayNode arr(String... xs) {
        ArrayNode a = MAPPER.createArrayNode();
        for (String x : xs) a.add(x);
        return a;
    }

    @Test
    void poemCompilesAndRunsToItsName() throws Exception {
        // 李白《静夜思》原词序即源码。
        String poem = """
            床前 明月光。
            疑是 地上霜，举头 望明月：
              低头 思故乡。
            """;

        // 诗句 → Canonicalizer（别名归一 + 字面量宏展开 思故乡→"静夜思"）→ ANTLR → Core IR。
        Canonicalizer canon = new Canonicalizer(jingYeSiLexicon(), jingYeSiVocabIndex());
        String canonical = canon.canonicalize(poem);
        assertNotNull(canonical);

        var lexer = new aster.core.parser.AsterCustomLexer(CharStreams.fromString(canonical));
        var tokens = new CommonTokenStream(lexer);
        tokens.fill();
        tokens.seek(0);
        var parser = new aster.core.parser.AsterParser(tokens);
        parser.removeErrorListeners();
        var moduleCtx = parser.module();
        var ast = new aster.core.parser.AstBuilder().visitModule(moduleCtx);
        CoreModel.Module coreModule = new CoreLowering().lowerModule(ast);
        String coreJson = MAPPER.writeValueAsString(coreModule);

        // 用与生产 TrufflePolicyRuntime 一致的受限沙箱执行 Core IR。入口函数即 rule 名 明月光。
        try (Context context = Context.newBuilder("aster")
                .allowHostAccess(HostAccess.EXPLICIT)
                .allowIO(false)
                .allowCreateProcess(false)
                .allowNativeAccess(false)
                .option("engine.WarnInterpreterOnly", "false")
                .build()) {
            Source src = Source.newBuilder("aster", coreJson, "jingyesi.json").build();
            // 模块 明月光（床前 明月光）、rule 地上霜（疑是 地上霜）。eval 返回入口 rule 值。
            Value program = context.eval(src);
            Value result = program.canExecute() ? program.execute() : program;
            assertEquals("静夜思", result.asString(),
                "运行诗句构造的源码应输出该诗的名字「静夜思」");
        }
    }
}
