package aster.truffle;

import aster.core.identifier.DomainVocabulary;
import aster.core.identifier.IdentifierKind;
import aster.core.identifier.IdentifierMapping;
import aster.core.identifier.VocabularyScopes;
import aster.core.lexicon.Lexicon;
import aster.core.lexicon.LexiconRegistry;
import aster.core.typecheck.ErrorCode;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ADR 0051 §1 四个生产复现：德语源码 + 领域词汇经 truffle 编译路径（canonicalize → R1 → lower → R2）
 * 得到只含规范名的 Core，并在 Truffle 上执行得出正确结果；E8xx 编译失败且不执行。
 * 用例与 aster-lang-core VocabularyResolutionIntegrationTest 对应，关键词改用 de-DE 词法。
 */
class VocabularyResolutionExecutionTest {

    private static Lexicon german() {
        return LexiconRegistry.getInstance().getOrThrow("de-DE");
    }

    private static IdentifierMapping member(String parent, String canonical, String localized) {
        return new IdentifierMapping(canonical, localized, IdentifierKind.ENUM_VALUE, parent, null, null);
    }

    private static VocabularyScopes scopesOf(List<IdentifierMapping> ms) {
        DomainVocabulary vocab = new DomainVocabulary("test.vocab", "test", "de-DE", "1",
            ofKind(ms, IdentifierKind.STRUCT), ofKind(ms, IdentifierKind.FIELD), ofKind(ms, IdentifierKind.FUNCTION),
            ofKind(ms, IdentifierKind.ENUM_VALUE), ofKind(ms, IdentifierKind.LITERAL), null);
        return VocabularyScopes.build(vocab);
    }

    private static List<IdentifierMapping> ofKind(List<IdentifierMapping> ms, IdentifierKind kind) {
        return ms.stream().filter(m -> m.kind() == kind).toList();
    }

    /** 经 truffle 编译路径得到 Core JSON，再以首个 Func 为入口执行。 */
    private static Value compileAndRun(Context ctx, String source, VocabularyScopes scopes, Object... args) throws Exception {
        String json = CnlCompiler.compileWithVocabulary(source, german(), scopes);
        Value program = ctx.eval(Source.newBuilder("aster", json, "vocab.json").build());
        return program.canExecute() ? program.execute(args) : program;
    }

    // §1 复现 1 + 3：类型 Stufe 与字段 stufe 并存；参数 fahrer 与类型 Fahrer 同词
    private static final String LEVEL_FIELD_SOURCE = "Modul versicherung.\n\n"
        + "Regel bewerten gegeben fahrer als Fahrer, liefert Ganzzahl:\n"
        + "  gib zurueck fahrer.stufe.punkte plus fahrer.alter.\n\n"
        + "Definiere Stufe hat punkte als Ganzzahl.\n\n"
        + "Definiere Fahrer hat stufe als Stufe, alter als Ganzzahl.\n";

    private static final List<IdentifierMapping> LEVEL_FIELD_VOCAB = List.of(
        IdentifierMapping.struct("Level", "Stufe"), IdentifierMapping.struct("Driver", "Fahrer"),
        IdentifierMapping.field("points", "punkte", "Level"), IdentifierMapping.field("level", "stufe", "Driver"),
        IdentifierMapping.field("age", "alter", "Driver"));

    // §1 复现 2：规则 hoch 与枚举成员 Hoch 并存，限定写法 Stufe.Hoch
    private static String enumSource(String ret) {
        return "Modul versicherung.\n\n"
            + "Regel einstufen gegeben x als Ganzzahl, liefert Stufe:\n  gib zurueck " + ret + ".\n\n"
            + "Definiere Stufe als eines von Hoch, Niedrig.\n\n"
            + "Regel hoch gegeben x als Ganzzahl, liefert Ganzzahl:\n  gib zurueck x.\n";
    }

    private static final List<IdentifierMapping> ENUM_VOCAB = List.of(
        IdentifierMapping.struct("Level", "Stufe"), member("Level", "High", "Hoch"), member("Level", "Low", "Niedrig"),
        IdentifierMapping.function("high", "hoch"));

    // §1 复现 4：同一本地化字段名 alter 在两个 struct 中对应不同规范字段
    private static final String SAME_FIELD_SOURCE = "Modul versicherung.\n\n"
        + "Regel summe gegeben fahrer als Fahrer, wagen als Fahrzeug, liefert Ganzzahl:\n"
        + "  gib zurueck fahrer.alter plus wagen.alter.\n\n"
        + "Definiere Fahrer hat alter als Ganzzahl.\n\n"
        + "Definiere Fahrzeug hat alter als Ganzzahl.\n";

    private static final List<IdentifierMapping> SAME_FIELD_VOCAB = List.of(
        IdentifierMapping.struct("Driver", "Fahrer"), IdentifierMapping.struct("Vehicle", "Fahrzeug"),
        IdentifierMapping.field("age", "alter", "Driver"), IdentifierMapping.field("vehicleAge", "alter", "Vehicle"));

    @Test
    void levelTypeAndLevelFieldCoexistAndParameterFahrerExecutes() throws Exception {
        // 运行时对象按规范字段名提供：解析后 Core 访问的是 fahrer.level.points / fahrer.age
        ProxyObject fahrer = ProxyObject.fromMap(Map.of(
            "level", ProxyObject.fromMap(Map.of("points", 3)), "age", 40));
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertEquals(43, compileAndRun(ctx, LEVEL_FIELD_SOURCE, scopesOf(LEVEL_FIELD_VOCAB), fahrer).asInt());
        }
    }

    @Test
    void qualifiedStufeHochExecutesToLevelHigh() throws Exception {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value v = compileAndRun(ctx, enumSource("Stufe.Hoch"), scopesOf(ENUM_VOCAB), 1);
            assertEquals("Level", v.getMember("_enum").asString());
            assertEquals("High", v.getMember("value").asString());
        }
    }

    @Test
    void sameLocalizedFieldResolvesPerOwningStructAndExecutes() throws Exception {
        ProxyObject fahrer = ProxyObject.fromMap(Map.of("age", 30));
        ProxyObject wagen = ProxyObject.fromMap(Map.of("vehicleAge", 5));
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertEquals(35, compileAndRun(ctx, SAME_FIELD_SOURCE, scopesOf(SAME_FIELD_VOCAB), fahrer, wagen).asInt());
        }
    }

    @Test
    void ambiguousBareHochFailsCompilationWithE804AndNoIr() {
        CnlCompiler.CompilationException e = assertThrows(CnlCompiler.CompilationException.class,
            () -> CnlCompiler.compileWithVocabulary(enumSource("Hoch"), german(), scopesOf(ENUM_VOCAB)));
        assertTrue(e.diagnostics().stream().anyMatch(d -> d.code() == ErrorCode.AMBIGUOUS_NAME), e.diagnostics().toString());
        assertTrue(e.getMessage().contains("E804"), e.getMessage());
    }

    @Test
    void unresolvedFieldFailsCompilationWithoutVocabulary() {
        String src = "Module m.\n\nRule f given x as Fahrer, produce Int:\n  Return x.gewicht.\n\n"
            + "Define Fahrer has alter as Int.\n";
        CnlCompiler.CompilationException e = assertThrows(CnlCompiler.CompilationException.class,
            () -> CnlCompiler.compile(src, "en-US"));
        assertTrue(e.diagnostics().stream().anyMatch(d -> d.code() == ErrorCode.UNRESOLVED_FIELD), e.diagnostics().toString());
    }

    @Test
    void polyglotSourceWithE8xxIsRejectedBeforeExecution() {
        String src = "Module m.\n\nRule f given x as Fahrer, produce Int:\n  Return x.gewicht.\n\n"
            + "Define Fahrer has alter as Int.\n";
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            var e = assertThrows(org.graalvm.polyglot.PolyglotException.class,
                () -> ctx.eval(Source.newBuilder("aster", src, "bad.aster").build()));
            assertTrue(e.getMessage().contains("E80"), e.getMessage());
        }
    }
}
