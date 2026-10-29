package aster.truffle;

import aster.core.ast.Block;
import aster.core.ast.Decl;
import aster.core.ast.Expr;
import aster.core.ast.Pattern;
import aster.core.ast.Span;
import aster.core.ast.Stmt;
import aster.core.ast.Type;
import aster.core.resolve.NameResolution;
import aster.core.resolve.ResolutionContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Match 的 case 体为多语句块时，Core IR 以 {@code kind:"Block"} 表示（core 与 ts 的 lowering 一致）。
 * core 一侧：Java 文法的 {@code When pat,} 后不接换行缩进块（既有限制），故用 AST 直接构造 Block 后走真实的
 * NameResolution + CoreLowering；ts 一侧：{@code src/test/resources/ts-ir/*.json} 由 aster-lang-ts 1.1.0 的
 * canonicalize → lex → parse → lowerModule 对同构 CNL 源码生成：
 * block-case.json 源码为 {@code Match x: When 1, <缩进块 Let y be 10. Return y.> When n, Return n.}；
 * block-case-nested.json 在块内再嵌一层 {@code Match y}。
 */
class BlockCaseBodyTest {

    private static Value run(Context ctx, String json, Object... args) throws java.io.IOException {
        Value program = ctx.eval(Source.newBuilder("aster", json, "block-case.json").build());
        return program.canExecute() ? program.execute(args) : program;
    }

    private static Context newContext() {
        return Context.newBuilder("aster").allowAllAccess(true).build();
    }

    private static final Span SPAN = new Span(new Span.Position(1, 1), new Span.Position(1, 2));
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Type.TypeName intType() {
        return new Type.TypeName("Int", List.of(), SPAN);
    }

    private static Decl.Func func(List<Decl.Parameter> params, Block body) {
        return new Decl.Func("pick", SPAN, List.of(), params, intType(), List.of(), List.of(), body,
            List.of(), List.of(), false, SPAN);
    }

    private static Decl.Parameter param(String name) {
        return new Decl.Parameter(name, intType(), List.of(), SPAN);
    }

    private static Stmt.Return ret(String name) {
        return new Stmt.Return(new Expr.Name(name, SPAN), SPAN);
    }

    private static Stmt.Let let(String name, int value) {
        return new Stmt.Let(name, new Expr.Int(value, SPAN), SPAN);
    }

    private static Stmt.Case caseOf(Pattern pattern, Stmt.Case.CaseBody body) {
        return new Stmt.Case(pattern, body, SPAN);
    }

    private static Stmt.Match match(String subject, Stmt.Case... cases) {
        return new Stmt.Match(new Expr.Name(subject, SPAN), List.of(cases), SPAN);
    }

    /** 经 core 的真实降级流水线（名称解析 + CoreLowering）把 AST 变成 Core IR JSON。 */
    private static String lowerWithCore(Decl.Func fn) throws Exception {
        aster.core.ast.Module ast = new aster.core.ast.Module("test.blockcase", null, List.of(fn), SPAN);
        var core = NameResolution.resolveAndLowerChecked(ast, new ResolutionContext(null, null)).core();
        return MAPPER.writeValueAsString(core);
    }

    private static String resource(String name) throws Exception {
        try (var in = BlockCaseBodyTest.class.getResourceAsStream("/ts-ir/" + name)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void coreLoweredBlockCaseBodyEvaluates() throws Exception {
        Block multi = new Block(List.<Stmt>of(let("y", 10), ret("y")), SPAN);
        String json = lowerWithCore(func(List.of(param("x")), new Block(List.<Stmt>of(
            match("x", caseOf(new Pattern.PatternInt(1, SPAN), multi),
                caseOf(new Pattern.PatternName("n", SPAN), new Stmt.Return(new Expr.Name("n", SPAN), SPAN)))), SPAN)));
        assertTrue(json.contains("\"Block\""), "前提：core 对块体 case 产出 Block: " + json);
        try (Context ctx = newContext()) {
            assertEquals(10, run(ctx, json, 1).asInt());
            assertEquals(7, run(ctx, json, 7).asInt());
        }
    }

    @Test
    void coreLoweredNestedMatchInBlockCaseBody() throws Exception {
        Block inner = new Block(List.<Stmt>of(let("inner", 20), ret("inner")), SPAN);
        Block outer = new Block(List.<Stmt>of(let("base", 100),
            match("y", caseOf(new Pattern.PatternInt(2, SPAN), inner),
                caseOf(new Pattern.PatternName("m", SPAN), new Stmt.Return(new Expr.Name("m", SPAN), SPAN)))), SPAN);
        String json = lowerWithCore(func(List.of(param("x"), param("y")), new Block(List.<Stmt>of(
            match("x", caseOf(new Pattern.PatternInt(1, SPAN), outer),
                caseOf(new Pattern.PatternName("n", SPAN), new Stmt.Return(new Expr.Name("n", SPAN), SPAN)))), SPAN)));
        try (Context ctx = newContext()) {
            assertEquals(20, run(ctx, json, 1, 2).asInt());
            assertEquals(5, run(ctx, json, 1, 5).asInt());
            assertEquals(9, run(ctx, json, 9, 2).asInt());
        }
    }

    @Test
    void tsLoweredBlockCaseBodyEvaluates() throws Exception {
        String json = resource("block-case.json");
        try (Context ctx = newContext()) {
            assertEquals(10, run(ctx, json, 1).asInt());
            assertEquals(7, run(ctx, json, 7).asInt());
        }
    }

    @Test
    void tsLoweredNestedMatchInBlockCaseBody() throws Exception {
        String json = resource("block-case-nested.json");
        try (Context ctx = newContext()) {
            assertEquals(20, run(ctx, json, 1, 2).asInt());
            assertEquals(5, run(ctx, json, 1, 5).asInt());
            assertEquals(9, run(ctx, json, 9, 2).asInt());
        }
    }

    /** ts lower_to_core 产出形态：case.body = Block{statements}，带 origin。 */
    private static String tsShapedModule(String caseStatements) {
        return """
            { "kind": "Module", "name": "m", "decls": [
              { "kind": "Func", "name": "entry",
                "params": [ { "name": "x", "type": { "kind": "TypeName", "name": "Int" } } ],
                "ret": { "kind": "TypeName", "name": "Int" }, "effects": [],
                "body": { "kind": "Block", "statements": [
                  { "kind": "Match", "expr": { "kind": "Name", "name": "x" }, "cases": [
                    { "kind": "Case", "pattern": { "kind": "PatInt", "value": 1 },
                      "body": { "kind": "Block", "statements": %s } },
                    { "kind": "Case", "pattern": { "kind": "PatName", "name": "n" },
                      "body": { "kind": "Return", "expr": { "kind": "Name", "name": "n" } } }
                  ] } ] } } ] }
            """.formatted(caseStatements);
    }

    @Test
    void emptyBlockCaseBodyFallsThroughWithoutError() throws Exception {
        try (Context ctx = newContext()) {
            assertTrue(run(ctx, tsShapedModule("[]"), 1).isNull(), "空块不返回值");
            assertEquals(3, run(ctx, tsShapedModule("[]"), 3).asInt());
        }
    }

    @Test
    void blockCaseBodyWithLetSetAndReturn() throws Exception {
        String stmts = """
            [ { "kind": "Let", "name": "a", "expr": { "kind": "Int", "value": 1 } },
              { "kind": "Set", "name": "a", "expr": { "kind": "Int", "value": 8 } },
              { "kind": "Return", "expr": { "kind": "Name", "name": "a" } } ]
            """;
        try (Context ctx = newContext()) {
            assertEquals(8, run(ctx, tsShapedModule(stmts), 1).asInt());
        }
    }

    @Test
    void scopeCaseBodyStillWorks() throws Exception {
        String json = tsShapedModule("[]").replace("\"kind\": \"Block\", \"statements\": []",
            "\"kind\": \"Scope\", \"statements\": [ { \"kind\": \"Return\", \"expr\": { \"kind\": \"Int\", \"value\": 4 } } ]");
        try (Context ctx = newContext()) {
            assertEquals(4, run(ctx, json, 1).asInt());
        }
    }

    @Test
    void unknownCaseBodyKindIsStillRejected() {
        String json = tsShapedModule("[]").replace("\"body\": { \"kind\": \"Block\", \"statements\": [] }",
            "\"body\": { \"kind\": \"Nope\" }");
        try (Context ctx = newContext()) {
            assertThrows(PolyglotException.class, () -> run(ctx, json, 1));
        }
    }
}
