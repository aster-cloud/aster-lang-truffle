package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 限定枚举成员 {@code E.M}（ADR 0051 R18：解析保留原写法，Core 名为 {@code Level.High}）与裸成员 {@code M}
 * 求值为同一枚举值；同名局部 / 参数优先，按字段访问。
 */
class QualifiedEnumMemberTest {

    /** 模块：枚举 Level{High, Low}、Priority{High}（共享变体名）、结构 Box{High}；入口为首个 Func。 */
    private static String module(String entryBody, String entryParams) {
        return """
            {
              "kind": "Module", "name": "m",
              "decls": [
                { "kind": "Func", "name": "entry", "params": %s,
                  "ret": { "kind": "TypeName", "name": "Any" }, "effects": [],
                  "body": { "kind": "Block", "statements": %s } },
                { "kind": "Enum", "name": "Level", "variants": ["High", "Low"] },
                { "kind": "Enum", "name": "Priority", "variants": ["High"] },
                { "kind": "Data", "name": "Box", "fields": [ { "name": "High", "type": { "kind": "TypeName", "name": "Int" } } ] }
              ]
            }
            """.formatted(entryParams, entryBody);
    }

    private static String returnName(String name) {
        return "[ { \"kind\": \"Return\", \"expr\": { \"kind\": \"Name\", \"name\": \"" + name + "\" } } ]";
    }

    private static Value run(Context ctx, String json, Object... args) throws IOException {
        Value program = ctx.eval(Source.newBuilder("aster", json, "qualified-enum.json").build());
        return program.canExecute() ? program.execute(args) : program;
    }

    private static String enumOf(Value v) {
        return v.getMember("_enum").asString() + "." + v.getMember("value").asString();
    }

    @Test
    void qualifiedMemberEvaluatesToSameValueAsBareMember() throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertEquals("Level.High", enumOf(run(ctx, module(returnName("Level.High"), "[]"))));
            assertEquals("Level.Low", enumOf(run(ctx, module(returnName("Level.Low"), "[]"))));
            assertEquals(enumOf(run(ctx, module(returnName("Low"), "[]"))),
                enumOf(run(ctx, module(returnName("Level.Low"), "[]"))));
        }
    }

    @Test
    void sharedVariantNameIsDisambiguatedByQualifier() throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertEquals("Priority.High", enumOf(run(ctx, module(returnName("Priority.High"), "[]"))));
            assertEquals("Level.High", enumOf(run(ctx, module(returnName("Level.High"), "[]"))));
        }
    }

    @Test
    void qualifiedMemberComparesEqualToBareMember() throws IOException {
        String body = """
            [ { "kind": "Return", "expr": { "kind": "Call",
                "target": { "kind": "Name", "name": "==" },
                "args": [ { "kind": "Name", "name": "Level.Low" }, { "kind": "Name", "name": "Low" } ] } } ]
            """;
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertEquals(true, run(ctx, module(body, "[]")).asBoolean());
        }
    }

    @Test
    void localNamedLikeEnumKeepsFieldAccess() throws IOException {
        String body = """
            [ { "kind": "Let", "name": "Level", "expr": { "kind": "Construct", "typeName": "Box",
                  "fields": [ { "name": "High", "expr": { "kind": "Int", "value": 7 } } ] } },
              { "kind": "Return", "expr": { "kind": "Name", "name": "Level.High" } } ]
            """;
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertEquals(7, run(ctx, module(body, "[]")).asInt());
        }
    }

    @Test
    void parameterNamedLikeEnumKeepsFieldAccess() throws IOException {
        String params = "[ { \"name\": \"Level\", \"type\": { \"kind\": \"TypeName\", \"name\": \"Box\" } } ]";
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value box = run(ctx, module("""
                [ { "kind": "Return", "expr": { "kind": "Construct", "typeName": "Box",
                    "fields": [ { "name": "High", "expr": { "kind": "Int", "value": 9 } } ] } } ]
                """, "[]"));
            assertEquals(9, run(ctx, module(returnName("Level.High"), params), box).asInt());
        }
    }
}
