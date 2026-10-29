package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void unknownMemberOfKnownEnumIsNotAnEnumValue() throws IOException {
        // Level 存在但没有 Nope：不得静默返回枚举值，按成员访问链处理；Level 未绑定，求值失败
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertThrows(PolyglotException.class, () -> run(ctx, module(returnName("Level.Nope"), "[]")));
        }
    }

    @Test
    void unknownEnumQualifierFails() throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertThrows(PolyglotException.class, () -> run(ctx, module(returnName("Zed.Nope"), "[]")));
        }
    }

    @Test
    void knownMemberQualifiedByWrongEnumFails() throws IOException {
        // Low 属于 Level 而非 Priority：Priority.Low 不能借用其它枚举的成员
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertThrows(PolyglotException.class, () -> run(ctx, module(returnName("Priority.Low"), "[]")));
        }
    }

    @Test
    void localNamedLikeEnumWithUnknownFieldFails() throws IOException {
        // 局部 Level 是 Box（只有 High 字段）：Level.Nope 走字段访问并失败，不回落到枚举
        String body = """
            [ { "kind": "Let", "name": "Level", "expr": { "kind": "Construct", "typeName": "Box",
                  "fields": [ { "name": "High", "expr": { "kind": "Int", "value": 7 } } ] } },
              { "kind": "Return", "expr": { "kind": "Name", "name": "Level.Nope" } } ]
            """;
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertThrows(PolyglotException.class, () -> run(ctx, module(body, "[]")));
        }
    }
}
