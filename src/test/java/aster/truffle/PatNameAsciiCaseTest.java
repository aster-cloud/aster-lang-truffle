package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PatName 只按首字母是否为 ASCII 大写区分变体与绑定（ADR 0051 裁决 R55），与 aster-lang-ts 解释器的
 * {@code /^[A-Z]/}、R2 的判定及 parity 归一化一致。非 ASCII 大写开头（如德语 {@code Ärger}）的 PatName
 * 是绑定：R2 已把这类名字中的枚举成员产出无参数 PatCtor，剩下的 PatName 都是绑定。
 */
class PatNameAsciiCaseTest {

    private static String module(String patternName, String body) {
        return """
            { "kind": "Module", "name": "m", "decls": [
              { "kind": "Func", "name": "entry",
                "params": [ { "name": "n", "type": { "kind": "TypeName", "name": "Int" } } ],
                "ret": { "kind": "TypeName", "name": "Int" }, "effects": [],
                "body": { "kind": "Block", "statements": [
                  { "kind": "Match", "expr": { "kind": "Name", "name": "n" }, "cases": [
                    { "kind": "Case", "pattern": { "kind": "PatName", "name": "%s" }, "body": %s } ] },
                  { "kind": "Return", "expr": { "kind": "Int", "value": -1 } } ] } } ] }
            """.formatted(patternName, body);
    }

    private static Value run(String json, int arg) throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value program = ctx.eval(Source.newBuilder("aster", json, "pat.json").build());
            Value v = program.execute(arg);
            return v.isNull() ? null : Value.asValue(v.asInt());
        }
    }

    private static String retName(String name) {
        return "{ \"kind\": \"Return\", \"expr\": { \"kind\": \"Name\", \"name\": \"" + name + "\" } }";
    }

    @Test
    void nonAsciiUppercaseInitialBindsTheScrutinee() throws IOException {
        assertEquals(7, run(module("Ärger", retName("Ärger")), 7).asInt());
    }

    @Test
    void bindingNamedLikeAnEnumMemberIsReadAsTheBinding() throws IOException {
        // 整数主语下 `bei Ärger,` 是绑定（R12）；分支体读 Ärger 取绑定值，局部名优先于同名枚举成员（ADR 0051 §6.2）
        String json = """
            { "kind": "Module", "name": "m", "decls": [
              { "kind": "Enum", "name": "Laune", "variants": ["Übel", "Ärger"] },
              { "kind": "Func", "name": "entry",
                "params": [ { "name": "n", "type": { "kind": "TypeName", "name": "Int" } } ],
                "ret": { "kind": "TypeName", "name": "Int" }, "effects": [],
                "body": { "kind": "Block", "statements": [
                  { "kind": "Match", "expr": { "kind": "Name", "name": "n" }, "cases": [
                    { "kind": "Case", "pattern": { "kind": "PatName", "name": "Ärger" },
                      "body": { "kind": "Return", "expr": { "kind": "Name", "name": "Ärger" } } } ] },
                  { "kind": "Return", "expr": { "kind": "Int", "value": -1 } } ] } } ] }
            """;
        assertEquals(7, run(json, 7).asInt());
    }

    @Test
    void asciiUppercaseInitialIsStillAVariantMatch() throws IOException {
        // Int 主语不等于变体名：不匹配，落到 Return -1
        assertEquals(-1, run(module("Red", retName("n")), 7).asInt());
    }

    @Test
    void caselessInitialBinds() throws IOException {
        Value v = run(module("其他", retName("其他")), 5);
        assertTrue(v != null);
        assertEquals(5, v.asInt());
    }
}
