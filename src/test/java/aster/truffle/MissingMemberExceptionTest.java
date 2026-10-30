package aster.truffle;

import aster.truffle.runtime.interop.AsterListValue;
import aster.truffle.runtime.interop.AsterMapValue;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 成员缺失时抛结构化异常（aster-lang-truffle#153）。
 *
 * <p>宿主侧（aster-api）据异常的 guest 对象取回成员名与<b>被访问对象引用</b>，按对象身份在调用方输入里定位，
 * 不再依赖文案匹配与同名猜测。文案保持原样以兼容旧消费方。
 */
class MissingMemberExceptionTest {

    private static final String ANY = "{ \"kind\": \"TypeName\", \"name\": \"Any\" }";

    private static String module(String decls) {
        return "{ \"kind\": \"Module\", \"name\": \"m\", \"decls\": [" + decls + "] }";
    }

    private static String func(String body) {
        return """
            { "kind": "Func", "name": "entry",
              "params": [ { "name": "obj", "type": %s } ],
              "ret": %s, "effects": [],
              "body": { "kind": "Block", "statements": %s } }
            """.formatted(ANY, ANY, body);
    }

    private static String ret(String name) {
        return "{ \"kind\": \"Return\", \"expr\": { \"kind\": \"Name\", \"name\": \"" + name + "\" } }";
    }

    private static String let(String name, String expr) {
        return "{ \"kind\": \"Let\", \"name\": \"" + name + "\", \"expr\": " + expr + " }";
    }

    private static AsterMapValue map(Object... kv) {
        Map<String, Object> entries = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            entries.put((String) kv[i], kv[i + 1]);
        }
        return new AsterMapValue(entries);
    }

    private static PolyglotException fail(Context ctx, String json, Object arg) throws IOException {
        Value program = ctx.eval(Source.newBuilder("aster", json, "missing-member.json").build());
        return assertThrows(PolyglotException.class, () -> program.execute(arg));
    }

    /** 断言异常为结构化缺成员异常，并返回其 guest 对象。 */
    private static Value missingMember(PolyglotException ex, String member) {
        assertTrue(ex.isGuestException(), "应为 guest 异常而非引擎内部错误：" + ex.getMessage());
        assertFalse(ex.isInternalError());
        Value guest = ex.getGuestObject();
        assertTrue(guest.isException());
        assertTrue(guest.hasMember("member") && guest.hasMember("base") && guest.hasMember("declaredType"));
        assertEquals(member, guest.getMember("member").asString());
        return guest;
    }

    @Test
    @DisplayName("输入映射缺键：文案不变，base 是调用方传入的同一对象")
    void inputMapMissingKeyCarriesBaseReference() throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            AsterMapValue input = map("other", 1);
            PolyglotException ex = fail(ctx, module(func("[" + ret("obj.value") + "]")), input);

            assertEquals("Map 中不存在键：value", ex.getMessage());
            Value guest = missingMember(ex, "value");
            assertEquals(ctx.asValue(input), guest.getMember("base"), "base 必须与输入对象同一身份");
            assertTrue(guest.getMember("declaredType").isNull());
        }
    }

    @Test
    @DisplayName("规则内部 None 读成员：base 是内部值，与任何输入对象都不同")
    void internalNoneMissingKeyIsNotTheInput() throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            AsterMapValue input = map("value", 1);
            String body = "[" + let("n", "{ \"kind\": \"None\" }") + "," + ret("n.value") + "]";
            PolyglotException ex = fail(ctx, module(func(body)), input);

            Value guest = missingMember(ex, "value");
            assertNotEquals(ctx.asValue(input), guest.getMember("base"));
        }
    }

    @Test
    @DisplayName("列表多个元素都缺字段：base 是规则实际读取的那个元素")
    void listElementBaseIsTheOneActuallyRead() throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            // 两个元素内容完全相同：只有按身份比较才能区分规则读的是哪一个
            AsterMapValue first = map("other", 1);
            AsterMapValue second = map("other", 1);
            AsterListValue input = new AsterListValue(List.of(first, second));
            String get = """
                { "kind": "Call", "target": { "kind": "Name", "name": "List.get" },
                  "args": [ { "kind": "Name", "name": "obj" }, { "kind": "Int", "value": 1 } ] }
                """;
            String body = "[" + let("item", get) + "," + ret("item.value") + "]";
            PolyglotException ex = fail(ctx, module(func(body)), input);

            Value base = missingMember(ex, "value").getMember("base");
            assertEquals(ctx.asValue(second), base);
            assertNotEquals(ctx.asValue(first), base);
        }
    }

    @Test
    @DisplayName("结构体缺字段：带声明类型名，文案不变")
    void dataValueMissingFieldCarriesDeclaredType() throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            String data = """
                { "kind": "Data", "name": "P",
                  "fields": [ { "name": "a", "type": { "kind": "TypeName", "name": "Int" } } ] }
                """;
            String cons = """
                { "kind": "Construct", "typeName": "P",
                  "fields": [ { "name": "a", "expr": { "kind": "Int", "value": 1 } } ] }
                """;
            String body = "[" + let("p", cons) + "," + ret("p.b") + "]";
            PolyglotException ex = fail(ctx, module(data + "," + func(body)), map());

            assertEquals("类型 P 不存在字段：b", ex.getMessage());
            Value guest = missingMember(ex, "b");
            assertEquals("P", guest.getMember("declaredType").asString());
            assertTrue(guest.getMember("base").hasMember("a"));
        }
    }

    @Test
    @DisplayName("宿主对象缺成员：同样结构化，文案仍列出可用成员")
    void hostObjectMissingMemberIsStructured() throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowHostAccess(HostAccess.ALL).build()) {
            ProxyObject input = ProxyObject.fromMap(Map.of("wrongKey", 1));
            PolyglotException ex = fail(ctx, module(func("[" + ret("obj.value") + "]")), input);

            assertTrue(ex.getMessage().startsWith("无法访问成员：value。该对象的可用成员为 [wrongKey]"), ex.getMessage());
            Value guest = missingMember(ex, "value");
            assertTrue(guest.getMember("base").isProxyObject());
            assertTrue(guest.getMember("base").asProxyObject() == input, "宿主对象应能原样取回");
        }
    }

    @Test
    @DisplayName("对 null 读成员：base 为 null，文案不变")
    void nullBaseIsStructured() throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            String body = "[" + let("n", "{ \"kind\": \"Null\" }") + "," + ret("n.value") + "]";
            PolyglotException ex = fail(ctx, module(func(body)), map());

            assertEquals("无法访问 null 对象的成员：value", ex.getMessage());
            assertTrue(missingMember(ex, "value").getMember("base").isNull());
        }
    }
}
