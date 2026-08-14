package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Issue #16 / Task 1: member access on host-wrapped objects must work through
 * {@link com.oracle.truffle.api.interop.InteropLibrary} exclusively — without any
 * reflective unwrap of Truffle-internal {@code HostObject}/{@code HostProxy} fields.
 *
 * <p>Both fixtures are passed in as host arguments and reach {@code MemberAccessNode}
 * as Truffle interop objects:
 * <ul>
 *   <li>a {@link ProxyObject} — exposes {@code hasMembers}/{@code readMember}, the
 *       canonical interop member surface;</li>
 *   <li>a plain host POJO under {@link HostAccess#ALL} — its public fields are
 *       exposed as readable interop members.</li>
 * </ul>
 * Reading {@code obj.field} on each must return the underlying value via interop.
 */
class MemberAccessInteropTest {

    /** Program: {@code func get(obj) { return obj.value }} (Name "obj.value" -> member access). */
    private static final String MEMBER_ACCESS_PROGRAM = """
        {
          "name": "test.member.access",
          "decls": [
            {
              "kind": "Func",
              "name": "get",
              "params": [{ "name": "obj", "type": { "kind": "TypeName", "name": "Any" } }],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [{
                  "kind": "Return",
                  "expr": { "kind": "Name", "name": "obj.value" }
                }]
              }
            }
          ]
        }
        """;

    @Test
    @DisplayName("Task 1: member access on a host ProxyObject works via InteropLibrary")
    void memberAccessOnProxyObject() throws IOException {
        try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Source source = Source.newBuilder("aster", MEMBER_ACCESS_PROGRAM, "member-proxy.json").build();
            Value program = context.eval(source);

            ProxyObject host = ProxyObject.fromMap(Map.of("value", 123));
            Value result = program.execute(host);

            assertEquals(123, result.asInt(), "obj.value on a host ProxyObject should resolve via interop");
        }
    }

    /** Public-field POJO used as a host member-access fixture. */
    public static final class HostBean {
        public final int value;
        public HostBean(int value) { this.value = value; }
    }

    @Test
    @DisplayName("Task 1: member access on a host POJO (HostAccess.ALL) works via InteropLibrary")
    void memberAccessOnHostPojo() throws IOException {
        try (Context context = Context.newBuilder("aster")
                .allowHostAccess(HostAccess.ALL)
                .build()) {
            Source source = Source.newBuilder("aster", MEMBER_ACCESS_PROGRAM, "member-pojo.json").build();
            Value program = context.eval(source);

            Value result = program.execute(new HostBean(456));

            assertEquals(456, result.asInt(), "obj.value on a host POJO should resolve via interop members");
        }
    }

    /**
     * 成员不存在时，错误必须指向**真因**（键名对不上），并列出可用成员。
     *
     * <p>api#244：此前文案是「对象类型 HostObject 不支持成员访问……请确认 polyglot
     * Context 配置了恰当的 HostAccess」。用户据此去查引擎配置，而真实原因只是
     * context 里的键名与规则参数不一致——排查方向被带偏两层。
     */
    @Test
    @DisplayName("api#244: 成员不存在时列出可用键名，而不是指向 HostAccess 配置")
    void missingMemberErrorListsAvailableKeys() throws IOException {
        try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Source source = Source.newBuilder("aster", MEMBER_ACCESS_PROGRAM, "member-missing.json").build();
            Value program = context.eval(source);

            // 规则要 obj.value，调用方却传 {"wrongKey": {...}}——issue 里的真实形状
            RuntimeException ex = org.junit.jupiter.api.Assertions.assertThrows(
                RuntimeException.class,
                () -> program.execute(ProxyObject.fromMap(Map.of("wrongKey", Map.of("age", 30)))));

            String msg = String.valueOf(ex.getMessage());
            org.junit.jupiter.api.Assertions.assertTrue(msg.contains("wrongKey"),
                "★必须列出实际可用的键名，用户才能一眼看出自己传错了形状；实际文案：" + msg);
            org.junit.jupiter.api.Assertions.assertFalse(msg.contains("请确认 polyglot Context 配置"),
                "★不得再把真因是键名不匹配的错误指向 HostAccess 配置；实际文案：" + msg);
        }
    }

    /**
     * 宿主 POJO 走的是 members 而非 hash 条目——两条枚举路径都要覆盖。
     *
     * <p>先前一次尝试只用 {@code getMembers}，对宿主 {@code Map} 拿不到东西而回退；
     * 只测 Map 又会漏掉 POJO。故两条都钉住。
     */
    @Test
    @DisplayName("api#244: 宿主 POJO 缺成员时同样列出可用成员")
    void missingMemberOnPojoAlsoListsMembers() throws IOException {
        try (Context context = Context.newBuilder("aster")
                .allowHostAccess(HostAccess.ALL)
                .build()) {
            Source source = Source.newBuilder("aster", MISSING_MEMBER_PROGRAM, "member-pojo-missing.json").build();
            Value program = context.eval(source);

            RuntimeException ex = org.junit.jupiter.api.Assertions.assertThrows(
                RuntimeException.class, () -> program.execute(new HostBean(456)));

            String msg = String.valueOf(ex.getMessage());
            org.junit.jupiter.api.Assertions.assertTrue(msg.contains("value"),
                "★POJO 的可用成员应被列出（这里是 value）；实际文案：" + msg);
        }
    }

    /** 读一个**不存在**的成员 {@code obj.missing}，用于触发错误路径。 */
    private static final String MISSING_MEMBER_PROGRAM = MEMBER_ACCESS_PROGRAM
        .replace("\"name\": \"obj.value\"", "\"name\": \"obj.missing\"")
        .replace("test.member.access", "test.member.missing");
}
