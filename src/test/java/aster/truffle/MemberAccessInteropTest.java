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
}
