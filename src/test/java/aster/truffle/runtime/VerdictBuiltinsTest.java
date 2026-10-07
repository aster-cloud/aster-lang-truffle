package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

/** Verdict 内置（ADR 0039 §2.3）：形状、键序、参数校验。 */
class VerdictBuiltinsTest {

  @Test
  void allow形状() throws Exception {
    Map<?, ?> r = (Map<?, ?>) Builtins.call("Verdict.allow", new Object[0]);
    assertEquals(List.of("__type", "outcome"), new ArrayList<>(r.keySet()));
    assertEquals("Verdict", r.get("__type"));
    assertEquals("ALLOW", r.get("outcome"));
  }

  @Test
  void requireApproval键序() throws Exception {
    Map<?, ?> r =
        (Map<?, ?>)
            Builtins.call("Verdict.require_approval", new Object[] {"Senior Underwriter", "over cap"});
    assertEquals(List.of("__type", "outcome", "role", "reason"), new ArrayList<>(r.keySet()));
    assertEquals("REQUIRE_APPROVAL", r.get("outcome"));
    assertEquals("Senior Underwriter", r.get("role"));
    assertEquals("over cap", r.get("reason"));
  }

  @Test
  void deny与escalate() throws Exception {
    Map<?, ?> d = (Map<?, ?>) Builtins.call("Verdict.deny", new Object[] {"no"});
    assertEquals("DENY", d.get("outcome"));
    assertEquals(List.of("__type", "outcome", "reason"), new ArrayList<>(d.keySet()));
    assertEquals("ESCALATE", ((Map<?, ?>) Builtins.call("Verdict.escalate", new Object[] {"low"})).get("outcome"));
  }

  @Test
  void 空reason抛异常() {
    assertThrows(Builtins.BuiltinException.class, () -> Builtins.call("Verdict.deny", new Object[] {""}));
  }

  @Test
  void 空白reason与role抛异常() {
    assertThrows(Builtins.BuiltinException.class, () -> Builtins.call("Verdict.deny", new Object[] {"   "}));
    assertThrows(
        Builtins.BuiltinException.class,
        () -> Builtins.call("Verdict.require_approval", new Object[] {" \t", "over cap"}));
  }

  @Test
  void Verdict在布尔上下文直接失败() throws Exception {
    Object deny = Builtins.call("Verdict.deny", new Object[] {"no"});
    var e = assertThrows(Builtins.BuiltinException.class, () -> Builtins.toBool(deny));
    assertTrue(e.getMessage().contains("Verdict cannot be used as Bool"), e.getMessage());
    assertThrows(Builtins.BuiltinException.class, () -> Builtins.call("not", new Object[] {deny}));
  }

  @Test
  void If条件为Verdict时求值失败而非放行() throws Exception {
    String json = Files.readString(Path.of("src/test/resources/verdict/verdict-if-condition_core.json"));
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      var e =
          assertThrows(
              PolyglotException.class,
              () -> {
                Value r = ctx.eval(Source.newBuilder("aster", json, "verdict-if").build());
                if (r.canExecute()) r.execute();
              });
      assertTrue(e.getMessage().contains("Verdict cannot be used as Bool"), e.getMessage());
    }
  }

  @Test
  void 非Text抛异常() {
    assertThrows(Builtins.BuiltinException.class, () -> Builtins.call("Verdict.deny", new Object[] {42L}));
  }

  @Test
  void 参数个数错误抛异常() {
    assertThrows(Builtins.BuiltinException.class, () -> Builtins.call("Verdict.allow", new Object[] {"x"}));
  }

  @Test
  void 命名空间被识别() {
    assertTrue(Builtins.hasNamespace("Verdict"));
  }

  @Test
  void 端到端CoreIR求值() throws Exception {
    String json = Files.readString(Path.of("src/test/resources/verdict/verdict-require-approval_core.json"));
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value r = ctx.eval(Source.newBuilder("aster", json, "verdict").build());
      Value v = r.canExecute() ? r.execute() : r;
      Map<?, ?> m = v.as(Map.class);
      assertEquals("Verdict", String.valueOf(m.get("__type")));
      assertEquals("REQUIRE_APPROVAL", String.valueOf(m.get("outcome")));
      assertEquals("Senior Underwriter", String.valueOf(m.get("role")));
    }
  }
}
