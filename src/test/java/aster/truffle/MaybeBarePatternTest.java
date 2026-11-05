package aster.truffle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

/**
 * 可空值 {@code T?} 上的 {@code When Some(v)} / {@code When None}（aster-lang-ts#277）。
 * <p>
 * {@code T?} 位置的值在运行期有两种表示：宿主传入或规则直接传入的裸值 / null（类型检查放行 T → T?），
 * 以及 {@code Some(x)} / {@code None} 构造出的带 {@code __type} 的 Map。两种表示上模式匹配须得到同一结果：
 * 裸的非 null 值匹配 {@code Some(v)} 并绑定 v，null 匹配 {@code None}。此前裸值两臂都落空，Match 静默返回 null。
 * Ok / Err 等其它带标签的值不因此匹配 {@code Some(v)}。语义与 aster-lang-ts 解释器一致。
 */
class MaybeBarePatternTest {

  private static final String INT = "{ \"kind\": \"TypeName\", \"name\": \"Int\" }";
  private static final String MAYBE_INT = "{ \"kind\": \"Maybe\", \"type\": " + INT + " }";

  private static String module(String paramType, String... stmts) {
    return """
        { "kind": "Module", "name": "m", "decls": [
          { "kind": "Func", "name": "entry",
            "params": [ { "name": "x", "type": %s } ],
            "ret": %s, "effects": [],
            "body": { "kind": "Block", "statements": [%s] } } ] }
        """.formatted(paramType, INT, String.join(",", stmts));
  }

  private static String name(String n) {
    return "{ \"kind\": \"Name\", \"name\": \"" + n + "\" }";
  }

  private static String num(int v) {
    return "{ \"kind\": \"Int\", \"value\": " + v + " }";
  }

  private static String ret(String expr) {
    return "{ \"kind\": \"Return\", \"expr\": " + expr + " }";
  }

  private static String let(String n, String expr) {
    return "{ \"kind\": \"Let\", \"name\": \"" + n + "\", \"expr\": " + expr + " }";
  }

  private static String whenNone(String body) {
    return "{ \"kind\": \"Case\", \"pattern\": { \"kind\": \"PatName\", \"name\": \"None\" }, \"body\": " + body + " }";
  }

  private static String whenSome(String bind, String body) {
    return "{ \"kind\": \"Case\", \"pattern\": { \"kind\": \"PatCtor\", \"typeName\": \"Some\", \"names\": [\"" + bind + "\"] }, \"body\": " + body + " }";
  }

  private static String whenName(String bind, String body) {
    return "{ \"kind\": \"Case\", \"pattern\": { \"kind\": \"PatName\", \"name\": \"" + bind + "\" }, \"body\": " + body + " }";
  }

  private static String match(String subject, String... cases) {
    return "{ \"kind\": \"Match\", \"expr\": " + name(subject) + ", \"cases\": [" + String.join(",", cases) + "] }";
  }

  private static int runInt(String json, Object arg) throws IOException {
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value program = ctx.eval(Source.newBuilder("aster", json, "maybe.json").build());
      return program.execute(arg).asInt();
    }
  }

  @Test
  void bareValueMatchesSomeWhenNoneComesFirst() throws IOException {
    String json = module(MAYBE_INT, match("x", whenNone(ret(num(0))), whenSome("v", ret(name("v")))));
    assertEquals(3, runInt(json, 3));
  }

  @Test
  void bareValueMatchesSomeWhenSomeComesFirst() throws IOException {
    String json = module(MAYBE_INT, match("x", whenSome("v", ret(name("v"))), whenNone(ret(num(0)))));
    assertEquals(3, runInt(json, 3));
  }

  @Test
  void hostNullMatchesNone() throws IOException {
    String json = module(MAYBE_INT, match("x", whenSome("v", ret(name("v"))), whenNone(ret(num(0)))));
    assertEquals(0, runInt(json, null));
  }

  @Test
  void taggedSomeAndNoneStillMatch() throws IOException {
    String some = module(INT,
        let("y", "{ \"kind\": \"Some\", \"expr\": " + name("x") + " }"),
        match("y", whenNone(ret(num(0))), whenSome("v", ret(name("v")))));
    assertEquals(5, runInt(some, 5));
    String none = module(INT,
        let("y", "{ \"kind\": \"None\" }"),
        match("y", whenSome("v", ret(name("v"))), whenNone(ret(num(0)))));
    assertEquals(0, runInt(none, 5));
  }

  @Test
  void okValueDoesNotMatchSome() throws IOException {
    String json = module(INT,
        let("y", "{ \"kind\": \"Ok\", \"expr\": " + name("x") + " }"),
        match("y", whenSome("v", ret(num(1))), whenName("other", ret(num(2)))));
    assertEquals(2, runInt(json, 5));
  }
}
