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

  private static final String INT = IrV2.typeName("Int");
  private static final String MAYBE_INT = "{ \"kind\": \"Maybe\", \"type\": " + INT + ", \"origin\": " + IrV2.ORIGIN + " }";

  private static String module(String paramType, String... stmts) {
    return IrV2.module("m",
        IrV2.func("entry", java.util.List.of(IrV2.param("x", paramType)), INT, java.util.List.of(), IrV2.block(stmts)));
  }

  private static String name(String n) {
    return IrV2.name(n);
  }

  private static String num(int v) {
    return IrV2.intLit(v);
  }

  private static String ret(String expr) {
    return IrV2.ret(expr);
  }

  private static String let(String n, String expr) {
    return IrV2.let(n, expr);
  }

  /** {@code When None}：v2 中关键字构造器是 0 参 PatCtor（ADR 0055 #16）。 */
  private static String whenNone(String body) {
    return IrV2.caseOf(IrV2.patCtor("None"), body);
  }

  private static String whenSome(String bind, String body) {
    return IrV2.caseOf(IrV2.patCtor("Some", IrV2.patName(bind)), body);
  }

  private static String whenName(String bind, String body) {
    return IrV2.caseOf(IrV2.patName(bind), body);
  }

  private static String match(String subject, String... cases) {
    return IrV2.match(name(subject), cases);
  }

  /** 单子表达式节点（Some / Ok / Err）。 */
  private static String wrap(String kind, String expr) {
    return "{ \"kind\": \"" + kind + "\", \"expr\": " + expr + ", \"origin\": " + IrV2.ORIGIN + " }";
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
        let("y", wrap("Some", name("x"))),
        match("y", whenNone(ret(num(0))), whenSome("v", ret(name("v")))));
    assertEquals(5, runInt(some, 5));
    String none = module(INT,
        let("y", "{ \"kind\": \"None\", \"origin\": " + IrV2.ORIGIN + " }"),
        match("y", whenSome("v", ret(name("v"))), whenNone(ret(num(0)))));
    assertEquals(0, runInt(none, 5));
  }

  @Test
  void okValueDoesNotMatchSome() throws IOException {
    String json = module(INT,
        let("y", wrap("Ok", name("x"))),
        match("y", whenSome("v", ret(num(1))), whenName("other", ret(num(2)))));
    assertEquals(2, runInt(json, 5));
  }
}
