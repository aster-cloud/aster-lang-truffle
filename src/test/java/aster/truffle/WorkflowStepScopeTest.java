package aster.truffle;

import aster.truffle.runtime.Builtins;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * workflow 的 step 体与 compensate 体各是一层词法作用域（#159，与 case / Scope 一致，ADR 0052 §2.1）。
 *
 * <p>此前 step / compensate 不开层：step 内的 {@code Let} 落在规则体那一层，与外层同名时覆盖外层 slot；
 * 并行 step 在同一个 frame 上执行，同名 {@code Let} 写同一个 slot，结果取决于调度顺序。
 * 现在每个 step / compensate 体的声明分配本层的 slot，互不重叠；读与 {@code Set} 仍由内向外取最近一层，
 * 没有同名声明时 {@code Set} 写回外层。
 */
class WorkflowStepScopeTest {

  private static final String INT = IrV2.typeName("Int");
  private final ConcurrentLinkedQueue<Object> recorded = new ConcurrentLinkedQueue<>();

  @BeforeEach
  void registerProbes() {
    Builtins.register("__recordStep", new Builtins.BuiltinDef(args -> {
      recorded.add(args[0]);
      return args[0];
    }));
    // 先睡再返回：给并行 step 留出写同一 slot 的窗口
    Builtins.register("__pause", new Builtins.BuiltinDef(args -> {
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return args[0];
    }));
    Builtins.register("__failStep", new Builtins.BuiltinDef(args -> {
      throw new RuntimeException("step failed on purpose");
    }));
  }

  private static String num(int v) {
    return IrV2.intLit(v);
  }

  private static String name(String n) {
    return IrV2.name(n);
  }

  private static String call(String target, String... args) {
    return IrV2.call(name(target), args);
  }

  private static String let(String n, String expr) {
    return IrV2.let(n, expr);
  }

  private static String set(String n, String expr) {
    return IrV2.set(n, expr);
  }

  private static String ret(String expr) {
    return IrV2.ret(expr);
  }

  private static String block(String... stmts) {
    return IrV2.block(stmts);
  }

  /** {@code deps} 为逗号拼接的 JSON 字符串列表内容（如 {@code "\"a\""}，可为空串）。 */
  private static String step(String stepName, String deps, String body, String compensate) {
    return "{\"kind\":\"step\",\"name\":" + IrV2.q(stepName) + ",\"body\":" + body + ",\"compensate\":"
        + (compensate == null ? "null" : compensate) + ",\"dependencies\":[" + deps + "],\"effectCaps\":[],\"origin\":"
        + IrV2.ORIGIN + "}";
  }

  private static String program(String... stmts) {
    return IrV2.module("wf.scope", IrV2.func("run", List.of(), INT, List.of("io"), block(stmts)));
  }

  private static String workflow(String... steps) {
    return "{\"kind\":\"workflow\",\"steps\":[" + String.join(",", steps)
        + "],\"effectCaps\":[],\"retry\":null,\"timeout\":null,\"origin\":" + IrV2.ORIGIN + "}";
  }

  private static Value run(String json) throws Exception {
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value r = ctx.eval(Source.newBuilder("aster", json, "wf-scope.json").build());
      Value v = r.canExecute() ? r.execute() : r;
      return Value.asValue(v.isNumber() ? (Object) v.asInt() : null);
    }
  }

  @Test
  void stepLetShadowsOuterInsteadOfOverwritingIt() throws Exception {
    // 外层 x = 0；step 内 Let x = 1 只遮蔽。修复前沿用外层 slot，workflow 之后读到 1
    String json = program(
        let("x", num(0)),
        workflow(step("a", "", block(let("x", num(1)), ret(call("__recordStep", name("x")))), null)),
        ret(name("x")));
    assertEquals(0, run(json).asInt());
    assertEquals(List.of(1), recorded.stream().map(o -> ((Number) o).intValue()).toList());
  }

  @Test
  void stepWithoutLetWritesBackToOuterViaSet() throws Exception {
    // step 内没有同名 Let 时 Set 写回外层（与 case / Scope 相同）
    String json = program(
        let("x", num(0)),
        workflow(step("a", "", block(set("x", num(7)), ret(num(0))), null)),
        ret(name("x")));
    assertEquals(7, run(json).asInt());
  }

  @Test
  void parallelStepsWithSameNamedLetDoNotShareASlot() throws Exception {
    // 两个无依赖 step 并行声明同名 y：a 写 1 后暂停，b 在此期间写 2。修复前共用一个 slot，a 记下 2
    String json = program(
        workflow(
            step("a", "", block(let("y", num(1)), let("p", call("__pause", num(0))),
                ret(call("__recordStep", name("y")))), null),
            step("b", "", block(let("y", num(2)), ret(call("__recordStep", name("y")))), null)),
        ret(num(0)));
    run(json);
    assertEquals(List.of(1, 2), recorded.stream().map(o -> ((Number) o).intValue()).sorted().toList());
  }

  @Test
  void compensateLetShadowsStepAndOuterBindings() throws Exception {
    // a 完成后 b 失败，触发 a 的补偿。补偿体内 Let x 是本层绑定，读外层 base 仍可见
    String compensate = block(let("x", call("+", name("base"), num(5))), ret(call("__recordStep", name("x"))));
    String json = program(
        let("base", num(100)),
        let("x", num(0)),
        workflow(
            step("a", "", block(let("x", num(1)), ret(call("__recordStep", name("x")))), compensate),
            step("b", "\"a\"", block(ret(call("__failStep", num(0)))), null)),
        ret(name("x")));
    assertThrows(Exception.class, () -> run(json));
    assertEquals(List.of(1, 105), recorded.stream().map(o -> ((Number) o).intValue()).toList());
  }

  @Test
  void stepLocalIsNotVisibleToALaterStep() throws Exception {
    // step 体的 Let 只在本 step 内可见：b 读 a 的 v 不再取到 a 的局部 1（与 case 分支内 Let 出块不可见一致）。
    // 未声明的名字由类型检查拦截；运行期按既有的 NameNodeEnv 回退处理，这里只断言不再读到 a 的局部
    String json = program(
        workflow(
            step("a", "", block(let("v", num(1)), ret(name("v"))), null),
            step("b", "\"a\"", block(ret(call("__recordStep", name("v")))), null)),
        ret(num(0)));
    run(json);
    assertEquals(1, recorded.size(), "实际: " + recorded);
    assertFalse(Integer.valueOf(1).equals(recorded.peek()), "b 不应读到 a 的局部 v；实际: " + recorded);
  }
}
