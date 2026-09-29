package aster.truffle;

import aster.truffle.runtime.Builtins;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 多个未命名 step 的 workflow 必须照常执行（issue #128）。
 *
 * <p>step 名在 Core IR 里并非必填（Loader 对 null step / 缺 name 都会产出 null）。
 * 原实现以 name 为键登记 taskId，判重跳过 null 但 put 不跳：第二个未命名 step 静默覆盖
 * 第一个，随后按名反查两次拿到同一 taskId、对它注册两次，报出的却是 registry 的
 * "Task already exists"。修复后按 step 下标登记，null name 不再是特殊情况。
 *
 * <p>走真实产品路径（polyglot Context → Loader → WorkflowNode → AsyncTaskRegistry），
 * 并用 host builtin 记录每个 step 实际执行时传入的标记，证明两个 step 各自执行、
 * 而非同一任务的结果被复制两份。
 */
class WorkflowUnnamedStepsTest {

  private final ConcurrentLinkedQueue<Object> recorded = new ConcurrentLinkedQueue<>();

  @BeforeEach
  void registerProbe() {
    // 与 WorkflowRegistrationRollbackTest 同一手法：guest 侧回调 host builtin 采集证据
    Builtins.register("__recordStep", new Builtins.BuiltinDef(args -> {
      recorded.add(args[0]);
      return args[0];
    }));
  }

  private String fixture(String name) throws Exception {
    try (InputStream in = getClass().getClassLoader()
        .getResourceAsStream("workflow-unnamed/" + name)) {
      assertNotNull(in, "fixture 缺失: " + name);
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private Value run(Context ctx, String json, String name) throws Exception {
    Value r = ctx.eval(Source.newBuilder("aster", json, name).build());
    return r.canExecute() ? r.execute() : r;
  }

  private Set<Integer> recordedInts() {
    return recorded.stream().map(o -> ((Number) o).intValue())
        .collect(java.util.stream.Collectors.toSet());
  }

  @Test
  void twoUnnamedStepsBothExecute() throws Exception {
    // ★核心回归：修复前恰好在出现第二个 null name 时报 "Task already exists: task-2"
    String json = fixture("two-unnamed-steps.json");

    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value v = assertDoesNotThrow(() -> run(ctx, json, "unnamed"),
          "两个未命名 step 不得互相覆盖导致重复注册");
      assertEquals(0, v.asInt());
    }
    assertEquals(Set.of(11, 22), recordedInts(), "两个未命名 step 必须各自执行一次");
    assertEquals(2, recorded.size(), "不得有 step 被重复执行");
  }

  @Test
  void namedAndUnnamedStepsCoexist() throws Exception {
    // issue 的对照实验：1 个具名 + 2 个未命名，修复前报 "Task already exists: task-3"
    String json = fixture("mixed-named-and-unnamed.json");

    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      assertEquals(0, assertDoesNotThrow(() -> run(ctx, json, "mixed")).asInt());
    }
    assertEquals(List.of(1, 11, 22), recorded.stream().map(o -> ((Number) o).intValue()).sorted().toList());
  }

  @Test
  void unnamedStepsLeaveNoRegistryResidue() throws Exception {
    // 清理按下标遍历：每个生成了 id 的 step 都必须被 removeTask，而不是只清「进了 map」的那些。
    // 否则未命名 step 在池化 Context 下逐次累积泄漏。跑三次后再由 host 侧探针读任务数。
    String json = fixture("two-unnamed-steps.json");
    final int[] captured = {-1};
    Builtins.register("__probeTaskCount", new Builtins.BuiltinDef(args -> {
      captured[0] = AsterLanguage.getContext().getAsyncRegistry().getTaskCount();
      return captured[0];
    }));
    String probe = "{\"name\":\"probe\",\"decls\":[{\"kind\":\"Func\",\"name\":\"run\",\"params\":[],"
        + "\"ret\":{\"kind\":\"TypeName\",\"name\":\"Int\"},\"effects\":[],\"body\":{\"kind\":\"Block\","
        + "\"statements\":[{\"kind\":\"Return\",\"expr\":{\"kind\":\"Call\",\"target\":{\"kind\":\"Name\","
        + "\"name\":\"__probeTaskCount\"},\"args\":[]}}]}}]}";

    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      for (int i = 0; i < 3; i++) {
        run(ctx, json, "unnamed" + i);
      }
      run(ctx, probe, "probe");
    }
    assertEquals(0, captured[0], "三次 workflow 后 registry 不得残留任务");
  }
}
