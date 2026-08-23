package aster.truffle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import aster.truffle.runtime.Builtins;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Option.map 别名验证（aster-lang-ts#122）。
 *
 * <p>背景：TS 引擎里 {@code Option.map} 与 {@code Maybe.map} 共用同一个 case，
 * 等于免费获得别名；JVM 侧逐个显式注册，于是 {@code Option.map} 长期缺失——
 * 同一段规则一边能跑、一边报 "Undefined function"，属双引擎分叉。
 *
 * <p>不走 Core IR fixture 的原因：TS 编译器把 {@code Some(21)} 降级成通用
 * {@code Call} 节点，而本引擎期望独立的 {@code Some} 节点（{@code CoreModel.Some}），
 * 于是端到端跑会先撞上 "Unknown call target: Some"——那是**另一条**分叉，
 * 与本别名无关（已用 {@code Maybe.map} 做对照组确认：对照同样失败）。
 * 故此处直接对 Builtins 注册表断言。
 */
class OptionMapAliasTest {

  /** 构造 {@code Some(v)} 的运行期表示（与 Builtins 内部一致：_type→value 的插入序 Map）。 */
  private static Map<String, Object> some(Object value) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("_type", "Some");
    m.put("value", value);
    return m;
  }

  @Test
  void optionMapIsRegistered() {
    assertTrue(Builtins.has("Maybe.map"), "Maybe.map 应已注册");
    assertTrue(Builtins.has("Option.map"), "Option.map 应已注册（此前 JVM 侧缺失 → 双引擎分叉）");
  }

  /**
   * ★锁住「复用同一实例」而非仅仅「行为相同」。
   *
   * <p>下面两条行为等价性断言**证明不了**这一点——交叉审查做过反事实验证：
   * 把别名换成逐字复制的实现体，那两条依然全绿。而复制体正是漂移的源头
   * （TS/JVM 分叉即由此而来）。故必须直接比较实例同一性。
   */
  @Test
  void optionMapReusesTheSameInstance() {
    assertSame(
        Builtins.defOf("Maybe.map"),
        Builtins.defOf("Option.map"),
        "Option.map 必须复用 Maybe.map 的同一个 BuiltinDef，而不是复制一份实现体");
  }

  @Test
  void optionMapRejectsNonLambdaLikeMaybeMap() throws Exception {
    // 两个名字对**同一种误用**必须给出同样的失败，作为行为等价的佐证；「同一实例」由 optionMapReusesTheSameInstance 单独锁死。
    // 传一个非 Lambda 的第二参：Maybe.map 与 Option.map 都应抛出同类错误。
    Object[] args = {some(21), "not-a-lambda"};

    Exception fromMaybe = null;
    try {
      Builtins.call("Maybe.map", args);
    } catch (Exception e) {
      fromMaybe = e;
    }

    Exception fromOption = null;
    try {
      Builtins.call("Option.map", args);
    } catch (Exception e) {
      fromOption = e;
    }

    assertTrue(fromMaybe != null && fromOption != null, "两者对非 Lambda 第二参都应失败");
    assertEquals(
        fromMaybe.getClass(), fromOption.getClass(), "Option.map 与 Maybe.map 的失败类型应一致");
    assertEquals(
        fromMaybe.getMessage(),
        fromOption.getMessage(),
        "Option.map 复用 Maybe.map 的实现，错误信息应逐字一致");
  }

  @Test
  void optionMapPassesNoneThrough() throws Exception {
    // None 分支不需要 Lambda（实现里先判 None 直接返回），故可直接验证正向行为。
    Map<String, Object> none = new LinkedHashMap<>();
    none.put("_type", "None");
    Object viaMaybe = Builtins.call("Maybe.map", new Object[] {none, "unused"});
    Object viaOption = Builtins.call("Option.map", new Object[] {none, "unused"});
    assertEquals(viaMaybe, viaOption, "None 穿透行为两名字应一致");
    assertEquals(none, viaOption, "None 应原样返回");
  }
}
