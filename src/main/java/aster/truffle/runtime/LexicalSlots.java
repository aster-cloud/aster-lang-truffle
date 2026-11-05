package aster.truffle.runtime;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * 一个 frame（规则体或 lambda 体）在构建期的词法作用域链：名字 → frame 槽位。
 *
 * <p>最外层是 frame 本身（形参、捕获变量与规则体顶层的 Let）。case 分支（含模式绑定）、Scope、workflow 的 step 体与 compensate 体
 * 各开一层，离开时弹出（ADR 0052 §2.1）。If 分支不开层：它与所在块共享作用域（E104 规则）。
 *
 * <p>声明（Let、模式绑定）只写当前层：同层重复声明沿用原槽位；外层已有同名绑定时分配新槽位，
 * 即遮蔽——之后本层的读写都落在新槽位上，弹出后外层槽位原值可见。查找（读、Set）由内向外取最近一层，
 * 所以没有同名声明的块里 {@code Set} 写回外层。
 *
 * <p>与 aster-lang-ts 解释器的 {@code Env.child()} 链同构，只是在构建期一次解析到槽位，
 * 每次调用的值都在自己的 frame 里，递归与并发调用互不干扰。
 */
public final class LexicalSlots {
  private final FrameSlotBuilder slots;
  private final Deque<Map<String, Integer>> layers = new ArrayDeque<>();

  /**
   * @param slots 该 frame 的槽位分配器（形参、捕获变量须已分配）
   * @param base  最外层已有的绑定（形参、捕获变量）及其槽位
   */
  public LexicalSlots(FrameSlotBuilder slots, Map<String, Integer> base) {
    this.slots = slots;
    layers.push(new HashMap<>(base));
  }

  /** 进入一层块作用域（case 分支、Scope、workflow 的 step / compensate 体）。 */
  public void enter() {
    layers.push(new HashMap<>());
  }

  /** 离开 {@link #enter} 打开的那一层；最外层不可弹出。 */
  public void exit() {
    if (layers.size() <= 1) throw new IllegalStateException("cannot exit the frame layer");
    layers.pop();
  }

  /** 在当前层声明 name，返回其槽位。 */
  public int declare(String name) {
    Map<String, Integer> current = layers.peek();
    Integer existing = current.get(name);
    if (existing != null) return existing;
    int slot = slots.addLocal(uniqueKey(name));
    current.put(name, slot);
    return slot;
  }

  /** 由内向外查找 name 最近的绑定槽位；不是局部名（全局函数、未定义名）时返回 null。 */
  public Integer lookup(String name) {
    for (Map<String, Integer> layer : layers) {
      Integer slot = layer.get(name);
      if (slot != null) return slot;
    }
    return null;
  }

  /** 槽位名只用于调试显示：首次用原名，遮蔽绑定加 {@code #n} 后缀，避免分配器的重名告警。 */
  private String uniqueKey(String name) {
    if (!slots.hasVariable(name)) return name;
    int n = 2;
    while (slots.hasVariable(name + "#" + n)) n++;
    return name + "#" + n;
  }
}
