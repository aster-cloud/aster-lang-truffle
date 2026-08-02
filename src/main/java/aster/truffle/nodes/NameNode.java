package aster.truffle.nodes;

import aster.truffle.runtime.ErrorMessages;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

/**
 * 变量读取节点（Frame 版本），使用 Truffle DSL 类型特化。
 *
 * 通过 @Specialization 注解，Truffle DSL 自动生成类型特化代码：
 * - 优先尝试从 Frame 槽位读取类型化值（frame.getInt/getLong/getDouble）
 * - 当类型不匹配时（FrameSlotTypeException），自动重写为更通用的特化
 * - 最终回退到 frame.getObject() 或 Env 查找
 *
 * 配合 LetNode/SetNode 的类型化写入，可以充分利用 Truffle 的类型特化优化。
 */
public abstract class NameNode extends AsterExpressionNode {
  @CompilationFinal protected final String name;
  @CompilationFinal protected final int slotIndex;
  @Child protected EnvLookupNode envFallback;

  /**
   * Frame 模式：根据槽位索引读取变量。
   */
  protected NameNode(String name, int slotIndex) {
    this.name = name;
    this.slotIndex = slotIndex;
    this.envFallback = null;
  }

  /**
   * Env 回退模式：保留旧行为。
   */
  protected NameNode(String name, Env env) {
    this.name = name;
    this.slotIndex = -1;
    this.envFallback = (env != null) ? new EnvLookupNode(name, env) : null;
  }

  @Specialization(guards = "slotIndex >= 0", rewriteOn = FrameSlotTypeException.class)
  protected int readInt(VirtualFrame frame) throws FrameSlotTypeException {
    Profiler.inc("name_int");
    if (frame == null) {
      throw new RuntimeException(ErrorMessages.variableNotInitialized(name));
    }
    return frame.getInt(slotIndex);
  }

  @Specialization(guards = "slotIndex >= 0", rewriteOn = FrameSlotTypeException.class, replaces = "readInt")
  protected long readLong(VirtualFrame frame) throws FrameSlotTypeException {
    Profiler.inc("name_long");
    if (frame == null) {
      throw new RuntimeException(ErrorMessages.variableNotInitialized(name));
    }
    return frame.getLong(slotIndex);
  }

  @Specialization(guards = "slotIndex >= 0", rewriteOn = FrameSlotTypeException.class, replaces = {"readInt", "readLong"})
  protected double readDouble(VirtualFrame frame) throws FrameSlotTypeException {
    Profiler.inc("name_double");
    if (frame == null) {
      throw new RuntimeException(ErrorMessages.variableNotInitialized(name));
    }
    return frame.getDouble(slotIndex);
  }

  @Specialization(guards = "slotIndex >= 0", rewriteOn = FrameSlotTypeException.class, replaces = {"readInt", "readLong", "readDouble"})
  protected boolean readBoolean(VirtualFrame frame) throws FrameSlotTypeException {
    Profiler.inc("name_boolean");
    if (frame == null) {
      throw new RuntimeException(ErrorMessages.variableNotInitialized(name));
    }
    return frame.getBoolean(slotIndex);
  }

  @Specialization(guards = "slotIndex >= 0", replaces = {"readInt", "readLong", "readDouble", "readBoolean"})
  protected Object readObject(VirtualFrame frame) {
    Profiler.inc("name_object");
    if (frame == null) {
      throw new RuntimeException(ErrorMessages.variableNotInitialized(name));
    }
    // ★本特化是**终点**（无 rewriteOn），故必须能读任意 tag 的槽位，否则一旦到达即硬失败。
    //   槽位 tag 由**写入方**决定：LetNode/SetNode 的 writeLong/writeInt/... 用 frame.setLong()
    //   等类型化写入，会把 tag 从声明的 Object 改成对应原始类型。当同一变量在不同执行路径上
    //   先被原始类型写入、后又需要按 Object 读取（典型：递归函数的返回值绑定，先 readInt 撞
    //   FrameSlotTypeException → 沿 rewriteOn 链一路升级到本特化），getObject 就会抛异常。
    //   原实现把它转成 RuntimeException 直接终止求值——这正是 benchmarkBinaryTree 的
    //   「读取变量失败：leftSum @ slot 4」（见 #60）。
    //   ★不改写入侧（改成一律 setObject 会让所有原始类型装箱，损失 R32 P0 的类型特化收益）；
    //   在读取终点按实际 tag 取值即可，原始值在此自动装箱——语义与装箱写入完全一致。
    return getByActualKind(frame);
  }

  /**
   * 按槽位**实际** tag 读取。
   *
   * ★不能用 FrameDescriptor.getSlotKind() 判断：实测（#60）该值恒为声明时的 Object，
   *   而槽位真实持有 int —— 声明 kind 与实际 tag 是两回事，前者不随类型化写入更新。
   *   故改用 frame.getValue()，它按**实际** tag 取值并对原始类型自动装箱，
   *   是唯一能读任意 tag 的通用读法。
   */
  private Object getByActualKind(VirtualFrame frame) {
    try {
      return frame.getValue(slotIndex);
    } catch (RuntimeException ex) {
      throw new RuntimeException("读取变量失败：" + name + " @ slot " + slotIndex, ex);
    }
  }

  @Specialization(guards = "slotIndex < 0")
  protected Object readFromEnv(VirtualFrame frame) {
    Profiler.inc("name_env");
    if (envFallback != null) {
      return envFallback.execute(frame);
    }
    throw new RuntimeException("变量未找到：" + name);
  }

  public String getName() {
    return name;
  }

  /**
   * Env 查找节点，用于全局变量回退。
   */
  protected static final class EnvLookupNode extends Node {
    private final String name;
    private final Env env;

    protected EnvLookupNode(String name, Env env) {
      this.name = name;
      this.env = env;
    }

    protected Object execute(VirtualFrame frame) {
      return env.get(name);
    }
  }
}
