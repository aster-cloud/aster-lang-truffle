package aster.truffle.nodes;

import aster.truffle.runtime.AsterConfig;
import aster.truffle.runtime.ErrorMessages;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

/**
 * Set 语句节点（Frame 版本），使用 Truffle DSL 类型特化。
 *
 * 通过 @Specialization 注解，Truffle DSL 自动生成类型特化代码：
 * - 当 valueNode 返回 int 时，使用 frame.setInt() 写入
 * - 当 valueNode 返回 long 时，使用 frame.setLong() 写入
 * - 当 valueNode 返回 double 时，使用 frame.setDouble() 写入
 * - 当 valueNode 返回其他类型时，使用 frame.setObject() 写入
 *
 * 这样可以充分利用 Truffle 的类型特化优化，在运行时根据实际值类型选择最优的存储路径。
 */
@NodeChild(value = "valueNode", type = AsterExpressionNode.class)
public abstract class SetNode extends AsterExpressionNode {
  protected final String name;
  protected final int slotIndex;

  protected SetNode(String name, int slotIndex) {
    this.name = name;
    this.slotIndex = slotIndex;
  }

  @Specialization
  protected int writeInt(VirtualFrame frame, int value) {
    Profiler.inc("set_int");
    if (frame == null) {
      throw new RuntimeException(ErrorMessages.variableNotInitialized(name));
    }
    frame.setInt(slotIndex, value);
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: set " + name + "=" + value + " @slot " + slotIndex + " (int)");
    }
    return value;
  }

  @Specialization
  protected long writeLong(VirtualFrame frame, long value) {
    Profiler.inc("set_long");
    if (frame == null) {
      throw new RuntimeException(ErrorMessages.variableNotInitialized(name));
    }
    frame.setLong(slotIndex, value);
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: set " + name + "=" + value + " @slot " + slotIndex + " (long)");
    }
    return value;
  }

  @Specialization
  protected double writeDouble(VirtualFrame frame, double value) {
    Profiler.inc("set_double");
    if (frame == null) {
      throw new RuntimeException(ErrorMessages.variableNotInitialized(name));
    }
    frame.setDouble(slotIndex, value);
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: set " + name + "=" + value + " @slot " + slotIndex + " (double)");
    }
    return value;
  }

  @Specialization
  protected boolean writeBoolean(VirtualFrame frame, boolean value) {
    Profiler.inc("set_boolean");
    if (frame == null) {
      throw new RuntimeException(ErrorMessages.variableNotInitialized(name));
    }
    frame.setBoolean(slotIndex, value);
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: set " + name + "=" + value + " @slot " + slotIndex + " (boolean)");
    }
    return value;
  }

  @Specialization
  protected String writeString(VirtualFrame frame, String value) {
    Profiler.inc("set_string");
    if (frame == null) {
      throw new RuntimeException(ErrorMessages.variableNotInitialized(name));
    }
    frame.setObject(slotIndex, value);
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: set " + name + "=" + value + " @slot " + slotIndex + " (String)");
    }
    return value;
  }

  @Specialization(replaces = {"writeInt", "writeLong", "writeDouble", "writeBoolean", "writeString"})
  protected Object writeObject(VirtualFrame frame, Object value) {
    Profiler.inc("set_object");
    if (frame == null) {
      throw new RuntimeException(ErrorMessages.variableNotInitialized(name));
    }
    frame.setObject(slotIndex, value);
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: set " + name + "=" + value + " @slot " + slotIndex + " (Object)");
    }
    return value;
  }

  public String getName() {
    return name;
  }
}
