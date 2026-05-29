package aster.truffle.runtime;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlotKind;
import java.util.HashMap;
import java.util.Map;

/**
 * Frame 槽位分配器，管理变量名到槽位索引的映射。
 *
 * 分配策略：
 * - 参数：索引 0..N-1
 * - 局部变量：索引 N 开始，按声明顺序
 */
public final class FrameSlotBuilder {
  private final FrameDescriptor.Builder descriptorBuilder;
  private final Map<String, Integer> variableToSlot = new HashMap<>();
  private int nextSlotIndex = 0;

  public FrameSlotBuilder() {
    this.descriptorBuilder = FrameDescriptor.newBuilder();
  }

  /**
   * 为参数分配槽位（必须首先调用）
   */
  public void addParameter(String name) {
    if (variableToSlot.containsKey(name)) {
      throw new IllegalStateException("Duplicate variable: " + name);
    }
    int slotIndex = nextSlotIndex++;
    // R32 P0：原 FrameSlotKind.Illegal 让 Truffle 在每次写入时强制 deopt-
    // and-tag。Aster 是动态语言，主流类型是 Object（含 Long / Boolean /
    // String 装箱）。用 Object 作为初始 kind 配合 setObject 写入，PE
    // 把 frame access 编译为单一 getObject，type profiling 由 SetNode /
    // LetNode 的 @Specialization 提供，整体编译质量优于 Illegal 起点。
    // 详见 GraalVM Truffle slot-kind tuning guide。
    descriptorBuilder.addSlot(FrameSlotKind.Object, name, null);
    variableToSlot.put(name, slotIndex);
  }

  /**
   * 为局部变量分配槽位
   */
  public void addLocal(String name) {
    if (variableToSlot.containsKey(name)) {
      System.err.println("Warning: Re-binding variable: " + name);
    }
    int slotIndex = nextSlotIndex++;
    // R32 P0：见 addParameter 注释，同因同果。
    descriptorBuilder.addSlot(FrameSlotKind.Object, name, null);
    variableToSlot.put(name, slotIndex);
  }

  /**
   * 获取变量的槽位索引
   */
  public int getSlotIndex(String name) {
    Integer index = variableToSlot.get(name);
    if (index == null) {
      throw new IllegalArgumentException("Unknown variable: " + name);
    }
    return index;
  }

  /**
   * 检查变量是否已分配
   */
  public boolean hasVariable(String name) {
    return variableToSlot.containsKey(name);
  }

  /**
   * 构建最终的 FrameDescriptor
   */
  public FrameDescriptor build() {
    return descriptorBuilder.build();
  }

  /**
   * 获取符号表（用于调试）
   */
  public Map<String, Integer> getSymbolTable() {
    return new HashMap<>(variableToSlot);
  }

  /**
   * 获取已分配的槽位数量
   */
  public int getSlotCount() {
    return nextSlotIndex;
  }
}
