package aster.truffle.runtime;

/**
 * Frame 槽位信息，用于在节点构建时传递槽位索引
 */
public final class FrameSlotInfo {
  private final int slotIndex;
  private final String variableName;

  public FrameSlotInfo(int slotIndex, String variableName) {
    this.slotIndex = slotIndex;
    this.variableName = variableName;
  }

  public int getSlotIndex() {
    return slotIndex;
  }

  public String getVariableName() {
    return variableName;
  }

  @Override
  public String toString() {
    return String.format("FrameSlot[%s -> %d]", variableName, slotIndex);
  }
}
