package aster.truffle.runtime;

import com.oracle.truffle.api.frame.FrameDescriptor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FrameSlotBuilderTest {

  @Test
  public void testParameterAllocation() {
    FrameSlotBuilder builder = new FrameSlotBuilder();

    builder.addParameter("x");
    builder.addParameter("y");

    assertEquals(0, builder.getSlotIndex("x"));
    assertEquals(1, builder.getSlotIndex("y"));
    assertEquals(2, builder.getSlotCount());
  }

  @Test
  public void testLocalVariableAllocation() {
    FrameSlotBuilder builder = new FrameSlotBuilder();

    builder.addParameter("x");
    builder.addLocal("temp");
    builder.addLocal("result");

    assertEquals(0, builder.getSlotIndex("x"));
    assertEquals(1, builder.getSlotIndex("temp"));
    assertEquals(2, builder.getSlotIndex("result"));
  }

  @Test
  public void testBuildFrameDescriptor() {
    FrameSlotBuilder builder = new FrameSlotBuilder();

    builder.addParameter("x");
    builder.addLocal("y");

    FrameDescriptor descriptor = builder.build();
    assertNotNull(descriptor);
    assertEquals(2, builder.getSlotCount());
  }

  @Test
  public void testUnknownVariable() {
    FrameSlotBuilder builder = new FrameSlotBuilder();
    builder.addParameter("x");

    assertThrows(IllegalArgumentException.class, () -> builder.getSlotIndex("unknown"));
  }
}
