package aster.truffle.nodes;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * #14 回归测试：
 *   1. 当 builtin 参数在求值时抛出控制流信号（ReturnNode.ReturnException，它
 *      extends ControlFlowException extends RuntimeException），该信号必须原样
 *      向上传播 —— 不得被快速路径的 catch 吞掉。
 *   2. builtin 的参数节点恰好求值一次 —— 带副作用的参数不得被二次执行。
 *
 * 测试不经过 Loader/JSON，而是直接用自定义 AsterExpressionNode 作为参数节点，
 * 把 BuiltinCallNode 包进一个最小 RootNode 后执行，以精确控制参数节点行为。
 */
public class BuiltinCallNodeControlFlowTest {

  /** 把任意节点包成可执行的 CallTarget。 */
  private static CallTarget wrap(AsterExpressionNode body) {
    RootNode root = new RootNode(null, new FrameDescriptor()) {
      @Override
      public Object execute(VirtualFrame frame) {
        return body.executeGeneric(frame);
      }
    };
    return root.getCallTarget();
  }

  /** 求值时抛出 ReturnException（控制流信号）的参数节点。 */
  private static final class ThrowsReturnNode extends AsterExpressionNode {
    @Override
    public Object executeGeneric(VirtualFrame frame) {
      throw new ReturnNode.ReturnException(99);
    }
  }

  /** 每次求值递增计数器、返回固定整数的参数节点。 */
  private static final class CountingIntNode extends AsterExpressionNode {
    final AtomicInteger counter;
    final int value;
    CountingIntNode(AtomicInteger counter, int value) { this.counter = counter; this.value = value; }
    @Override
    public Object executeGeneric(VirtualFrame frame) {
      counter.incrementAndGet();
      return value;
    }
  }

  /** 第一次求值返回 String（迫使 int 快速路径回退），并记录求值次数。 */
  private static final class CountingStringNode extends AsterExpressionNode {
    final AtomicInteger counter;
    final String value;
    CountingStringNode(AtomicInteger counter, String value) { this.counter = counter; this.value = value; }
    @Override
    public Object executeGeneric(VirtualFrame frame) {
      counter.incrementAndGet();
      return value;
    }
  }

  @Test
  public void controlFlowFromArgumentPropagates() {
    // eq(<return 99>, 1) —— 第一个参数抛出 ReturnException，必须传播而非被吞掉。
    AsterExpressionNode call = BuiltinCallNodeGen.create(
        "eq",
        new AsterExpressionNode[]{ new ThrowsReturnNode(), intLit(1) });
    CallTarget target = wrap(call);
    try {
      target.call();
      fail("expected ReturnException to propagate out of the builtin call");
    } catch (ReturnNode.ReturnException r) {
      assertEquals(99, r.value);
    }
  }

  @Test
  public void controlFlowFromSecondArgumentPropagates() {
    // eq(1, <return 99>) —— 控制流信号从第二个参数抛出时同样必须传播。
    AsterExpressionNode call = BuiltinCallNodeGen.create(
        "eq",
        new AsterExpressionNode[]{ intLit(1), new ThrowsReturnNode() });
    CallTarget target = wrap(call);
    try {
      target.call();
      fail("expected ReturnException to propagate out of the builtin call");
    } catch (ReturnNode.ReturnException r) {
      assertEquals(99, r.value);
    }
  }

  @Test
  public void sideEffectingArgumentEvaluatedExactlyOnceOnFastPath() {
    // eq(int, int) 命中整数快速路径：两个参数各求值一次。
    AtomicInteger c0 = new AtomicInteger();
    AtomicInteger c1 = new AtomicInteger();
    AsterExpressionNode call = BuiltinCallNodeGen.create(
        "eq",
        new AsterExpressionNode[]{ new CountingIntNode(c0, 3), new CountingIntNode(c1, 3) });
    Object result = wrap(call).call();
    assertEquals(Boolean.TRUE, result);
    assertEquals(1, c0.get(), "arg0 must be evaluated exactly once");
    assertEquals(1, c1.get(), "arg1 must be evaluated exactly once");
  }

  @Test
  public void sideEffectingArgumentNotDoubleEvaluatedOnFallback() {
    // Text.concat(String, int) —— 第二个参数非 String，快速路径回退到通用路径。
    // 修复前：通用路径会重新 executeGeneric 两个参数，导致副作用参数被求值两次。
    // 修复后：复用已求值的参数，每个参数恰好一次。
    AtomicInteger c0 = new AtomicInteger();
    AtomicInteger c1 = new AtomicInteger();
    AsterExpressionNode call = BuiltinCallNodeGen.create(
        "Text.concat",
        new AsterExpressionNode[]{
            new CountingStringNode(c0, "a"),
            new CountingIntNode(c1, 7) });
    Object result = wrap(call).call();
    assertTrue(result instanceof String, "Text.concat should produce a String");
    assertEquals(1, c0.get(), "string arg must be evaluated exactly once even on fallback");
    assertEquals(1, c1.get(), "non-string arg must be evaluated exactly once even on fallback");
  }

  private static AsterExpressionNode intLit(int v) {
    return new AsterExpressionNode() {
      @Override
      public Object executeGeneric(VirtualFrame frame) { return v; }
    };
  }
}
