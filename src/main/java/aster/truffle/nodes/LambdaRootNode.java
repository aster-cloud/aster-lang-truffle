package aster.truffle.nodes;

import aster.truffle.AsterLanguage;
import aster.truffle.core.CoreModel;
import aster.truffle.runtime.AsterConfig;
import aster.truffle.runtime.PiiSupport;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.RootNode;

/**
 * Lambda 函数的 RootNode
 *
 * 每个 Lambda 都有独立的 RootNode 和 CallTarget,支持:
 * 1. 独立的 Frame 和 FrameDescriptor
 * 2. 参数通过 Frame arguments 传递
 * 3. 闭包变量通过 captures 数组传递
 * 4. JIT 优化和内联
 */
public final class LambdaRootNode extends RootNode {
  @CompilationFinal private final String name;
  @CompilationFinal private final int paramCount;
  @CompilationFinal private final int captureCount;
  @CompilationFinal(dimensions = 1) private final CoreModel.Type[] paramTypes;
  @Child private com.oracle.truffle.api.nodes.Node bodyNode;

  /**
   * 创建 Lambda RootNode
   *
   * @param language AsterLanguage 实例
   * @param frameDescriptor Frame 描述符(包含参数和闭包变量槽位)
   * @param name Lambda 名称(用于调试)
   * @param paramCount 参数数量
   * @param captureCount 闭包变量数量
   * @param bodyNode Lambda 函数体节点
   */
  public LambdaRootNode(
      AsterLanguage language,
      FrameDescriptor frameDescriptor,
      String name,
      int paramCount,
      int captureCount,
      com.oracle.truffle.api.nodes.Node bodyNode,
      CoreModel.Type[] paramTypes
  ) {
    super(language, frameDescriptor);
    this.name = name;
    this.paramCount = paramCount;
    this.captureCount = captureCount;
    this.bodyNode = bodyNode;
    this.paramTypes = paramTypes == null ? new CoreModel.Type[0] : paramTypes.clone();
  }

  @Override
  public Object execute(VirtualFrame frame) {
    Profiler.inc("lambda_execute");

    Object[] args = frame.getArguments();
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: lambda_execute name=" + name +
          ", paramCount=" + paramCount +
          ", captureCount=" + captureCount +
          ", args.length=" + (args != null ? args.length : 0));
    }

    // 将参数写入 Frame slots (槽位 0..paramCount-1)
    if (args != null) {
      bindParameters(frame, args);
      bindCaptures(frame, args);
    }

    try {
      Object result = Exec.exec(bodyNode, frame);
      if (AsterConfig.DEBUG) {
        System.err.println("DEBUG: lambda body returned=" + result);
      }
      return result;
    } catch (ReturnNode.ReturnException r) {
      return r.value;
    }
  }

  /**
   * 将参数绑定到 Frame 槽位
   * @ExplodeLoop 展开循环以优化 JIT 编译
   */
  @ExplodeLoop
  private void bindParameters(VirtualFrame frame, Object[] args) {
    // 边界检查：确保有足够的参数
    int expectedMinLength = paramCount + captureCount;
    if (args.length < expectedMinLength) {
      throw new IllegalArgumentException(
          "Lambda " + name + " expects at least " + expectedMinLength +
          " arguments (params=" + paramCount + ", captures=" + captureCount +
          "), but got " + args.length);
    }

    for (int i = 0; i < paramCount; i++) {
      Object arg = args[i];
      CoreModel.Type expectedType = (i < paramTypes.length) ? paramTypes[i] : null;
      if (expectedType != null) {
        arg = PiiSupport.wrapValue(arg, expectedType);
      }
      frame.setObject(i, arg);
      if (AsterConfig.DEBUG) {
        System.err.println("DEBUG: lambda param slot[" + i + "]=" + arg);
      }
    }
  }

  /**
   * 将闭包变量绑定到 Frame 槽位
   * @ExplodeLoop 展开循环以优化 JIT 编译
   */
  @ExplodeLoop
  private void bindCaptures(VirtualFrame frame, Object[] args) {
    // 闭包值通过 arguments[paramCount..paramCount+captureCount-1] 传递
    for (int i = 0; i < captureCount; i++) {
      int captureSlot = paramCount + i;
      frame.setObject(captureSlot, args[paramCount + i]);
      if (AsterConfig.DEBUG) {
        System.err.println("DEBUG: lambda capture slot[" + captureSlot + "]=" + args[paramCount + i]);
      }
    }
  }

  @Override
  public String getName() {
    return name;
  }
}
