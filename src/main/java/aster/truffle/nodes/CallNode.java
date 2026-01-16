package aster.truffle.nodes;

import aster.truffle.runtime.AsterConfig;
import com.oracle.truffle.api.dsl.Bind;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import java.util.List;

/**
 * 函数调用节点
 * 支持：
 * 1. Lambda/closure调用 (通过 CallTarget + InvokeNode DSL 优化)
 * 2. Builtin函数调用（通过Builtins注册表）
 *
 * 性能优化：
 * - 使用 InvokeNode (Truffle DSL) 替代 IndirectCallNode
 * - 支持 Monomorphic/Polymorphic/Megamorphic 调用优化
 * - DirectCallNode 缓存实现内联和激进优化
 *
 * 内存优化：
 * - InvokeNode 使用 @GenerateInline 内联模式，内存占用从 28 字节降至 9 字节
 * - 使用 @Cached 注入内联节点，Node 参数自动绑定 $node (inlining target)
 */
public abstract class CallNode extends AsterExpressionNode {
  @Child protected Node target;
  @Children protected final Node[] args;

  protected CallNode(Node target, List<Node> args) {
    this.target = target;
    this.args = args.toArray(new Node[0]);
  }

  public static CallNode create(Node target, List<Node> args) {
    return CallNodeGen.create(target, args);
  }

  @SuppressWarnings({"truffle-static-method", "truffle-unused", "truffle-sharing", "truffle"})
  @Specialization
  protected Object doCall(
      VirtualFrame frame,
      @Bind("$node") Node node,
      @Cached(inline = true) InvokeNode invokeNode) {

    Profiler.inc("call");
    Object t = Exec.exec(target, frame);
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: call target=" + t + " (" + (t==null?"null":t.getClass().getName()) + ")");
    }

    // 1. Lambda/closure call via InvokeNode (with inline optimization)
    if (t instanceof LambdaValue lv) {
      Object[] av = new Object[args.length];
      for (int i = 0; i < args.length; i++) av[i] = Exec.exec(args[i], frame);
      if (AsterConfig.DEBUG) {
        System.err.println("DEBUG: call args=" + java.util.Arrays.toString(av));
      }

      // Use InvokeNode (Truffle DSL) for optimized CallTarget invocation
      // Supports DirectCallNode caching with monomorphic/polymorphic optimization
      com.oracle.truffle.api.CallTarget callTarget = lv.getCallTarget();
      if (callTarget != null) {
        // Pack arguments: [callArgs..., captureValues...]
        // LambdaRootNode expects captures at positions paramCount..paramCount+captureCount-1
        Object[] capturedValues = lv.getCapturedValues();
        Object[] packedArgs = new Object[av.length + capturedValues.length];
        System.arraycopy(av, 0, packedArgs, 0, av.length);
        System.arraycopy(capturedValues, 0, packedArgs, av.length, capturedValues.length);

        try {
          // 使用内联的 InvokeNode，实现节点对象内联优化（内存占用从 28 字节降至 9 字节）
          Object result = invokeNode.execute(node, callTarget, packedArgs);
          if (AsterConfig.DEBUG) {
            System.err.println("DEBUG: CallTarget result=" + result);
          }
          return result;
        } catch (ReturnNode.ReturnException r) {
          return r.value;
        }
      } else {
        // Fallback to apply() for legacy mode (non-CallTarget)
        return lv.apply(av, frame);
      }
    }

    // 2. Builtin function call
    String name = (t instanceof String) ? (String)t : null;
    if (name != null && aster.truffle.runtime.Builtins.has(name)) {
      Object[] av = new Object[args.length];
      for (int i = 0; i < args.length; i++) av[i] = Exec.exec(args[i], frame);
      try {
        Object result = aster.truffle.runtime.Builtins.call(name, av);
        if (result != null) return result;
      } catch (aster.truffle.runtime.Builtins.BuiltinException e) {
        // 转换为运行时异常，包含参数信息用于调试
        String argsInfo = "args=[";
        for (int i = 0; i < av.length; i++) {
          if (i > 0) argsInfo += ", ";
          Object arg = av[i];
          argsInfo += (arg == null ? "null" : arg.getClass().getSimpleName() + ":" + arg);
        }
        argsInfo += "]";
        throw new RuntimeException("Builtin call failed: " + name + " with " + argsInfo + " - " + e.getMessage(), e);
      }
    }

    // 3. Unknown call target - return null
    return null;
  }
}
