package aster.truffle.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

/**
 * 调用目标与单段 builtin 同名、且作用域内有同名局部（Let、形参、模式绑定、捕获变量）时的调用节点。
 *
 * <p>局部的值在运行期才知道是不是函数：是 {@link LambdaValue} 就按普通调用执行（{@code callPath}），
 * 否则执行 builtin（{@code builtinPath}，含 and / or 的短路节点）。两条路径各有一份实参子树，每次只执行其中一条，
 * 实参只求值一次。与 aster-lang-ts 解释器（ts#273）、aster-lang-core 类型检查（core#231：builtin 同名的
 * 非函数局部不报 E001）一致。没有同名局部时 Loader 仍直接构建 builtin 节点，不经过本节点。
 */
public final class LocalOrBuiltinCallNode extends AsterExpressionNode {
  @Child private AsterExpressionNode local;
  @Child private Node callPath;
  @Child private AsterExpressionNode builtinPath;

  public LocalOrBuiltinCallNode(AsterExpressionNode local, Node callPath, AsterExpressionNode builtinPath) {
    this.local = local;
    this.callPath = callPath;
    this.builtinPath = builtinPath;
  }

  @Override
  public Object executeGeneric(VirtualFrame frame) {
    Object value = local.executeGeneric(frame);
    if (value instanceof LambdaValue) {
      return Exec.exec(callPath, frame);
    }
    return builtinPath.executeGeneric(frame);
  }
}
