package aster.truffle.nodes;

import aster.truffle.runtime.AsterConfig;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;

/**
 * 语句块节点 - 顺序执行子语句，保持 ReturnNode 快速返回。
 */
public abstract class BlockNode extends AsterExpressionNode {
  @Children private final Node[] statements;

  protected BlockNode(java.util.List<Node> statements) {
    this.statements = statements.toArray(new Node[0]);
  }

  public static BlockNode create(java.util.List<Node> statements) {
    return BlockNodeGen.create(statements);
  }

  @Specialization
  @ExplodeLoop
  protected Object doBlock(VirtualFrame frame) {
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: block size=" + statements.length);
    }
    for (int i = 0; i < statements.length; i++) {
      Node stmt = statements[i];
      if (AsterConfig.DEBUG) {
        System.err.println("DEBUG: stmt[" + i + "]=" + stmt.getClass().getSimpleName());
      }
      if (stmt instanceof ReturnNode returnNode) {
        return returnNode.execute(frame);
      }
      Exec.exec(stmt, frame);
    }
    return null;
  }
}
