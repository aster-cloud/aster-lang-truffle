package aster.truffle.nodes;

import aster.truffle.types.AsterTypes;
import com.oracle.truffle.api.dsl.TypeSystemReference;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

/**
 * Aster 表达式节点的抽象基类
 *
 * 所有表达式节点应继承此类并使用 @Specialization 注解提供类型特化实现。
 * Truffle DSL 会自动生成优化的执行路径。
 *
 * 已完成迁移：LiteralNode, NameNode, CallNode, LetNode, SetNode, ConstructNode,
 * LambdaNode, AwaitNode, IfNode, MatchNode, BlockNode 均已继承此基类。
 */
@TypeSystemReference(AsterTypes.class)
public abstract class AsterExpressionNode extends Node {

  /**
   * 执行此节点并返回结果（通用版本）
   *
   * @param frame 当前执行帧
   * @return 节点的执行结果（任意类型）
   */
  public abstract Object executeGeneric(VirtualFrame frame);

  /**
   * 执行此节点并返回 int 结果
   *
   * @param frame 当前执行帧
   * @return int 类型的结果
   * @throws UnexpectedResultException 如果结果不是 int 类型
   */
  public int executeInt(VirtualFrame frame) throws UnexpectedResultException {
    Object result = executeGeneric(frame);
    if (result instanceof Integer) {
      return (int) result;
    }
    throw new UnexpectedResultException(result);
  }

  /**
   * 执行此节点并返回 long 结果
   *
   * @param frame 当前执行帧
   * @return long 类型的结果
   * @throws UnexpectedResultException 如果结果不是 long 类型
   */
  public long executeLong(VirtualFrame frame) throws UnexpectedResultException {
    Object result = executeGeneric(frame);
    if (result instanceof Long) {
      return (long) result;
    }
    throw new UnexpectedResultException(result);
  }

  /**
   * 执行此节点并返回 double 结果
   *
   * @param frame 当前执行帧
   * @return double 类型的结果
   * @throws UnexpectedResultException 如果结果不是 double 类型
   */
  public double executeDouble(VirtualFrame frame) throws UnexpectedResultException {
    Object result = executeGeneric(frame);
    if (result instanceof Double) {
      return (double) result;
    }
    throw new UnexpectedResultException(result);
  }

  /**
   * 执行此节点并返回 boolean 结果
   *
   * @param frame 当前执行帧
   * @return boolean 类型的结果
   * @throws UnexpectedResultException 如果结果不是 boolean 类型
   */
  public boolean executeBoolean(VirtualFrame frame) throws UnexpectedResultException {
    Object result = executeGeneric(frame);
    if (result instanceof Boolean) {
      return (boolean) result;
    }
    throw new UnexpectedResultException(result);
  }

  /**
   * 执行此节点并返回 String 结果
   *
   * @param frame 当前执行帧
   * @return String 类型的结果
   * @throws UnexpectedResultException 如果结果不是 String 类型
   */
  public String executeString(VirtualFrame frame) throws UnexpectedResultException {
    Object result = executeGeneric(frame);
    if (result instanceof String) {
      return (String) result;
    }
    throw new UnexpectedResultException(result);
  }
}
