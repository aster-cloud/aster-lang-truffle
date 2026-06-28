package aster.truffle.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;

import java.util.ArrayList;
import java.util.List;

/**
 * 列表字面量节点（ADR 0024 C0）：{@code [a, b, c]}。
 *
 * 逐元素求值产出一个 {@link java.util.ArrayList}——与 List.* builtin 的运行时表示一致
 * （{@code List.empty} 返回 ArrayList、{@code asList} 接受 java.util.List），因此字面量
 * 构造的列表可被 List.length/get/map/filter/reduce 等直接消费。
 *
 * 取代旧的把 {@code [..]} 降成 {@code Construct("List",{0:..})} 伪 struct 的方案——后者在
 * {@link aster.truffle.Loader#buildConstruct} 查不到名为 "List" 的 Data 定义而运行时崩溃。
 */
public final class ListLiteralNode extends AsterExpressionNode {
  @Children private final AsterExpressionNode[] elementNodes;

  private ListLiteralNode(AsterExpressionNode[] elementNodes) {
    this.elementNodes = elementNodes;
  }

  public static ListLiteralNode create(AsterExpressionNode[] elementNodes) {
    return new ListLiteralNode(elementNodes);
  }

  @Override
  @ExplodeLoop
  public Object executeGeneric(VirtualFrame frame) {
    Profiler.inc("listLit");
    List<Object> out = new ArrayList<>(elementNodes.length);
    for (AsterExpressionNode el : elementNodes) {
      out.add(Exec.exec(el, frame));
    }
    return out;
  }
}
