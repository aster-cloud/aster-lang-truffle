package aster.truffle.nodes;

import aster.truffle.core.CoreModel;
import aster.truffle.runtime.AsterDataValue;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import java.util.Map;

public abstract class ConstructNode extends AsterExpressionNode {
  private final String typeName;
  private final String[] fieldNames;
  private final CoreModel.Data definition;
  @Children private final AsterExpressionNode[] fieldNodes;

  protected ConstructNode(String typeName, Map<String, AsterExpressionNode> fields, CoreModel.Data definition) {
    this.typeName = typeName;
    this.fieldNames = fields.keySet().toArray(new String[0]);
    this.fieldNodes = fields.values().toArray(new AsterExpressionNode[0]);
    this.definition = definition;
  }

  public static ConstructNode create(String typeName, Map<String, AsterExpressionNode> fields, CoreModel.Data definition) {
    return ConstructNodeGen.create(typeName, fields, definition);
  }

  @Specialization
  @ExplodeLoop
  protected Object doConstruct(VirtualFrame frame) {
    Profiler.inc("construct");
    Object[] values = new Object[fieldNames.length];
    for (int i = 0; i < fieldNames.length; i++) {
      values[i] = fieldNodes[i].executeGeneric(frame);
    }
    return new AsterDataValue(typeName, fieldNames, values, definition);
  }
}
