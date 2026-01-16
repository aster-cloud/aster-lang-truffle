package aster.truffle.nodes;

import aster.truffle.runtime.AsterConfig;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

/**
 * SetNodeEnv - Env 回退版本，兼容未分配槽位的场景。
 */
public final class SetNodeEnv extends Node {
  private final String name;
  @Child private Node valueNode;
  private final Env env;

  public SetNodeEnv(String name, Node valueNode, Env env) {
    this.name = name;
    this.valueNode = valueNode;
    this.env = env;
  }

  public Object execute(VirtualFrame frame) {
    Profiler.inc("set");
    Object value = Exec.exec(valueNode, frame);
    env.set(name, value);
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: set(env) " + name + "=" + value);
    }
    return value;
  }

  public String getName() {
    return name;
  }
}
