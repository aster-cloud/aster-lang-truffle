package aster.truffle.nodes;

import aster.truffle.runtime.AsterConfig;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

/**
 * LetNodeEnv - Env 回退版本，兼容未分配槽位的场景。
 */
public final class LetNodeEnv extends Node {
  private final String name;
  @Child private Node valueNode;
  private final Env env;

  public LetNodeEnv(String name, Node valueNode, Env env) {
    this.name = name;
    this.valueNode = valueNode;
    this.env = env;
  }

  public Object execute(VirtualFrame frame) {
    Profiler.inc("let");
    Object value = Exec.exec(valueNode, frame);
    env.set(name, value);
    if (AsterConfig.DEBUG) {
      System.err.println("DEBUG: let(env) " + name + "=" + value);
    }
    return value;
  }

  public String getName() {
    return name;
  }
}
