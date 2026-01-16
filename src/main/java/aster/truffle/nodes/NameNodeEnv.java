package aster.truffle.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;

/**
 * 变量读取节点（Env 版本，过渡期保留）。
 *
 * 直接从 Env 读取变量值，供全局/内建符号使用。
 */
public final class NameNodeEnv extends AsterExpressionNode {
  private final String name;
  private final Env env;

  public NameNodeEnv(Env env, String name) {
    this.env = env;
    this.name = name;
  }

  @Override
  public Object executeGeneric(VirtualFrame frame) {
    Profiler.inc("name");
    Object value = env.get(name);
    // If not found in env, return the name itself (for builtin functions)
    return (value != null) ? value : name;
  }
}
