package aster.truffle.nodes;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class Env {
  private final Env parent;
  private final Map<String,Object> vars = new HashMap<>();

  public Env() {
    this(null);
  }

  private Env(Env parent) {
    this.parent = parent;
  }

  /**
   * 创建子环境，读取可向父环境回溯，写入仅影响当前作用域。
   */
  public Env createChild() { return new Env(this); }

  public Object get(String name) {
    if (vars.containsKey(name)) {
      return vars.get(name);
    }
    return parent != null ? parent.get(name) : null;
  }

  public void set(String name, Object v) {
    if (vars.containsKey(name)) {
      vars.put(name, v);
      return;
    }
    if (parent != null && parent.contains(name)) {
      parent.set(name, v);
      return;
    }
    vars.put(name, v);
  }

  public boolean contains(String name) {
    if (vars.containsKey(name)) return true;
    return parent != null && parent.contains(name);
  }

  public Set<String> getAllKeys() {
    LinkedHashSet<String> keys = new LinkedHashSet<>();
    collectKeys(keys);
    return Collections.unmodifiableSet(keys);
  }

  private void collectKeys(Set<String> keys) {
    if (parent != null) parent.collectKeys(keys);
    keys.addAll(vars.keySet());
  }
}
