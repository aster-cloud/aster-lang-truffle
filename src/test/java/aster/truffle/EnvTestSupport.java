package aster.truffle;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * 测试环境变量辅助类，提供便捷的快照、恢复与设置能力，方便各测试类共享。
 */
final class EnvTestSupport {

  private EnvTestSupport() {
  }

  static Map<String, String> snapshotEnv() {
    return new HashMap<>(System.getenv());
  }

  static void restoreEnv(Map<String, String> env) throws Exception {
    setEnv(new HashMap<>(env));
  }

  static void setEnvVar(String key, String value) throws Exception {
    Map<String, String> newEnv = new HashMap<>(System.getenv());
    if (value == null) {
      newEnv.remove(key);
    } else {
      newEnv.put(key, value);
    }
    setEnv(newEnv);
  }

  @SuppressWarnings("unchecked")
  private static void setEnv(Map<String, String> newEnv) throws Exception {
    try {
      Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
      Field envField = pe.getDeclaredField("theEnvironment");
      envField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) envField.get(null);
      env.clear();
      env.putAll(newEnv);
      Field cienvField = pe.getDeclaredField("theCaseInsensitiveEnvironment");
      cienvField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) cienvField.get(null);
      cienv.clear();
      cienv.putAll(newEnv);
    } catch (NoSuchFieldException e) {
      Field m = System.getenv().getClass().getDeclaredField("m");
      m.setAccessible(true);
      Map<String, String> env = (Map<String, String>) m.get(System.getenv());
      env.clear();
      env.putAll(newEnv);
    }
  }
}
