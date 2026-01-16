package aster.truffle.runtime;

import aster.truffle.core.CoreModel;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * PII 类型辅助工具：统一处理类型检测与运行时包装。
 */
public final class PiiSupport {
  private PiiSupport() {}

  public static boolean containsPii(CoreModel.Type type) {
    return containsPii(type, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  private static boolean containsPii(CoreModel.Type type, Set<CoreModel.Type> visited) {
    if (type == null) return false;
    if (!visited.add(type)) return false;
    if (type instanceof CoreModel.PiiType) return true;
    if (type instanceof CoreModel.Result res) {
      return containsPii(res.ok, visited) || containsPii(res.err, visited);
    }
    if (type instanceof CoreModel.Maybe maybe) {
      return containsPii(maybe.type, visited);
    }
    if (type instanceof CoreModel.Option opt) {
      return containsPii(opt.type, visited);
    }
    if (type instanceof CoreModel.ListT list) {
      return containsPii(list.type, visited);
    }
    if (type instanceof CoreModel.MapT map) {
      return containsPii(map.key, visited) || containsPii(map.val, visited);
    }
    if (type instanceof CoreModel.TypeApp app) {
      if (containsPii(app.base, visited)) {
        return true;
      }
      if (app.args != null) {
        for (CoreModel.Type arg : app.args) {
          if (containsPii(arg, visited)) {
            return true;
          }
        }
      }
    }
    if (type instanceof CoreModel.FuncType func) {
      if (func.params != null) {
        for (CoreModel.Type param : func.params) {
          if (containsPii(param, visited)) {
            return true;
          }
        }
      }
      return containsPii(func.returnType, visited);
    }
    return false;
  }

  /**
   * 按声明类型对值进行包装，确保嵌套结构中的 PII 信息完整保留。
   */
  public static Object wrapValue(Object value, CoreModel.Type type) {
    if (type == null || value == null) {
      return value;
    }

    if (type instanceof CoreModel.PiiType pii) {
      if (value instanceof AsterPiiValue existing) {
        return existing.enrich(pii);
      }
      Object inner = wrapValue(value, pii.baseType);
      return new AsterPiiValue(inner, pii);
    }

    if (!containsPii(type)) {
      return value;
    }

    if (type instanceof CoreModel.Result res && value instanceof Map<?,?> rawMap) {
      return wrapResult(rawMap, res);
    }

    if (type instanceof CoreModel.Maybe maybe && value instanceof Map<?,?> rawMaybe) {
      return wrapOptional(rawMaybe, maybe.type);
    }

    if (type instanceof CoreModel.Option opt && value instanceof Map<?,?> rawOption) {
      return wrapOptional(rawOption, opt.type);
    }

    if (type instanceof CoreModel.ListT list && value instanceof List<?> rawList) {
      java.util.ArrayList<Object> wrapped = new ArrayList<>(rawList.size());
      for (Object elem : rawList) {
        wrapped.add(wrapValue(elem, list.type));
      }
      return wrapped;
    }

    if (type instanceof CoreModel.MapT mapT && value instanceof Map<?,?> rawMap) {
      java.util.LinkedHashMap<Object,Object> wrapped = new LinkedHashMap<>(rawMap.size());
      for (Map.Entry<?,?> entry : rawMap.entrySet()) {
        Object key = wrapValue(entry.getKey(), mapT.key);
        Object val = wrapValue(entry.getValue(), mapT.val);
        wrapped.put(key, val);
      }
      return wrapped;
    }

    return value;
  }

  @SuppressWarnings("unchecked")
  private static Map<String,Object> wrapResult(Map<?,?> input, CoreModel.Result res) {
    Map<String,Object> copy = copyMap(input);
    Object variant = copy.get("_type");
    if (!(variant instanceof String name)) {
      return copy;
    }
    if ("Ok".equals(name)) {
      copy.put("value", wrapValue(copy.get("value"), res.ok));
    } else if ("Err".equals(name)) {
      copy.put("value", wrapValue(copy.get("value"), res.err));
    }
    return copy;
  }

  private static Map<String,Object> wrapOptional(Map<?,?> input, CoreModel.Type innerType) {
    Map<String,Object> copy = copyMap(input);
    Object variant = copy.get("_type");
    if (Objects.equals(variant, "Some")) {
      copy.put("value", wrapValue(copy.get("value"), innerType));
    }
    return copy;
  }

  private static Map<String,Object> copyMap(Map<?,?> raw) {
    LinkedHashMap<String,Object> copy = new LinkedHashMap<>();
    for (Map.Entry<?,?> entry : raw.entrySet()) {
      if (entry.getKey() instanceof String key) {
        copy.put(key, entry.getValue());
      }
    }
    return copy;
  }
}
