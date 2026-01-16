package aster.truffle.runtime.interop;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 将运行时产生的 Java 对象转换为 Truffle 可互操作的值。
 */
public final class AsterInteropAdapter {
  private AsterInteropAdapter() {}

  public static Object adapt(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof AsterListValue || value instanceof AsterMapValue) {
      return value;
    }

    if (value instanceof List<?> list) {
      List<Object> adapted = new ArrayList<>(list.size());
      for (Object element : list) {
        adapted.add(adapt(element));
      }
      return new AsterListValue(adapted);
    }

    if (value instanceof Map<?, ?> map) {
      Map<String, Object> adapted = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        String key = String.valueOf(entry.getKey());
        adapted.put(key, adapt(entry.getValue()));
      }
      return new AsterMapValue(adapted);
    }

    return value;
  }
}
