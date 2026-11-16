package aster.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** 严格 JSON 树（{@link StrictJson}）的小工具。 */
final class Json {

    private Json() {}

    /** 逐级取对象成员；中途不是对象或缺失返回 null。 */
    static Object at(Object tree, String... keys) {
        Object cur = tree;
        for (String k : keys) {
            if (!(cur instanceof Map<?, ?> m)) {
                return null;
            }
            cur = m.get(k);
        }
        return cur;
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> objects(Object list) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (list instanceof List<?> l) {
            for (Object x : l) {
                if (x instanceof Map<?, ?>) {
                    out.add((Map<String, Object>) x);
                }
            }
        }
        return out;
    }
}
