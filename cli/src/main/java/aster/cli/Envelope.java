package aster.cli;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 各子命令 stdout 的一行值规范 JSON（aster-lang-test {@code conformance/package-v1/envelope.schema.json}）。
 *
 * <p>{@code run} 成功信封里的 {@code value} 单独按值规范 JSON 写出再拼入：值的嵌套层级从值本身算起（第 0 层），
 * 不受信封外层影响（VALUE-FORMS §2）。
 */
final class Envelope {

    private Envelope() {}

    static String ok(Meta meta, String valueJson) {
        return "{\"meta\":" + ValueJson.write(meta.toTree()) + ",\"ok\":true,\"value\":" + valueJson + "}\n";
    }

    static String error(ErrorCode code, Meta meta) {
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("code", code.name());
        err.put("message", code.message());
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("error", err);
        env.put("meta", meta.toTree());
        env.put("ok", false);
        return ValueJson.write(env) + "\n";
    }

    static String inspect(Meta meta, Map<String, Object> inspect) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("inspect", inspect);
        env.put("meta", meta.toTree());
        env.put("ok", true);
        return ValueJson.write(env) + "\n";
    }

    static String verify(Meta meta, List<Map<String, Object>> checks, boolean ok) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("checks", checks);
        env.put("meta", meta.toTree());
        env.put("ok", ok);
        return ValueJson.write(env) + "\n";
    }

    static Map<String, Object> check(String name, ErrorCode failure) {
        Map<String, Object> c = new LinkedHashMap<>();
        if (failure != null) {
            c.put("code", failure.name());
        }
        c.put("name", name);
        c.put("ok", failure == null);
        return c;
    }
}
