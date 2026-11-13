package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.junit.jupiter.api.Test;

import java.util.List;

import static aster.truffle.IrV2.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Loader 严格读入 Core IR v2（ADR 0055 §5）：解析为树 → core {@code CoreIrJson.validate}
 * （版本预检、schema、不变式）→ 严格绑定。任一步失败都抛 {@link IllegalArgumentException}，不执行。
 */
class LoaderV2Test {

    /** 合法的最小模块：main 返回给定表达式。 */
    private static String returning(String expr) {
        return module("m", func("main", List.of(), typeName("Int"), List.of(), block(ret(expr))));
    }

    private static IllegalArgumentException rejected(String json) {
        return assertThrows(IllegalArgumentException.class, () -> Loader.readModule(json));
    }

    @Test
    void accepts_minimal_v2_module() {
        assertEquals("m", Loader.readModule(returning(intLit(1))).name);
    }

    @Test
    void loader_rejects_unknown_field() {
        String json = returning(intLit(1)).replace("\"kind\":\"Int\",", "\"kind\":\"Int\",\"extra\":1,");
        IllegalArgumentException e = rejected(json);
        assertTrue(e.getMessage().startsWith("invalid_request"), e.getMessage());
    }

    @Test
    void loader_rejects_schema_version_1() {
        IllegalArgumentException e = rejected(returning(intLit(1)).replace("\"schemaVersion\":2", "\"schemaVersion\":1"));
        assertTrue(e.getMessage().startsWith("unsupported_schema_version"), e.getMessage());
    }

    @Test
    void loader_rejects_missing_or_string_schema_version() {
        String json = returning(intLit(1));
        assertTrue(rejected(json.replace("\"schemaVersion\":2,", "")).getMessage().startsWith("unsupported_schema_version"));
        assertTrue(rejected(json.replace("\"schemaVersion\":2", "\"schemaVersion\":\"2\"")).getMessage()
            .startsWith("unsupported_schema_version"));
    }

    @Test
    void loader_rejects_missing_required_field() {
        IllegalArgumentException e = rejected(returning(intLit(1)).replace("\"effectParams\":[],", ""));
        assertTrue(e.getMessage().startsWith("invalid_request"), e.getMessage());
        assertTrue(e.getMessage().contains("schema"), e.getMessage());
    }

    @Test
    void loader_rejects_dup_annotation_arg() {
        String json = returning(intLit(1)).replace("\"annotations\":[],\"retAnnotations\"",
            "\"annotations\":[{\"name\":\"id\",\"args\":[{\"name\":\"$0\",\"value\":\"a\"},{\"name\":\"$0\",\"value\":\"b\"}]}],"
                + "\"retAnnotations\"");
        IllegalArgumentException e = rejected(json);
        assertTrue(e.getMessage().contains("dup-annotation-arg"), e.getMessage());
    }

    @Test
    void loader_rejects_duplicate_json_key() {
        IllegalArgumentException e = rejected(returning(intLit(1)).replace("\"name\":\"m\"", "\"name\":\"m\",\"name\":\"n\""));
        assertTrue(e.getMessage().startsWith("invalid_request"), e.getMessage());
    }

    @Test
    void loader_rejects_noncanonical_double() {
        // 规范十进制串不允许尾随零（schema pattern）
        assertTrue(rejected(returning(doubleLit("1.50"))).getMessage().startsWith("invalid_request"));
    }

    @Test
    void polyglot_eval_rejects_v1_ir() {
        String v1 = "{\"name\":\"m\",\"decls\":[{\"kind\":\"Func\",\"name\":\"main\",\"params\":[],"
            + "\"ret\":{\"kind\":\"TypeName\",\"name\":\"Int\"},\"effects\":[],"
            + "\"body\":{\"kind\":\"Block\",\"statements\":[{\"kind\":\"Return\",\"expr\":{\"kind\":\"Int\",\"value\":1}}]}}]}";
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            PolyglotException e = assertThrows(PolyglotException.class,
                () -> ctx.eval(Source.newBuilder("aster", v1, "v1.json").build()));
            assertTrue(e.getMessage().contains("unsupported_schema_version"), e.getMessage());
        }
    }

    @Test
    void double_string_evaluates() throws Exception {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertEquals(0.1, ctx.eval(Source.newBuilder("aster", returning(doubleLit("0.1")), "d1.json").build()).asDouble());
            assertEquals(1.0E-7,
                ctx.eval(Source.newBuilder("aster", returning(doubleLit("0.0000001")), "d2.json").build()).asDouble());
            // 超出 double 精度的规范串照常求值（只在求值时转 double）
            assertEquals(Double.parseDouble("123456789012345678901.5"),
                ctx.eval(Source.newBuilder("aster", returning(doubleLit("123456789012345678901.5")), "d3.json").build())
                    .asDouble());
        }
    }

    @Test
    void long_string_evaluates() throws Exception {
        String json = returning("{\"kind\":\"Long\",\"value\":\"9007199254740993\",\"origin\":" + ORIGIN + "}");
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            assertEquals(9007199254740993L, ctx.eval(Source.newBuilder("aster", json, "l.json").build()).asLong());
        }
    }
}
