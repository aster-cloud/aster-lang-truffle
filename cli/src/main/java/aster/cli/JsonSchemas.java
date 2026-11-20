package aster.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * manifest / 签名文件的 JSON Schema 2020-12 校验（networknt，与 core 同为 2.x）。schema 是 aster-lang-test {@code conformance/package-v1/schema/}
 * 的逐字节副本（{@code SchemaParityTest} 守住）。schema 中的模式一律以 {@code (?![\s\S])} 收尾，java.util.regex 的
 * {@code find()} 下也不会接受末尾换行（README「正则语义」）。
 */
final class JsonSchemas {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static final Schema MANIFEST = load("manifest-1.0.schema.json");
    /** formatVersion 1.1（ADR 0059 修订 4 D2）：1.0 + 可选 display。 */
    static final Schema MANIFEST_1_1 = load("manifest-1.1.schema.json");
    static final Schema SIG = load("sig-1.0.schema.json");

    private JsonSchemas() {}

    private static Schema load(String name) {
        try (InputStream in = JsonSchemas.class.getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException("缺少内置 schema " + name);
            }
            JsonNode node = MAPPER.readTree(in);
            // 与 core CoreIrJson 相同的 networknt 2.x 用法（版本随 core 传递依赖）
            return SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
                .getSchema(SchemaLocation.of(node.path("$id").asText()), node);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 严格 JSON 树（{@link StrictJson}）是否符合 schema；不符合时返回首条错误，符合返回 null。 */
    static String firstError(Schema schema, Object tree) {
        JsonNode node = MAPPER.valueToTree(tree);
        return schema.validate(node).stream().map(Object::toString).findFirst().orElse(null);
    }
}
