package aster.cli;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** aster-lang-test {@code conformance/package-v1/vectors/} 与逐字节副本的一致性。 */
class VectorsTest {

    @Test
    void schemaCopiesAreByteIdentical() throws Exception {
        for (String name : new String[]{"manifest-1.0.schema.json", "manifest-1.1.schema.json", "sig-1.0.schema.json"}) {
            byte[] truth = Files.readAllBytes(Fixtures.PACKAGE_V1.resolve("schema").resolve(name));
            byte[] copy = AsterCli.class.getResourceAsStream(name).readAllBytes();
            assertArrayEquals(truth, copy, name + " 副本与真相源不一致");
        }
    }

    @Test
    void exitCodesMatchTruthSource() throws Exception {
        // 本 CLI 支持 1.0 与 1.1：完整表 = codes ∪ codesSince["1.1"]（aster-lang-test README「输出」）
        com.fasterxml.jackson.databind.node.ObjectNode codes = Fixtures.json("exit-codes.json").path("codes").deepCopy();
        codes.setAll((com.fasterxml.jackson.databind.node.ObjectNode) Fixtures.json("exit-codes.json").path("codesSince").path("1.1"));
        assertEquals(codes.size(), ErrorCode.values().length);
        for (ErrorCode c : ErrorCode.values()) {
            assertEquals(codes.path(c.name()).path("exit").asInt(), c.exit(), c.name());
            assertEquals(codes.path(c.name()).path("message").asText(), c.message(), c.name());
        }
    }

    @Test
    void valueJsonVectors() throws Exception {
        JsonNode doc = Fixtures.json("vectors/value-json.json");
        int n = 0;
        for (JsonNode v : doc.path("vectors")) {
            String name = v.path("name").asText();
            Object tree = valueTree(v.path("treeJson").asText());
            if (v.path("error").asBoolean(false)) {
                assertThrows(ValueJson.ValueJsonException.class, () -> ValueJson.write(tree), name);
            } else {
                assertEquals(v.path("expected").asText(), ValueJson.write(tree), name);
            }
            n++;
        }
        assertTrue(n > 30, "value-json 向量过少");
        for (String group : new String[]{"randomDoubles", "randomPlainDoubles"}) {
            for (JsonNode item : doc.path(group).path("items")) {
                double d = ByteBuffer.wrap(HexFormat.of().parseHex(item.path("bits").asText())).getDouble();
                assertEquals(item.path("expected").asText(), ValueJson.number(d), group + " " + item.path("bits").asText());
            }
        }
    }

    /**
     * 向量的值树：整数 token 为 BigInteger、其余数字为 double；不用 {@link StrictJson}——值树可以含孤立代理项
     * （向量 text-lone-surrogate 要求的是写出失败，而不是读入失败）。
     */
    private static Object valueTree(String json) throws Exception {
        return Fixtures.MAPPER.readerFor(Object.class)
            .with(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
            .readValue(json);
    }

    @Test
    void stdoutNormalizeVectors() throws Exception {
        for (JsonNode v : Fixtures.json("vectors/stdout-normalize.json").path("vectors")) {
            assertEquals(v.path("expected").asText(), StdoutNormalizer.normalize(v.path("stdout").asText()),
                v.path("name").asText());
        }
    }

    @Test
    void signingVectors() throws Exception {
        JsonNode doc = Fixtures.json("vectors/signing.json");
        TrustAnchor anchor = TrustAnchor.fromJwks(Files.readAllBytes(Fixtures.PACKAGE_V1.resolve(doc.path("trustAnchor").asText())));
        for (JsonNode v : doc.path("vectors")) {
            String name = v.path("name").asText();
            byte[] manifest = Base64.getUrlDecoder().decode(v.path("manifestFileB64u").asText());
            byte[] sig = Base64.getUrlDecoder().decode(v.path("sigFileB64u").asText());
            String code = null;
            try {
                ManifestCheck.canonicalTree(manifest);
                SignatureVerifier.verify(sig, manifest, anchor, false);
            } catch (CliException e) {
                code = e.code().name();
            }
            String expected = v.path("code").isNull() ? null : v.path("code").asText();
            assertEquals(expected, code, name);
            assertEquals(v.path("valid").asBoolean(), code == null, name);
            if (!v.path("canonicalUtf8B64u").isNull() && code == null) {
                assertEquals(new String(manifest, StandardCharsets.UTF_8),
                    new String(Base64.getUrlDecoder().decode(v.path("canonicalUtf8B64u").asText()), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void timeoutSyntax() throws Exception {
        assertEquals(300_000, AsterCli.parseTimeout("300000ms").toMillis());
        assertEquals(5_000, AsterCli.parseTimeout(null).toMillis());
        for (String bad : new String[]{"300001ms", "0s", "1.5s", "01s", "5", "1s\n", "99999999999s"}) {
            CliException e = assertThrows(CliException.class, () -> AsterCli.parseTimeout(bad), bad);
            assertEquals(ErrorCode.USAGE, e.code());
        }
    }
}
