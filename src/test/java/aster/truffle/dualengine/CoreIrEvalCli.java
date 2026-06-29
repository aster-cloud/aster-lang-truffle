package aster.truffle.dualengine;

import aster.core.canonicalizer.Canonicalizer;
import aster.core.ir.CoreModel;
import aster.core.lowering.CoreLowering;
import aster.core.parser.AstBuilder;
import aster.core.parser.AsterCustomLexer;
import aster.core.parser.AsterParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Evaluator output emitter for the dual-engine parity gate (Phase C).
 *
 * <p>Like {@link aster.core.dualengine.CoreIrFingerprintCli} (Phase B), this is
 * a JUnit {@code @Test} CLI shim — it no-ops when its driving system properties
 * are absent. When invoked with {@code -Dparity.eval.input=...} and
 * {@code -Dparity.eval.output=...} it streams a JSONL request file through the
 * full Java pipeline:
 *
 * <pre>
 *   source (.aster)  →  Canonicalize  →  ANTLR parse  →  AstBuilder
 *                    →  CoreLowering   →  Core IR JSON
 *                    →  Truffle Context.eval("aster", coreIrJson)
 *                    →  (with args) Value.execute(args)
 *                    →  normalized JSON
 * </pre>
 *
 * <p>The input file format is JSONL — one request per line:
 * <pre>
 *   {"samplePath":"/abs/path/to/foo.aster","entry":"add","input":[1,2],"caseName":"…","caseIndex":0}
 * </pre>
 *
 * <p>The output file is JSONL — one record per line:
 * <pre>
 *   {"samplePath":"…","caseIndex":0,"ok":true,"value":3}
 *   {"samplePath":"…","caseIndex":1,"ok":false,"error":"…"}
 * </pre>
 *
 * <p>Why a JUnit-resident CLI rather than a Gradle JavaExec task:
 *   <ul>
 *     <li>Reuses Truffle's test classpath (truffle-api, graal-sdk, jackson)
 *         without new build plumbing.</li>
 *     <li>Identical invocation pattern as {@code CoreIrFingerprintCli} — the
 *         parity runner can shell out to {@code ./gradlew test} with two
 *         {@code -D} properties in both phases.</li>
 *     <li>Same {@code System.getProperty}-guarded no-op makes it safe to
 *         leave on in regular {@code ./gradlew test} runs.</li>
 *   </ul>
 *
 * <p>The {@code @Tag("parity-eval")} keeps it discoverable; the only
 * test-task config required is that {@code parity.eval.*} properties are
 * forwarded to the test JVM (see {@code aster-lang-truffle/build.gradle.kts}).
 *
 * <p>Result normalization. {@code Value} from Truffle can be a primitive, a
 * string, a boolean, or a host object. We render via {@link Value} accessors
 * into a JsonNode the parity runner can compare against TS's evaluator output:
 *   <ul>
 *     <li>{@code isNull()} → JSON null</li>
 *     <li>{@code isBoolean()} → JSON boolean</li>
 *     <li>{@code isNumber()}: prefer long → int, then double</li>
 *     <li>{@code isString()} → JSON string</li>
 *     <li>otherwise → {@code toString()} JSON string (lossy fallback,
 *         flagged by including the host type in a sidecar field for the
 *         runner to surface)</li>
 *   </ul>
 */
@Tag("parity-eval")
class CoreIrEvalCli {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void emitEvaluatorOutputsWhenInvoked() throws Exception {
        String inputProp = System.getProperty("parity.eval.input");
        String outputProp = System.getProperty("parity.eval.output");
        if (inputProp == null || outputProp == null) {
            // No-op for ordinary `./gradlew test` runs.
            return;
        }

        Path inputPath = Paths.get(inputProp);
        Path outputPath = Paths.get(outputProp);
        if (!Files.exists(inputPath)) {
            throw new IllegalStateException("parity.eval.input not found: " + inputPath);
        }

        Canonicalizer canonicalizer = new Canonicalizer();
        CoreLowering lowering = new CoreLowering();

        try (BufferedReader reader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) continue;

                ObjectNode record = MAPPER.createObjectNode();
                JsonNode request;
                try {
                    request = MAPPER.readTree(trimmed);
                } catch (Exception parseEx) {
                    // Skip malformed lines but emit a record so the runner
                    // can see something went wrong on this input row.
                    record.put("ok", false);
                    record.put("error", "malformed request line: " + truncate(parseEx.getMessage()));
                    writer.write(MAPPER.writeValueAsString(record));
                    writer.newLine();
                    continue;
                }

                String samplePath = request.path("samplePath").asText("");
                int caseIndex = request.path("caseIndex").asInt(-1);
                record.put("samplePath", samplePath);
                record.put("caseIndex", caseIndex);

                try {
                    JsonNode value = evalOne(canonicalizer, lowering, request);
                    record.put("ok", true);
                    record.set("value", value);
                } catch (Throwable t) {
                    record.put("ok", false);
                    record.put("error", truncate(t.getMessage() != null ? t.getMessage()
                        : t.getClass().getSimpleName()));
                }
                writer.write(MAPPER.writeValueAsString(record));
                writer.newLine();
            }
        }
    }

    /**
     * Evaluate a single request through the full Java pipeline and return
     * the result as a JsonNode. Each request gets a fresh polyglot
     * Context so test ordering can't leak state across samples.
     */
    private JsonNode evalOne(Canonicalizer canonicalizer, CoreLowering lowering, JsonNode request)
            throws Exception {
        String samplePath = request.path("samplePath").asText("");
        JsonNode inputArr = request.path("input");
        String entry = request.path("entry").asText("");
        if (samplePath.isEmpty()) {
            throw new IllegalArgumentException("samplePath required");
        }

        String source = Files.readString(Paths.get(samplePath), StandardCharsets.UTF_8);
        String canonical = canonicalizer.canonicalize(source);

        AsterCustomLexer lexer = new AsterCustomLexer(CharStreams.fromString(canonical));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        tokens.seek(0);

        AsterParser parser = new AsterParser(tokens);
        AsterParser.ModuleContext moduleCtx = parser.module();

        AstBuilder builder = new AstBuilder();
        aster.core.ast.Module ast = builder.visitModule(moduleCtx);

        CoreModel.Module coreModule = lowering.lowerModule(ast);

        // The language's parse() picks the entry function via DEFAULT_FUNCTION
        // (env ASTER_TRUFFLE_FUNC, default "main"); when absent it falls back to
        // the *first* Func declaration. Multi-rule samples therefore always ran
        // their first rule regardless of the requested entry. Hoist the requested
        // entry Func to the front of the decls so the loader's first-Func fallback
        // selects it. (Test-only IR reshaping; production runtime is untouched.)
        if (!entry.isEmpty() && coreModule.decls != null) {
            java.util.List<CoreModel.Decl> decls = new java.util.ArrayList<>(coreModule.decls);
            int idx = -1;
            for (int i = 0; i < decls.size(); i++) {
                if (decls.get(i) instanceof CoreModel.Func f && entry.equals(f.name)) {
                    idx = i;
                    break;
                }
            }
            if (idx > 0) {
                decls.add(0, decls.remove(idx));
                coreModule.decls = decls;
            }
        }

        String coreJson = MAPPER.writeValueAsString(coreModule);

        // Restricted polyglot context — Codex review R-Phase-C-C1. Even
        // though this CLI runs in CI under read-only repo permissions,
        // `.allowAllAccess(true)` would let a malicious .aster sample
        // (e.g. one introduced through a stale PR that gets merged
        // before review) call Java host APIs, spawn processes, or read
        // arbitrary files. The production policy runtime in
        // `aster-api/src/main/java/io/aster/policy/runtime/TrufflePolicyRuntime.java`
        // uses HostAccess.EXPLICIT + IOAccess.NONE; mirror that posture
        // here so the parity gate matches the security envelope of the
        // surface it's modeling.
        try (Context context = Context.newBuilder("aster")
                .allowHostAccess(HostAccess.EXPLICIT)
                .allowIO(false)
                .allowCreateProcess(false)
                .allowNativeAccess(false)
                .option("engine.WarnInterpreterOnly", "false")
                .build()) {
            Source src = Source.newBuilder("aster", coreJson, samplePath + ".json").build();
            Value result;

            if (inputArr.isArray() && inputArr.size() > 0) {
                Value program = context.eval(src);
                Object[] args = new Object[inputArr.size()];
                for (int i = 0; i < inputArr.size(); i++) {
                    args[i] = jsonToHostArg(inputArr.get(i));
                }
                if (!program.canExecute()) {
                    throw new IllegalStateException(
                        "evaluated program is not executable but input args were provided");
                }
                result = program.execute(args);
            } else {
                result = context.eval(src);
            }

            return valueToJson(result);
        }
    }

    /**
     * Convert a JSON input argument to a host Java object suitable for
     * passing to {@code Value.execute(...)}. The TS evaluator accepts
     * JSON-native values directly; Truffle host-side wants concrete Java
     * types. This is the minimum subset the tier1 .cases.json files use.
     */
    private Object jsonToHostArg(JsonNode node) {
        if (node == null || node.isNull()) return null;
        if (node.isBoolean()) return node.asBoolean();
        if (node.isInt()) return node.asInt();
        if (node.isLong()) return node.asLong();
        if (node.isDouble() || node.isFloat()) return node.asDouble();
        if (node.isTextual()) return node.asText();
        // Arrays → java.util.List; objects (structs) → java.util.Map. Both are
        // member/element-accessible by the guest. A `__type` key in the JSON is
        // kept as `_type` so guest struct/enum matching sees it (TS uses __type,
        // the guest's AsterDataValue exposes _type). The tier1 .cases.json files
        // pass structs as plain JSON objects.
        // Inject collections as the guest's own interop values (AsterListValue /
        // AsterMapValue) rather than raw host List/Map. A host collection crossing the
        // HostAccess.EXPLICIT boundary becomes an opaque HostObject the List.*/Map.*
        // builtins can't see into; the guest-native wrappers are recognized directly by
        // Builtins.asList/asMap (and expose members for struct/enum pattern matching).
        if (node.isArray()) {
            java.util.List<Object> list = new java.util.ArrayList<>();
            node.forEach(el -> list.add(jsonToHostArg(el)));
            return new aster.truffle.runtime.interop.AsterListValue(list);
        }
        if (node.isObject()) {
            java.util.Map<String, Object> map = new java.util.LinkedHashMap<>();
            node.fields().forEachRemaining(e -> {
                String key = "__type".equals(e.getKey()) ? "_type" : e.getKey();
                map.put(key, jsonToHostArg(e.getValue()));
            });
            return new aster.truffle.runtime.interop.AsterMapValue(map);
        }
        return node;
    }

    private JsonNode valueToJson(Value v) {
        if (v == null || v.isNull()) return MAPPER.nullNode();
        if (v.isBoolean()) return MAPPER.getNodeFactory().booleanNode(v.asBoolean());
        // Decimal（ADR 0025）：运行时是 guest AsterDecimalValue，暴露 isString()=true +
        // asString()=canonical 十进制串（toPlainString，去尾零）。它走下方 isString 分支输出
        // JSON **字符串**（如 "107.9"），与 TS decimal.js 的 JSON.stringify（toJSON→字符串）
        // 逐位一致——eval-parity 比 JSON.stringify 字符串相等。NOT isNumber，刻意避免 asDouble
        // 丢精度 + 双引擎分歧。
        if (v.isNumber()) {
            if (v.fitsInInt()) return MAPPER.getNodeFactory().numberNode(v.asInt());
            if (v.fitsInLong()) return MAPPER.getNodeFactory().numberNode(v.asLong());
            return MAPPER.getNodeFactory().numberNode(v.asDouble());
        }
        if (v.isString()) return MAPPER.getNodeFactory().textNode(v.asString());
        // Lists → JSON arrays.
        if (v.hasArrayElements()) {
            var arr = MAPPER.createArrayNode();
            long n = v.getArraySize();
            for (long i = 0; i < n; i++) arr.add(valueToJson(v.getArrayElement(i)));
            return arr;
        }
        // Structs / data values → { "__type": TypeName, ...fields } — the same
        // shape the TS interpreter emits, so construct-returning samples can be
        // compared structurally (was a lossy "__display" string before).
        if (v.hasMembers()) {
            var keys = v.getMemberKeys();
            // The Aster type name is exposed as a `_type` member; surface it as
            // `__type` (TS's key) and drop the internal `_type` from the body.
            String typeName = keys.contains("_type") && v.getMember("_type").isString()
                ? v.getMember("_type").asString()
                : (v.getMetaObject() != null ? v.getMetaObject().getMetaQualifiedName() : "?");
            ObjectNode out = MAPPER.createObjectNode();
            out.put("__type", typeName);
            for (String key : keys) {
                if ("_type".equals(key)) continue;
                out.set(key, valueToJson(v.getMember(key)));
            }
            return out;
        }
        // Truly opaque fallback (should be rare now).
        ObjectNode out = MAPPER.createObjectNode();
        out.put("__type", v.getMetaObject() != null ? v.getMetaObject().getMetaQualifiedName() : "?");
        out.put("__display", v.toString());
        return out;
    }

    private static String truncate(String msg) {
        if (msg == null) return "";
        return msg.length() > 240 ? msg.substring(0, 240) : msg;
    }
}
