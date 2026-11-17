package aster.cli;

import aster.truffle.embed.Outcome;
import aster.truffle.embed.SandboxedExecutor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JVM {@code aster} CLI 入口（ADR 0059 P1 T8）：{@code compile / run / verify / inspect / version}，命令契约与 Node CLI 相同，
 * 行为以 aster-lang-test {@code conformance/package-v1/README.md} 为准。stdout 恒为一行值规范 JSON（{@code compile} 输出 IR 字节），
 * 诊断只写 stderr。
 */
public final class AsterCli {

    private static final Pattern TIMEOUT = Pattern.compile("(0|[1-9][0-9]*)(ms|s)");
    private static final long MAX_TIMEOUT_MS = 300_000L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AsterCli() {}

    public static void main(String[] args) {
        // 解析器在 INFO 级记录每次编译；CLI 只把警告以上写进 stderr
        java.util.logging.Logger.getLogger("").setLevel(Level.WARNING);
        for (var h : java.util.logging.Logger.getLogger("").getHandlers()) {
            h.setLevel(Level.WARNING);
        }
        int code = run(args, System.in, System.out, System.err);
        System.out.flush();
        System.exit(code);
    }

    /** 以当前目录与进程环境运行；返回退出码。 */
    public static int run(String[] args, InputStream stdin, PrintStream out, PrintStream err) {
        return run(args, stdin, out, err, Path.of("").toAbsolutePath(), System.getenv());
    }

    /** 可注入工作目录与环境变量（测试用）。 */
    public static int run(String[] args, InputStream stdin, PrintStream out, PrintStream err, Path cwd,
                          Map<String, String> env) {
        Invocation inv = new Invocation(stdin, out, err, cwd);
        try {
            return inv.dispatch(args, env);
        } catch (CliException e) {
            return inv.fail(e, new Meta());
        }
    }

    // ── 单次调用 ──────────────────────────────────────────────────────

    private static final class Invocation {
        private final InputStream stdin;
        private final PrintStream out;
        private final PrintStream err;
        private final Path cwd;

        Invocation(InputStream stdin, PrintStream out, PrintStream err, Path cwd) {
            this.stdin = stdin;
            this.out = out;
            this.err = err;
            this.cwd = cwd;
        }

        int dispatch(String[] args, Map<String, String> env) throws CliException {
            // usage 阶段（README「检查顺序」）：命令行语法 → --timeout → compile 的源码 / 配置文件或信任锚 → ASTER_TRUFFLE_FUNC
            Argv argv = Argv.parse(args);
            Duration timeout = parseTimeout(argv.value("--timeout"));
            CompileRequest compile = null;
            TrustAnchor anchor = null;
            if (argv.command.equals("compile")) {
                compile = CompileRequest.check(argv, cwd);
                if (compile.isPackage()) {
                    anchor = TrustAnchor.load(argv.value("--pubkey"), argv.value("--jwks"), cwd, env);
                }
            } else if (!argv.command.equals("version")) {
                anchor = TrustAnchor.load(argv.value("--pubkey"), argv.value("--jwks"), cwd, env);
            }
            // ASTER_TRUFFLE_FUNC 在类加载时改写 Truffle 的缺省入口；拒绝它使优先入口集合恒为 {main}（修订 1 A5）
            if (env.containsKey("ASTER_TRUFFLE_FUNC")) {
                throw new CliException(ErrorCode.USAGE, "不支持环境变量 ASTER_TRUFFLE_FUNC，请取消设置后重试");
            }
            boolean allowUnsigned = argv.flag("--allow-unsigned");
            return switch (argv.command) {
                case "run" -> runCommand(argv, anchor, timeout);
                case "verify" -> verifyCommand(argv, anchor, timeout);
                case "inspect" -> inspectCommand(argv.positional.get(0), anchor, allowUnsigned);
                case "compile" -> compileCommand(compile, anchor, allowUnsigned);
                default -> versionCommand(env);
            };
        }

        int fail(CliException e, Meta meta) {
            err.println("aster: " + e.code().name() + ": " + e.getMessage());
            out.print(Envelope.error(e.code(), meta));
            return e.code().exit();
        }

        // ── run ──

        private int runCommand(Argv a, TrustAnchor anchor, Duration timeout) throws CliException {
            PackagePipeline p = new PackagePipeline();
            try {
                p.trusted(cwd.resolve(a.positional.get(0)), anchor, a.flag("--allow-unsigned"));
                warnFacts(p);
                String entry = a.value("--entry") != null ? a.value("--entry") : p.defaultEntry();
                String coreJson = p.obtainIr(entry);
                p.checkEntry(coreJson, entry);
                byte[] input = readInput(a.value("--input"));
                JsonNode ir = irTree(coreJson);
                Object[] args = InputParser.parse(input, ir, InputParser.func(ir, entry));
                Evaluated r = evaluate(coreJson, entry, args, timeout);
                if (r.failure != null) {
                    throw r.failure;
                }
                out.print(Envelope.ok(p.meta, r.valueJson));
                return 0;
            } catch (CliException e) {
                return fail(e, p.meta);
            }
        }

        private byte[] readInput(String input) throws CliException {
            try {
                if (input.equals("-")) {
                    return stdin.readNBytes(InputParser.MAX_INPUT_BYTES + 1);
                }
                Path f = cwd.resolve(input);
                if (Files.size(f) > InputParser.MAX_INPUT_BYTES) {
                    throw new CliException(ErrorCode.INPUT_INVALID, "输入超过 16 MiB");
                }
                return Files.readAllBytes(f);
            } catch (IOException | RuntimeException e) {
                throw new CliException(ErrorCode.INPUT_INVALID, "无法读取输入: " + e.getMessage());
            }
        }

        // ── verify ──

        private int verifyCommand(Argv a, TrustAnchor anchor, Duration timeout) throws CliException {
            PackagePipeline p = new PackagePipeline();
            List<Map<String, Object>> checks = new ArrayList<>();
            String stage = "integrity";
            try {
                p.read(cwd.resolve(a.positional.get(0)));
                stage = "schema";
                p.manifest();
                checks.add(Envelope.check("schema", null));
                stage = "signature";
                p.signature(anchor, a.flag("--allow-unsigned"));
                checks.add(Envelope.check("signature", null));
                stage = "integrity";
                p.integrity();
                checks.add(Envelope.check("integrity", null));
                p.meta.entry = p.defaultEntry();
                p.meta.coreIrSha256 = p.coreIrSha256();
                String coreJson = null;
                if (p.isModeA()) {
                    stage = "ir";
                    coreJson = p.readCompiledIr();
                    checks.add(Envelope.check("ir", null));
                }
                if (a.flag("--recompile")) {
                    stage = "recompile";
                    String local = p.recompile(ErrorCode.PKG_IR_DRIFT, true);
                    checks.add(Envelope.check("recompile", null));
                    p.meta.irRecompiled = true;
                    if (!p.isModeA()) {
                        coreJson = local;
                    }
                }
                if (a.flag("--probes")) {
                    stage = "probes";
                    if (coreJson == null) {
                        coreJson = p.recompile(ErrorCode.PKG_IR_DRIFT, true);
                    }
                    runProbes(p, coreJson, timeout);
                    checks.add(Envelope.check("probes", null));
                }
                out.print(Envelope.verify(p.meta, checks, true));
                return 0;
            } catch (CliException e) {
                if (e.code() == ErrorCode.PKG_INVALID_MANIFEST && p.manifest == null) {
                    stage = "schema";
                }
                err.println("aster: " + e.code().name() + ": " + e.getMessage());
                checks.add(Envelope.check(stage, e.code()));
                out.print(Envelope.verify(p.meta, checks, false));
                return e.code().exit();
            }
        }

        private void runProbes(PackagePipeline p, String coreJson, Duration timeout) throws CliException {
            Object doc;
            try {
                doc = StrictJson.parse(p.files.get("tests/probes.json"), 64);
            } catch (StrictJson.StrictJsonException e) {
                throw new CliException(ErrorCode.PKG_INTEGRITY, e.getMessage());
            }
            JsonNode ir = irTree(coreJson);
            int k = 0;
            for (Map<String, Object> probe : Json.objects(Json.at(doc, "probes"))) {
                String entry = (String) probe.get("entry");
                Object expected = probe.get("expected");
                String why = probeMismatch(p, coreJson, ir, entry, probe.get("input"), expected, timeout);
                if (why != null) {
                    throw new CliException(ErrorCode.PROBE_MISMATCH, "probe #" + k + "（" + entry + "）: " + why);
                }
                k++;
            }
        }

        /** 返回不符的原因；相符返回 null。期望为 {@code {"ok":true,"value":V}} 或 {@code {"code":"EVAL_ERROR","ok":false}}。 */
        private String probeMismatch(PackagePipeline p, String coreJson, JsonNode ir, String entry, Object input,
                                     Object expected, Duration timeout) {
            Evaluated r;
            try {
                p.checkEntry(coreJson, entry);
                byte[] bytes = ValueJson.write(input).getBytes(StandardCharsets.UTF_8);
                Object[] args = InputParser.parse(bytes, ir, InputParser.func(ir, entry));
                r = evaluate(coreJson, entry, args, timeout);
            } catch (CliException e) {
                return e.code().name() + " " + e.getMessage();
            }
            if (Boolean.TRUE.equals(Json.at(expected, "ok"))) {
                if (r.failure != null) {
                    return "期望成功，实际 " + r.failure.code().name();
                }
                String want = ValueJson.write(Json.at(expected, "value"));
                return want.equals(r.valueJson) ? null : "期望 " + want + "，实际 " + r.valueJson;
            }
            if (r.failure != null && r.failure.code() == ErrorCode.EVAL_ERROR
                    && "EVAL_ERROR".equals(Json.at(expected, "code"))) {
                return null;
            }
            return "期望 " + Json.at(expected, "code") + "，实际 " + (r.failure == null ? r.valueJson : r.failure.code().name());
        }

        // ── inspect ──

        private int inspectCommand(String pkg, TrustAnchor anchor, boolean allowUnsigned) {
            PackagePipeline p = new PackagePipeline();
            try {
                p.trusted(cwd.resolve(pkg), anchor, allowUnsigned);
                warnFacts(p);
                Map<String, Object> inspect = new LinkedHashMap<>();
                for (String k : new String[]{"effects", "governance", "policy", "runtime", "tests"}) {
                    inspect.put(k, p.manifest.get(k));
                }
                Map<String, Object> schemas = new LinkedHashMap<>();
                schemas.put("input", payloadJson(p, "schema/input.json"));
                schemas.put("output", payloadJson(p, "schema/output.json"));
                inspect.put("schemas", schemas);
                p.meta.entry = p.defaultEntry();
                p.meta.coreIrSha256 = p.coreIrSha256();
                out.print(Envelope.inspect(p.meta, inspect));
                return 0;
            } catch (CliException e) {
                return fail(e, p.meta);
            }
        }

        private static Object payloadJson(PackagePipeline p, String path) throws CliException {
            byte[] b = p.files.get(path);
            if (b == null) {
                return null;
            }
            try {
                return StrictJson.parse(b, 64);
            } catch (StrictJson.StrictJsonException e) {
                throw new CliException(ErrorCode.PKG_INTEGRITY, path + " 不是严格 JSON: " + e.getMessage());
            }
        }

        // ── compile ──

        private int compileCommand(CompileRequest c, TrustAnchor anchor, boolean allowUnsigned) {
            PackagePipeline p = new PackagePipeline();
            String coreJson;
            try {
                if (c.isPackage()) {
                    p.trusted(c.target(), anchor, allowUnsigned);
                    coreJson = p.recompile(ErrorCode.COMPILE_ERROR, false);
                } else {
                    coreJson = c.compileSource();
                }
                if (c.out() == null) {
                    // stdout：IR 加一个换行；--out：文件只含 IR 字节，stdout 不输出
                    byte[] bytes = (coreJson + "\n").getBytes(StandardCharsets.UTF_8);
                    out.write(bytes, 0, bytes.length);
                } else {
                    c.writeOut(coreJson);
                }
                return 0;
            } catch (CliException e) {
                return fail(e, p.meta);
            }
        }

        // ── version ──

        private int versionCommand(Map<String, String> env) throws CliException {
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("cliVersion", Version.CLI_VERSION);
            v.put("coreIrVersions", List.of(2));
            v.put("engine", Meta.ENGINE);
            v.put("formatVersions", List.of("1.0"));
            v.put("toolchainId", Version.toolchainId());
            v.put("trustAnchorKids", TrustAnchor.load(null, null, cwd, env).kids());
            out.print(ValueJson.write(v) + "\n");
            return 0;
        }

        // ── 共用 ──

        private void warnFacts(PackagePipeline p) {
            Object sources = Json.at(p.manifest, "governance", "factSources");
            if (sources instanceof List<?> l && !l.isEmpty()) {
                err.println("facts unverified: " + String.join(", ", l.stream().map(String::valueOf).toList()));
            }
        }

        private static JsonNode irTree(String coreJson) throws CliException {
            try {
                return MAPPER.readTree(coreJson);
            } catch (IOException e) {
                throw new CliException(ErrorCode.PKG_UNSUPPORTED_IR, e.getMessage());
            }
        }
    }

    /** 求值结果：成功为值规范 JSON，失败为对应码。 */
    private record Evaluated(String valueJson, CliException failure) {}

    private static Evaluated evaluate(String coreJson, String entry, Object[] args, Duration timeout) {
        Outcome outcome = Executor.INSTANCE.execute(coreJson, entry, args, timeout);
        if (outcome instanceof Outcome.Failed f) {
            return new Evaluated(null, new CliException(ErrorCode.valueOf(f.code()), String.valueOf(f.message())));
        }
        try {
            return new Evaluated(ValueJson.write(((Outcome.Ok) outcome).hostValue()), null);
        } catch (ValueJson.ValueJsonException e) {
            return new Evaluated(null, new CliException(ErrorCode.EVAL_ERROR, "结果无法写出: " + e.getMessage()));
        }
    }

    /** 进程内共享一个执行器（共享 Engine；CLI 每进程只求值一次，测试在同一 JVM 中复用）。 */
    private static final class Executor {
        static final SandboxedExecutor INSTANCE = new SandboxedExecutor();
    }

    /** {@code --timeout}：整体匹配 {@code (0|[1-9][0-9]*)(ms|s)}，> 0 且 ≤ 300 s；缺省 5 s。 */
    static Duration parseTimeout(String text) throws CliException {
        if (text == null) {
            return Duration.ofSeconds(5);
        }
        Matcher m = TIMEOUT.matcher(text);
        if (!m.matches() || m.group(1).length() > 7) {
            throw new CliException(ErrorCode.USAGE, "--timeout 语法: " + text);
        }
        long ms = Long.parseLong(m.group(1)) * (m.group(2).equals("s") ? 1000 : 1);
        if (ms <= 0 || ms > MAX_TIMEOUT_MS) {
            throw new CliException(ErrorCode.USAGE, "--timeout 须 > 0 且 ≤ 300s: " + text);
        }
        return Duration.ofMillis(ms);
    }
}
