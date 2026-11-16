package aster.cli;

import aster.core.embed.PolicySourceCompiler;
import aster.core.ir.CoreIrFormatException;
import aster.core.ir.CoreIrJson;
import aster.truffle.embed.CoreIrEntrySelector;
import aster.truffle.embed.EntryFunctionException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * 包检查流水线（README「检查顺序」）：read → manifest → signature → integrity → ir → entry。各阶段通过后填写 {@link Meta}，
 * 第一处失败即抛 {@link CliException}；{@code run} / {@code verify} / {@code inspect} / {@code compile} 共用。
 */
final class PackagePipeline {

    final Meta meta = new Meta();
    Map<String, byte[]> files;
    Map<String, Object> manifest;
    private byte[] manifestBytes;

    /** read：目录或 zip；缺 manifest.json 为 {@code PKG_INVALID_MANIFEST}。 */
    void read(Path pkg) throws CliException {
        files = PackageReader.read(pkg);
        manifestBytes = files.get("manifest.json");
        if (manifestBytes == null) {
            throw new CliException(ErrorCode.PKG_INVALID_MANIFEST, "包内没有 manifest.json");
        }
    }

    void manifest() throws CliException {
        manifest = ManifestCheck.check(manifestBytes);
    }

    void signature(TrustAnchor anchor, boolean allowUnsigned) throws CliException {
        meta.signed = SignatureVerifier.verify(files.get("manifest.sig.json"), manifestBytes, anchor, allowUnsigned);
    }

    /** integrity 通过后 {@code meta.coreIrSha256} 与缺省入口可用（verify / inspect 在此填写）。 */
    void integrity() throws CliException {
        Integrity.check(files, manifest, manifestBytes);
    }

    /** read → manifest → signature → integrity。 */
    void trusted(Path pkg, TrustAnchor anchor, boolean allowUnsigned) throws CliException {
        read(pkg);
        manifest();
        signature(anchor, allowUnsigned);
        integrity();
    }

    boolean isModeA() {
        return "a".equals(manifest.get("mode"));
    }

    String coreIrSha256() {
        return (String) Json.at(manifest, "coreIr", "sha256");
    }

    String defaultEntry() {
        for (Map<String, Object> e : Json.objects(Json.at(manifest, "policy", "entries"))) {
            if (Boolean.TRUE.equals(e.get("default"))) {
                return (String) e.get("function");
            }
        }
        return null;
    }

    List<String> entryNames() {
        return Json.objects(Json.at(manifest, "policy", "entries")).stream().map(e -> (String) e.get("function")).toList();
    }

    /** A 包：严格读入 {@code compiled/core.json}，失败为 {@code PKG_UNSUPPORTED_IR}。 */
    String readCompiledIr() throws CliException {
        String coreJson = new String(files.get("compiled/core.json"), StandardCharsets.UTF_8);
        try {
            CoreIrJson.read(coreJson);
        } catch (CoreIrFormatException | RuntimeException e) {
            throw new CliException(ErrorCode.PKG_UNSUPPORTED_IR, "compiled/core.json 严格读入失败: " + e.getMessage(), e);
        }
        return coreJson;
    }

    /**
     * 按 manifest 本地编译 {@code policy/}（含 aliasSet 校验），按 {@code coreIr.positionFreeSha256} 比对。
     *
     * @param failure 失败时报的码：{@code run} / {@code verify} 为 {@code PKG_IR_DRIFT}，{@code compile} 为 {@code COMPILE_ERROR}
     * @param compare 是否比对投影哈希（{@code compile} 只取 IR，不比对）
     */
    String recompile(ErrorCode failure, boolean compare) throws CliException {
        String coreJson;
        try {
            coreJson = PolicySourceCompiler.compileCanonical(
                utf8(files.get("policy/policy.aster")),
                (String) Json.at(manifest, "source", "locale"),
                PolicySourceCompiler.vocabularyScopesOf(utf8(files.get("policy/compile/vocabulary.json"))),
                PolicySourceCompiler.parseAliasSetJson(utf8(files.get("policy/compile/alias-set.json"))),
                Boolean.TRUE.equals(Json.at(manifest, "source", "allowStructural")));
        } catch (RuntimeException | StackOverflowError e) {
            throw new CliException(failure, "本地编译失败: " + e.getMessage(), e);
        }
        if (compare) {
            String local;
            try {
                local = PolicySourceCompiler.positionFreeSha256(coreJson);
            } catch (CoreIrFormatException | RuntimeException e) {
                throw new CliException(failure, "本地 IR 无法严格读入: " + e.getMessage(), e);
            }
            Object expected = Json.at(manifest, "coreIr", "positionFreeSha256");
            if (!local.equals(expected)) {
                throw new CliException(failure, "本地 IR 投影哈希 " + local + " ≠ coreIr.positionFreeSha256 " + expected);
            }
        }
        return coreJson;
    }

    /** ir 阶段：A 读 {@code compiled/core.json}，B 本地编译并比对。成功后填写 meta。 */
    String obtainIr(String entry) throws CliException {
        String coreJson = isModeA() ? readCompiledIr() : recompile(ErrorCode.PKG_IR_DRIFT, true);
        meta.entry = entry;
        meta.coreIrSha256 = coreIrSha256();
        meta.irRecompiled = !isModeA();
        return coreJson;
    }

    /** entry 阶段：必须是 manifest 入口，且 JVM 能执行（不被 {@code main} 抢占）。 */
    void checkEntry(String coreJson, String entry) throws CliException {
        if (!entryNames().contains(entry)) {
            throw new CliException(ErrorCode.ENTRY_NOT_FOUND, "入口 " + entry + " 不在 policy.entries 中");
        }
        try {
            CoreIrEntrySelector.withEntry(coreJson, entry);
        } catch (EntryFunctionException e) {
            throw new CliException(ErrorCode.ENTRY_NOT_FOUND, e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            throw new CliException(ErrorCode.EVAL_ERROR, e.getMessage(), e);
        }
    }

    private static String utf8(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }
}
