package aster.cli;

import aster.core.layout.DisplayReconstructor;
import aster.core.layout.LayoutException;
import aster.core.layout.LayoutLimits;
import aster.core.layout.LayoutSpan;
import aster.core.layout.PackageLayoutRules;
import aster.core.layout.PolicyLayout;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 包格式 1.1 显示排版的编排（ADR 0059 修订 4 D5–D8；aster-lang-test README「显示排版」「show」「run --display」）。
 * 规则本身在 core {@code aster.core.layout}，这里只取编译输入、映射错误码、写 stderr 说明。
 */
final class DisplayStage {

    static final String LAYOUT_PATH = "policy/display/layout.json";
    static final String DISPLAY_PATH = "policy/display/display.aster";
    /** 显示文本文件的大小上限：display 字节上限 + 开头 BOM；超出不读内容，reconstruct 阶段为 DISPLAY_UNMAPPABLE。 */
    static final int MAX_DISPLAY_FILE_BYTES = LayoutLimits.DISPLAY_BYTES + 3;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** 编辑后的显示文本：已严格 UTF-8 解码、去掉至多一个 BOM；oversize 时 text 为 null。 */
    record Edited(String text, boolean oversize) {
    }

    /** 还原并通过 L2、L5–L9 的派生程序。 */
    record Derived(PolicyLayout layout, String canonical, String coreJson) {
    }

    private final PackagePipeline p;
    private final List<String> vocabularyLocalized;
    private final List<String> literalKeywords;
    private final List<String> operatorSpellings;
    private final JsonNode vocabulary;

    private DisplayStage(PackagePipeline p) throws CliException {
        this.p = p;
        String locale = (String) Json.at(p.manifest, "source", "locale");
        this.vocabulary = readJson(p.files.get("policy/compile/vocabulary.json"));
        this.vocabularyLocalized = PackageLayoutRules.vocabularyLocalized(vocabulary);
        this.literalKeywords = PackageLayoutRules.literalKeywords(locale);
        this.operatorSpellings = PackageLayoutRules.operatorSpellings(locale, aliasSet(readJson(p.files.get("policy/compile/alias-set.json"))));
    }

    /** manifest 是否带 display（只有 1.1 schema 允许它）。 */
    static boolean hasDisplay(PackagePipeline p) {
        return p.manifest != null && p.manifest.containsKey("display");
    }

    static DisplayStage of(PackagePipeline p) throws CliException {
        return new DisplayStage(p);
    }

    // ── usage 阶段：读编辑后的显示文本 ──

    /** 不存在、不是普通文件、不可读、非法 UTF-8 为 USAGE；超过上限不读内容；去掉至多一个开头 BOM。 */
    static Edited readEdited(Path file) throws CliException {
        try {
            if (!Files.isRegularFile(file)) {
                throw new CliException(ErrorCode.USAGE, "显示文本文件不存在或不是文件: " + file);
            }
            if (Files.size(file) > MAX_DISPLAY_FILE_BYTES) {
                return new Edited(null, true);
            }
            String text = CompileRequest.decodeSource(Files.readAllBytes(file));
            return new Edited(text, false);
        } catch (StrictJson.StrictJsonException e) {
            throw new CliException(ErrorCode.USAGE, "显示文本文件不是合法 UTF-8: " + file);
        } catch (IOException | RuntimeException e) {
            throw new CliException(ErrorCode.USAGE, "无法读取显示文本文件: " + e.getMessage());
        }
    }

    // ── display 阶段（L1–L7） ──

    /** 包排版 L1–L7；包没有排版或任一规则失败为 PKG_DISPLAY_INVALID。 */
    PolicyLayout checkPackage() throws CliException {
        if (!hasDisplay(p)) {
            throw new CliException(ErrorCode.PKG_DISPLAY_INVALID, "包没有显示排版，无从对照");
        }
        Object v = Json.at(p.manifest, "display", "layoutVersion");
        int layoutVersion = v instanceof BigInteger b ? b.intValue() : -1;
        try {
            return PackageLayoutRules.checkPackage(p.files.get(LAYOUT_PATH), p.files.get(DISPLAY_PATH), p.files.get("policy/policy.aster"),
                layoutVersion, vocabularyLocalized, literalKeywords, operatorSpellings);
        } catch (LayoutException e) {
            throw new CliException(ErrorCode.PKG_DISPLAY_INVALID, "显示排版不合规 " + e.getMessage());
        }
    }

    // ── display-ir（L8–L9） ──

    /**
     * L8：IR 的名字不整词出现在结构 canonical 中；L9：补空格后的源码编译成功且投影哈希不变。
     * {@code coreJson} 为该排版规范源码的 IR；失败为 {@code failure}。
     */
    void checkIr(PolicyLayout layout, String coreJson, ErrorCode failure) throws CliException {
        List<String> hits = PackageLayoutRules.structuralNameHits(layout, PackageLayoutRules.irNameStrings(coreJson));
        if (!hits.isEmpty()) {
            throw new CliException(failure, "L8: 结构 span 含 IR 名字 " + hits);
        }
        String padded = PackageLayoutRules.padStructural(layout);
        String paddedIr;
        try {
            paddedIr = p.compileSource(padded, failure);
        } catch (CliException e) {
            throw new CliException(failure, "L9: 补空格后的源码无法编译（" + e.getMessage() + "）");
        }
        if (!PackagePipeline.positionFreeSha256(paddedIr, failure).equals(PackagePipeline.positionFreeSha256(coreJson, failure))) {
            throw new CliException(failure, "L9: 补空格后投影哈希改变——有值被切进结构 span");
        }
    }

    // ── run --display / show --from-display ──

    /** reconstruct → derived（L2、L5–L7）→ compile → L8/L9。 */
    Derived derive(PolicyLayout layout, Edited edited, PrintStream err) throws CliException {
        if (edited.oversize()) {
            throw new CliException(ErrorCode.DISPLAY_UNMAPPABLE, "显示文本超过 " + LayoutLimits.DISPLAY_BYTES + " 字节");
        }
        DisplayReconstructor.Reconstruction r = DisplayReconstructor.reconstruct(layout, edited.text());
        PolicyLayout derived = switch (r) {
            case DisplayReconstructor.Unique u -> u.derived();
            case DisplayReconstructor.Unmappable u -> {
                if (u.crlfHint()) {
                    err.println("文件使用 CRLF 换行；显示排版以 \\n 为界，请改用 LF 保存");
                }
                throw new CliException(ErrorCode.DISPLAY_UNMAPPABLE, "无法定位编辑后的显示文本（偏移 " + u.offset() + "）");
            }
            case DisplayReconstructor.Ambiguous a ->
                throw new CliException(ErrorCode.DISPLAY_AMBIGUOUS, "编辑后的显示文本有多种还原方式（偏移 " + a.offset() + "）");
        };
        try {
            PackageLayoutRules.checkDerived(derived, vocabularyLocalized, literalKeywords, operatorSpellings);
        } catch (LayoutException e) {
            throw new CliException(ErrorCode.DISPLAY_REJECTED, "派生排版 " + e.getMessage());
        }
        String canonical = derived.toCanonical();
        String coreJson = p.compileSource(canonical, ErrorCode.COMPILE_ERROR);
        checkIr(derived, coreJson, ErrorCode.DISPLAY_REJECTED);
        return new Derived(derived, canonical, coreJson);
    }

    /** meta.display：C′ 的 sha256 与投影哈希是否等于 manifest 记录的。 */
    Map<String, Object> metaDisplay(Derived d) throws CliException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("canonicalSha256", Integrity.sha256(d.canonical().getBytes(StandardCharsets.UTF_8)));
        m.put("matchesPackage", PackagePipeline.positionFreeSha256(d.coreJson(), ErrorCode.COMPILE_ERROR)
            .equals(Json.at(p.manifest, "coreIr", "positionFreeSha256")));
        return m;
    }

    // ── stderr 说明（不跨 CLI 比较） ──

    /** 结构映射（display ⇐ canonical，去重保序）与字面量宏（触发词 → 展开值）。 */
    void printNotes(PolicyLayout layout, PrintStream err) {
        Set<String> lines = new LinkedHashSet<>();
        for (LayoutSpan s : layout.spans()) {
            if (s instanceof LayoutSpan.Structural st && !st.display().equals(st.canonical())) {
                lines.add(ValueJson.write(st.display()) + " ⇐ " + ValueJson.write(st.canonical()));
            }
        }
        if (!lines.isEmpty()) {
            err.println("结构映射（显示 ⇐ 规范；这些文字只是排版，不参与编译）：");
            lines.forEach(l -> err.println("  " + l));
        }
        List<String> macros = new ArrayList<>();
        if (vocabulary != null) {
            for (JsonNode e : vocabulary.path("literals")) {
                if (e.path("localized").isTextual() && e.path("canonical").isTextual()) {
                    macros.add(e.path("localized").textValue() + " → " + e.path("canonical").textValue());
                }
            }
        }
        if (!macros.isEmpty()) {
            err.println("字面量宏（显示的是触发词，编译的是展开值）：");
            macros.forEach(l -> err.println("  " + l));
        }
    }

    private static JsonNode readJson(byte[] bytes) throws CliException {
        if (bytes == null) {
            return null;
        }
        try {
            return MAPPER.readTree(bytes);
        } catch (IOException e) {
            throw new CliException(ErrorCode.PKG_INTEGRITY, "编译输入不是 JSON: " + e.getMessage());
        }
    }

    private static Map<String, List<String>> aliasSet(JsonNode node) {
        if (node == null || !node.isObject()) {
            return Map.of();
        }
        Map<String, List<String>> out = new LinkedHashMap<>();
        node.fields().forEachRemaining(e -> {
            List<String> list = new ArrayList<>();
            e.getValue().forEach(x -> {
                if (x.isTextual()) {
                    list.add(x.textValue());
                }
            });
            out.put(e.getKey(), list);
        });
        return out;
    }
}
