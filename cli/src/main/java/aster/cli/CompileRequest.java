package aster.cli;

import aster.core.embed.PolicySourceCompiler;
import aster.core.lexicon.SemanticTokenKind;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@code compile} 子命令（README「compile」）：usage 阶段的全部检查在 {@link #check} 里完成（与参照实现 {@code argv.mjs} 的
 * {@code checkCompile} 同序），之后的编译失败才是 {@code COMPILE_ERROR}。
 *
 * <ul>
 *   <li>位置参数以 {@code .aster} 结尾即源码形态，不是普通文件（不存在、目录……）为 USAGE；否则为包形态；</li>
 *   <li>{@code --out} 的父目录在编译之前判定；写出为同目录临时文件再原子改名，失败时不留下任何文件；</li>
 *   <li>源码形态：{@code --locale} 只支持四个内置词法表；{@code --allow-structural} 只能与 {@code --alias-set} 同用；
 *       aliasSet / 词汇文件不可读或不是严格 JSON 为 USAGE；aliasSet 形状不对（非对象、值不是字符串数组、未知 kind）为 COMPILE_ERROR，
 *       {@code null} 等同于没有；</li>
 *   <li>只解析与降级，不做类型检查（与 B 包本地重编译同一语义）。</li>
 * </ul>
 */
record CompileRequest(Path target, boolean isPackage, Path out, String locale, Object aliasSet, String vocabulary,
                      boolean allowStructural) {

    static final Set<String> SUPPORTED_LOCALES = Set.of("en-US", "zh-CN", "de-DE", "hi-IN");

    static CompileRequest check(Argv a, Path cwd) throws CliException {
        Path out = a.value("--out") == null ? null : cwd.resolve(a.value("--out"));
        if (out != null) {
            Path parent = out.toAbsolutePath().getParent();
            if (parent == null || !Files.isDirectory(parent)) {
                throw usage("--out 的父目录不存在或不是目录");
            }
            if (Files.isDirectory(out)) {
                throw usage("--out 指向已存在的目录");
            }
        }
        String arg = a.positional.get(0);
        Path target = cwd.resolve(arg);
        if (!arg.endsWith(".aster")) {
            for (String o : new String[]{"--locale", "--alias-set", "--vocabulary", "--allow-structural"}) {
                if (a.value(o) != null || a.flag(o)) {
                    throw usage("编译包时不接受 " + o);
                }
            }
            return new CompileRequest(target, true, out, null, null, null, false);
        }
        if (!Files.isRegularFile(target)) {
            throw usage("源码文件不存在或不是普通文件: " + arg);
        }
        for (String o : new String[]{"--jwks", "--pubkey", "--allow-unsigned"}) {
            if (a.value(o) != null || a.flag(o)) {
                throw usage("编译 .aster 源码时不接受 " + o);
            }
        }
        if (a.flag("--allow-structural") && a.value("--alias-set") == null) {
            throw usage("--allow-structural 只能与 --alias-set 同用");
        }
        String locale = a.value("--locale") == null ? "en-US" : a.value("--locale");
        if (!SUPPORTED_LOCALES.contains(locale)) {
            throw usage("--locale 不受支持: " + locale);
        }
        Object aliasSet = a.value("--alias-set") == null ? null : strictJsonFile(cwd, a.value("--alias-set"));
        String vocabulary = null;
        if (a.value("--vocabulary") != null) {
            strictJsonFile(cwd, a.value("--vocabulary"));
            vocabulary = readString(cwd.resolve(a.value("--vocabulary")));
        }
        return new CompileRequest(target, false, out, locale, aliasSet, vocabulary, a.flag("--allow-structural"));
    }

    /** 源码形态：编译失败（含 aliasSet 形状与校验）为 COMPILE_ERROR。 */
    String compileSource() throws CliException {
        String source = readSource(target);
        if (isAsciiBlank(source)) {
            throw compileError("源码为空或只有空白");
        }
        Map<SemanticTokenKind, List<String>> aliases = aliasSetOf(aliasSet);
        try {
            return PolicySourceCompiler.compileCanonical(source, locale,
                PolicySourceCompiler.vocabularyScopesOf(vocabulary), aliases, allowStructural);
        } catch (RuntimeException | StackOverflowError e) {
            throw new CliException(ErrorCode.COMPILE_ERROR, String.valueOf(e.getMessage()), e);
        }
    }

    /** aliasSet 的形状：{@code {"<SemanticTokenKind>":[字符串…]}}；{@code null} 为无别名。 */
    private static Map<SemanticTokenKind, List<String>> aliasSetOf(Object tree) throws CliException {
        if (tree == null) {
            return Map.of();
        }
        if (!(tree instanceof Map<?, ?> map)) {
            throw compileError("aliasSet 不是对象");
        }
        Map<SemanticTokenKind, List<String>> out = new EnumMap<>(SemanticTokenKind.class);
        for (Map.Entry<?, ?> e : map.entrySet()) {
            SemanticTokenKind kind;
            try {
                kind = SemanticTokenKind.valueOf((String) e.getKey());
            } catch (IllegalArgumentException ex) {
                throw compileError("aliasSet 含未知 kind '" + e.getKey() + "'");
            }
            if (!(e.getValue() instanceof List<?> list) || !list.stream().allMatch(String.class::isInstance)) {
                throw compileError("aliasSet 的值必须是字符串数组: " + e.getKey());
            }
            out.put(kind, list.stream().map(String.class::cast).toList());
        }
        return out;
    }

    /** 原子写出 {@code --out}：同目录临时文件再改名；失败为 USAGE，且不留下临时文件、不改动目标。 */
    void writeOut(String coreJson) throws CliException {
        Path parent = out.toAbsolutePath().getParent();
        Path tmp = null;
        try {
            tmp = Files.createTempFile(parent, "." + out.getFileName() + ".", ".tmp");
            Files.writeString(tmp, coreJson, StandardCharsets.UTF_8);
            try {
                Files.move(tmp, out, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, out, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException | RuntimeException e) {
            if (tmp != null) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignored) {
                    // 尽力清理
                }
            }
            throw usage("无法写入 --out: " + e.getMessage());
        }
    }

    private static Object strictJsonFile(Path cwd, String file) throws CliException {
        Path p = cwd.resolve(file);
        if (!Files.isRegularFile(p)) {
            throw usage("文件不存在或不是普通文件: " + file);
        }
        try {
            return StrictJson.parse(Files.readAllBytes(p), 64);
        } catch (IOException e) {
            throw usage("无法读取 " + file);
        } catch (StrictJson.StrictJsonException e) {
            throw usage(file + " 不是严格 JSON: " + e.getMessage());
        }
    }

    /** 源码按严格 UTF-8 读入（非法字节序列为 USAGE），去掉一个开头的 BOM。 */
    private static String readSource(Path p) throws CliException {
        String text;
        try {
            text = StrictJson.decodeUtf8(Files.readAllBytes(p));
        } catch (IOException e) {
            throw usage("无法读取 " + p.getFileName());
        } catch (StrictJson.StrictJsonException e) {
            throw usage(p.getFileName() + " 不是合法 UTF-8");
        }
        return text.startsWith("\uFEFF") ? text.substring(1) : text;
    }

    /** 词汇文件已按严格 JSON 校验过，这里只取文本。 */
    private static String readString(Path p) throws CliException {
        try {
            return new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw usage("无法读取 " + p.getFileName());
        }
    }

    /** 「空白」只指 ASCII 空白（空格、\t、\n、\r），与 Node 侧相同；不用 {@link String#isBlank}（它还认 Unicode 空白）。 */
    private static boolean isAsciiBlank(String s) {
        return s.chars().allMatch(c -> c == ' ' || c == '\t' || c == '\n' || c == '\r');
    }

    private static CliException usage(String why) {
        return new CliException(ErrorCode.USAGE, why);
    }

    private static CliException compileError(String why) {
        return new CliException(ErrorCode.COMPILE_ERROR, why);
    }
}
