package aster.truffle.embed;

import aster.truffle.runtime.Builtins;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 导出 builtin 白名单 {@code aster/truffle/export-builtins.json}（ADR 0059 P1 T10）与引擎同步。
 *
 * <p>口径（计划 T10「白名单口径」）：{@code names} = {@link Builtins#registeredNames()} 去掉 {@code IO.} 前缀者，按字符串升序、
 * 无重复；{@code aliases} = {@link Builtins#canonicalName} 的别名表（小写别名 → 注册名）。P2 的导出检查先按 aliases 归一、再查 names。
 *
 * <p>注册表是全局静态的，同一 JVM 里其它测试会往里注册探针，所以在独立类加载器里重新加载 {@link Builtins}；别名表取自
 * {@code canonicalName} 的 switch 分支（与 {@code CallTargetBuiltinsSyncTest} / core {@code CallTargetBuiltinsTruffleSyncTest}
 * 同一解析方式），再逐条经 {@code canonicalName} 回验。改了注册表或别名后运行
 * {@code ./gradlew test --tests aster.truffle.embed.ExportBuiltinsTest -Daster.exportBuiltins.update=true} 重新生成并提交。
 */
class ExportBuiltinsTest {

    private static final Path FILE = Path.of("src/main/resources/aster/truffle/export-builtins.json");
    private static final Pattern ALIAS_CASE = Pattern.compile("case ((?:\"[^\"]*\",?\\s*)+)->\\s*\"([a-z]+)\";");
    private static final Pattern QUOTED = Pattern.compile("\"([^\"]*)\"");
    private static final ObjectMapper MAPPER = JsonMapper.builder()
        .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
        .build();

    @Test
    void fileMatchesEngine() throws Exception {
        String want = expected();
        if (Boolean.getBoolean("aster.exportBuiltins.update")) {
            Files.writeString(FILE, want, StandardCharsets.UTF_8);
        }
        try (InputStream in = ExportBuiltinsTest.class.getResourceAsStream("/aster/truffle/export-builtins.json")) {
            assertNotNull(in, "缺少 aster/truffle/export-builtins.json");
            assertEquals(want, new String(in.readAllBytes(), StandardCharsets.UTF_8),
                "导出白名单与 Builtins 不同步；按类注释重新生成");
        }
    }

    @Test
    void namesAreSortedUniqueAndExcludeIo() throws Exception {
        List<String> names = names();
        assertTrue(names.size() > 50, "白名单过小：" + names.size());
        for (int k = 1; k < names.size(); k++) {
            assertTrue(names.get(k - 1).compareTo(names.get(k)) < 0, "未严格升序：" + names.get(k));
        }
        assertTrue(names.stream().noneMatch(n -> n.startsWith("IO.")), "白名单不得含 IO.*");
        assertTrue(names.contains("add") && names.contains("Text.concat"), "缺少常用 builtin");
    }

    @Test
    void aliasesResolveThroughCanonicalName() throws Exception {
        Map<String, String> aliases = aliases();
        assertTrue(aliases.size() >= 30, "别名表过小：" + aliases.size());
        List<String> names = names();
        for (Map.Entry<String, String> e : aliases.entrySet()) {
            assertEquals(e.getValue(), Builtins.canonicalName(e.getKey()), e.getKey());
            assertTrue(names.contains(e.getValue()), e.getKey() + " → " + e.getValue() + " 不在 names 中");
        }
    }

    private static String expected() throws Exception {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("aliases", aliases());
        doc.put("engine", "aster-lang-truffle");
        doc.put("names", names());
        return MAPPER.writeValueAsString(doc) + "\n";
    }

    @SuppressWarnings("unchecked")
    private static List<String> names() throws Exception {
        Class<?> fresh = freshBuiltinsClass();
        SortedSet<String> all = (SortedSet<String>) fresh.getMethod("registeredNames").invoke(null);
        List<String> out = new ArrayList<>();
        for (String n : all) {
            if (!n.startsWith("IO.")) {
                out.add(n);
            }
        }
        return out;
    }

    private static Map<String, String> aliases() throws Exception {
        String source = Files.readString(Path.of("src/main/java/aster/truffle/runtime/Builtins.java"));
        String body = source.substring(source.indexOf("public static String canonicalName("));
        body = body.substring(0, body.indexOf("default ->"));
        Map<String, String> out = new TreeMap<>();
        Matcher cases = ALIAS_CASE.matcher(body);
        while (cases.find()) {
            Matcher quoted = QUOTED.matcher(cases.group(1));
            while (quoted.find()) {
                out.put(quoted.group(1), cases.group(2));
            }
        }
        return out;
    }

    /** 在只以平台类加载器为父的新类加载器中加载 Builtins，得到未被其它测试污染的注册表。 */
    private static Class<?> freshBuiltinsClass() throws ClassNotFoundException {
        List<URL> urls = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            if (entry.isEmpty()) {
                continue;
            }
            try {
                urls.add(new File(entry).toURI().toURL());
            } catch (MalformedURLException e) {
                throw new IllegalStateException("无效的 classpath 条目：" + entry, e);
            }
        }
        ClassLoader isolated = new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader());
        return Class.forName(Builtins.class.getName(), true, isolated);
    }
}
