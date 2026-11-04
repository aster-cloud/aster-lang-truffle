package aster.truffle.runtime;

import aster.core.typecheck.CallTargetBuiltins;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * aster-lang-core {@link CallTargetBuiltins#NAMES} 必须与本仓 {@link Builtins} 中不带点的注册名完全一致。
 *
 * <p>core 的类型检查据此判断「调用位置的名字是不是由运行期直接分派的 builtin」：命中即不报
 * 「调用非函数局部名」（aster-lang-core#229 / #231 的 E001），因为 Loader 在调用位置先分派 builtin（同名局部不是函数值时）。
 * 两边任一侧增删单段 builtin 而另一侧不跟，类型检查就会对能正常运行的代码报错，或放过运行期才失败的调用。
 *
 * <p>core 侧的 {@code CallTargetBuiltinsTruffleSyncTest} 需要 truffle 源码，CI 中找不到时跳过；
 * truffle 依赖 core，所以由本测试在 truffle CI 中做同一项核对。
 *
 * <p>注册表是静态的，同一 JVM 里其它测试会往里注册探针（如 {@code __concProbe*}、{@code double}），
 * 所以在独立的类加载器里重新加载 {@link Builtins}，只看静态初始化注册的名字。
 */
class CallTargetBuiltinsSyncTest {

    @Test
    void singleSegmentBuiltinNamesMatchCoreCallTargetBuiltins() throws ReflectiveOperationException {
        Class<?> fresh = freshBuiltinsClass();
        assertNotSame(Builtins.class, fresh, "未能隔离加载 Builtins");
        Field field = fresh.getDeclaredField("REGISTRY");
        field.setAccessible(true);
        Map<?, ?> registry = (Map<?, ?>) field.get(null);

        Set<String> truffleNames = new TreeSet<>();
        for (Object key : registry.keySet()) {
            String name = (String) key;
            if (!name.contains(".")) truffleNames.add(name);
        }
        assertEquals(new TreeSet<>(CallTargetBuiltins.NAMES), truffleNames,
            "aster-lang-core CallTargetBuiltins.NAMES 与 truffle Builtins 的单段注册名不一致，两侧须同步修改");
    }

    /** 在只以平台类加载器为父的新类加载器中加载 Builtins，得到未被其它测试污染的注册表。 */
    private static Class<?> freshBuiltinsClass() throws ClassNotFoundException {
        List<URL> urls = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            if (entry.isEmpty()) continue;
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
