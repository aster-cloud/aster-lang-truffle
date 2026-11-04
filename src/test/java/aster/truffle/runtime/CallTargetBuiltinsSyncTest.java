package aster.truffle.runtime;

import aster.core.typecheck.CallTargetBuiltins;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * aster-lang-core {@link CallTargetBuiltins#NAMES} 必须与本仓 {@link Builtins} 中不带点的注册名完全一致。
 *
 * <p>core 的类型检查据此判断「调用位置的名字是不是由运行期直接分派的 builtin」：命中即不报
 * 「调用非函数局部名」（aster-lang-core#229 / #231 的 E001），因为 Loader 在调用位置先分派 builtin、不看同名局部。
 * 两边任一侧增删单段 builtin 而另一侧不跟，类型检查就会对能正常运行的代码报错，或放过运行期才失败的调用。
 *
 * <p>core 侧的 {@code CallTargetBuiltinsTruffleSyncTest} 需要 truffle 源码，CI 中找不到时跳过；
 * truffle 依赖 core，所以由本测试在 truffle CI 中做同一项核对。
 */
class CallTargetBuiltinsSyncTest {

    @Test
    void singleSegmentBuiltinNamesMatchCoreCallTargetBuiltins() throws ReflectiveOperationException {
        Field field = Builtins.class.getDeclaredField("REGISTRY");
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
}
