package aster.cli;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 信封的 {@code meta}（README「输出」中 {@code M} 的取值时机）：各阶段通过后由 {@link PackagePipeline} 逐项填写，
 * 失败信封按失败时的状态原样输出。
 */
final class Meta {

    static final String ENGINE = "aster-lang-truffle";

    boolean signed;
    String entry;
    String coreIrSha256;
    boolean irRecompiled;

    Map<String, Object> toTree() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("cliVersion", Version.CLI_VERSION);
        m.put("coreIrSha256", coreIrSha256);
        m.put("engine", ENGINE);
        m.put("entry", entry);
        m.put("irRecompiled", irRecompiled);
        m.put("signed", signed);
        return m;
    }
}
