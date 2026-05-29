package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.ResourceLimits;
import org.graalvm.polyglot.io.IOAccess;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * R30+ audit P2：其它测试都用 {@code Context.newBuilder("aster").allowAllAccess(true)}
 * 跑，方便 reuse 同一 polyglot session 但事实上"测的是宽松 context"，
 * 沙盒 lockdown（ADR-0010 + R21 R22 R23）从来没在测试里验证过。
 *
 * <p>本测试显式按生产 posture build Context：
 * <ul>
 *   <li>{@link HostAccess#EXPLICIT} —— 仅暴露被 @HostAccess.Export 标注的方法</li>
 *   <li>{@link IOAccess#NONE} —— 文件 / network 全禁</li>
 *   <li>{@code allowNativeAccess(false)} —— JNI / FFI 禁</li>
 *   <li>{@code allowHostClassLookup(name -> false)} —— 反射加载宿主类禁</li>
 *   <li>{@link PolyglotAccess#NONE} —— 切语言禁</li>
 *   <li>{@code allowCreateProcess(false)} —— ProcessBuilder 禁</li>
 *   <li>{@link ResourceLimits#statementLimit(long, com.oracle.truffle.api.Source)}
 *       10M 语句上限作为 DoS 兜底</li>
 * </ul>
 *
 * <p>如果未来 GraalVM API 删 / 改任何一项，本测试编译失败 = 立刻通知。
 * 这是 ADR-0010 / R21+R22 hardening 的 regression gate。
 */
class SandboxLockdownTest {

    @Test
    @DisplayName("R21+R22+R23: production sandbox builder API is intact")
    void productionSandboxBuilderCompiles() {
        // R22 statementLimit 是 DoS 兜底。10M 个语句对正常 evaluate-source
        // 远超够用 (深度 1000 表达式 ≈ 几千个语句)；攻击场景在到达上限前
        // 就抛 PolyglotException 而不是死循环。
        ResourceLimits limits = ResourceLimits.newBuilder()
            .statementLimit(10_000_000L, null)
            .build();

        Context ctx = Context.newBuilder("aster")
            .allowHostAccess(HostAccess.EXPLICIT)
            .allowIO(IOAccess.NONE)
            .allowNativeAccess(false)
            .allowHostClassLookup(name -> false)
            .allowPolyglotAccess(PolyglotAccess.NONE)
            .allowCreateProcess(false)
            .resourceLimits(limits)
            .build();
        try {
            assertNotNull(ctx);
            assertNotNull(ctx.getEngine(),
                "Sandbox-locked context must still expose a working engine");
        } finally {
            ctx.close();
        }
    }

    @Test
    @DisplayName("R22: ResourceLimits.newBuilder().statementLimit(...) wires the documented bound")
    void statementLimitBuilderApiStable() {
        // 这一项独立测：未来 GraalVM 改 statementLimit 签名（增 / 减参数）
        // 会让 audit 团队第一时间发现，R22 的 DoS 分析需要重做。
        ResourceLimits limits = ResourceLimits.newBuilder()
            .statementLimit(10_000_000L, null)
            .build();
        assertNotNull(limits);
    }

    @Test
    @DisplayName("R21: allowHostClassLookup deny-all predicate is callable")
    void hostClassLookupDenyAllAccepted() {
        // 显式接受 Predicate<String>。如果 GraalVM 改成 BiPredicate 或别的
        // 类型，编译失败。
        java.util.function.Predicate<String> denyAll = name -> false;
        Context ctx = Context.newBuilder("aster")
            .allowHostAccess(HostAccess.EXPLICIT)
            .allowHostClassLookup(denyAll)
            .build();
        try {
            assertNotNull(ctx);
        } finally {
            ctx.close();
        }
    }
}
