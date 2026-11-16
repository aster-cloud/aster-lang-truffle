package aster.truffle.embed;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.ResourceLimits;
import org.graalvm.polyglot.io.IOAccess;

/**
 * 策略求值 Context 的沙箱配置（ADR 0059 P1 T7）。
 *
 * <p>{@link #PRODUCTION} 逐项复刻 aster-api {@code TrufflePolicyRuntime.newPooledContext()}：宿主访问以
 * {@link HostAccess#EXPLICIT} 为基线、只放开数组 / List / Map 读取（入参以 host Map / List 传入，必须可读）；
 * 禁止 IO、native、宿主类查找、polyglot 互访、建进程；{@code statementLimit 10_000_000}。生产对建线程取缺省
 * （false），这里显式写出。{@link #apply(Context.Builder)} 是把它翻译成 Polyglot 选项的唯一位置，
 * {@link SandboxedExecutor} 只经它构建 Context——分散在多处写迟早漂移。
 *
 * <p>注意 aster-api 已实测 {@code statementLimit} 在当前 AsterLanguage 上不触发（未声明 StatementTag，aster-api#235）；
 * 保留它是为与生产一致，真实上界由 {@link SandboxedExecutor} 的看门狗提供。
 *
 * @param hostArrayListMapAccess 是否放开 host 数组 / List / Map 的读取（其余宿主访问恒为 EXPLICIT）
 * @param io                     是否允许 IO（false 即 {@link IOAccess#NONE}）
 * @param nativeAccess           是否允许 native 访问
 * @param hostClassLookup        是否允许宿主类查找
 * @param polyglotAccess         是否允许 polyglot 互访（false 即 {@link PolyglotAccess#NONE}）
 * @param createProcess          是否允许建进程
 * @param createThread           是否允许建线程
 * @param statementLimit         语句数上限（≤ 0 表示不设）
 */
public record SandboxPolicy(boolean hostArrayListMapAccess, boolean io, boolean nativeAccess,
                            boolean hostClassLookup, boolean polyglotAccess, boolean createProcess,
                            boolean createThread, long statementLimit) {

    /** 与 aster-api 生产 Context 相同的配置。 */
    public static final SandboxPolicy PRODUCTION =
        new SandboxPolicy(true, false, false, false, false, false, false, 10_000_000L);

    /** 把本配置写入 Context.Builder 并返回同一个 builder。 */
    public Context.Builder apply(Context.Builder builder) {
        HostAccess hostAccess = hostArrayListMapAccess
            ? HostAccess.newBuilder(HostAccess.EXPLICIT)
                .allowArrayAccess(true)
                .allowListAccess(true)
                .allowMapAccess(true)
                .build()
            : HostAccess.EXPLICIT;
        builder.allowHostAccess(hostAccess)
            .allowIO(io ? IOAccess.ALL : IOAccess.NONE)
            .allowNativeAccess(nativeAccess)
            .allowHostClassLookup(name -> hostClassLookup)
            .allowPolyglotAccess(polyglotAccess ? PolyglotAccess.ALL : PolyglotAccess.NONE)
            .allowCreateProcess(createProcess)
            .allowCreateThread(createThread);
        if (statementLimit > 0) {
            builder.resourceLimits(ResourceLimits.newBuilder().statementLimit(statementLimit, null).build());
        }
        return builder;
    }
}
