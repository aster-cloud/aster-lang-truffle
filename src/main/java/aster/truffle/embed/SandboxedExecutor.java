package aster.truffle.embed;

import aster.truffle.nodes.LambdaValue;
import aster.truffle.runtime.CallDepthExceededError;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 按生产沙箱单次求值 Core IR 的执行器（ADR 0059 P1 T7），供 JVM {@code aster} CLI 使用。
 *
 * <p>与 aster-api {@code TrufflePolicyRuntime} 的对应关系：
 * <ul>
 *   <li>共享一个 {@link Engine}（{@code engine.WarnInterpreterOnly=false}），每次 {@link #execute} 新建 Context
 *       （CLI 单次执行，不池化），Context 只经 {@link SandboxPolicy#apply} 构建；</li>
 *   <li>入口由 {@link CoreIrEntrySelector#withEntry} 把目标 Func 挪到最前（与平台 {@code PolicyEvaluationService} /
 *       {@code DynamicCnlExecutor} 相同），{@code ctx.eval} 后执行返回的可执行值，结果经 {@link PolyglotValues#toHost}；</li>
 *   <li>看门狗在 IR 加载、入口选定之后、执行入口之前开始计时，到点 {@code context.close(true)}，被取消即 {@link Outcome#TIMEOUT}。</li>
 * </ul>
 *
 * <p>失败归类（README「检查顺序」eval 阶段）：
 * <ul>
 *   <li>入口不存在、或被 {@code main}（{@code ASTER_TRUFFLE_FUNC}）抢占 → {@link Outcome#ENTRY_NOT_FOUND}；</li>
 *   <li>看门狗已触发后的取消 / 资源耗尽 → {@link Outcome#TIMEOUT}（超时优先）；</li>
 *   <li>{@code statementLimit} 耗尽 → {@link Outcome#TIMEOUT}：它与超时同是「计算量超出预算」，Node CLI 没有语句上限，
 *       只会在同一策略上得到 {@code TIMEOUT}；若归为资源耗尽，快机器上 {@code a-timeout} 会先撞语句上限而与 Node 分叉；</li>
 *   <li>栈溢出等其余资源耗尽 → {@link Outcome#RESOURCE_EXHAUSTED}；</li>
 *   <li>其余 guest / 宿主异常、结果形态违例 → {@link Outcome#EVAL_ERROR}。</li>
 * </ul>
 *
 * <p>求值在新建的线程上进行，栈大小固定为 {@link #EVAL_THREAD_STACK_BYTES}（不依赖 {@code -Xss} 或原生镜像的缺省线程栈），
 * 与调用线程的栈无关。调用深度由 {@link aster.truffle.runtime.AllocationBudget#MAX_CALL_DEPTH}（10 000，与 Node CLI 相同）
 * 截住，超限为 {@link Outcome#RESOURCE_EXHAUSTED}。
 */
public final class SandboxedExecutor implements AutoCloseable {

    /** Truffle 语句上限耗尽时 {@link PolyglotException#getMessage()} 的固定前缀。 */
    static final String STATEMENT_LIMIT_MESSAGE = "Statement count limit";

    /** 求值前阶段（选入口、转换实参、建 Context、加载 IR）的上限；超出为 RESOURCE_EXHAUSTED。 */
    public static final Duration PRE_EVAL_LIMIT = Duration.ofSeconds(60);

    /**
     * 超时之后的兜底宽限：{@code close(true)} 依赖 guest 代码轮询安全点，长时间不轮询的宿主侧 builtin 可能迟迟不退出；
     * 到 {@code timeout + 宽限} 仍未结束即按 TIMEOUT 返回，不再等待（求值线程是 daemon，CLI 随即退出）。
     */
    public static final Duration BACKSTOP_GRACE = Duration.ofSeconds(5);

    /**
     * 求值线程的栈大小：64 MiB。让调用深度上限（10 000）先于真实栈溢出生效，结果才在 jar 与原生可执行文件、各平台之间确定。
     * 实测（macOS arm64，jar）线性递归每层约 0.8–1.6 KiB：缺省 2 MiB 约 1 000 层即溢出，32 MiB 能到 20 000 层以上；
     * 取 64 MiB 给帧更大的函数（调用夹在 List.map 回调、深层表达式里）留余量。只是保留的虚拟地址，按需提交，不占常驻内存。
     * {@code Thread} 的 stackSize 参数在 HotSpot（Linux / macOS / Windows）与 SubstrateVM 上都生效；
     * 实测覆盖 macOS 的 jar 与原生可执行文件、Linux x64 的 jar（CI 的 shadowJarSmokeTest）。
     * Node CLI 的 Worker 对应值是 {@code stackSizeMb = 32}（V8 帧更小）。
     */
    public static final long EVAL_THREAD_STACK_BYTES = 64L * 1024 * 1024;

    private final SandboxPolicy policy;
    private final Duration backstopGrace;
    private final Runnable afterStart;
    private final Duration preEvalLimit;
    private final Runnable beforeExecute;
    private final Engine engine;
    private final ScheduledExecutorService watchdog;
    /** 执行取消动作：{@code close(true)} 会等待求值线程退出，不能占住唯一的调度线程（同 aster-api {@code ExecutionWatchdog}）。 */
    private final ExecutorService closer;

    /** 使用 {@link SandboxPolicy#PRODUCTION}。 */
    public SandboxedExecutor() {
        this(SandboxPolicy.PRODUCTION);
    }

    public SandboxedExecutor(SandboxPolicy policy) {
        this(policy, PRE_EVAL_LIMIT, () -> { });
    }

    /**
     * 可注入求值前阶段的上限与一个在执行入口前调用的钩子（测试用：在钩子里人为卡住以验证上限）。
     */
    SandboxedExecutor(SandboxPolicy policy, Duration preEvalLimit, Runnable beforeExecute) {
        this(policy, preEvalLimit, beforeExecute, BACKSTOP_GRACE, () -> { });
    }

    /**
     * 另可注入超时后的兜底宽限与一个在执行开始（看门狗已布防）之后调用的钩子（测试用：模拟不轮询安全点的宿主操作）。
     */
    SandboxedExecutor(SandboxPolicy policy, Duration preEvalLimit, Runnable beforeExecute, Duration backstopGrace,
                      Runnable afterStart) {
        this.backstopGrace = Objects.requireNonNull(backstopGrace, "backstopGrace");
        this.afterStart = Objects.requireNonNull(afterStart, "afterStart");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.preEvalLimit = Objects.requireNonNull(preEvalLimit, "preEvalLimit");
        this.beforeExecute = Objects.requireNonNull(beforeExecute, "beforeExecute");
        this.engine = Engine.newBuilder("aster")
            .option("engine.WarnInterpreterOnly", "false")
            .build();
        this.watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "aster-sandbox-watchdog");
            t.setDaemon(true);
            return t;
        });
        this.closer = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "aster-sandbox-close");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 求值 {@code coreJson} 的入口 {@code entry}。
     *
     * <p>两段时限：选入口、转换实参、建 Context、加载 IR 这些求值前阶段受 {@link #PRE_EVAL_LIMIT}（60 s）约束，超出为
     * {@link Outcome#RESOURCE_EXHAUSTED}（资源类结果，不在逐字节一致的承诺内）；执行入口受 {@code timeout} 约束，超出为
     * {@link Outcome#TIMEOUT}（README「命令行」计时起点）。
     *
     * @param coreJson Core IR v2 JSON（调用方已严格读入校验）
     * @param entry    入口函数名
     * @param hostArgs 按 IR 形参顺序排列的宿主实参（Map / List / 标量 / BigDecimal…，经 {@link PolyglotValues#toGuestArgs}）
     * @param timeout  执行入口的墙钟上限（> 0）
     */
    public Outcome execute(String coreJson, String entry, Object[] hostArgs, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout 必须为正: " + timeout);
        }
        AtomicReference<Context> running = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);
        CountDownLatch executing = new CountDownLatch(1);
        FutureTask<Outcome> task = new FutureTask<>(
            () -> evaluate(coreJson, entry, hostArgs, timeout, running, cancelled, executing));
        Thread worker = new Thread(null, task, "aster-sandbox-eval", EVAL_THREAD_STACK_BYTES);
        worker.setDaemon(true);
        worker.start();
        try {
            if (!executing.await(preEvalLimit.toNanos(), TimeUnit.NANOSECONDS)) {
                cancel(running, cancelled);
                return new Outcome.Failed(Outcome.RESOURCE_EXHAUSTED,
                    "求值前阶段（建 Context、加载 IR、选入口、转换实参）超过 " + preEvalLimit.toSeconds() + " s");
            }
            try {
                return task.get(timeout.plus(backstopGrace).toNanos(), TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                cancel(running, cancelled);
                return new Outcome.Failed(Outcome.TIMEOUT,
                    "超过 " + timeout.toMillis() + "ms 且取消后 " + backstopGrace.toSeconds() + " s 内未结束");
            }
        } catch (InterruptedException e) {
            // 调用线程被中断：取消仍在进行的求值，不留下继续运行的 guest 代码
            cancel(running, cancelled);
            Thread.currentThread().interrupt();
            return new Outcome.Failed(Outcome.EVAL_ERROR, "求值被中断");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StackOverflowError so) {
                return new Outcome.Failed(Outcome.RESOURCE_EXHAUSTED, stackMessage(so));
            }
            return new Outcome.Failed(Outcome.EVAL_ERROR, String.valueOf(cause));
        }
    }

    /** 先置位再读 Context：求值线程发布 Context 后会复查该标志，两种先后顺序下都有一方负责关闭。 */
    private void cancel(AtomicReference<Context> running, AtomicBoolean cancelled) {
        cancelled.set(true);
        Context ctx = running.get();
        if (ctx != null) {
            closer.execute(() -> closeQuietly(ctx));
        }
    }

    /**
     * 求值线程：求值前阶段结束（或提前失败）时放开 {@code executing}，之后只受执行超时约束。
     */
    private Outcome evaluate(String coreJson, String entry, Object[] hostArgs, Duration timeout,
                             AtomicReference<Context> running, AtomicBoolean cancelled, CountDownLatch executing) {
        try {
            String ir;
            try {
                ir = CoreIrEntrySelector.withEntry(coreJson, entry);
            } catch (EntryFunctionException e) {
                return new Outcome.Failed(Outcome.ENTRY_NOT_FOUND, e.getMessage());
            } catch (IllegalArgumentException e) {
                return new Outcome.Failed(Outcome.EVAL_ERROR, e.getMessage());
            }
            Object[] guestArgs;
            try {
                guestArgs = PolyglotValues.toGuestArgs(hostArgs == null ? new Object[0] : hostArgs);
            } catch (ValueShapeException e) {
                return new Outcome.Failed(Outcome.EVAL_ERROR, e.getMessage());
            }
            return evaluateInContext(ir, guestArgs, timeout, running, cancelled, executing);
        } finally {
            executing.countDown();
        }
    }

    private Outcome evaluateInContext(String ir, Object[] guestArgs, Duration timeout,
                                      AtomicReference<Context> running, AtomicBoolean cancelled,
                                      CountDownLatch executing) {
        Context ctx = policy.apply(Context.newBuilder("aster").engine(engine)).build();
        running.set(ctx);
        if (cancelled.get()) {
            closeQuietly(ctx);
            return new Outcome.Failed(Outcome.EVAL_ERROR, "求值被中断");
        }
        AtomicBoolean fired = new AtomicBoolean(false);
        ScheduledFuture<?> guard = null;
        try {
            // 计时起点（README「命令行」）：Context 已创建、IR 已加载、入口已选好之后，紧挨着执行入口之前；
            // Engine / Context 创建与 IR 加载不计入超时（它们受求值前阶段的 60 s 上限约束）
            Value evalResult = ctx.eval(Source.create("aster", ir));
            beforeExecute.run();
            if (cancelled.get()) {
                return new Outcome.Failed(Outcome.RESOURCE_EXHAUSTED, "求值前阶段超时后被取消");
            }
            guard = watchdog.schedule(() -> {
                fired.set(true);
                closer.execute(() -> closeQuietly(ctx));
            }, timeout.toNanos(), TimeUnit.NANOSECONDS);
            executing.countDown();
            afterStart.run();
            return new Outcome.Ok(executeFunction(evalResult, guestArgs));
        } catch (PolyglotException e) {
            return classify(e, fired.get());
        } catch (StackOverflowError e) {
            return new Outcome.Failed(Outcome.RESOURCE_EXHAUSTED, stackMessage(e));
        } catch (ValueShapeException e) {
            return new Outcome.Failed(Outcome.EVAL_ERROR, e.getMessage());
        } catch (RuntimeException e) {
            // Context 被看门狗关闭后继续访问会抛 IllegalStateException
            return fired.get()
                ? new Outcome.Failed(Outcome.TIMEOUT, "超过 " + timeout.toMillis() + "ms")
                : new Outcome.Failed(Outcome.EVAL_ERROR, String.valueOf(e));
        } finally {
            if (guard != null) {
                guard.cancel(false);
            }
            closeQuietly(ctx);
        }
    }

    /** 调用深度超限带自己的消息（与 Node CLI 相同）；真实栈溢出的 JDK 消息为空，统一写「栈溢出」。 */
    private static String stackMessage(StackOverflowError e) {
        return e instanceof CallDepthExceededError ? e.getMessage() : "栈溢出";
    }

    private static void closeQuietly(Context ctx) {
        try {
            ctx.close(true);
        } catch (RuntimeException ignored) {
            // 已关闭或正在被另一线程关闭
        }
    }

    /** 与 aster-api {@code TrufflePolicyRuntime.executeFunction} 相同的取值方式。 */
    private static Object executeFunction(Value evalResult, Object[] guestArgs) {
        if (evalResult.canExecute()) {
            return PolyglotValues.toHost(evalResult.execute(guestArgs));
        }
        if (evalResult.isHostObject()) {
            Object hostObject = evalResult.asHostObject();
            return hostObject instanceof LambdaValue lambda ? lambda.apply(guestArgs, null) : hostObject;
        }
        return PolyglotValues.toHost(evalResult);
    }

    static Outcome classify(PolyglotException e, boolean watchdogFired) {
        String message = String.valueOf(e.getMessage());
        if (e.isCancelled() || (watchdogFired && (e.isResourceExhausted() || e.isInterrupted()))) {
            return new Outcome.Failed(Outcome.TIMEOUT, message);
        }
        if (e.isResourceExhausted()) {
            return message.startsWith(STATEMENT_LIMIT_MESSAGE)
                ? new Outcome.Failed(Outcome.TIMEOUT, message)
                : new Outcome.Failed(Outcome.RESOURCE_EXHAUSTED, message);
        }
        return new Outcome.Failed(Outcome.EVAL_ERROR, message);
    }

    @Override
    public void close() {
        watchdog.shutdownNow();
        closer.shutdown();
        engine.close(true);
    }
}
