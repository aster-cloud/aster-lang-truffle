package aster.truffle.embed;

import aster.truffle.nodes.LambdaValue;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 按生产沙箱单次求值 Core IR 的执行器（ADR 0059 P1 T7），供 JVM {@code aster} CLI 使用。
 *
 * <p>与 aster-api {@code TrufflePolicyRuntime} 的对应关系：
 * <ul>
 *   <li>共享一个 {@link Engine}（{@code engine.WarnInterpreterOnly=false}），每次 {@link #execute} 新建 Context
 *       （CLI 单次执行，不池化），Context 只经 {@link SandboxPolicy#apply} 构建；</li>
 *   <li>入口由 {@link CoreIrEntrySelector#withEntry} 把目标 Func 挪到最前（与平台 {@code PolicyEvaluationService} /
 *       {@code DynamicCnlExecutor} 相同），{@code ctx.eval} 后执行返回的可执行值，结果经 {@link PolyglotValues#toHost}；</li>
 *   <li>看门狗到点 {@code context.close(true)}，被取消即 {@link Outcome#TIMEOUT}。</li>
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
 * <p>求值在新建的、缺省栈大小的线程上进行（不调 {@code -Xss}，修订 1 A4），与调用线程的栈无关。
 */
public final class SandboxedExecutor implements AutoCloseable {

    /** Truffle 语句上限耗尽时 {@link PolyglotException#getMessage()} 的固定前缀。 */
    static final String STATEMENT_LIMIT_MESSAGE = "Statement count limit";

    private final SandboxPolicy policy;
    private final Engine engine;
    private final ScheduledExecutorService watchdog;

    /** 使用 {@link SandboxPolicy#PRODUCTION}。 */
    public SandboxedExecutor() {
        this(SandboxPolicy.PRODUCTION);
    }

    public SandboxedExecutor(SandboxPolicy policy) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.engine = Engine.newBuilder("aster")
            .option("engine.WarnInterpreterOnly", "false")
            .build();
        this.watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "aster-sandbox-watchdog");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 求值 {@code coreJson} 的入口 {@code entry}。
     *
     * @param coreJson Core IR v2 JSON（调用方已严格读入校验）
     * @param entry    入口函数名
     * @param hostArgs 按 IR 形参顺序排列的宿主实参（Map / List / 标量 / BigDecimal…，经 {@link PolyglotValues#toGuestArgs}）
     * @param timeout  墙钟上限（> 0）
     */
    public Outcome execute(String coreJson, String entry, Object[] hostArgs, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout 必须为正: " + timeout);
        }
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

        FutureTask<Outcome> task = new FutureTask<>(() -> evaluate(ir, guestArgs, timeout));
        // stackSize 0 = JVM 缺省线程栈
        Thread worker = new Thread(null, task, "aster-sandbox-eval", 0);
        worker.setDaemon(true);
        worker.start();
        try {
            return task.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Outcome.Failed(Outcome.EVAL_ERROR, "求值被中断");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StackOverflowError) {
                return new Outcome.Failed(Outcome.RESOURCE_EXHAUSTED, "栈溢出");
            }
            return new Outcome.Failed(Outcome.EVAL_ERROR, String.valueOf(cause));
        }
    }

    private Outcome evaluate(String ir, Object[] guestArgs, Duration timeout) {
        Context ctx = policy.apply(Context.newBuilder("aster").engine(engine)).build();
        AtomicBoolean fired = new AtomicBoolean(false);
        ScheduledFuture<?> guard = watchdog.schedule(() -> {
            fired.set(true);
            ctx.close(true);
        }, timeout.toNanos(), TimeUnit.NANOSECONDS);
        try {
            Value evalResult = ctx.eval(Source.create("aster", ir));
            return new Outcome.Ok(executeFunction(evalResult, guestArgs));
        } catch (PolyglotException e) {
            return classify(e, fired.get());
        } catch (StackOverflowError e) {
            return new Outcome.Failed(Outcome.RESOURCE_EXHAUSTED, "栈溢出");
        } catch (ValueShapeException e) {
            return new Outcome.Failed(Outcome.EVAL_ERROR, e.getMessage());
        } catch (RuntimeException e) {
            // Context 被看门狗关闭后继续访问会抛 IllegalStateException
            return fired.get()
                ? new Outcome.Failed(Outcome.TIMEOUT, "超过 " + timeout.toMillis() + "ms")
                : new Outcome.Failed(Outcome.EVAL_ERROR, String.valueOf(e));
        } finally {
            guard.cancel(false);
            try {
                ctx.close(true);
            } catch (RuntimeException ignored) {
                // 已被看门狗关闭
            }
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
        engine.close(true);
    }
}
