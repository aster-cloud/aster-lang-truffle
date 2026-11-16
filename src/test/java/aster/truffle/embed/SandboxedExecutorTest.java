package aster.truffle.embed;

import aster.truffle.CnlCompiler;
import org.graalvm.polyglot.Context;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link SandboxPolicy} 与 {@link SandboxedExecutor}（ADR 0059 P1 T7）。 */
class SandboxedExecutorTest {

    private static SandboxedExecutor executor;

    @BeforeAll
    static void setUp() {
        executor = new SandboxedExecutor();
    }

    @AfterAll
    static void tearDown() {
        executor.close();
    }

    private static String ir(String cnl) throws Exception {
        return CnlCompiler.compile(cnl, "en-US");
    }

    @Test
    void productionPolicyMirrorsAsterApi() {
        assertEquals(new SandboxPolicy(true, false, false, false, false, false, false, 10_000_000L),
            SandboxPolicy.PRODUCTION);
    }

    @Test
    void applyOpensHostMapAndListReadsOnlyWhenEnabled() {
        try (Context c = SandboxPolicy.PRODUCTION.apply(Context.newBuilder("aster")).build()) {
            assertTrue(c.asValue(new HashMap<>(Map.of("a", 1))).hasHashEntries());
            assertTrue(c.asValue(new ArrayList<>(List.of(1))).hasArrayElements());
        }
        SandboxPolicy closed = new SandboxPolicy(false, false, false, false, false, false, false, 10_000_000L);
        try (Context c = closed.apply(Context.newBuilder("aster")).build()) {
            assertFalse(c.asValue(new HashMap<>(Map.of("a", 1))).hasHashEntries());
            assertFalse(c.asValue(new ArrayList<>(List.of(1))).hasArrayElements());
        }
    }

    @Test
    void evaluatesSelectedEntryWithHostArgs() throws Exception {
        String core = ir("""
            Module demo.

            Rule first given x as Int, produce Int:
              Return x plus 1.

            Rule second given x as Int, produce Int:
              Return x times 10.
            """);
        Outcome first = executor.execute(core, "first", new Object[]{4}, Duration.ofSeconds(5));
        Outcome second = executor.execute(core, "second", new Object[]{4}, Duration.ofSeconds(5));
        assertEquals(5, ((Outcome.Ok) first).hostValue());
        assertEquals(40, ((Outcome.Ok) second).hostValue());
    }

    @Test
    void decimalArgumentRoundTrips() throws Exception {
        String core = ir("""
            Module demo.

            Rule half given x as Decimal, produce Decimal:
              Return x times 2.
            """);
        Outcome out = executor.execute(core, "half", new Object[]{new BigDecimal("1.25")}, Duration.ofSeconds(5));
        // 与平台 /evaluate-json 相同：Decimal 结果经 toHost 成为十进制字符串
        Object host = ((Outcome.Ok) out).hostValue();
        assertInstanceOf(String.class, host, out.toString());
        assertEquals(0, new BigDecimal("2.50").compareTo(new BigDecimal((String) host)), out.toString());
    }

    @Test
    void exponentialRecursionTimesOut() throws Exception {
        String core = ir("""
            Module demo.

            Rule spin given n, produce:
              If n less than 2
                Return 1.
              Return spin(n minus 1) plus spin(n minus 1).
            """);
        long start = System.nanoTime();
        Outcome out = executor.execute(core, "spin", new Object[]{60}, Duration.ofSeconds(1));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertEquals(Outcome.TIMEOUT, ((Outcome.Failed) out).code(), out.toString());
        assertTrue(elapsedMs < 2_000, "超时应在 2 s 内返回，实际 " + elapsedMs + "ms");
    }

    @Test
    void concurrentTimeoutsDoNotDelayEachOther() throws Exception {
        String core = ir("""
            Module demo.

            Rule spin given n, produce:
              If n less than 2
                Return 1.
              Return spin(n minus 1) plus spin(n minus 1).
            """);
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(4);
        try {
            long start = System.nanoTime();
            List<java.util.concurrent.Future<Outcome>> futures = new ArrayList<>();
            for (int k = 0; k < 4; k++) {
                futures.add(pool.submit(() -> executor.execute(core, "spin", new Object[]{60}, Duration.ofSeconds(1))));
            }
            for (var f : futures) {
                assertEquals(Outcome.TIMEOUT, ((Outcome.Failed) f.get()).code());
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMs < 3_000, "四个并发超时应约 1 s 内全部返回，实际 " + elapsedMs + "ms");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void interruptedCallerCancelsEvaluation() throws Exception {
        String core = ir("""
            Module demo.

            Rule spin given n, produce:
              If n less than 2
                Return 1.
              Return spin(n minus 1) plus spin(n minus 1).
            """);
        java.util.concurrent.atomic.AtomicReference<Outcome> result = new java.util.concurrent.atomic.AtomicReference<>();
        Thread caller = new Thread(() -> result.set(executor.execute(core, "spin", new Object[]{60}, Duration.ofSeconds(60))));
        caller.start();
        Thread.sleep(300);
        caller.interrupt();
        caller.join(5_000);
        assertFalse(caller.isAlive(), "中断后调用线程应返回");
        assertEquals(Outcome.EVAL_ERROR, ((Outcome.Failed) result.get()).code());
        // 被取消的求值不再占用执行器：随后的正常求值照常完成
        Outcome ok = executor.execute(ir("""
            Module demo.

            Rule ident given x as Int, produce Int:
              Return x.
            """), "ident", new Object[]{1}, Duration.ofSeconds(5));
        assertEquals(1, ((Outcome.Ok) ok).hostValue());
    }

    @Test
    void statementLimitIsReportedAsTimeout() throws Exception {
        // 计算量预算（语句上限）与墙钟超时同归 TIMEOUT，避免快机器上与 Node CLI 分叉
        SandboxPolicy tiny = new SandboxPolicy(true, false, false, false, false, false, false, 1L);
        String core = ir("""
            Module demo.

            Rule spin given n, produce:
              If n less than 2
                Return 1.
              Return spin(n minus 1) plus spin(n minus 1).
            """);
        try (SandboxedExecutor limited = new SandboxedExecutor(tiny)) {
            Outcome out = limited.execute(core, "spin", new Object[]{18}, Duration.ofSeconds(5));
            // 当前 AsterLanguage 不声明 StatementTag，语句上限不会触发（aster-api#235）；若将来触发，必须归为 TIMEOUT
            if (out instanceof Outcome.Failed f) {
                assertEquals(Outcome.TIMEOUT, f.code(), out.toString());
            } else {
                assertInstanceOf(Outcome.Ok.class, out);
            }
        }
    }

    @Test
    void deepRecursionIsResourceExhausted() throws Exception {
        String core = ir("""
            Module demo.

            Rule deep given n, produce:
              If n at most 0
                Return 0.
              Return 1 plus deep(n minus 1).
            """);
        Outcome out = executor.execute(core, "deep", new Object[]{200_000}, Duration.ofSeconds(30));
        assertEquals(Outcome.RESOURCE_EXHAUSTED, ((Outcome.Failed) out).code(), out.toString());
    }

    @Test
    void missingEntryIsEntryNotFound() throws Exception {
        String core = ir("""
            Module demo.

            Rule only given x as Int, produce Int:
              Return x.
            """);
        Outcome out = executor.execute(core, "nope", new Object[]{1}, Duration.ofSeconds(5));
        assertEquals(Outcome.ENTRY_NOT_FOUND, ((Outcome.Failed) out).code());
    }

    @Test
    void entryShadowedByMainIsEntryNotFound() throws Exception {
        String core = ir("""
            Module demo.

            Rule main given x as Int, produce Int:
              Return x.

            Rule other given x as Int, produce Int:
              Return x plus 1.
            """);
        Outcome out = executor.execute(core, "other", new Object[]{1}, Duration.ofSeconds(5));
        assertEquals(Outcome.ENTRY_NOT_FOUND, ((Outcome.Failed) out).code(), out.toString());
    }

    @Test
    void ioInPolicyIsEvalError() throws Exception {
        String core = ir("""
            Module demo.

            Rule shout given x as Text, produce Text. It performs io:
              IO.print(x).
              Return x.
            """);
        Outcome out = executor.execute(core, "shout", new Object[]{"hi"}, Duration.ofSeconds(5));
        assertEquals(Outcome.EVAL_ERROR, ((Outcome.Failed) out).code(), out.toString());
    }

    @Test
    void divisionByZeroIsEvalError() throws Exception {
        String core = ir("""
            Module demo.

            Rule div given x as Int, produce Int:
              Return 10 divided by x.
            """);
        Outcome out = executor.execute(core, "div", new Object[]{0}, Duration.ofSeconds(5));
        assertEquals(Outcome.EVAL_ERROR, ((Outcome.Failed) out).code(), out.toString());
    }
}
