package aster.truffle.spike;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.ResourceLimits;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.IOAccess;

/**
 * ADR 0059 P0-T0.1 实测入口（spike，不合并）：按 aster-api 生产沙箱配置在 stock JRE 上求值规范 Core IR。
 * 用法：StockJreSpike <core.json> <iterations> [--warn] [--timeout-ms N] [int 实参...]
 * 输出：stderr 打印各阶段耗时，stdout 打印结果。
 */
public final class StockJreSpike {
  public static void main(String[] args) throws Exception {
    long t0 = System.nanoTime();
    long jvmStartMs = java.lang.management.ManagementFactory.getRuntimeMXBean().getStartTime();
    String coreJson = Files.readString(Path.of(args[0]));
    int iterations = Integer.parseInt(args[1]);
    boolean warn = false;
    long timeoutMs = 0;
    java.util.List<Object> fnArgs = new java.util.ArrayList<>();
    for (int i = 2; i < args.length; i++) {
      if ("--warn".equals(args[i])) warn = true;
      else if ("--timeout-ms".equals(args[i])) timeoutMs = Long.parseLong(args[++i]);
      else fnArgs.add(Integer.parseInt(args[i]));
    }
    Engine.Builder eb = Engine.newBuilder();
    if (!warn) eb.option("engine.WarnInterpreterOnly", "false");
    try (Engine engine = eb.build()) {
      long tEngine = System.nanoTime();
      Source source = Source.newBuilder("aster", coreJson, "core").cached(true).build();
      long[] times = new long[iterations];
      Object last = null;
      for (int i = 0; i < iterations; i++) {
        long s = System.nanoTime();
        Context ctx = Context.newBuilder("aster").engine(engine)
            .allowHostAccess(HostAccess.newBuilder(HostAccess.EXPLICIT)
                .allowArrayAccess(true).allowListAccess(true).allowMapAccess(true).build())
            .allowIO(IOAccess.NONE).allowNativeAccess(false).allowHostClassLookup(n -> false)
            .allowPolyglotAccess(PolyglotAccess.NONE).allowCreateProcess(false).allowCreateThread(false)
            .resourceLimits(ResourceLimits.newBuilder().statementLimit(10_000_000L, null).build())
            .build();
        java.util.concurrent.ScheduledExecutorService wd = null;
        try {
          if (timeoutMs > 0) {
            wd = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
            final Context c = ctx;
            wd.schedule(() -> { c.close(true); }, timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
          }
          Value fn = ctx.eval(source);
          Value r = fn.canExecute() ? fn.execute(fnArgs.toArray()) : fn;
          last = r.toString();
        } catch (PolyglotException e) {
          last = "PolyglotException cancelled=" + e.isCancelled() + " resourceExhausted=" + e.isResourceExhausted() + " msg=" + e.getMessage();
        } finally {
          if (wd != null) wd.shutdownNow();
          try { ctx.close(); } catch (Exception ignored) { }
        }
        times[i] = System.nanoTime() - s;
      }
      long tEnd = System.nanoTime();
      long[] sorted = times.clone();
      Arrays.sort(sorted);
      System.err.printf("jvmStart->main=%dms main->engine=%dms firstEval=%.1fms medianEval=%.2fms total(main->end)=%dms%n",
          (t0 / 1_000_000 - 0) == 0 ? 0 : (System.currentTimeMillis() - jvmStartMs) - (tEnd - t0) / 1_000_000,
          (tEngine - t0) / 1_000_000, times[0] / 1e6, sorted[sorted.length / 2] / 1e6, (tEnd - t0) / 1_000_000);
      System.err.println("evalTimesMs=" + Arrays.toString(Arrays.stream(times).mapToDouble(x -> x / 1e6).toArray()));
      System.out.println(last);
    }
  }
}
