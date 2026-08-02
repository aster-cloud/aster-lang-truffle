package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * benchmark 用例的**正确性**断言（与耗时测量解耦）。
 *
 * ★为什么需要本测试（#60）：`GraalVMJitBenchmark` 整体在 CI 被
 * `-PexcludeBenchmarks=true` 排除（ci.yml / release.yml 都用它），初衷是「基准耗时太长」。
 * 副作用是把这些用例的**求值正确性**也一并挡在门外——`benchmarkBinaryTree` 因此
 * 稳定失败（「读取变量失败：leftSum @ slot 4」）而 CI 长期全绿，无人发现。
 *
 * 本测试只做一次求值并断言结果，不做冷启动/稳定化/多轮测量，**不受排除名单影响**，
 * 因此同类求值缺陷会立刻在 CI 暴露。耗时测量仍留在 GraalVMJitBenchmark 中按需运行。
 *
 * ★期望值直接复用 GraalVMJitBenchmark 的 BenchmarkCase 常量，不手工抄写——
 * 避免两处期望值漂移（手抄还容易在 `479_001_600` 这类下划线字面量上出错）。
 */
public class BenchmarkCorrectnessTest {

  static Stream<Arguments> caseNames() {
    return Stream.of(
        Arguments.of(GraalVMJitBenchmark.FACTORIAL.displayName(), GraalVMJitBenchmark.FACTORIAL),
        Arguments.of(GraalVMJitBenchmark.FIBONACCI.displayName(), GraalVMJitBenchmark.FIBONACCI),
        Arguments.of(GraalVMJitBenchmark.LIST_MAP.displayName(), GraalVMJitBenchmark.LIST_MAP),
        Arguments.of(GraalVMJitBenchmark.QUICK_SORT.displayName(), GraalVMJitBenchmark.QUICK_SORT),
        Arguments.of(GraalVMJitBenchmark.BINARY_TREE.displayName(), GraalVMJitBenchmark.BINARY_TREE),
        Arguments.of(GraalVMJitBenchmark.STRING_OPS.displayName(), GraalVMJitBenchmark.STRING_OPS),
        Arguments.of(GraalVMJitBenchmark.FACTORIAL_HEAVY.displayName(), GraalVMJitBenchmark.FACTORIAL_HEAVY),
        Arguments.of(GraalVMJitBenchmark.FIBONACCI_HEAVY.displayName(), GraalVMJitBenchmark.FIBONACCI_HEAVY),
        Arguments.of(GraalVMJitBenchmark.LIST_MAP_HEAVY.displayName(), GraalVMJitBenchmark.LIST_MAP_HEAVY),
        Arguments.of(GraalVMJitBenchmark.ARITHMETIC.displayName(), GraalVMJitBenchmark.ARITHMETIC));
  }

  // ★额外传一个 displayName 作首参而非直接用 case 的 toString：BenchmarkCase 是 record，
  //   默认 toString 会把整个 JSON fixture 打进用例名，CI 日志完全不可读（实测刷屏数十行）。
  @ParameterizedTest(name = "{0}")
  @MethodSource("caseNames")
  @DisplayName("benchmark fixture 求值结果正确（不测耗时）")
  void evaluatesToExpectedResult(String displayName, GraalVMJitBenchmark.BenchmarkCase config) {
    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", config.json(), config.sourceName()).build();
      Value result = context.eval(source);

      // 与 GraalVMJitBenchmark.assertNumericEquals 同语义：按 double 取值后向下取整比较。
      // ★不能直接 asLong()：ARITHMETIC 用例本就返回 double（233.33…），整数化比较才是既定契约。
      assertTrue(result.isNumber(),
          config.displayName() + "：求值结果不是数值（" + result + "）");
      assertEquals(config.expectedResult(), (long) Math.floor(result.asDouble()),
          config.displayName() + "：求值结果与期望不符（实得 " + result + "）");
    } catch (java.io.IOException e) {
      throw new IllegalStateException("读取 benchmark fixture 失败：" + config.sourceName(), e);
    }
  }
}
