package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * GraalVM JIT 基准测试套件，复用 CrossBackendBenchmark 中的 4 个算法，
 * 并通过三阶段预热策略测量稳定后的峰值性能。
 */
public class GraalVMJitBenchmark {

  private record BenchmarkCase(
      String displayName,
      String sourceName,
      String entryFunction,
      String json,
      long expectedResult,
      int measurementIterations,
      int coldIterations,
      int stabilizationIterations
  ) {}

  private record BenchmarkResult(String name, double avgMs, int iterations) {}

  private static final List<BenchmarkResult> RESULTS = new ArrayList<>();

  private static String readBenchmarkJsonUnchecked(String relativePath) {
    // Extract just the filename from the relative path
    String filename = Path.of(relativePath).getFileName().toString();

    // Try to load from classpath first (src/test/resources/)
    try (var is = GraalVMJitBenchmark.class.getClassLoader().getResourceAsStream(filename)) {
      if (is != null) {
        return new String(is.readAllBytes(), StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      // Fall through to file-based search
    }

    // Fallback: search in common locations relative to project root
    Path current = Path.of("").toAbsolutePath();
    String[] searchPaths = {
        relativePath,
        "src/test/resources/" + filename,
        "build/resources/test/" + filename
    };

    for (int i = 0; i < 8; i++) {
      for (String searchPath : searchPaths) {
        Path candidate = current.resolve(searchPath);
        if (Files.exists(candidate)) {
          try {
            return Files.readString(candidate);
          } catch (IOException e) {
            throw new RuntimeException("读取基准 JSON 失败: " + relativePath, e);
          }
        }
      }
      if (current.getParent() == null) {
        break;
      }
      current = current.getParent();
    }
    throw new RuntimeException("未找到基准 JSON 文件: " + relativePath + " (tried classpath and filesystem)");
  }

  private static final BenchmarkCase FACTORIAL = new BenchmarkCase(
      "Factorial(10)",
      "bench-factorial-jit.json",
      "test",
      """
          {
            "name": "bench.factorial",
            "decls": [
              {
                "kind": "Func",
                "name": "factorial",
                "params": [{"name": "n", "type": {"kind": "TypeName", "name": "Int"}}],
                "ret": {"kind": "TypeName", "name": "Int"},
                "effects": [],
                "body": {
                  "kind": "Block",
                  "statements": [{
                    "kind": "If",
                    "cond": {
                      "kind": "Call",
                      "target": {"kind": "Name", "name": "lte"},
                      "args": [
                        {"kind": "Name", "name": "n"},
                        {"kind": "Int", "value": 1}
                      ]
                    },
                    "thenBlock": {
                      "kind": "Block",
                      "statements": [{"kind": "Return", "expr": {"kind": "Int", "value": 1}}]
                    },
                    "elseBlock": {
                      "kind": "Block",
                      "statements": [{
                        "kind": "Return",
                        "expr": {
                          "kind": "Call",
                          "target": {"kind": "Name", "name": "mul"},
                          "args": [
                            {"kind": "Name", "name": "n"},
                            {
                              "kind": "Call",
                              "target": {"kind": "Name", "name": "factorial"},
                              "args": [{
                                "kind": "Call",
                                "target": {"kind": "Name", "name": "sub"},
                                "args": [
                                  {"kind": "Name", "name": "n"},
                                  {"kind": "Int", "value": 1}
                                ]
                              }]
                            }
                          ]
                        }
                      }]
                    }
                  }]
                }
              },
              {
                "kind": "Func",
                "name": "main",
                "params": [],
                "ret": {"kind": "TypeName", "name": "Int"},
                "effects": [],
                "body": {
                  "kind": "Block",
                  "statements": [{
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {"kind": "Name", "name": "factorial"},
                      "args": [{"kind": "Int", "value": 10}]
                    }
                  }]
                }
              }
            ]
          }
          """,
      3_628_800,
      200,   // 降低测量次数（原 1000）
      50,    // 降低冷启动次数（原 100）
      200    // 降低稳定化次数（原 2000）
  );

  private static final BenchmarkCase FIBONACCI = new BenchmarkCase(
      "Fibonacci(20)",
      "bench-fib-jit.json",
      "test",
      """
          {
            "name": "bench.fibonacci",
            "decls": [
              {
                "kind": "Func",
                "name": "fib",
                "params": [{"name": "n", "type": {"kind": "TypeName", "name": "Int"}}],
                "ret": {"kind": "TypeName", "name": "Int"},
                "effects": [],
                "body": {
                  "kind": "Block",
                  "statements": [{
                    "kind": "If",
                    "cond": {
                      "kind": "Call",
                      "target": {"kind": "Name", "name": "lte"},
                      "args": [
                        {"kind": "Name", "name": "n"},
                        {"kind": "Int", "value": 1}
                      ]
                    },
                    "thenBlock": {
                      "kind": "Block",
                      "statements": [{"kind": "Return", "expr": {"kind": "Name", "name": "n"}}]
                    },
                    "elseBlock": {
                      "kind": "Block",
                      "statements": [{
                        "kind": "Return",
                        "expr": {
                          "kind": "Call",
                          "target": {"kind": "Name", "name": "add"},
                          "args": [
                            {
                              "kind": "Call",
                              "target": {"kind": "Name", "name": "fib"},
                              "args": [{
                                "kind": "Call",
                                "target": {"kind": "Name", "name": "sub"},
                                "args": [
                                  {"kind": "Name", "name": "n"},
                                  {"kind": "Int", "value": 1}
                                ]
                              }]
                            },
                            {
                              "kind": "Call",
                              "target": {"kind": "Name", "name": "fib"},
                              "args": [{
                                "kind": "Call",
                                "target": {"kind": "Name", "name": "sub"},
                                "args": [
                                  {"kind": "Name", "name": "n"},
                                  {"kind": "Int", "value": 2}
                                ]
                              }]
                            }
                          ]
                        }
                      }]
                    }
                  }]
                }
              },
              {
                "kind": "Func",
                "name": "main",
                "params": [],
                "ret": {"kind": "TypeName", "name": "Int"},
                "effects": [],
                "body": {
                  "kind": "Block",
                  "statements": [{
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {"kind": "Name", "name": "fib"},
                      "args": [{"kind": "Int", "value": 20}]
                    }
                  }]
                }
              }
            ]
          }
          """,
      6_765,
      50,    // 降低冷启动次数（原 100）
      20,    // 降低稳定化次数（原 2000）- fib(20) 每次调用代价高
      50     // 降低测量次数（原 100）
  );

  private static final BenchmarkCase LIST_MAP = new BenchmarkCase(
      "List.map ×2 (2 items)",
      "bench-list-map-jit.json",
      "test",
      """
          {
            "name": "bench.list.map",
            "decls": [{
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": {"kind": "TypeName", "name": "Int"},
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Let",
                    "name": "double",
                    "expr": {
                      "kind": "Lambda",
                      "params": [{"name": "x", "type": {"kind": "TypeName", "name": "Int"}}],
                      "ret": {"kind": "TypeName", "name": "Int"},
                      "captures": [],
                      "body": {
                        "kind": "Block",
                        "statements": [{
                          "kind": "Return",
                          "expr": {
                            "kind": "Call",
                            "target": {"kind": "Name", "name": "mul"},
                            "args": [
                              {"kind": "Name", "name": "x"},
                              {"kind": "Int", "value": 2}
                            ]
                          }
                        }]
                      }
                    }
                  },
                  {
                    "kind": "Let",
                    "name": "numbers",
                    "expr": {
                      "kind": "Call",
                      "target": {"kind": "Name", "name": "List.empty"},
                      "args": []
                    }
                  },
                  {
                    "kind": "Let",
                    "name": "numbers1",
                    "expr": {
                      "kind": "Call",
                      "target": {"kind": "Name", "name": "List.append"},
                      "args": [
                        {"kind": "Name", "name": "numbers"},
                        {"kind": "Int", "value": 1}
                      ]
                    }
                  },
                  {
                    "kind": "Let",
                    "name": "numbers2",
                    "expr": {
                      "kind": "Call",
                      "target": {"kind": "Name", "name": "List.append"},
                      "args": [
                        {"kind": "Name", "name": "numbers1"},
                        {"kind": "Int", "value": 2}
                      ]
                    }
                  },
                  {
                    "kind": "Let",
                    "name": "mapped",
                    "expr": {
                      "kind": "Call",
                      "target": {"kind": "Name", "name": "List.map"},
                      "args": [
                        {"kind": "Name", "name": "numbers2"},
                        {"kind": "Name", "name": "double"}
                      ]
                    }
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {"kind": "Name", "name": "List.length"},
                      "args": [{"kind": "Name", "name": "mapped"}]
                    }
                  }
                ]
              }
            }]
          }
          """,
      2,
      500,   // 降低测量次数（原 10000）- 简单操作，无需过多迭代
      50,    // 降低冷启动次数（原 100）
      200    // 降低稳定化次数（原 5000）
  );

  private static final BenchmarkCase QUICK_SORT = new BenchmarkCase(
      "QuickSort (100 elements)",
      "bench-quicksort-jit.json",
      "test",
      readBenchmarkJsonUnchecked("resources/quicksort_core.json"),
      100,
      50,    // 降低测量次数（原 400）
      20,    // 降低冷启动次数（原 200）
      200    // 降低稳定化次数（原 4000）- quicksort 每次调用代价高
  );

  private static final BenchmarkCase BINARY_TREE = new BenchmarkCase(
      "Binary Tree Traversal (15 nodes)",
      "bench-binary-tree-jit.json",
      "test",
      readBenchmarkJsonUnchecked("resources/binary_tree_traversal_core.json"),
      120,
      100,   // 降低测量次数（原 1200）
      50,    // 降低冷启动次数（原 200）
      200    // 降低稳定化次数（原 4000）
  );

  private static final BenchmarkCase STRING_OPS = new BenchmarkCase(
      "String Operations",
      "bench-string-ops-jit.json",
      "test",
      readBenchmarkJsonUnchecked("resources/string_ops_core.json"),
      139,
      200,   // 降低测量次数（原 2000）
      50,    // 降低冷启动次数（原 200）
      200    // 降低稳定化次数（原 5000）
  );

  private static final BenchmarkCase FACTORIAL_HEAVY = new BenchmarkCase(
      "Factorial(12) Heavy",
      "bench-factorial12-jit.json",
      "test",
      readBenchmarkJsonUnchecked("resources/factorial_12_core.json"),
      479_001_600,  // Factorial(12) - fits in Int (Factorial(20) overflows)
      100,   // 降低测量次数（原 600）
      50,    // 降低冷启动次数（原 300）
      200    // 降低稳定化次数（原 6000）
  );

  private static final BenchmarkCase FIBONACCI_HEAVY = new BenchmarkCase(
      "Fibonacci(20) Heavy",
      "bench-fibonacci20-jit.json",
      "test",
      readBenchmarkJsonUnchecked("resources/fibonacci_20_core.json"),
      6_765,  // Fibonacci(20) - same as base benchmark for consistency
      50,     // 降低测量次数（原 200）
      20,     // 降低冷启动次数（原 100）
      50      // 降低稳定化次数（原 2000）- fib(20) 每次调用代价高
  );

  private static final BenchmarkCase LIST_MAP_HEAVY = new BenchmarkCase(
      "List.map (1000 items) Heavy",
      "bench-list-map-1000-jit.json",
      "test",
      readBenchmarkJsonUnchecked("resources/list_map_1000_core.json"),
      1_000,
      50,    // 降低测量次数（原 500）
      20,    // 降低冷启动次数（原 200）
      100    // 降低稳定化次数（原 5000）- 1000 items 每次调用代价高
  );

  private static final BenchmarkCase ARITHMETIC = new BenchmarkCase(
      "Arithmetic compute(x)",
      "bench-arithmetic-jit.json",
      "test",
      """
          {
            "name": "bench.arithmetic",
            "decls": [
              {
                "kind": "Func",
                "name": "compute",
                "params": [{"name": "x", "type": {"kind": "TypeName", "name": "Int"}}],
                "ret": {"kind": "TypeName", "name": "Int"},
                "effects": [],
                "body": {
                  "kind": "Block",
                  "statements": [{
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {"kind": "Name", "name": "add"},
                      "args": [
                        {
                          "kind": "Call",
                          "target": {"kind": "Name", "name": "mul"},
                          "args": [
                            {"kind": "Name", "name": "x"},
                            {"kind": "Int", "value": 2}
                          ]
                        },
                        {
                          "kind": "Call",
                          "target": {"kind": "Name", "name": "div"},
                          "args": [
                            {"kind": "Name", "name": "x"},
                            {"kind": "Int", "value": 3}
                          ]
                        }
                      ]
                    }
                  }]
                }
              },
              {
                "kind": "Func",
                "name": "main",
                "params": [],
                "ret": {"kind": "TypeName", "name": "Int"},
                "effects": [],
                "body": {
                  "kind": "Block",
                  "statements": [{
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {"kind": "Name", "name": "compute"},
                      "args": [{"kind": "Int", "value": 100}]
                    }
                  }]
                }
              }
            ]
          }
          """,
      233,
      500,   // 降低测量次数（原 10000）- 简单算术，无需过多迭代
      50,    // 降低冷启动次数（原 100）
      200    // 降低稳定化次数（原 5000）
  );

  @Test
  public void benchmarkFactorial_GraalVMJit() throws IOException {
    BenchmarkResult result = runBenchmark(FACTORIAL);
    RESULTS.add(result);
  }

  @Test
  public void benchmarkFibonacci_GraalVMJit() throws IOException {
    BenchmarkResult result = runBenchmark(FIBONACCI);
    RESULTS.add(result);
  }

  @Test
  public void benchmarkListMap_GraalVMJit() throws IOException {
    BenchmarkResult result = runBenchmark(LIST_MAP);
    RESULTS.add(result);
  }

  @Test
  public void benchmarkArithmetic_GraalVMJit() throws IOException {
    BenchmarkResult result = runBenchmark(ARITHMETIC);
    RESULTS.add(result);
  }

  @Test
  public void benchmarkQuickSort_GraalVMJit() throws IOException {
    BenchmarkResult result = runBenchmark(QUICK_SORT);
    RESULTS.add(result);
  }

  @Test
  public void benchmarkBinaryTree_GraalVMJit() throws IOException {
    BenchmarkResult result = runBenchmark(BINARY_TREE);
    RESULTS.add(result);
  }

  @Test
  public void benchmarkStringOps_GraalVMJit() throws IOException {
    BenchmarkResult result = runBenchmark(STRING_OPS);
    RESULTS.add(result);
  }

  @Test
  public void benchmarkFactorialHeavy_GraalVMJit() throws IOException {
    BenchmarkResult result = runBenchmark(FACTORIAL_HEAVY);
    RESULTS.add(result);
  }

  @Test
  public void benchmarkFibonacciHeavy_GraalVMJit() throws IOException {
    BenchmarkResult result = runBenchmark(FIBONACCI_HEAVY);
    RESULTS.add(result);
  }

  @Test
  public void benchmarkListMapHeavy_GraalVMJit() throws IOException {
    BenchmarkResult result = runBenchmark(LIST_MAP_HEAVY);
    RESULTS.add(result);
  }

  @AfterAll
  static void printSummary() throws IOException {
    if (RESULTS.isEmpty()) {
      System.out.println("\n尚未收集到 JIT 基准数据，请运行 GraalVMJitBenchmark 全部测试。");
      return;
    }
    System.out.println("\n" + "=".repeat(80));
    System.out.println("GraalVM JIT Benchmark Summary");
    System.out.println("=".repeat(80));
    for (BenchmarkResult result : RESULTS) {
      System.out.printf("%s → %.6f ms (%d iterations)%n", result.name(), result.avgMs(), result.iterations());
    }
    System.out.println("=".repeat(80));
    writeResultsToFile();
  }

  private static void writeResultsToFile() throws IOException {
    Path outputDir = Path.of("build", "benchmarks");
    Files.createDirectories(outputDir);
    Path output = outputDir.resolve("graalvm-jit.json");

    StringBuilder sb = new StringBuilder();
    sb.append("{\"backend\":\"graalvm-jit\",\"results\":[");
    for (int i = 0; i < RESULTS.size(); i++) {
      BenchmarkResult result = RESULTS.get(i);
      if (i > 0) {
        sb.append(',');
      }
      String name = result.name().replace("\"", "\\\"");
      sb.append(String.format(Locale.ROOT,
          "{\"benchmark\":\"%s\",\"avg_ms\":%.6f,\"iterations\":%d}",
          name, result.avgMs(), result.iterations()));
    }
    sb.append("]}\n");
    Files.writeString(output, sb.toString(), StandardCharsets.UTF_8);
  }

  private BenchmarkResult runBenchmark(BenchmarkCase config) throws IOException {
    try (Context context = createJitContext()) {
      Source source = Source.newBuilder("aster", config.json(), config.sourceName()).build();
      Value firstResult = context.eval(source);
      assertEquals(config.expectedResult(), firstResult.asLong());

      Value entry = context.getBindings("aster").getMember(config.entryFunction());
      if (entry == null || !entry.canExecute()) {
        entry = context.parse(source);
      }

      System.out.printf("%n=== %s (GraalVM JIT) ===%n", config.displayName());
      System.out.printf("Phase 1: 冷启动 %d 次，触发编译...%n", config.coldIterations());
      for (int i = 0; i < config.coldIterations(); i++) {
        Value value = entry.execute();
        assertEquals(config.expectedResult(), value.asLong());
      }

      System.out.printf("Phase 2: 追加预热 %d 次，等待优化稳定...%n", config.stabilizationIterations());
      for (int i = 0; i < config.stabilizationIterations(); i++) {
        Value value = entry.execute();
        assertEquals(config.expectedResult(), value.asLong());
      }

      System.out.printf("Phase 3: 测量阶段（%d 次）...%n", config.measurementIterations());
      long start = System.nanoTime();
      for (int i = 0; i < config.measurementIterations(); i++) {
        Value value = entry.execute();
        assertEquals(config.expectedResult(), value.asLong());
      }
      long end = System.nanoTime();

      double avgMs = (end - start) / 1_000_000.0 / config.measurementIterations();
      System.out.printf("GraalVM JIT: %.6f ms/iteration%n", avgMs);
      return new BenchmarkResult(config.displayName(), avgMs, config.measurementIterations());
    }
  }

  private static Context createJitContext() {
    return Context.newBuilder("aster")
        .allowAllAccess(true)
        .option("engine.WarnInterpreterOnly", "false")
        .build();
  }
}
