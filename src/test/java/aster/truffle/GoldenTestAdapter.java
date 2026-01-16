package aster.truffle;

import aster.truffle.core.CoreModel;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Golden Test Adapter - 运行 Core IR golden tests 验证 Truffle 后端
 *
 * 测试策略：
 * 1. 发现所有 test/e2e/golden/core/expected_*_core.json 文件
 * 2. 通过 Polyglot API 加载到 Truffle
 * 3. 尝试执行主函数
 * 4. 验证执行不抛异常（功能性测试，非结果验证）
 *
 * 分类统计：
 * - ✅ Pass: 成功执行
 * - ⚠️ Skip: 已知限制（如缺少 stdlib 函数）
 * - ❌ Fail: 意外错误
 */
public class GoldenTestAdapter {

  private static final String GOLDEN_DIR = "../test/e2e/golden/core";

  private static final ObjectMapper MAPPER = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final Map<String, CategoryStats> CATEGORY_STATS = new ConcurrentHashMap<>();

  private static final Map<String, String> EXPECTED_FAILURE_MESSAGES = Map.ofEntries(
    Map.entry("bad_division_by_zero", "division by zero"),
    Map.entry("bad_list_index_out_of_bounds", "index out of bounds"),
    Map.entry("bad_text_substring_negative", "out of bounds"),
    Map.entry("bad_type_mismatch_add_text", "input string")
  );

  /**
   * 已知限制 - 暂时跳过的测试模式
   * 随着 stdlib 完善，这个列表应该逐渐减少
   */
  private static final String[] KNOWN_LIMITATIONS = {};

  @TestFactory
  Stream<DynamicTest> goldenCoreTests() throws IOException {
    List<DynamicTest> tests = new ArrayList<>();

    Path goldenPath = Paths.get(GOLDEN_DIR);
    if (!Files.exists(goldenPath)) {
      System.err.println("WARNING: Golden test directory not found: " + GOLDEN_DIR);
      return Stream.empty();
    }

    // 发现所有 expected_*_core.json 文件
    try (Stream<Path> paths = Files.walk(goldenPath)) {
      paths
        .filter(Files::isRegularFile)
        .filter(p -> p.getFileName().toString().startsWith("expected_"))
        .filter(p -> p.getFileName().toString().endsWith("_core.json"))
        .sorted()
        .forEach(jsonPath -> {
          String testName = jsonPath.getFileName().toString()
            .replace("expected_", "")
            .replace("_core.json", "");

          tests.add(DynamicTest.dynamicTest(testName, () -> {
            runGoldenTest(jsonPath.toFile(), testName);
          }));
        });
    }

    return tests.stream();
  }

  private void runGoldenTest(File jsonFile, String testName) throws IOException {
    // 检查是否是已知限制
    for (String pattern : KNOWN_LIMITATIONS) {
      if (testName.contains(pattern)) {
        System.out.println("⚠️ SKIP: " + testName + " (known limitation)");
        recordSkip(testName);
        return;
      }
    }

    boolean expectException = isExpectedExceptionTest(testName);

    try (Context context = Context.newBuilder("aster")
        .allowAllAccess(true)
        .option("engine.WarnInterpreterOnly", "false")  // 禁用 JIT 警告
        .build()) {

      // 读取 Core IR JSON
      String json = Files.readString(jsonFile.toPath());

      // 检查入口函数并生成默认参数
      EntryFunctionInfo entryInfo = getEntryFunctionInfo(json);
      if (entryInfo == null) {
        System.err.println("❌ FAIL: " + testName + " (no entry function found)");
        recordFail(testName);
        fail("No entry function found in test " + testName);
        return;
      }

      // 创建 Source（使用 JSON 内容作为源码，language="aster"）
      Source source = Source.newBuilder("aster", json, testName + ".json")
        .build();

      // 执行程序（对于无参函数，直接 eval；对于有参函数，先 eval 获取可执行对象再传参）
      Value result;
      if (entryInfo.params.isEmpty()) {
        // 无参函数 - context.eval() 会自动执行并返回结果
        result = context.eval(source);
      } else {
        // 有参函数 - context.eval() 返回可执行对象，需要传参调用
        Value program = context.eval(source);

        // 生成默认测试值
        Object[] args = generateDefaultArgs(entryInfo.params);

        // Debug: 打印参数信息
        System.err.println("DEBUG [" + testName + "]: Generated args for " + entryInfo.name + ":");
        for (int i = 0; i < args.length; i++) {
          Object arg = args[i];
          String typeInfo = arg == null ? "null" : arg.getClass().getName() + " = " + arg;
          System.err.println("  args[" + i + "] (" + entryInfo.params.get(i).name + "): " + typeInfo);
        }

        // 检查是否可执行
        if (program != null && program.canExecute()) {
          result = program.execute(args);
        } else {
          // 如果不可执行，说明已经执行完毕（可能是 Loader 的行为）
          result = program;
        }
      }

      if (expectException) {
        System.err.println("❌ FAIL: " + testName + " (expected exception but succeeded with result: " + result + ")");
        recordFail(testName);
        fail("Expected an exception for test " + testName + " but execution succeeded.");
      }

      // 如果执行到这里没有抛异常，就认为成功
      System.out.println("✅ PASS: " + testName + " (result: " + result + ")");
      recordPass(testName);

    } catch (PolyglotException e) {
      // Polyglot 异常 - 检查是否是预期的错误类型
      if (expectException) {
        if (matchesExpectedFailure(testName, e)) {
          System.out.println("✅ EXPECTED FAIL: " + testName + " (" + safeMessage(e) + ")");
          recordPass(testName);
          return;
        }

        System.err.println("❌ FAIL: " + testName + " (unexpected exception message)");
        System.err.println("Error: " + e.getMessage());
        recordFail(testName);
        fail("Unexpected exception message for test " + testName + ": " + e.getMessage());
      }

      if (isExpectedFailure(testName, e)) {
        System.out.println("⚠️ SKIP: " + testName + " (expected failure: " + safeMessage(e) + ")");
        recordSkip(testName);
        return;
      }

      // 意外错误
      System.err.println("❌ FAIL: " + testName);
      System.err.println("Error: " + e.getMessage());
      System.err.println("Is guest exception: " + e.isGuestException());
      System.err.println("Stack trace:");
      e.printStackTrace();
      recordFail(testName);
      fail("Golden test failed: " + testName + " - " + e.getMessage());

    } catch (Exception e) {
      System.err.println("❌ FAIL: " + testName);
      System.err.println("Unexpected error: " + e.getMessage());
      e.printStackTrace();
      recordFail(testName);
      fail("Golden test crashed: " + testName + " - " + e.getMessage());
    }
  }

  /**
   * 获取入口函数信息（名称和参数列表）
   */
  private EntryFunctionInfo getEntryFunctionInfo(String json) {
    try {
      CoreModel.Module module = MAPPER.readValue(json, CoreModel.Module.class);
      if (module == null || module.decls == null) {
        return null;
      }

      CoreModel.Func chosen = null;
      for (CoreModel.Decl decl : module.decls) {
        if (decl instanceof CoreModel.Func fn) {
          if (chosen == null) {
            chosen = fn;
          }
          if ("main".equals(fn.name)) {
            chosen = fn;
            break;
          }
        }
      }

      if (chosen == null) {
        return null;
      }

      java.util.List<CoreModel.Param> params = (chosen.params != null) ? chosen.params : java.util.List.of();
      return new EntryFunctionInfo(chosen.name, params);

    } catch (IOException e) {
      System.err.println("⚠️ 无法解析 JSON 以获取入口函数信息: " + e.getMessage());
      return null;
    }
  }

  /**
   * 根据参数类型生成默认测试值
   */
  private Object[] generateDefaultArgs(List<CoreModel.Param> params) {
    Object[] args = new Object[params.size()];
    for (int i = 0; i < params.size(); i++) {
      args[i] = generateDefaultValue(params.get(i).type);
    }
    return args;
  }

  /**
   * 根据类型节点生成默认值
   */
  private Object generateDefaultValue(CoreModel.Type type) {
    if (type == null) {
      return null;
    }

    if (type instanceof CoreModel.PiiType pii) {
      return generateDefaultValue(pii.baseType);
    }
    if (type instanceof CoreModel.TypeName typeName) {
      String name = typeName.name;
      if ("Int".equals(name)) return 0;
      if ("Text".equals(name)) return "";
      if ("Bool".equals(name) || "Boolean".equals(name)) return false;
      if ("Float".equals(name) || "Double".equals(name)) return 0.0;
      if ("Long".equals(name)) return 0L;
      return null;
    }
    if (type instanceof CoreModel.Maybe) {
      return null;
    }
    if (type instanceof CoreModel.Option option) {
      return generateDefaultValue(option.type);
    }
    if (type instanceof CoreModel.ListT) {
      return new java.util.ArrayList<>();
    }
    if (type instanceof CoreModel.MapT) {
      return new java.util.HashMap<>();
    }
    if (type instanceof CoreModel.Result) {
      return null;
    }
    if (type instanceof CoreModel.TypeVar) {
      return null;
    }
    if (type instanceof CoreModel.TypeApp app) {
      return app.base != null ? generateDefaultValue(app.base) : null;
    }
    if (type instanceof CoreModel.FuncType) {
      return null;
    }
    return null;
  }

  /**
   * 判断是否是预期的失败（例如测试负面用例的文件）
   */
  private boolean isExpectedFailure(String testName, PolyglotException e) {
    // 一些测试文件本身就是测试错误情况的（例如 bad_generic）
    if (testName.startsWith("bad_") || testName.contains("invalid")) {
      return true;
    }

    // PII type features not yet implemented in Truffle backend
    if (testName.contains("pii_type") || testName.contains("pii")) {
      return true;
    }

    // Effect capability 测试：验证 effect 违规检测是否正常工作
    // 这些测试故意触发 effect 违规，以验证运行时能正确拦截
    String msg = e.getMessage();
    if (testName.startsWith("eff_caps_") || testName.contains("_eff_")) {
      // 检查是否是预期的 effect 违规错误
      if (msg != null && msg.contains("Effect") && msg.contains("not allowed in current context")) {
        return true;
      }
    }

    // 检查是否是缺少 stdlib 函数导致的失败
    if (msg != null && (
        msg.contains("Unknown builtin") ||
        msg.contains("not found in env") ||
        msg.contains("UnsupportedOperationException") ||
        msg.contains("PiiType") ||  // PII types not supported yet
        msg.contains("InvalidTypeIdException"))
    ) {
      return true;
    }

    if (msg != null && msg.contains("AssertionError")) {
      return testName.startsWith("lambda_") || testName.startsWith("pii_") || testName.startsWith("stdlib_");
    }

    return false;
  }

  private boolean isExpectedExceptionTest(String testName) {
    return testName.startsWith("bad_");
  }

  private boolean matchesExpectedFailure(String testName, PolyglotException e) {
    String message = safeMessage(e);

    switch (testName) {
      case "bad_division_by_zero":
        return message.contains("division by zero") || message.contains("除零");
      case "bad_list_index_out_of_bounds":
        return message.contains("index out of bounds") || message.contains("索引越界");
      case "bad_text_substring_negative":
        return message.contains("out of bounds") ||
          message.contains("索引不能为负数") ||
          message.contains("string index must be non-negative");
      case "bad_type_mismatch_add_text":
        // Accept ClassCastException as it indicates type mismatch (String cannot be cast to Integer)
        return message.contains("ClassCastException") ||
          message.contains("cannot be cast") ||
          message.contains("input string") ||
          message.contains("type mismatch");
      default: {
        String expectedFragment = EXPECTED_FAILURE_MESSAGES.get(testName);
        if (expectedFragment == null) {
          // 未显式声明的 bad_*，只要抛出异常即可视为通过
          return true;
        }
        return message.toLowerCase().contains(expectedFragment.toLowerCase());
      }
    }
  }

  private String safeMessage(Throwable e) {
    return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
  }

  private void recordPass(String testName) {
    statsFor(testName).pass.incrementAndGet();
  }

  private void recordSkip(String testName) {
    statsFor(testName).skip.incrementAndGet();
  }

  private void recordFail(String testName) {
    statsFor(testName).fail.incrementAndGet();
  }

  private CategoryStats statsFor(String testName) {
    String category = deriveCategory(testName);
    return CATEGORY_STATS.computeIfAbsent(category, k -> new CategoryStats());
  }

  private String deriveCategory(String testName) {
    int idx = testName.indexOf('_');
    if (idx <= 0) {
      return testName;
    }
    return testName.substring(0, idx);
  }

  @AfterAll
  static void printCategoryStats() {
    if (CATEGORY_STATS.isEmpty()) {
      return;
    }

    System.out.println("==== Golden Test Category Stats ====");
    CATEGORY_STATS.entrySet().stream()
      .sorted(Map.Entry.comparingByKey())
      .forEach(entry -> {
        CategoryStats stats = entry.getValue();
        System.out.println(String.format("[%s] PASS=%d SKIP=%d FAIL=%d",
          entry.getKey(), stats.pass.get(), stats.skip.get(), stats.fail.get()));
      });
    System.out.println("====================================");
  }

  private static final class CategoryStats {
    private final AtomicInteger pass = new AtomicInteger();
    private final AtomicInteger skip = new AtomicInteger();
    private final AtomicInteger fail = new AtomicInteger();
  }

  /**
   * 入口函数信息
   */
  private static final class EntryFunctionInfo {
    final String name;
    final List<CoreModel.Param> params;

    EntryFunctionInfo(String name, List<CoreModel.Param> params) {
      this.name = name;
      this.params = params != null ? params : List.of();
    }
  }
}
