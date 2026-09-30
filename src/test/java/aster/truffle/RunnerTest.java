package aster.truffle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Runner CLI 对数值结果的打印（issue #129）。
 *
 * <p>Core IR 同时有 Int/Long/Double 字面量，原实现只要 {@code isNumber()} 就无条件
 * {@code asInt()}：程序求值已经成功，却在打印环节以 ClassCastException 非零退出。
 * 本测试走真实入口 {@code Runner.main}，捕获 stdout 断言输出文本。
 */
class RunnerTest {

  @TempDir
  Path tmp;

  /** 与 simple_hello_core.json 同形，仅替换返回表达式与声明的返回类型。 */
  private String program(String retType, String returnExpr) {
    return "{\"name\":\"runner.probe\",\"decls\":[{\"kind\":\"Func\",\"name\":\"main\",\"params\":[],"
        + "\"ret\":{\"kind\":\"TypeName\",\"name\":\"" + retType + "\"},\"effects\":[],"
        + "\"body\":{\"kind\":\"Block\",\"statements\":[{\"kind\":\"Return\",\"expr\":" + returnExpr + "}]}}]}";
  }

  private String runAndCaptureStdout(String fileName, String json) throws Exception {
    Path file = tmp.resolve(fileName);
    Files.writeString(file, json, StandardCharsets.UTF_8);

    PrintStream original = System.out;
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    System.setOut(new PrintStream(buf, true, StandardCharsets.UTF_8));
    try {
      Runner.main(new String[] {file.toString()});
    } finally {
      System.setOut(original);
    }
    return buf.toString(StandardCharsets.UTF_8).trim();
  }

  @Test
  void printsLongBeyondIntRange() throws Exception {
    // 修复前：ClassCastException: Cannot convert '9999999999' ... using Value.asInt()
    String out = assertDoesNotThrow(() ->
        runAndCaptureStdout("longlit.json", program("Long", "{\"kind\":\"Long\",\"value\":9999999999}")));
    assertEquals("9999999999", out);
  }

  @Test
  void printsDouble() throws Exception {
    // 修复前：ClassCastException: Cannot convert '3.75' ... using Value.asInt()
    String out = assertDoesNotThrow(() ->
        runAndCaptureStdout("dbl.json", program("Double", "{\"kind\":\"Double\",\"value\":3.75}")));
    assertEquals("3.75", out);
  }

  @Test
  void stillPrintsIntAsInt() throws Exception {
    // 反向护栏：能无损转 int 的值仍走 asInt，输出形态不变
    String out = runAndCaptureStdout("int.json", program("Int", "{\"kind\":\"Int\",\"value\":42}"));
    assertEquals("42", out);
  }

  @Test
  void printsLongThatFitsInIntWithoutDecimal() throws Exception {
    // Long 字面量落在 int 范围内时按 fitsInInt 走 asInt，不得输出成浮点形态
    String out = runAndCaptureStdout("smalllong.json", program("Long", "{\"kind\":\"Long\",\"value\":7}"));
    assertEquals("7", out);
  }
}
