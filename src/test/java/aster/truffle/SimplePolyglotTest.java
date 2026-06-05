package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 简单Polyglot集成测试 - 验证基础功能
 */
public class SimplePolyglotTest {

  @Test
  public void testSimpleFunctionCall() throws IOException {
    String json = """
        {
          "name": "test.simple",
          "decls": [
            {
              "kind": "Func",
              "name": "identity",
              "params": [{"name": "x", "type": {"kind": "TypeName", "name": "Int"}}],
              "ret": {"kind": "TypeName", "name": "Int"},
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [{
                  "kind": "Return",
                  "expr": {"kind": "Name", "name": "x"}
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
                    "target": {"kind": "Name", "name": "identity"},
                    "args": [{"kind": "Int", "value": 42}]
                  }
                }]
              }
            }
          ]
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "simple.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "identity(42) should be 42");
    }
  }

  /**
   * 回归：带参数的入口函数若 body 调用二元运算符（Core IR target Name "+"），
   * 过去因 builtin 注册名是 "add" 而 "+" 解析不到 → CallNode 返回 null →
   * GraalVM 转 host Value 时抛 "arg2Value is null" NPE。
   * Builtins.canonicalName("+") -> "add" 后修复。覆盖 program.execute(args) 路径。
   */
  @Test
  public void testParamEntryWithBinaryOperator() throws IOException {
    // Rule add given x, y, produce: Return x + y.
    String json = """
        {
          "kind": "Module",
          "name": "test.binop",
          "decls": [{
            "kind": "Func",
            "name": "add",
            "params": [
              {"name": "x", "type": {"kind": "TypeName", "name": "Int"}},
              {"name": "y", "type": {"kind": "TypeName", "name": "Int"}}
            ],
            "ret": {"kind": "TypeName", "name": "Int"},
            "effects": [],
            "body": {
              "kind": "Block",
              "statements": [{
                "kind": "Return",
                "expr": {
                  "kind": "Call",
                  "target": {"kind": "Name", "name": "+"},
                  "args": [
                    {"kind": "Name", "name": "x"},
                    {"kind": "Name", "name": "y"}
                  ]
                }
              }]
            }
          }]
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "binop.json").build();
      Value program = context.eval(source);
      assertTrue(program.canExecute(), "two-param entry should be executable");
      Value result = program.execute(1, 2);
      assertEquals(3, result.asInt(), "add(1, 2) via '+' operator should be 3");
    }
  }
}
