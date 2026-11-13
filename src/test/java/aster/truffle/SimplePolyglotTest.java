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
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.simple",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "identity",
              "typeParams": [],
              "params": [
                {
                  "name": "x",
                  "type": {
                    "kind": "TypeName",
                    "name": "Int",
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  "annotations": [],
                  "constraints": [],
                  "typeInferred": false
                }
              ],
              "ret": {
                "kind": "TypeName",
                "name": "Int",
                "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
              },
              "retTypeInferred": false,
              "annotations": [],
              "retAnnotations": [],
              "effects": [],
              "effectParams": [],
              "effectCaps": [],
              "effectCapsExplicit": false,
              "inputRefs": null,
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Name",
                      "name": "x",
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  }
                ],
                "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
              },
              "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
            },
            {
              "kind": "Func",
              "name": "main",
              "typeParams": [],
              "params": [],
              "ret": {
                "kind": "TypeName",
                "name": "Int",
                "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
              },
              "retTypeInferred": false,
              "annotations": [],
              "retAnnotations": [],
              "effects": [],
              "effectParams": [],
              "effectCaps": [],
              "effectCapsExplicit": false,
              "inputRefs": null,
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "identity",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 42,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        }
                      ],
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  }
                ],
                "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
              },
              "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
            }
          ],
          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
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
          "schemaVersion": 2,
          "name": "test.binop",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "add",
              "typeParams": [],
              "params": [
                {
                  "name": "x",
                  "type": {
                    "kind": "TypeName",
                    "name": "Int",
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  "annotations": [],
                  "constraints": [],
                  "typeInferred": false
                },
                {
                  "name": "y",
                  "type": {
                    "kind": "TypeName",
                    "name": "Int",
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  "annotations": [],
                  "constraints": [],
                  "typeInferred": false
                }
              ],
              "ret": {
                "kind": "TypeName",
                "name": "Int",
                "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
              },
              "retTypeInferred": false,
              "annotations": [],
              "retAnnotations": [],
              "effects": [],
              "effectParams": [],
              "effectCaps": [],
              "effectCapsExplicit": false,
              "inputRefs": null,
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "+",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "x",
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Name",
                          "name": "y",
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        }
                      ],
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  }
                ],
                "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
              },
              "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
            }
          ],
          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
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
