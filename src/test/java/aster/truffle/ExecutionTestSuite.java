package aster.truffle;

import aster.truffle.runtime.AsyncTaskRegistry;
import aster.truffle.runtime.DependencyGraph;
import aster.truffle.runtime.WorkflowScheduler;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Execution Test Suite - 验证 Truffle 后端的正确执行
 *
 * 与 GoldenTestAdapter 的区别：
 * - GoldenTestAdapter: 验证执行不崩溃（功能性测试）
 * - ExecutionTestSuite: 验证执行结果正确性（正确性测试）
 *
 * 测试策略：
 * 1. 明确的输入参数
 * 2. 明确的预期输出
 * 3. 涵盖所有核心功能
 * 4. 边界条件和错误情况
 */
public class ExecutionTestSuite {

  // ==================== Arithmetic Operations ====================

  @Test
  public void testAddition() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.add",
          "profile": null,
          "decls": [
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
                        "name": "add",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 10,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 20,
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(30, result.asInt(), "10 + 20 should equal 30");
    }
  }

  @Test
  public void testSubtraction() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.sub",
          "profile": null,
          "decls": [
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
                        "name": "sub",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 50,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 20,
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(30, result.asInt(), "50 - 20 should equal 30");
    }
  }

  @Test
  public void testMultiplication() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.mul",
          "profile": null,
          "decls": [
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
                        "name": "mul",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 6,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 7,
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "6 * 7 should equal 42");
    }
  }

  @Test
  public void testDivision() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.div",
          "profile": null,
          "decls": [
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
                        "name": "div",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 100,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 5,
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(20, result.asInt(), "100 / 5 should equal 20");
    }
  }

  // ==================== Comparison Operations ====================

  @Test
  public void testEquality() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.eq",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "typeParams": [],
              "params": [],
              "ret": {
                "kind": "TypeName",
                "name": "Bool",
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
                        "name": "eq",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 42,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertTrue(result.asBoolean(), "42 == 42 should be true");
    }
  }

  @Test
  public void testLessThan() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.lt",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "typeParams": [],
              "params": [],
              "ret": {
                "kind": "TypeName",
                "name": "Bool",
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
                        "name": "lt",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 10,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 20,
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertTrue(result.asBoolean(), "10 < 20 should be true");
    }
  }

  // ==================== Text Operations ====================

  @Test
  public void testTextConcat() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.text",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "typeParams": [],
              "params": [],
              "ret": {
                "kind": "TypeName",
                "name": "Text",
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
                        "name": "Text.concat",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "String",
                          "value": "Hello",
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "String",
                          "value": "World",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals("HelloWorld", result.asString(), "Text.concat should concatenate strings");
    }
  }

  @Test
  public void testTextLength() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.text.length",
          "profile": null,
          "decls": [
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
                        "name": "Text.length",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "String",
                          "value": "Hello",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(5, result.asInt(), "Text.length('Hello') should be 5");
    }
  }

  @Test
  public void testTextContains() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.text.contains",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "typeParams": [],
              "params": [],
              "ret": {
                "kind": "TypeName",
                "name": "Bool",
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
                        "name": "Text.contains",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "String",
                          "value": "Hello World",
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "String",
                          "value": "World",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertTrue(result.asBoolean(), "Text.contains should find 'World' in 'Hello World'");
    }
  }

  // ==================== Control Flow ====================

  @Test
  public void testIfTrueBranch() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.if.true",
          "profile": null,
          "decls": [
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
                    "kind": "If",
                    "cond": {
                      "kind": "Bool",
                      "value": true,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "thenBlock": {
                      "kind": "Block",
                      "statements": [
                        {
                          "kind": "Return",
                          "expr": {
                            "kind": "Int",
                            "value": 42,
                            "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                          },
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        }
                      ],
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "elseBlock": {
                      "kind": "Block",
                      "statements": [
                        {
                          "kind": "Return",
                          "expr": {
                            "kind": "Int",
                            "value": 0,
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
              },
              "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
            }
          ],
          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "If true should return 42");
    }
  }

  @Test
  public void testIfFalseBranch() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.if.false",
          "profile": null,
          "decls": [
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
                    "kind": "If",
                    "cond": {
                      "kind": "Bool",
                      "value": false,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "thenBlock": {
                      "kind": "Block",
                      "statements": [
                        {
                          "kind": "Return",
                          "expr": {
                            "kind": "Int",
                            "value": 0,
                            "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                          },
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        }
                      ],
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "elseBlock": {
                      "kind": "Block",
                      "statements": [
                        {
                          "kind": "Return",
                          "expr": {
                            "kind": "Int",
                            "value": 99,
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
              },
              "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
            }
          ],
          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(99, result.asInt(), "If false should return 99");
    }
  }

  // ==================== Variables (Let/Set) ====================

  @Test
  public void testLetBinding() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.let",
          "profile": null,
          "decls": [
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
                    "kind": "Let",
                    "name": "x",
                    "expr": {
                      "kind": "Int",
                      "value": 42,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
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
            }
          ],
          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "Let binding should work");
    }
  }

  @Test
  public void testSetVariable() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.set",
          "profile": null,
          "decls": [
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
                    "kind": "Let",
                    "name": "x",
                    "expr": {
                      "kind": "Int",
                      "value": 10,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Set",
                    "name": "x",
                    "expr": {
                      "kind": "Int",
                      "value": 20,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
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
            }
          ],
          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(20, result.asInt(), "Set should update variable value");
    }
  }

  // ==================== Lambda/Closure ====================

  @Test
  public void testSimpleLambda() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.lambda",
          "profile": null,
          "decls": [
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
                    "kind": "Let",
                    "name": "identity",
                    "expr": {
                      "kind": "Lambda",
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
                      "captures": [],
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "Lambda identity function should return input");
    }
  }

  // ==================== Result/Maybe Types ====================

  @Test
  public void testOkConstruction() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.ok",
          "profile": null,
          "decls": [
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
                    "kind": "Let",
                    "name": "result",
                    "expr": {
                      "kind": "Ok",
                      "expr": {
                        "kind": "Int",
                        "value": 42,
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "Result.unwrap",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "result",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "Ok value should unwrap to 42");
    }
  }

  // ==================== Result Operations ====================

  @Test
  public void testResultIsOk() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.result.isok",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "typeParams": [],
              "params": [],
              "ret": {
                "kind": "TypeName",
                "name": "Bool",
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
                    "kind": "Let",
                    "name": "result",
                    "expr": {
                      "kind": "Ok",
                      "expr": {
                        "kind": "Int",
                        "value": 100,
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "Result.isOk",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "result",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertTrue(result.asBoolean(), "Result.isOk should return true for Ok variant");
    }
  }

  @Test
  public void testResultIsErr() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.result.iserr",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "typeParams": [],
              "params": [],
              "ret": {
                "kind": "TypeName",
                "name": "Bool",
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
                    "kind": "Let",
                    "name": "result",
                    "expr": {
                      "kind": "Err",
                      "expr": {
                        "kind": "String",
                        "value": "error",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "Result.isErr",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "result",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertTrue(result.asBoolean(), "Result.isErr should return true for Err variant");
    }
  }

  // ==================== Option Operations ====================

  @Test
  public void testOptionIsSome() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.option.issome",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "typeParams": [],
              "params": [],
              "ret": {
                "kind": "TypeName",
                "name": "Bool",
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
                    "kind": "Let",
                    "name": "option",
                    "expr": {
                      "kind": "Some",
                      "expr": {
                        "kind": "Int",
                        "value": 42,
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "Option.isSome",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "option",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertTrue(result.asBoolean(), "Option.isSome should return true for Some variant");
    }
  }

  @Test
  public void testOptionIsNone() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.option.isnone",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "typeParams": [],
              "params": [],
              "ret": {
                "kind": "TypeName",
                "name": "Bool",
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
                    "kind": "Let",
                    "name": "option",
                    "expr": {
                      "kind": "None",
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "Option.isNone",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "option",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertTrue(result.asBoolean(), "Option.isNone should return true for None variant");
    }
  }

  // ==================== Async Basic Operations ====================

  // Note: Effect context propagation through async boundaries has been implemented
  // in StartNode to capture and restore parent effect permissions

  @Test
  public void testAsyncBasic() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.async.basic",
          "profile": null,
          "decls": [
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
                    "kind": "Start",
                    "name": "task1",
                    "expr": {
                      "kind": "Int",
                      "value": 42,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Await",
                      "expr": {
                        "kind": "Name",
                        "name": "task1",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "Async task should return 42");
    }
  }

  @Test
  public void testAsyncMultipleTasks() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.async.multiple",
          "profile": null,
          "decls": [
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
                    "kind": "Start",
                    "name": "task1",
                    "expr": {
                      "kind": "Int",
                      "value": 10,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Start",
                    "name": "task2",
                    "expr": {
                      "kind": "Int",
                      "value": 20,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Let",
                    "name": "result1",
                    "expr": {
                      "kind": "Await",
                      "expr": {
                        "kind": "Name",
                        "name": "task1",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Let",
                    "name": "result2",
                    "expr": {
                      "kind": "Await",
                      "expr": {
                        "kind": "Name",
                        "name": "task2",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Name",
                      "name": "result1",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(10, result.asInt(), "Multiple async tasks should return first result 10");
    }
  }

  @Test
  public void testAsyncWaitMultipleTasks() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.async.wait",
          "profile": null,
          "decls": [
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
                    "kind": "Start",
                    "name": "task1",
                    "expr": {
                      "kind": "Int",
                      "value": 5,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Start",
                    "name": "task2",
                    "expr": {
                      "kind": "Int",
                      "value": 10,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Wait",
                    "names": ["task1", "task2"],
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Int",
                      "value": 42,
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "Wait should complete all tasks and return 42");
    }
  }

  // ==================== Scope and Wait Semantics ====================

  @Test
  public void testScopeVariableShadowing() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.scope.shadowing",
          "profile": null,
          "decls": [
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
                    "kind": "Let",
                    "name": "x",
                    "expr": {
                      "kind": "Int",
                      "value": 10,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Scope",
                    "statements": [
                      {
                        "kind": "Let",
                        "name": "x",
                        "expr": {
                          "kind": "Int",
                          "value": 20,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      }
                    ],
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
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
            }
          ],
          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(10, result.asInt(), "Inner scope binding should not override outer variable");
    }
  }

  @Test
  public void testScopeNoVariableLeaking() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.scope.isolation",
          "profile": null,
          "decls": [
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
                    "kind": "Let",
                    "name": "outer",
                    "expr": {
                      "kind": "Int",
                      "value": 1,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Scope",
                    "statements": [
                      {
                        "kind": "Let",
                        "name": "inner",
                        "expr": {
                          "kind": "Int",
                          "value": 2,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      {
                        "kind": "Set",
                        "name": "outer",
                        "expr": {
                          "kind": "Int",
                          "value": 3,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      }
                    ],
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Name",
                      "name": "outer",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(3, result.asInt(), "Scope should allow updating outer variables while inner bindings remain isolated");
    }
  }

  @Test
  public void testWaitSingleTaskResult() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.wait.single",
          "profile": null,
          "decls": [
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
                    "kind": "Start",
                    "name": "task1",
                    "expr": {
                      "kind": "Int",
                      "value": 42,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Wait",
                    "names": ["task1"],
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Name",
                      "name": "task1",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "wait task should expose completed result in the same scope");
    }
  }

  @Test
  public void testWaitMultipleTasksResult() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.wait.multi",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "typeParams": [],
              "params": [],
              "ret": {
                "kind": "TypeName",
                "name": "List",
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
                    "kind": "Start",
                    "name": "task1",
                    "expr": {
                      "kind": "Int",
                      "value": 10,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Start",
                    "name": "task2",
                    "expr": {
                      "kind": "Int",
                      "value": 20,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Wait",
                    "names": ["task1", "task2"],
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "List.append",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Call",
                          "target": {
                            "kind": "Name",
                            "name": "List.append",
                            "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                          },
                          "args": [
                            {
                              "kind": "Call",
                              "target": {
                                "kind": "Name",
                                "name": "List.empty",
                                "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                              },
                              "args": [],
                              "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                            },
                            {
                              "kind": "Name",
                              "name": "task1",
                              "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                            }
                          ],
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Name",
                          "name": "task2",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertTrue(result.hasArrayElements(), "wait should allow aggregating results into a list");
      assertEquals(2, result.getArraySize(), "Aggregated wait result should contain both task values");
      assertEquals(10, result.getArrayElement(0).asInt(), "First task result should be 10");
      assertEquals(20, result.getArrayElement(1).asInt(), "Second task result should be 20");
    }
  }

  @Test
  public void testWaitInNestedScope() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.wait.scope",
          "profile": null,
          "decls": [
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
                    "kind": "Start",
                    "name": "task",
                    "expr": {
                      "kind": "Int",
                      "value": 99,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Scope",
                    "statements": [
                      {
                        "kind": "Wait",
                        "names": ["task"],
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      {
                        "kind": "Return",
                        "expr": {
                          "kind": "Name",
                          "name": "task",
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      }
                    ],
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(99, result.asInt(), "wait inside nested scope should still return the awaited value");
    }
  }

  // ==================== Effect Violations ====================

  /**
   * async 不是效应（ADR 0055 #5）：Start 是并发构造，不需要声明任何效应即可执行。
   */
  @Test
  public void testStartNeedsNoDeclaredAsyncEffect() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.effect.async.granted",
          "profile": null,
          "decls": [
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
                    "kind": "Start",
                    "name": "task1",
                    "expr": {
                      "kind": "Int",
                      "value": 42,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Int",
                      "value": 0,
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      assertEquals(0, context.eval(source).asInt());
    }
  }

  @Test
  public void testEffectViolation_IO() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.effect.io.violation",
          "profile": null,
          "decls": [
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
                    "kind": "Let",
                    "name": "printed",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "IO.print",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "String",
                          "value": "Hello",
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        }
                      ],
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Int",
                      "value": 0,
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Exception exception = assertThrows(Exception.class, () -> context.eval(source));
      assertTrue(exception.getMessage().contains("effect not permitted"),
                 "Should throw exception for missing io effect: " + exception.getMessage());
    }
  }

  // ==================== Advanced Async Scenarios ====================

  @Test
  public void testAsyncLiterals() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.async.literals",
          "profile": null,
          "decls": [
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
                    "kind": "Start",
                    "name": "task",
                    "expr": {
                      "kind": "Int",
                      "value": 99,
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Await",
                      "expr": {
                        "kind": "Name",
                        "name": "task",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(99, result.asInt(), "Async literal should return 99");
    }
  }

  @Test
  public void testResultInAsync() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.result.in.async",
          "profile": null,
          "decls": [
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
                    "kind": "Start",
                    "name": "task",
                    "expr": {
                      "kind": "Ok",
                      "expr": {
                        "kind": "Int",
                        "value": 99,
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Let",
                    "name": "result",
                    "expr": {
                      "kind": "Await",
                      "expr": {
                        "kind": "Name",
                        "name": "task",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "Result.unwrap",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "result",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(99, result.asInt(), "Result in async should unwrap Ok value 99");
    }
  }

  @Test
  public void testOptionInAsync() throws Exception {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.option.in.async",
          "profile": null,
          "decls": [
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
                    "kind": "Start",
                    "name": "task",
                    "expr": {
                      "kind": "Some",
                      "expr": {
                        "kind": "Int",
                        "value": 77,
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Let",
                    "name": "option",
                    "expr": {
                      "kind": "Await",
                      "expr": {
                        "kind": "Name",
                        "name": "task",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "Option.unwrap",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "option",
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
      Source source = Source.newBuilder("aster", json, "test.json").build();
      Value result = context.eval(source);
      assertEquals(77, result.asInt(), "Option in async should unwrap Some value 77");
    }
  }

  // ==================== Workflow Integration ====================

  /**
   * 测试：线性工作流 A→B→C，验证阶段顺序与结果传递。
   */
  @Test
  public void testWorkflowLinearChain() {
    WorkflowTestFixture fx = new WorkflowTestFixture();
    AtomicInteger sequence = new AtomicInteger(0);

    fx.register("prepare", () -> fx.registry.setResult("prepare", "stage-" + sequence.incrementAndGet()),
        Collections.emptySet());
    fx.register("process", () -> fx.registry.setResult("process", "stage-" + sequence.incrementAndGet()),
        Set.of("prepare"));
    fx.register("finish", () -> fx.registry.setResult("finish", "stage-" + sequence.incrementAndGet()),
        Set.of("process"));

    fx.run(5_000);

    assertEquals("stage-1", fx.registry.getResult("prepare"));
    assertEquals("stage-2", fx.registry.getResult("process"));
    assertEquals("stage-3", fx.registry.getResult("finish"));
    // All tasks completed successfully if executeUntilComplete() returned without exception
  }

  /**
   * 测试：菱形工作流 A→B,C→D，验证分支合流逻辑。
   */
  @Test
  public void testWorkflowDiamond() {
    WorkflowTestFixture fx = new WorkflowTestFixture();

    fx.register("seed", () -> fx.registry.setResult("seed", 5), Collections.emptySet());
    fx.register("branch-left", () -> {
      int base = (int) fx.registry.getResult("seed");
      fx.registry.setResult("branch-left", base + 10);
    }, Set.of("seed"));

    fx.register("branch-right", () -> {
      int base = (int) fx.registry.getResult("seed");
      fx.registry.setResult("branch-right", base + 20);
    }, Set.of("seed"));

    fx.register("merge", () -> {
      int left = (int) fx.registry.getResult("branch-left");
      int right = (int) fx.registry.getResult("branch-right");
      fx.registry.setResult("merge", left + right);
    }, Set.of("branch-left", "branch-right"));

    fx.run(5_000);

    assertEquals(5, fx.registry.getResult("seed"));
    assertEquals(15, fx.registry.getResult("branch-left"));
    assertEquals(25, fx.registry.getResult("branch-right"));
    assertEquals(40, fx.registry.getResult("merge"));
  }

  /**
   * 测试：工作流中某个任务失败时，错误应向后传播并取消依赖链。
   */
  @Test
  public void testWorkflowError() {
    WorkflowTestFixture fx = new WorkflowTestFixture();

    fx.register("source", () -> fx.registry.setResult("source", "ok"), Collections.emptySet());
    fx.register("fail", () -> { throw new RuntimeException("intentional"); }, Set.of("source"));
    fx.register("sink", () -> fx.registry.setResult("sink", "never"), Set.of("fail"));

    // RuntimeException 直接透传，保留原始异常消息
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> fx.run(5_000));
    assertTrue(thrown.getMessage().contains("intentional"), "应保留原始异常消息");

    assertTrue(fx.registry.isCompleted("source"));
    assertTrue(fx.registry.isFailed("fail"));
    assertTrue(fx.registry.isCancelled("sink"));
  }

  /**
   * 工作流测试夹具：复用调度组件，简化每个用例的注册流程。
   */
  private static final class WorkflowTestFixture {
    final AsyncTaskRegistry registry = new AsyncTaskRegistry();
    final WorkflowScheduler scheduler = new WorkflowScheduler(registry);
    void register(String taskId, Runnable body, Set<String> deps) {
      Set<String> normalizedDeps = deps == null ? Collections.emptySet() : deps;
      registry.registerTaskWithDependencies(taskId, () -> {
        body.run();
        return null;
      }, normalizedDeps);
    }

    void run(long timeoutMs) {
      scheduler.executeUntilComplete();
    }
  }
}
