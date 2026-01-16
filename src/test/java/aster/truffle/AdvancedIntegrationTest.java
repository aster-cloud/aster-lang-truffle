package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 高级集成测试 - 测试递归、闭包、高阶函数等复杂场景
 */
public class AdvancedIntegrationTest {

  @Test
  public void testRecursiveFactorial() throws IOException {
    String json = """
        {
          "name": "test.recursive.factorial",
          "decls": [
            {
              "kind": "Func",
              "name": "factorial",
              "params": [
                {
                  "name": "n",
                  "type": { "kind": "TypeName", "name": "Int" }
                }
              ],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "If",
                    "cond": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "eq" },
                      "args": [
                        { "kind": "Name", "name": "n" },
                        { "kind": "Int", "value": 0 }
                      ]
                    },
                    "thenBlock": {
                      "kind": "Block",
                      "statements": [
                        {
                          "kind": "Return",
                          "expr": { "kind": "Int", "value": 1 }
                        }
                      ]
                    },
                    "elseBlock": {
                      "kind": "Block",
                      "statements": [
                        {
                          "kind": "Return",
                          "expr": {
                            "kind": "Call",
                            "target": { "kind": "Name", "name": "mul" },
                            "args": [
                              { "kind": "Name", "name": "n" },
                              {
                                "kind": "Call",
                                "target": { "kind": "Name", "name": "factorial" },
                                "args": [
                                  {
                                    "kind": "Call",
                                    "target": { "kind": "Name", "name": "sub" },
                                    "args": [
                                      { "kind": "Name", "name": "n" },
                                      { "kind": "Int", "value": 1 }
                                    ]
                                  }
                                ]
                              }
                            ]
                          }
                        }
                      ]
                    }
                  }
                ]
              }
            },
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "factorial" },
                      "args": [{ "kind": "Int", "value": 5 }]
                    }
                  }
                ]
              }
            }
          ]
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "factorial.json").build();
      Value result = context.eval(source);
      assertEquals(120, result.asInt(), "5! should be 120");
    }
  }

  @Test
  public void testRecursiveFibonacci() throws IOException {
    String json = """
        {
          "name": "test.recursive.fibonacci",
          "decls": [
            {
              "kind": "Func",
              "name": "fib",
              "params": [
                {
                  "name": "n",
                  "type": { "kind": "TypeName", "name": "Int" }
                }
              ],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "If",
                    "cond": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "lte" },
                      "args": [
                        { "kind": "Name", "name": "n" },
                        { "kind": "Int", "value": 1 }
                      ]
                    },
                    "thenBlock": {
                      "kind": "Block",
                      "statements": [
                        {
                          "kind": "Return",
                          "expr": { "kind": "Name", "name": "n" }
                        }
                      ]
                    },
                    "elseBlock": {
                      "kind": "Block",
                      "statements": [
                        {
                          "kind": "Return",
                          "expr": {
                            "kind": "Call",
                            "target": { "kind": "Name", "name": "add" },
                            "args": [
                              {
                                "kind": "Call",
                                "target": { "kind": "Name", "name": "fib" },
                                "args": [
                                  {
                                    "kind": "Call",
                                    "target": { "kind": "Name", "name": "sub" },
                                    "args": [
                                      { "kind": "Name", "name": "n" },
                                      { "kind": "Int", "value": 1 }
                                    ]
                                  }
                                ]
                              },
                              {
                                "kind": "Call",
                                "target": { "kind": "Name", "name": "fib" },
                                "args": [
                                  {
                                    "kind": "Call",
                                    "target": { "kind": "Name", "name": "sub" },
                                    "args": [
                                      { "kind": "Name", "name": "n" },
                                      { "kind": "Int", "value": 2 }
                                    ]
                                  }
                                ]
                              }
                            ]
                          }
                        }
                      ]
                    }
                  }
                ]
              }
            },
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "fib" },
                      "args": [{ "kind": "Int", "value": 10 }]
                    }
                  }
                ]
              }
            }
          ]
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "fibonacci.json").build();
      Value result = context.eval(source);
      assertEquals(55, result.asInt(), "fib(10) should be 55");
    }
  }

  @Test
  public void testClosureCapture() throws IOException {
    String json = """
        {
          "name": "test.closure.capture",
          "decls": [
            {
              "kind": "Func",
              "name": "makeAdder",
              "params": [
                {
                  "name": "x",
                  "type": { "kind": "TypeName", "name": "Int" }
                }
              ],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Lambda",
                      "params": [
                        {
                          "name": "y",
                          "type": { "kind": "TypeName", "name": "Int" }
                        }
                      ],
                      "ret": { "kind": "TypeName", "name": "Int" },
                      "captures": ["x"],
                      "body": {
                        "kind": "Block",
                        "statements": [
                          {
                            "kind": "Return",
                            "expr": {
                              "kind": "Call",
                              "target": { "kind": "Name", "name": "add" },
                              "args": [
                                { "kind": "Name", "name": "x" },
                                { "kind": "Name", "name": "y" }
                              ]
                            }
                          }
                        ]
                      }
                    }
                  }
                ]
              }
            },
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Let",
                    "name": "add5",
                    "expr": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "makeAdder" },
                      "args": [{ "kind": "Int", "value": 5 }]
                    }
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "add5" },
                      "args": [{ "kind": "Int", "value": 10 }]
                    }
                  }
                ]
              }
            }
          ]
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "closure.json").build();
      Value result = context.eval(source);
      assertEquals(15, result.asInt(), "add5(10) should be 15");
    }
  }

  @Test
  public void testHigherOrderFunction() throws IOException {
    String json = """
        {
          "name": "test.higher.order",
          "decls": [
            {
              "kind": "Func",
              "name": "apply",
              "params": [
                {
                  "name": "f",
                  "type": { "kind": "TypeName", "name": "Int" }
                },
                {
                  "name": "x",
                  "type": { "kind": "TypeName", "name": "Int" }
                }
              ],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "f" },
                      "args": [{ "kind": "Name", "name": "x" }]
                    }
                  }
                ]
              }
            },
            {
              "kind": "Func",
              "name": "double",
              "params": [
                {
                  "name": "n",
                  "type": { "kind": "TypeName", "name": "Int" }
                }
              ],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "mul" },
                      "args": [
                        { "kind": "Name", "name": "n" },
                        { "kind": "Int", "value": 2 }
                      ]
                    }
                  }
                ]
              }
            },
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "apply" },
                      "args": [
                        { "kind": "Name", "name": "double" },
                        { "kind": "Int", "value": 21 }
                      ]
                    }
                  }
                ]
              }
            }
          ]
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "higher-order.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "apply(double, 21) should be 42");
    }
  }

  @Test
  public void testNestedClosure() throws IOException {
    String json = """
        {
          "name": "test.nested.closure",
          "decls": [
            {
              "kind": "Func",
              "name": "makeMultiplier",
              "params": [
                {
                  "name": "x",
                  "type": { "kind": "TypeName", "name": "Int" }
                }
              ],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Lambda",
                      "params": [
                        {
                          "name": "y",
                          "type": { "kind": "TypeName", "name": "Int" }
                        }
                      ],
                      "ret": { "kind": "TypeName", "name": "Int" },
                      "captures": ["x"],
                      "body": {
                        "kind": "Block",
                        "statements": [
                          {
                            "kind": "Return",
                            "expr": {
                              "kind": "Lambda",
                              "params": [
                                {
                                  "name": "z",
                                  "type": { "kind": "TypeName", "name": "Int" }
                                }
                              ],
                              "ret": { "kind": "TypeName", "name": "Int" },
                              "captures": ["x", "y"],
                              "body": {
                                "kind": "Block",
                                "statements": [
                                  {
                                    "kind": "Return",
                                    "expr": {
                                      "kind": "Call",
                                      "target": { "kind": "Name", "name": "mul" },
                                      "args": [
                                        {
                                          "kind": "Call",
                                          "target": { "kind": "Name", "name": "mul" },
                                          "args": [
                                            { "kind": "Name", "name": "x" },
                                            { "kind": "Name", "name": "y" }
                                          ]
                                        },
                                        { "kind": "Name", "name": "z" }
                                      ]
                                    }
                                  }
                                ]
                              }
                            }
                          }
                        ]
                      }
                    }
                  }
                ]
              }
            },
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": { "kind": "TypeName", "name": "Int" },
              "effects": [],
              "body": {
                "kind": "Block",
                "statements": [
                  {
                    "kind": "Let",
                    "name": "f1",
                    "expr": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "makeMultiplier" },
                      "args": [{ "kind": "Int", "value": 2 }]
                    }
                  },
                  {
                    "kind": "Let",
                    "name": "f2",
                    "expr": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "f1" },
                      "args": [{ "kind": "Int", "value": 3 }]
                    }
                  },
                  {
                    "kind": "Return",
                    "expr": {
                      "kind": "Call",
                      "target": { "kind": "Name", "name": "f2" },
                      "args": [{ "kind": "Int", "value": 7 }]
                    }
                  }
                ]
              }
            }
          ]
        }
        """;

    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Source source = Source.newBuilder("aster", json, "nested-closure.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "2 * 3 * 7 should be 42");
    }
  }
}
