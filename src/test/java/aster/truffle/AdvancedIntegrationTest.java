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
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.recursive.factorial",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "factorial",
              "typeParams": [],
              "params": [
                {
                  "name": "n",
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
                    "kind": "If",
                    "cond": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "eq",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "n",
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 0,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        }
                      ],
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "thenBlock": {
                      "kind": "Block",
                      "statements": [
                        {
                          "kind": "Return",
                          "expr": {
                            "kind": "Int",
                            "value": 1,
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
                            "kind": "Call",
                            "target": {
                              "kind": "Name",
                              "name": "mul",
                              "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                            },
                            "args": [
                              {
                                "kind": "Name",
                                "name": "n",
                                "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                              },
                              {
                                "kind": "Call",
                                "target": {
                                  "kind": "Name",
                                  "name": "factorial",
                                  "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                },
                                "args": [
                                  {
                                    "kind": "Call",
                                    "target": {
                                      "kind": "Name",
                                      "name": "sub",
                                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                    },
                                    "args": [
                                      {
                                        "kind": "Name",
                                        "name": "n",
                                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                      },
                                      {
                                        "kind": "Int",
                                        "value": 1,
                                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                      }
                                    ],
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
                        "name": "factorial",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
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
      Source source = Source.newBuilder("aster", json, "factorial.json").build();
      Value result = context.eval(source);
      assertEquals(120, result.asInt(), "5! should be 120");
    }
  }

  @Test
  public void testRecursiveFibonacci() throws IOException {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.recursive.fibonacci",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "fib",
              "typeParams": [],
              "params": [
                {
                  "name": "n",
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
                    "kind": "If",
                    "cond": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "lte",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "n",
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 1,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        }
                      ],
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "thenBlock": {
                      "kind": "Block",
                      "statements": [
                        {
                          "kind": "Return",
                          "expr": {
                            "kind": "Name",
                            "name": "n",
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
                            "kind": "Call",
                            "target": {
                              "kind": "Name",
                              "name": "add",
                              "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                            },
                            "args": [
                              {
                                "kind": "Call",
                                "target": {
                                  "kind": "Name",
                                  "name": "fib",
                                  "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                },
                                "args": [
                                  {
                                    "kind": "Call",
                                    "target": {
                                      "kind": "Name",
                                      "name": "sub",
                                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                    },
                                    "args": [
                                      {
                                        "kind": "Name",
                                        "name": "n",
                                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                      },
                                      {
                                        "kind": "Int",
                                        "value": 1,
                                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                      }
                                    ],
                                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                  }
                                ],
                                "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                              },
                              {
                                "kind": "Call",
                                "target": {
                                  "kind": "Name",
                                  "name": "fib",
                                  "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                },
                                "args": [
                                  {
                                    "kind": "Call",
                                    "target": {
                                      "kind": "Name",
                                      "name": "sub",
                                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                    },
                                    "args": [
                                      {
                                        "kind": "Name",
                                        "name": "n",
                                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                      },
                                      {
                                        "kind": "Int",
                                        "value": 2,
                                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                      }
                                    ],
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
                        "name": "fib",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 10,
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
      Source source = Source.newBuilder("aster", json, "fibonacci.json").build();
      Value result = context.eval(source);
      assertEquals(55, result.asInt(), "fib(10) should be 55");
    }
  }

  @Test
  public void testClosureCapture() throws IOException {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.closure.capture",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "makeAdder",
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
                      "kind": "Lambda",
                      "params": [
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
                      "captures": ["x"],
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
                    "kind": "Let",
                    "name": "add5",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "makeAdder",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 5,
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
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "add5",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 10,
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
      Source source = Source.newBuilder("aster", json, "closure.json").build();
      Value result = context.eval(source);
      assertEquals(15, result.asInt(), "add5(10) should be 15");
    }
  }

  @Test
  public void testHigherOrderFunction() throws IOException {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.higher.order",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "apply",
              "typeParams": [],
              "params": [
                {
                  "name": "f",
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
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "f",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "x",
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
            },
            {
              "kind": "Func",
              "name": "double",
              "typeParams": [],
              "params": [
                {
                  "name": "n",
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
                        "name": "mul",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "n",
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 2,
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
                        "name": "apply",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Name",
                          "name": "double",
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 21,
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
      Source source = Source.newBuilder("aster", json, "higher-order.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "apply(double, 21) should be 42");
    }
  }

  @Test
  public void testNestedClosure() throws IOException {
    String json = """
        {
          "kind": "Module",
          "schemaVersion": 2,
          "name": "test.nested.closure",
          "profile": null,
          "decls": [
            {
              "kind": "Func",
              "name": "makeMultiplier",
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
                      "kind": "Lambda",
                      "params": [
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
                                      "kind": "Call",
                                      "target": {
                                        "kind": "Name",
                                        "name": "mul",
                                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                                      },
                                      "args": [
                                        {
                                          "kind": "Call",
                                          "target": {
                                            "kind": "Name",
                                            "name": "mul",
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
                                        {
                                          "kind": "Name",
                                          "name": "z",
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
                              "captures": ["x", "y"],
                              "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                            },
                            "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                          }
                        ],
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "captures": ["x"],
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
                    "kind": "Let",
                    "name": "f1",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "makeMultiplier",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 2,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        }
                      ],
                      "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                    },
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  {
                    "kind": "Let",
                    "name": "f2",
                    "expr": {
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "f1",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
                        {
                          "kind": "Int",
                          "value": 3,
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
                      "kind": "Call",
                      "target": {
                        "kind": "Name",
                        "name": "f2",
                        "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                      },
                      "args": [
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
      Source source = Source.newBuilder("aster", json, "nested-closure.json").build();
      Value result = context.eval(source);
      assertEquals(42, result.asInt(), "2 * 3 * 7 should be 42");
    }
  }
}
