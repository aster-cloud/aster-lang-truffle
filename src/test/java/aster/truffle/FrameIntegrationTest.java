package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Frame 集成测试 - 验证 Frame 槽位分配和变量访问的正确性
 *
 * 注意: 由于 context.eval() 不支持传递函数参数,这里的测试主要验证:
 * 1. Let/Set 的 Env 存储
 * 2. Builtin 函数调用
 */
public class FrameIntegrationTest {

  private Context context;

  @BeforeEach
  public void setup() {
    context = Context.newBuilder("aster")
        .allowAllAccess(true)
        .build();

    // 注册常用 builtin 函数
    aster.truffle.runtime.Builtins.register("add", new aster.truffle.runtime.Builtins.BuiltinDef(args -> {
      int a = (Integer) args[0];
      int b = (Integer) args[1];
      return a + b;
    }));
    aster.truffle.runtime.Builtins.register("double", new aster.truffle.runtime.Builtins.BuiltinDef(args -> {
      int n = (Integer) args[0];
      return n * 2;
    }));
  }

  @AfterEach
  public void tearDown() {
    if (context != null) {
      context.close();
    }
  }

  /**
   * 测试1: LetNode Env 存储
   *
   * 场景: func main() -> Int { let x = 42; return x; }
   * 验证: let 声明的变量使用 LetNodeEnv
   */
  @Test
  public void testLetNodeEnvStorage() {
    String json = """
        {
          "name": "test.let",
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": null,
              "body": {
                "statements": [
                  {
                    "kind": "Let",
                    "name": "x",
                    "expr": {"kind": "Int", "value": 42}
                  },
                  {
                    "kind": "Return",
                    "expr": {"kind": "Name", "name": "x"}
                  }
                ]
              }
            }
          ]
        }
        """;

    Value result = context.eval("aster", json);

    // 验证: x 应该等于 42
    assertEquals(42, result.asInt(), "LetNode 应正确存储和读取变量");
  }

  /**
   * 测试2: SetNode Env 更新
   *
   * 场景: func main() -> Int { let x = 10; set x = 100; return x; }
   * 验证: set 更新变量使用 SetNodeEnv
   */
  @Test
  public void testSetNodeEnvUpdate() {
    String json = """
        {
          "name": "test.set",
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": null,
              "body": {
                "statements": [
                  {
                    "kind": "Let",
                    "name": "x",
                    "expr": {"kind": "Int", "value": 10}
                  },
                  {
                    "kind": "Set",
                    "name": "x",
                    "expr": {"kind": "Int", "value": 100}
                  },
                  {
                    "kind": "Return",
                    "expr": {"kind": "Name", "name": "x"}
                  }
                ]
              }
            }
          ]
        }
        """;

    Value result = context.eval("aster", json);

    // 验证: x 应该被 set 更新为 100
    assertEquals(100, result.asInt(), "SetNode 应正确更新变量");
  }

  /**
   * 测试3: Let + Set 组合
   *
   * 场景: func main() -> Int { let x = 41; set x = 42; return x; }
   * 验证: let 和 set 可以配合使用(简化版，不调用函数)
   */
  @Test
  public void testLetSetCombination() {
    String json = """
        {
          "name": "test.let_set",
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": null,
              "body": {
                "statements": [
                  {
                    "kind": "Let",
                    "name": "x",
                    "expr": {"kind": "Int", "value": 41}
                  },
                  {
                    "kind": "Set",
                    "name": "x",
                    "expr": {"kind": "Int", "value": 42}
                  },
                  {
                    "kind": "Return",
                    "expr": {"kind": "Name", "name": "x"}
                  }
                ]
              }
            }
          ]
        }
        """;

    Value result = context.eval("aster", json);

    // 验证: x 被设置为 42
    assertEquals(42, result.asInt(), "Let 和 Set 应能配合使用");
  }

  /**
   * 测试4: 多变量交互
   *
   * 场景: func main() -> Int {
   *   let x = 10;
   *   let y = 20;
   *   let z = 30;
   *   return z;
   * }
   * 验证: 多个变量可以独立存储和访问
   */
  @Test
  public void testMultipleVariables() {
    String json = """
        {
          "name": "test.multiple_vars",
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": null,
              "body": {
                "statements": [
                  {
                    "kind": "Let",
                    "name": "x",
                    "expr": {"kind": "Int", "value": 10}
                  },
                  {
                    "kind": "Let",
                    "name": "y",
                    "expr": {"kind": "Int", "value": 20}
                  },
                  {
                    "kind": "Let",
                    "name": "z",
                    "expr": {"kind": "Int", "value": 30}
                  },
                  {
                    "kind": "Return",
                    "expr": {"kind": "Name", "name": "z"}
                  }
                ]
              }
            }
          ]
        }
        """;

    Value result = context.eval("aster", json);
    assertEquals(30, result.asInt(), "应正确访问第三个变量");
  }

  /**
   * 测试5: 变量在条件分支中的使用
   *
   * 场景: func main() -> Int {
   *   let x = 5;
   *   if (true) {
   *     set x = 15;
   *   }
   *   return x;
   * }
   * 验证: 条件分支内可以修改外部变量
   */
  @Test
  public void testVariableInIfBranch() {
    String json = """
        {
          "name": "test.if_branch",
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": null,
              "body": {
                "statements": [
                  {
                    "kind": "Let",
                    "name": "x",
                    "expr": {"kind": "Int", "value": 5}
                  },
                  {
                    "kind": "If",
                    "cond": {"kind": "Bool", "value": true},
                    "thenBlock": {
                      "statements": [
                        {
                          "kind": "Set",
                          "name": "x",
                          "expr": {"kind": "Int", "value": 15}
                        }
                      ]
                    },
                    "elseBlock": null
                  },
                  {
                    "kind": "Return",
                    "expr": {"kind": "Name", "name": "x"}
                  }
                ]
              }
            }
          ]
        }
        """;

    Value result = context.eval("aster", json);
    assertEquals(15, result.asInt(), "If 分支内应能修改外部变量");
  }

  /**
   * 测试6: 变量顺序访问和更新
   *
   * 场景: func main() -> Int {
   *   let x = 1;
   *   let y = 2;
   *   set x = 10;
   *   set y = 20;
   *   return y;
   * }
   * 验证: 多个变量可以按任意顺序声明和更新
   */
  @Test
  public void testVariableSequentialAccess() {
    String json = """
        {
          "name": "test.sequential",
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": null,
              "body": {
                "statements": [
                  {
                    "kind": "Let",
                    "name": "x",
                    "expr": {"kind": "Int", "value": 1}
                  },
                  {
                    "kind": "Let",
                    "name": "y",
                    "expr": {"kind": "Int", "value": 2}
                  },
                  {
                    "kind": "Set",
                    "name": "x",
                    "expr": {"kind": "Int", "value": 10}
                  },
                  {
                    "kind": "Set",
                    "name": "y",
                    "expr": {"kind": "Int", "value": 20}
                  },
                  {
                    "kind": "Return",
                    "expr": {"kind": "Name", "name": "y"}
                  }
                ]
              }
            }
          ]
        }
        """;

    Value result = context.eval("aster", json);
    assertEquals(20, result.asInt(), "应正确处理多变量的顺序更新");
  }

  /**
   * 测试7: Frame/Env 双模式兼容性
   *
   * 注意: 当前实现中所有变量都使用 Env 存储(通过 *Env 节点),
   * 这个测试验证 Env-based 的实现在复杂场景下的正确性。
   * 未来当引入 Frame-based 节点时,这个测试可以扩展为真正的双模式测试。
   *
   * 场景: func main() -> Int {
   *   let a = 10;
   *   let b = 20;
   *   if (true) {
   *     let c = 30;
   *     set a = 100;
   *   }
   *   set b = 200;
   *   return a;
   * }
   * 验证: 嵌套作用域中的变量声明和修改不会相互干扰
   */
  @Test
  public void testFrameEnvCompatibility() {
    String json = """
        {
          "name": "test.frame_env",
          "decls": [
            {
              "kind": "Func",
              "name": "main",
              "params": [],
              "ret": null,
              "body": {
                "statements": [
                  {
                    "kind": "Let",
                    "name": "a",
                    "expr": {"kind": "Int", "value": 10}
                  },
                  {
                    "kind": "Let",
                    "name": "b",
                    "expr": {"kind": "Int", "value": 20}
                  },
                  {
                    "kind": "If",
                    "cond": {"kind": "Bool", "value": true},
                    "thenBlock": {
                      "statements": [
                        {
                          "kind": "Let",
                          "name": "c",
                          "expr": {"kind": "Int", "value": 30}
                        },
                        {
                          "kind": "Set",
                          "name": "a",
                          "expr": {"kind": "Int", "value": 100}
                        }
                      ]
                    },
                    "elseBlock": null
                  },
                  {
                    "kind": "Set",
                    "name": "b",
                    "expr": {"kind": "Int", "value": 200}
                  },
                  {
                    "kind": "Return",
                    "expr": {"kind": "Name", "name": "a"}
                  }
                ]
              }
            }
          ]
        }
        """;

    Value result = context.eval("aster", json);
    assertEquals(100, result.asInt(), "嵌套作用域中修改的外部变量应正确反映");
  }
}
