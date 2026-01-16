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
}
