package aster.truffle;
import org.junit.jupiter.api.Test;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import static org.junit.jupiter.api.Assertions.*;

// ADR 0024 C0 探针：列表字面量 Core IR (ListLit) 经 Truffle 求值不再崩溃。
// 等价于 `Rule main produce Int: Return List.length([10,20,30]).`（旧 Construct("List") 会崩）。
class ListLiteralProbeTest {
  @Test void listLiteralLengthIsThree() throws Exception {
    String json = """
    {
      "kind": "Module",
      "schemaVersion": 2,
      "name": "probe",
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
                    "name": "List.length",
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  "args": [
                    {
                      "kind": "ListLit",
                      "elements": [
                        {
                          "kind": "Int",
                          "value": 10,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 20,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 30,
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
    }
    """;
    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value result = context.eval(Source.newBuilder("aster", json, "probe.json").build());
      assertEquals(3, result.asInt(), "List.length([10,20,30]) == 3");
    }
  }

  @Test void listLiteralGetIndex() throws Exception {
    String json = """
    {
      "kind": "Module",
      "schemaVersion": 2,
      "name": "probe2",
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
                    "name": "List.get",
                    "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                  },
                  "args": [
                    {
                      "kind": "ListLit",
                      "elements": [
                        {
                          "kind": "Int",
                          "value": 5,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 15,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        },
                        {
                          "kind": "Int",
                          "value": 25,
                          "origin": {"file": null, "start": {"line": 1, "col": 1}, "end": {"line": 1, "col": 1}, "written": null, "role": null}
                        }
                      ],
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
      Value result = context.eval(Source.newBuilder("aster", json, "probe2.json").build());
      assertEquals(15, result.asInt(), "List.get([5,15,25],1) == 15");
    }
  }
}
