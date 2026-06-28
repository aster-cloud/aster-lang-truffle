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
    {"name":"probe","decls":[
      {"kind":"Func","name":"main","params":[],"ret":{"kind":"TypeName","name":"Int"},"effects":[],
       "body":{"kind":"Block","statements":[
         {"kind":"Return","expr":
           {"kind":"Call","target":{"kind":"Name","name":"List.length"},"args":[
             {"kind":"ListLit","elements":[
               {"kind":"Int","value":10},{"kind":"Int","value":20},{"kind":"Int","value":30}]}]}}]}}]}
    """;
    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value result = context.eval(Source.newBuilder("aster", json, "probe.json").build());
      assertEquals(3, result.asInt(), "List.length([10,20,30]) == 3");
    }
  }

  @Test void listLiteralGetIndex() throws Exception {
    String json = """
    {"name":"probe2","decls":[
      {"kind":"Func","name":"main","params":[],"ret":{"kind":"TypeName","name":"Int"},"effects":[],
       "body":{"kind":"Block","statements":[
         {"kind":"Return","expr":
           {"kind":"Call","target":{"kind":"Name","name":"List.get"},"args":[
             {"kind":"ListLit","elements":[
               {"kind":"Int","value":5},{"kind":"Int","value":15},{"kind":"Int","value":25}]},
             {"kind":"Int","value":1}]}}]}}]}
    """;
    try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value result = context.eval(Source.newBuilder("aster", json, "probe2.json").build());
      assertEquals(15, result.asInt(), "List.get([5,15,25],1) == 15");
    }
  }
}
