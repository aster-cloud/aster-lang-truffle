package aster.truffle;
import org.junit.jupiter.api.Test;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import static org.junit.jupiter.api.Assertions.*;

// ADR 0024 受控 stdlib：fn 参 builtin（sortBy/maxBy/count/groupBy）端到端经 Truffle
// 求值，验证与 TS 引擎逐位一致。IR 由 ts 编译器生成（同形输入喂两引擎）。
// main 带 seed 参 → eval 返回可执行函数，传 0 调用之。
class StdlibHofProbeTest {
  private int evalMain(String resource) throws Exception {
    var url = getClass().getClassLoader().getResource(resource);
    assertNotNull(url, "resource " + resource);
    String json = Files.readString(Paths.get(url.toURI()), StandardCharsets.UTF_8);
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value r = ctx.eval(Source.newBuilder("aster", json, resource).build());
      if (r.canExecute()) r = r.execute(0);
      return r.asInt();
    }
  }
  @Test void sortBy() throws Exception { assertEquals(8, evalMain("stdlib-hof/hof-sortBy.json")); }
  @Test void maxBy()  throws Exception { assertEquals(8, evalMain("stdlib-hof/hof-maxBy.json")); }
  @Test void count()  throws Exception { assertEquals(3, evalMain("stdlib-hof/hof-count.json")); }
  @Test void groupBy() throws Exception { assertEquals(2, evalMain("stdlib-hof/hof-groupBy.json")); }
}
