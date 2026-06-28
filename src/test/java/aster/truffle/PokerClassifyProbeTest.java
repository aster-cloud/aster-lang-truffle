package aster.truffle;
import org.junit.jupiter.api.Test;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import static org.junit.jupiter.api.Assertions.*;

// ADR 0024 验收：纯 CNL classify（用 stdlib builtin 调用链判牌型）经 Truffle 求值，
// 与 TS 引擎逐位一致。IR 由 ts 编译器生成（同形喂两引擎）。
class PokerClassifyProbeTest {
  private String classify(String resource) throws Exception {
    var url = getClass().getClassLoader().getResource(resource);
    assertNotNull(url, "resource " + resource);
    String json = Files.readString(Paths.get(url.toURI()), StandardCharsets.UTF_8);
    try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
      Value r = ctx.eval(Source.newBuilder("aster", json, resource).build());
      if (r.canExecute()) r = r.execute(0);
      return r.asString();
    }
  }
  @Test void fourOfAKind() throws Exception { assertEquals("four of a kind", classify("poker-classify/classify-quads.json")); }
  @Test void twoPair()    throws Exception { assertEquals("two pair", classify("poker-classify/classify-twopair.json")); }
  @Test void flush()      throws Exception { assertEquals("flush", classify("poker-classify/classify-flush.json")); }
}
