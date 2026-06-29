package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Date.* 合规原语（Stable v1）：epoch-day Int + 纯整数 proleptic Gregorian + 禁 today()。
 * fixture 与 ts date-builtins.test.ts 同源（Codex 设计 session 019f12c6）——epoch-day 值
 * 必须与 TS 引擎逐位一致（双引擎 parity 契约；同 fixture 也进 tier1-parity corpus）。
 */
public class DateBuiltinTest {

  private static int i(Object o) { return ((Number) o).intValue(); }

  @Test void fromISO() {
    assertEquals(0, i(Builtins.call("Date.fromISO", new Object[]{"1970-01-01"})));
    assertEquals(1, i(Builtins.call("Date.fromISO", new Object[]{"1970-01-02"})));
    assertEquals(-1, i(Builtins.call("Date.fromISO", new Object[]{"1969-12-31"})));
    assertEquals(20633, i(Builtins.call("Date.fromISO", new Object[]{"2026-06-29"})));
    assertEquals(11016, i(Builtins.call("Date.fromISO", new Object[]{"2000-02-29"})));
    assertEquals(-719162, i(Builtins.call("Date.fromISO", new Object[]{"0001-01-01"})));
    assertEquals(2932896, i(Builtins.call("Date.fromISO", new Object[]{"9999-12-31"})));
  }

  @Test void leapYear() {
    assertEquals(29, i(Builtins.call("Date.day", new Object[]{Builtins.call("Date.fromISO", new Object[]{"2000-02-29"})})));
    assertEquals(29, i(Builtins.call("Date.day", new Object[]{Builtins.call("Date.fromISO", new Object[]{"2400-02-29"})})));
    assertEquals(29, i(Builtins.call("Date.day", new Object[]{Builtins.call("Date.fromISO", new Object[]{"2024-02-29"})})));
    assertThrows(Exception.class, () -> Builtins.call("Date.fromISO", new Object[]{"1900-02-29"}));
    assertThrows(Exception.class, () -> Builtins.call("Date.fromISO", new Object[]{"2100-02-29"}));
    assertThrows(Exception.class, () -> Builtins.call("Date.fromISO", new Object[]{"2023-02-29"}));
  }

  @Test void daysBetweenAndAddDays() {
    int d1 = i(Builtins.call("Date.fromISO", new Object[]{"1970-01-01"}));
    int d2 = i(Builtins.call("Date.fromISO", new Object[]{"1970-01-02"}));
    assertEquals(0, i(Builtins.call("Date.daysBetween", new Object[]{d1, d1})));
    assertEquals(1, i(Builtins.call("Date.daysBetween", new Object[]{d1, d2})));
    assertEquals(-1, i(Builtins.call("Date.daysBetween", new Object[]{d2, d1})));
    int leapStart = i(Builtins.call("Date.fromISO", new Object[]{"2024-02-28"}));
    int leapEnd = i(Builtins.call("Date.fromISO", new Object[]{"2024-03-01"}));
    assertEquals(2, i(Builtins.call("Date.daysBetween", new Object[]{leapStart, leapEnd})));
    int nonLeapEnd = i(Builtins.call("Date.fromISO", new Object[]{"2023-03-01"}));
    int nonLeapStart = i(Builtins.call("Date.fromISO", new Object[]{"2023-02-28"}));
    assertEquals(1, i(Builtins.call("Date.daysBetween", new Object[]{nonLeapStart, nonLeapEnd})));
    assertEquals(30, i(Builtins.call("Date.daysBetween", new Object[]{d1, Builtins.call("Date.addDays", new Object[]{d1, 30})})));
    // addDays 跨闰日：2024-02-28 + 1 = 02-29
    assertEquals(29, i(Builtins.call("Date.day", new Object[]{Builtins.call("Date.addDays", new Object[]{leapStart, 1})})));
  }

  @Test void extractors() {
    int d = i(Builtins.call("Date.fromISO", new Object[]{"2026-06-29"}));
    assertEquals(2026, i(Builtins.call("Date.year", new Object[]{d})));
    assertEquals(6, i(Builtins.call("Date.month", new Object[]{d})));
    assertEquals(29, i(Builtins.call("Date.day", new Object[]{d})));
    assertEquals(1, i(Builtins.call("Date.year", new Object[]{Builtins.call("Date.fromISO", new Object[]{"0001-01-01"})})));
    assertEquals(12, i(Builtins.call("Date.month", new Object[]{Builtins.call("Date.fromISO", new Object[]{"9999-12-31"})})));
  }

  @Test void strictFormatRejection() {
    String[] bad = {"", "2026-2-03", "2026-02-3", "2026-02-30", "2026-13-01", "2026-00-01",
        "2026-01-00", " 2026-01-01", "2026-01-01 ", "2026-01-01T00:00:00Z", "0000-01-01", "10000-01-01"};
    for (String b : bad) {
      assertThrows(Exception.class, () -> Builtins.call("Date.fromISO", new Object[]{b}), "应拒绝: " + b);
    }
  }
}
