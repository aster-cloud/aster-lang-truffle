package aster.truffle.runtime;

import aster.truffle.nodes.LambdaValue;
import aster.truffle.runtime.interop.AsterDecimalValue;
import aster.truffle.runtime.interop.AsterListValue;
import aster.truffle.runtime.interop.AsterMapValue;
import com.oracle.truffle.api.CallTarget;
import java.util.*;

/**
 * Stdlib Builtins Registry - 统一管理所有内置函数
 *
 * 架构重构：
 * - 之前：CallNode硬编码 if-else (lines 37-44)
 * - 现在：注册表 + 函数对象模式
 *
 * 设计原则：
 * - 单一职责：每个Builtin只做一件事
 * - 开放扩展：新增builtin无需修改CallNode
 * - 类型安全：参数校验前置
 */
public final class Builtins {

  /**
   * Builtin函数接口
   */
  @FunctionalInterface
  public interface BuiltinFunction {
    Object call(Object[] args) throws BuiltinException;
  }

  /**
   * Builtin异常
   */
  public static final class BuiltinException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public BuiltinException(String message) { super(message); }
    public BuiltinException(String message, Throwable cause) { super(message, cause); }
  }

  /**
   * Builtin 函数定义 (函数实现 + required effects)
   */
  public static final class BuiltinDef {
    final BuiltinFunction impl;
    final Set<String> requiredEffects;

    public BuiltinDef(BuiltinFunction impl, Set<String> effects) {
      this.impl = impl;
      this.requiredEffects = effects != null ? Set.copyOf(effects) : Set.of();
    }

    // 便利构造器：无 effect 的纯函数
    public BuiltinDef(BuiltinFunction impl) {
      this(impl, Set.of());
    }
  }

  private static final Map<String, BuiltinDef> REGISTRY = new HashMap<>();

  // Date.* epoch-day 边界（0001-01-01 .. 9999-12-31）。声明在 static 注册块之前——
  // 块内 Date.addDays 引用它们，Java 禁止前向引用后声明的 static 字段。
  private static final int DATE_MIN_EPOCH = -719162; // 0001-01-01
  private static final int DATE_MAX_EPOCH = 2932896; // 9999-12-31

  // 红队 P0-B：List.range 生成列表长度上限（防「小标量→巨列表」内存耗尽 DoS）。
  // 与 aster-lang-ts interpreter 的 MAX_RANGE_SIZE 保持一致，维持双引擎 parity。
  private static final long MAX_RANGE_SIZE = 1_000_000L;

  static {
    // === Arithmetic Operations (纯函数，无副作用) ===
    register("add", new BuiltinDef(args -> {
      checkArity("add", args, 2);
      // `+` 双语义，与 TS 解释器一致（interpreter.ts case '+'）：任一操作数
      // 是字符串 → 字符串拼接；否则数值相加。修复前强制 toInt 导致
      // "Hello, " + name 抛 NumberFormatException（双引擎 eval 分歧）。
      Object a = unwrap(args[0]);
      Object b = unwrap(args[1]);
      if (a instanceof String || b instanceof String) {
        return textValue(args[0]) + textValue(args[1]);
      }
      // 数值相加须支持 int+double 提升：div 现为浮点，`subtotal(int) + tax(double)`
      // 若强制 toInt 会丢失小数且与 TS（统一 number）分歧。任一为浮点 → double。
      return numericAdd(args[0], args[1]);
    }));

    register("sub", new BuiltinDef(args -> {
      checkArity("sub", args, 2);
      return numericSub(args[0], args[1]);
    }));

    register("mul", new BuiltinDef(args -> {
      checkArity("mul", args, 2);
      return numericMul(args[0], args[1]);
    }));

    register("div", new BuiltinDef(args -> {
      checkArity("div", args, 2);
      // Decimal（ADR 0025）：`/` 对 Decimal 禁用——除法需显式 scale+rounding mode，
      // 走 Decimal.divide(x, y, scale, mode) builtin（M2）。隐式浮点除法会丢精度。
      if (isDecimal(args[0]) || isDecimal(args[1])) {
        throw new BuiltinException("Decimal division not supported with `/`; use Decimal.divide (ADR 0025)");
      }
      // `/` 为浮点除法，与 TS 解释器一致（interpreter.ts case '/'）：`7 / 2`
      // 返回 3.5，而非整数截断的 3。修复前 toInt(a)/toInt(b) 做整数除法导致
      // 双引擎 eval 分歧。返回 double 即可对齐——CoreIrEvalCli.valueToJson 会
      // 把整除得到的整数值（如 20/4=5.0）经 fitsInInt 收敛回 `5`，
      // 与 TS 的 JSON 序列化（5.0 → 5、3.5 → 3.5）逐位一致。
      double divisor = toDouble(args[1]);
      if (divisor == 0.0) throw new BuiltinException(ErrorMessages.arithmeticDivisionByZero());
      return toDouble(args[0]) / divisor;
    }));

    register("intdiv", new BuiltinDef(args -> {
      checkArity("intdiv", args, 2);
      if (isDecimal(args[0]) || isDecimal(args[1])) {
        throw new BuiltinException("Decimal division not supported with `//`; use Decimal.divide (ADR 0025)");
      }
      // 整除：向零截断，与 TS 解释器一致（interpreter.ts case '//' = Math.trunc）
      // 及 Java/Go/C 语义：`-7 integer divided by 2 = -3`。
      // (#14) 两侧都是整数时直接做 long 除法，避免 double 在 |operand| > 2^53
      // 时丢失精度（如 9007199254740993 // 2 必须得到 4503599627370496，
      // 而非 double 路径四舍五入到 9007199254740992 后的 4503599627370496±）。
      if (!isFractional(args[0]) && !isFractional(args[1])) {
        long divisor = toLong(args[1]);
        if (divisor == 0L) throw new BuiltinException(ErrorMessages.arithmeticDivisionByZero());
        return toLong(args[0]) / divisor; // Java long 除法即向零截断
      }
      double divisor = toDouble(args[1]);
      if (divisor == 0.0) throw new BuiltinException(ErrorMessages.arithmeticDivisionByZero());
      return (long) (toDouble(args[0]) / divisor);
    }));

    register("mod", new BuiltinDef(args -> {
      checkArity("mod", args, 2);
      if (isDecimal(args[0]) || isDecimal(args[1])) {
        throw new BuiltinException("Decimal modulo not supported with `%` (ADR 0025)");
      }
      // 与 TS 的 `%` 一致：任一为浮点 → 浮点取模（Java `%` 对 double 有定义）。
      // #43：除数为 0 抛 guest "division by zero"（与 div/intdiv 一致），不再返回
      // 静默 NaN 或抛裸 ArithmeticException。整数路径提升到 long（match TS 统一
      // number；原 toInt 32 位截断会让 |operand| > 2^31 的取模结果错误）。
      if (isFractional(args[0]) || isFractional(args[1])) {
        double divisor = toDouble(args[1]);
        if (divisor == 0.0) throw new BuiltinException(ErrorMessages.arithmeticDivisionByZero());
        return toDouble(args[0]) % divisor;
      }
      long divisor = toLong(args[1]);
      if (divisor == 0L) throw new BuiltinException(ErrorMessages.arithmeticDivisionByZero());
      return toLong(args[0]) % divisor;
    }));

    // === Comparison Operations (纯函数) ===
    // 数值比较一律按 double 进行，与 TS 统一 number 语义一致：`0.0 == 0` 为真。
    // 修复前 eq 用 Objects.equals 比较 Double(0.0) 与 Integer(0) → false（div 改
    // 浮点后 `remainder equals to 0` 双引擎分歧）；lt/lte/gt/gte 强制 toInt 会丢
    // 小数。仅当两侧都是数值时走数值比较，否则退回结构相等。
    // Decimal（ADR 0025）：比较用 BigDecimal.compareTo（值语义，绕开 equals 的 scale
    // 敏感，使 `1.0m equals to 1.00m` 为真）；Int/Long 精确提升，禁 Double 混算（toDecimal）。
    register("eq", new BuiltinDef(args -> {
      checkArity("eq", args, 2);
      if (isDecimal(args[0]) || isDecimal(args[1])) return toDecimal(args[0]).compareTo(toDecimal(args[1])) == 0;
      if (isNumber(args[0]) && isNumber(args[1])) return toDouble(args[0]) == toDouble(args[1]);
      // ★用 valueEquals 而非 Objects.equals：AsterDataValue / AsterListValue /
      //   AsterMapValue 都没覆写 equals，Objects.equals 会落回**引用相等**——
      //   字段全同的两个结构体判 false，而 TS 侧是结构相等判 true。
      //   实测（tier1 eval 门）：`Point with x set to 1, y set to 2` 两个实例
      //   ts=true / java=false，同一条规则跨引擎决策翻转且不报错。
      //   本仓自身也不自洽：List.contains / List.distinct 一直走 valueEquals
      //   （结构相等），只有 == / != 走 Objects.equals。归一到 valueEquals。
      return valueEquals(args[0], args[1]);
    }));

    register("ne", new BuiltinDef(args -> {
      checkArity("ne", args, 2);
      if (isDecimal(args[0]) || isDecimal(args[1])) return toDecimal(args[0]).compareTo(toDecimal(args[1])) != 0;
      if (isNumber(args[0]) && isNumber(args[1])) return toDouble(args[0]) != toDouble(args[1]);
      // 与 eq 严格互补：必须走同一套语义，否则会出现 a==b 与 a!=b 同时为 true 的荒谬结果。
      return !valueEquals(args[0], args[1]);
    }));

    register("lt", new BuiltinDef(args -> {
      checkArity("lt", args, 2);
      if (isDecimal(args[0]) || isDecimal(args[1])) return toDecimal(args[0]).compareTo(toDecimal(args[1])) < 0;
      return toDouble(args[0]) < toDouble(args[1]);
    }));

    register("lte", new BuiltinDef(args -> {
      checkArity("lte", args, 2);
      if (isDecimal(args[0]) || isDecimal(args[1])) return toDecimal(args[0]).compareTo(toDecimal(args[1])) <= 0;
      return toDouble(args[0]) <= toDouble(args[1]);
    }));

    register("gt", new BuiltinDef(args -> {
      checkArity("gt", args, 2);
      if (isDecimal(args[0]) || isDecimal(args[1])) return toDecimal(args[0]).compareTo(toDecimal(args[1])) > 0;
      return toDouble(args[0]) > toDouble(args[1]);
    }));

    register("gte", new BuiltinDef(args -> {
      checkArity("gte", args, 2);
      if (isDecimal(args[0]) || isDecimal(args[1])) return toDecimal(args[0]).compareTo(toDecimal(args[1])) >= 0;
      return toDouble(args[0]) >= toDouble(args[1]);
    }));

    // === Boolean Operations (纯函数) ===
    register("and", new BuiltinDef(args -> {
      checkArity("and", args, 2);
      return toBool(args[0]) && toBool(args[1]);
    }));

    register("or", new BuiltinDef(args -> {
      checkArity("or", args, 2);
      return toBool(args[0]) || toBool(args[1]);
    }));

    register("not", new BuiltinDef(args -> {
      checkArity("not", args, 1);
      return !toBool(args[0]);
    }));

    // === Text Operations (纯函数) ===
    register("Text.concat", new BuiltinDef(args -> {
      checkArity("Text.concat", args, 2);
      return textValue(args[0]) + textValue(args[1]);
    }));

    // ★2026-08-17 审计：本 builtin 此前在 Truffle **完全缺失**，而 TS 侧三处齐全
    //   （interpreter.ts:771 / jvm/emitter.ts:140 / lsp/analysis.ts:253）。
    //
    //   后果不是「少一个函数」而是双引擎行为分叉：同一段 CNL 在 TS 返回布尔判定，
    //   在 Truffle 走 CallNode 抛 "Unknown call target"。
    //   命中处正是语料库里的**访问控制策略** policy_engine.aster：
    //       Return Text.equals(ownerId, userId).   -- 所有权判断本身
    //       Return Text.equals(role, "admin").     -- 角色判断
    //   该样本标了 evalExempt「只解析从不求值」，所以这条分歧从未在 parity 中暴露。
    //
    //   语义与 TS 逐字对齐：TS 是 `text(x) === text(y)`，其中 text = String(v)；
    //   这里用同仓既有的 textValue()（unwrap PII 后 String.valueOf），
    //   与相邻 Text.* builtin 的强制转换口径完全一致。
    register("Text.equals", new BuiltinDef(args -> {
      checkArity("Text.equals", args, 2);
      return textValue(args[0]).equals(textValue(args[1]));
    }));

    register("Text.toUpper", new BuiltinDef(args -> {
      checkArity("Text.toUpper", args, 1);
      // #43：显式 Locale.ROOT，避免默认 locale（如 tr_TR 的 i→İ）破坏确定性与 TS parity。
      return textValue(args[0]).toUpperCase(java.util.Locale.ROOT);
    }));

    register("Text.toLower", new BuiltinDef(args -> {
      checkArity("Text.toLower", args, 1);
      // #43：显式 Locale.ROOT，避免默认 locale（如 tr_TR 的 I→ı）破坏确定性与 TS parity。
      return textValue(args[0]).toLowerCase(java.util.Locale.ROOT);
    }));

    register("Text.startsWith", new BuiltinDef(args -> {
      checkArity("Text.startsWith", args, 2);
      return textValue(args[0]).startsWith(textValue(args[1]));
    }));

    register("Text.indexOf", new BuiltinDef(args -> {
      checkArity("Text.indexOf", args, 2);
      return textValue(args[0]).indexOf(textValue(args[1]));
    }));

    register("Text.length", new BuiltinDef(args -> {
      checkArity("Text.length", args, 1);
      return textValue(args[0]).length();
    }));

    register("Text.substring", new BuiltinDef(args -> {
      checkArity("Text.substring", args, 2, 3);
      String s = textValue(args[0]);
      int start = toInt(args[1]);
      if (start < 0) {
        throw new BuiltinException(ErrorMessages.stringIndexNegative(start));
      }
      if (args.length == 3) {
        int end = toInt(args[2]);
        if (end < 0) {
          throw new BuiltinException(ErrorMessages.stringIndexNegative(end));
        }
        return s.substring(start, end);
      }
      return s.substring(start);
    }));

    register("Text.trim", new BuiltinDef(args -> {
      checkArity("Text.trim", args, 1);
      return textValue(args[0]).trim();
    }));

    register("Text.split", new BuiltinDef(args -> {
      checkArity("Text.split", args, 2);
      String s = textValue(args[0]);
      String delimiter = textValue(args[1]);
      return Arrays.asList(s.split(java.util.regex.Pattern.quote(delimiter)));
    }));

    register("Text.replace", new BuiltinDef(args -> {
      checkArity("Text.replace", args, 3);
      String s = textValue(args[0]);
      String target = textValue(args[1]);
      String replacement = textValue(args[2]);
      return s.replace(target, replacement);
    }));

    register("Text.contains", new BuiltinDef(args -> {
      checkArity("Text.contains", args, 2);
      String haystack = textValue(args[0]);
      String needle = textValue(args[1]);
      return haystack.contains(needle);
    }));

    register("Text.redact", new BuiltinDef(args -> {
      checkArity("Text.redact", args, 1);
      Object target = args[0];
      if (target instanceof AsterPiiValue pii) {
        return pii.redact();
      }
      return textValue(target);
    }));

    // === PII Helpers ===
    register("PII.unwrap", new BuiltinDef(args -> {
      checkArity("PII.unwrap", args, 1);
      return AsterPiiValue.unwrap(args[0]);
    }));

    // === List Operations (纯函数) ===
    register("List.empty", new BuiltinDef(args -> {
      checkArity("List.empty", args, 0);
      return new ArrayList<>();
    }));

    register("List.length", new BuiltinDef(args -> {
      checkArity("List.length", args, 1);
      List<Object> l = asList(args[0]);
      if (l != null) return l.size();
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.length", "List", typeName(args[0])));
    }));

    register("List.get", new BuiltinDef(args -> {
      checkArity("List.get", args, 2);
      List<Object> l = asList(args[0]);
      if (l != null) {
        int idx = toInt(args[1]);
        if (idx < 0 || idx >= l.size()) throw new BuiltinException(ErrorMessages.collectionIndexOutOfBounds(idx, l.size()));
        return l.get(idx);
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.get", "List", typeName(args[0])));
    }));

    register("List.append", new BuiltinDef(args -> {
      checkArity("List.append", args, 2);
      List<Object> l = asList(args[0]);
      if (l != null) {
        List<Object> mutable = new ArrayList<>(l);
        mutable.add(args[1]);
        return mutable;
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.append", "List", typeName(args[0])));
    }));

    register("List.concat", new BuiltinDef(args -> {
      checkArity("List.concat", args, 2);
      List<Object> l1 = asList(args[0]);
      List<Object> l2 = asList(args[1]);
      if (l1 != null && l2 != null) {
        List<Object> result = new ArrayList<>(l1);
        result.addAll(l2);
        return result;
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.concat", "List, List", typeName(args[0]) + ", " + typeName(args[1])));
    }));

    register("List.contains", new BuiltinDef(args -> {
      checkArity("List.contains", args, 2);
      List<Object> l = asList(args[0]);
      if (l != null) {
        // 值相等而非引用相等：guest 载体的同值元素必须命中（与 TS valueEquals 对齐）。
        for (Object item : l) {
          if (valueEquals(item, args[1])) {
            return true;
          }
        }
        return false;
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.contains", "List", typeName(args[0])));
    }));

    register("List.isEmpty", new BuiltinDef(args -> {
      checkArity("List.isEmpty", args, 1);
      List<Object> l = asList(args[0]);
      if (l != null) return l.isEmpty();
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.isEmpty", "List", typeName(args[0])));
    }));

    register("List.slice", new BuiltinDef(args -> {
      checkArity("List.slice", args, 2, 3);
      List<Object> l = asList(args[0]);
      if (l != null) {
        int start = toInt(args[1]);
        int end = args.length == 3 ? toInt(args[2]) : l.size();
        return new ArrayList<>(l.subList(start, end));
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.slice", "List", typeName(args[0])));
    }));

    register("List.map", new BuiltinDef(args -> {
      checkArity("List.map", args, 2);
      List<Object> l = asList(args[0]);
      if (l == null) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("List.map", "List", typeName(args[0])));
      }
      if (!(args[1] instanceof LambdaValue lambda)) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("List.map", "Lambda", typeName(args[1])));
      }

      CallTarget callTarget = lambda.getCallTarget();
      if (callTarget == null) {
        throw new BuiltinException(ErrorMessages.lambdaMissingCallTarget("List.map"));
      }

      List<Object> result = new ArrayList<>();
      for (Object item : l) {
        // Prepare arguments: [item, ...captures]
        Object[] capturedValues = lambda.getCapturedValues();
        Object[] callArgs = new Object[1 + capturedValues.length];
        callArgs[0] = item;
        System.arraycopy(capturedValues, 0, callArgs, 1, capturedValues.length);

        Object mapped = callTarget.call(callArgs);
        result.add(mapped);
      }
      return result;
    }));

    register("List.filter", new BuiltinDef(args -> {
      checkArity("List.filter", args, 2);
      List<Object> l = asList(args[0]);
      if (l == null) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("List.filter", "List", typeName(args[0])));
      }
      if (!(args[1] instanceof LambdaValue lambda)) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("List.filter", "Lambda", typeName(args[1])));
      }

      CallTarget callTarget = lambda.getCallTarget();
      if (callTarget == null) {
        throw new BuiltinException(ErrorMessages.lambdaMissingCallTarget("List.filter"));
      }

      List<Object> result = new ArrayList<>();
      for (Object item : l) {
        // Prepare arguments: [item, ...captures]
        Object[] capturedValues = lambda.getCapturedValues();
        Object[] callArgs = new Object[1 + capturedValues.length];
        callArgs[0] = item;
        System.arraycopy(capturedValues, 0, callArgs, 1, capturedValues.length);

        Object predicate = callTarget.call(callArgs);
        if (Boolean.TRUE.equals(predicate)) {
          result.add(item);
        }
      }
      return result;
    }));

    register("List.reduce", new BuiltinDef(args -> {
      checkArity("List.reduce", args, 3);
      List<Object> l = asList(args[0]);
      if (l == null) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("List.reduce", "List", typeName(args[0])));
      }
      Object accumulator = args[1]; // initial value
      if (!(args[2] instanceof LambdaValue lambda)) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("List.reduce", "Lambda", typeName(args[2])));
      }

      CallTarget callTarget = lambda.getCallTarget();
      if (callTarget == null) {
        throw new BuiltinException(ErrorMessages.lambdaMissingCallTarget("List.reduce"));
      }

      for (Object item : l) {
        // Prepare arguments: [accumulator, item, ...captures]
        Object[] capturedValues = lambda.getCapturedValues();
        Object[] callArgs = new Object[2 + capturedValues.length];
        callArgs[0] = accumulator;
        callArgs[1] = item;
        System.arraycopy(capturedValues, 0, callArgs, 2, capturedValues.length);

        accumulator = callTarget.call(callArgs);
      }
      return accumulator;
    }));

    // === 通用集合 stdlib（ADR 0024 受控扩展：调现成强函数，不在 CNL 手写算法）===
    // 全部确定性纯函数，双引擎逐位一致（ts interpreter 镜像 + tier1-parity golden）。
    // 数值序：用 toDouble 比较（与算术/比较运算符一致）；排序**稳定、升序**。

    // List.sum(list)：数值求和（空列表 → 0；任一元素浮点 → double）。
    register("List.sum", new BuiltinDef(args -> {
      checkArity("List.sum", args, 1);
      List<Object> l = requireList("List.sum", args[0]);
      Object acc = 0;
      for (Object x : l) acc = numericAdd(acc, x);
      return acc;
    }));

    // List.min / List.max(list)：数值极值（空列表抛错——无意义）。
    register("List.min", new BuiltinDef(args -> {
      checkArity("List.min", args, 1);
      List<Object> l = requireNonEmpty("List.min", args[0]);
      Object best = l.get(0);
      for (Object x : l) if (toDouble(x) < toDouble(best)) best = x;
      return best;
    }));
    register("List.max", new BuiltinDef(args -> {
      checkArity("List.max", args, 1);
      List<Object> l = requireNonEmpty("List.max", args[0]);
      Object best = l.get(0);
      for (Object x : l) if (toDouble(x) > toDouble(best)) best = x;
      return best;
    }));

    // List.distinct(list)：保序去重（按值相等，unwrap 后 equals）。
    register("List.distinct", new BuiltinDef(args -> {
      checkArity("List.distinct", args, 1);
      List<Object> l = requireList("List.distinct", args[0]);
      List<Object> out = new ArrayList<>();
      for (Object x : l) {
        boolean seen = false;
        for (Object y : out) if (valueEquals(x, y)) { seen = true; break; }
        if (!seen) out.add(x);
      }
      return out;
    }));

    // List.range(startInclusive, endExclusive)：[start, end) 整数序列（start>=end → 空）。
    // 红队 P0-B：range 是唯一「从标量凭空造大列表」的 builtin，statementLimit 只数 Truffle
    // 语句不数 native for 循环 → range(0, 2e9) 会 OOM/占死 worker。先按 long 算长度，超上限即抛。
    register("List.range", new BuiltinDef(args -> {
      checkArity("List.range", args, 2);
      int start = toInt(args[0]);
      int end = toInt(args[1]);
      long size = (long) end - (long) start; // long 防 int 溢出（end/start 可为 Integer 极值）
      if (size > MAX_RANGE_SIZE) {
        throw new RuntimeException("List.range: 长度过大（" + size + " > " + MAX_RANGE_SIZE + "），拒绝以防内存耗尽 DoS");
      }
      List<Object> out = new ArrayList<>(size > 0 ? (int) size : 0);
      for (int i = start; i < end; i++) out.add(i);
      return out;
    }));

    // Date.* 合规原语（Stable v1，与 ts interpreter 逐位一致）：内部 epoch-day Int，纯整数
    // proleptic Gregorian（不用 java.time 的时区/locale 路径，手写 Hinnant days-from-civil）。
    // 禁 today()/now()——确定性铁律，"今天"必须作输入字段 evaluation_date。年份 0001-9999。
    register("Date.fromISO", new BuiltinDef(args -> {
      checkArity("Date.fromISO", args, 1);
      return dateFromISO(textValue(args[0]));
    }));
    register("Date.daysBetween", new BuiltinDef(args -> {
      checkArity("Date.daysBetween", args, 2);
      return toInt(args[1]) - toInt(args[0]);
    }));
    register("Date.addDays", new BuiltinDef(args -> {
      checkArity("Date.addDays", args, 2);
      long r = (long) toInt(args[0]) + (long) toInt(args[1]);
      if (r < DATE_MIN_EPOCH || r > DATE_MAX_EPOCH) throw new RuntimeException("Date.OutOfRange: epoch-day " + r);
      return (int) r;
    }));
    register("Date.year", new BuiltinDef(args -> { checkArity("Date.year", args, 1); return dateToCivil(toInt(args[0]))[0]; }));
    register("Date.month", new BuiltinDef(args -> { checkArity("Date.month", args, 1); return dateToCivil(toInt(args[0]))[1]; }));
    register("Date.day", new BuiltinDef(args -> { checkArity("Date.day", args, 1); return dateToCivil(toInt(args[0]))[2]; }));

    // Decimal.* 精确舍入/除法（ADR 0025 M2）。mode 字符串 HALF_UP/HALF_EVEN/DOWN，scale 0..18。
    // 与 TS decimal.js toDecimalPlaces/dividedBy 逐位一致（含 2.5→2 银行家舍入 + canonical 去尾零）。
    register("Decimal.round", new BuiltinDef(args -> {
      checkArity("Decimal.round", args, 3);
      java.math.BigDecimal x = toDecimal(args[0]);
      int scale = decimalScale(args[1]);
      java.math.RoundingMode mode = decimalRoundingMode(args[2]);
      return wrapDecimal(x.setScale(scale, mode));
    }));
    register("Decimal.divide", new BuiltinDef(args -> {
      checkArity("Decimal.divide", args, 4);
      java.math.BigDecimal x = toDecimal(args[0]);
      java.math.BigDecimal y = toDecimal(args[1]);
      if (y.signum() == 0) throw new BuiltinException("Decimal.divide: division by zero.");
      int scale = decimalScale(args[2]);
      java.math.RoundingMode mode = decimalRoundingMode(args[3]);
      return wrapDecimal(x.divide(y, scale, mode));
    }));

    // List.combinations(list, k)：list 的所有 k 元素子集，确定性递增索引字典序
    // （[0,1,2,3,4],[0,1,2,3,5],...）。与 TS interpreter 逐位一致（纯整数索引推进，
    // 不依赖语言细节）。DoS 防护：n≤64 + 结果数 C(n,k)≤上限（先算组合数超限即抛，不先
    // 生成）——多租户沙箱铁律。边界：k<0 抛错；k>n→空；k=0→[[]]。保留元素原值与相对顺序。
    register("List.combinations", new BuiltinDef(args -> {
      checkArity("List.combinations", args, 2);
      List<Object> l = requireList("List.combinations", args[0]);
      int k = toInt(args[1]);
      if (k < 0) throw new RuntimeException("List.combinations: k 须为非负整数，got " + k);
      int n = l.size();
      final int MAX_N = 64, MAX_RESULT = 5000;
      if (n > MAX_N) throw new RuntimeException("List.combinations: 列表过长（" + n + " > " + MAX_N + "），拒绝以防组合爆炸");
      List<Object> out = new ArrayList<>();
      if (k > n) return out;
      // 先算 C(n,k)，超限即抛（不先生成，防 DoS）。
      long count = 1;
      for (int i = 0; i < k; i++) {
        count = count * (n - i) / (i + 1);
        if (count > MAX_RESULT) throw new RuntimeException("List.combinations: 组合数过多（C(" + n + "," + k + ") > " + MAX_RESULT + "）");
      }
      if (k == 0) { out.add(new ArrayList<>()); return out; }
      int[] idx = new int[k];
      for (int i = 0; i < k; i++) idx[i] = i;
      for (;;) {
        List<Object> subset = new ArrayList<>(k);
        for (int i = 0; i < k; i++) subset.add(l.get(idx[i]));
        out.add(subset);
        int i = k - 1;
        while (i >= 0 && idx[i] == n - k + i) i--;
        if (i < 0) break;
        idx[i]++;
        for (int j = i + 1; j < k; j++) idx[j] = idx[j - 1] + 1;
      }
      return out;
    }));

    // List.sort(list)：数值升序、稳定。
    register("List.sort", new BuiltinDef(args -> {
      checkArity("List.sort", args, 1);
      List<Object> l = requireList("List.sort", args[0]);
      List<Object> out = new ArrayList<>(l);
      out.sort((x, y) -> Double.compare(toDouble(x), toDouble(y)));
      return out;
    }));

    // List.count(list, pred)：满足谓词的元素数（pred(item) 返回 Bool）。
    register("List.count", new BuiltinDef(args -> {
      checkArity("List.count", args, 2);
      List<Object> l = requireList("List.count", args[0]);
      LambdaValue pred = requireLambda("List.count", args[1]);
      int n = 0;
      for (Object item : l) if (Boolean.TRUE.equals(callLambda1(pred, item))) n++;
      return n;
    }));

    // List.sortBy(list, keyFn)：按 keyFn(item) 的数值键升序、稳定。
    register("List.sortBy", new BuiltinDef(args -> {
      checkArity("List.sortBy", args, 2);
      List<Object> l = requireList("List.sortBy", args[0]);
      LambdaValue keyFn = requireLambda("List.sortBy", args[1]);
      List<Object> out = new ArrayList<>(l);
      out.sort((x, y) -> Double.compare(toDouble(callLambda1(keyFn, x)), toDouble(callLambda1(keyFn, y))));
      return out;
    }));

    // List.minBy / List.maxBy(list, keyFn)：按 keyFn(item) 数值键取极值元素（空列表抛错）。
    register("List.minBy", new BuiltinDef(args -> {
      checkArity("List.minBy", args, 2);
      List<Object> l = requireNonEmpty("List.minBy", args[0]);
      LambdaValue keyFn = requireLambda("List.minBy", args[1]);
      Object best = l.get(0); double bestK = toDouble(callLambda1(keyFn, best));
      for (Object x : l) { double k = toDouble(callLambda1(keyFn, x)); if (k < bestK) { best = x; bestK = k; } }
      return best;
    }));
    register("List.maxBy", new BuiltinDef(args -> {
      checkArity("List.maxBy", args, 2);
      List<Object> l = requireNonEmpty("List.maxBy", args[0]);
      LambdaValue keyFn = requireLambda("List.maxBy", args[1]);
      Object best = l.get(0); double bestK = toDouble(callLambda1(keyFn, best));
      for (Object x : l) { double k = toDouble(callLambda1(keyFn, x)); if (k > bestK) { best = x; bestK = k; } }
      return best;
    }));

    // List.groupBy(list, keyFn)：按 keyFn(item) 分组 → Map<key文本, List<item>>。
    // key 用 textValue 归一为字符串键（与 Map.* 的字符串键一致，双引擎可比）。保序。
    register("List.groupBy", new BuiltinDef(args -> {
      checkArity("List.groupBy", args, 2);
      List<Object> l = requireList("List.groupBy", args[0]);
      LambdaValue keyFn = requireLambda("List.groupBy", args[1]);
      java.util.LinkedHashMap<String, Object> groups = new java.util.LinkedHashMap<>();
      for (Object item : l) {
        // ★键必须走 mapKey，而不是裸 textValue（#74 第 1 项的补漏）。
        //   groupBy 自己建 LinkedHashMap，绕过了 Map.* 共用的归一化点，于是
        //   「keyFn 返回真 null」与「keyFn 返回字符串 \"null\"」**塌陷到同一个桶**——
        //   实测两个元素被并进一个组（期望 2 组、实得 1 组）。
        //   与 Map.put 一样，null 分组键应响亮失败而非静默合并。
        //   同时 mapKey 还带来整数/浮点键归一，与 Map.* 口径一致。
        String key = String.valueOf(mapKey(callLambda1(keyFn, item)));
        @SuppressWarnings("unchecked")
        List<Object> bucket = (List<Object>) groups.computeIfAbsent(key, k -> new ArrayList<>());
        bucket.add(item);
      }
      return groups;
    }));

    // === Map Operations (纯函数) ===
    // 红队 P2-I：Map 全链用 LinkedHashMap（插入序），使 Map.keys/values 顺序确定且与 TS
    // 引擎逐字节一致（TS 用 JS object，Object.keys/values 返回插入序）。HashMap 是哈希序、
    // 非确定，破坏双引擎 parity 与可复现（Aster 决策必须可回放）。
    register("Map.empty", new BuiltinDef(args -> {
      checkArity("Map.empty", args, 0);
      return new java.util.LinkedHashMap<>();
    }));

    // ★Map.get 返回 Maybe（ADR 0035 档位 C）：缺键即 None，命中即 Some(v)。
    //
    //   此前返回裸 null，于是 `Map.get(m, k) plus 1` 能**编译通过、运行才炸**
    //   （类型检查拦不住 null 参与算术）。对合规决策引擎，这类「规则写出来、
    //   审批过了、上线后才在某条特定输入上炸」是最该消灭的一类。
    //
    //   ★这是对 Stable 集的**语义变更**，明知破坏 spec-1.0-freeze 的
    //   「Stable 1.x 内语义不变」承诺，由用户拍板接受。调用方须改为
    //   `Maybe.withDefault(Map.get(m,k), 兜底)` 或 `Match ... When null`。
    //
    //   命中值本身为 null 时仍返回 Some(null)——「键存在但值为 null」与
    //   「键不存在」是两件事，不能塌陷（Map 本就允许 null 值）。
    register("Map.get", new BuiltinDef(args -> {
      checkArity("Map.get", args, 2);
      Map<String, Object> m = asMap(args[0]);
      if (m != null) {
        Object key = mapKey(args[1]);
        if (!m.containsKey(key)) return maybeNone();
        return maybeSome(m.get(key));
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.get", "Map", typeName(args[0])));
    }));

    register("Map.put", new BuiltinDef(args -> {
      checkArity("Map.put", args, 3);
      // Maybe/Result 变体不是 Map（aster-lang-ts#134）——它们的运行期形态本身是
      // java.util.Map，不拦就会被下面的 instanceof 收下、把 _type/value 当成键来数。
      if (isVariantShaped(args[0])) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Map.put", "Map", typeName(args[0])));
      }
      if (args[0] instanceof Map<?,?> m) {
        // LinkedHashMap 拷贝保插入序；新键追加末尾（与 TS `{...m, [k]:v}` 一致）。
        @SuppressWarnings("unchecked")
        Map<Object,Object> mutable = new java.util.LinkedHashMap<>((Map<Object,Object>)m);
        mutable.put(mapKey(args[1]), args[2]);
        return mutable;
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.put", "Map", typeName(args[0])));
    }));

    register("Map.remove", new BuiltinDef(args -> {
      checkArity("Map.remove", args, 2);
      // Maybe/Result 变体不是 Map（aster-lang-ts#134）——它们的运行期形态本身是
      // java.util.Map，不拦就会被下面的 instanceof 收下、把 _type/value 当成键来数。
      if (isVariantShaped(args[0])) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Map.remove", "Map", typeName(args[0])));
      }
      if (args[0] instanceof Map<?,?> m) {
        @SuppressWarnings("unchecked")
        Map<Object,Object> mutable = new java.util.LinkedHashMap<>((Map<Object,Object>)m);
        mutable.remove(mapKey(args[1]));
        return mutable;
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.remove", "Map", typeName(args[0])));
    }));

    register("Map.contains", new BuiltinDef(args -> {
      checkArity("Map.contains", args, 2);
      Map<String, Object> m = asMap(args[0]);
      if (m != null) {
        return m.containsKey(mapKey(args[1]));
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.contains", "Map", typeName(args[0])));
    }));

    register("Map.keys", new BuiltinDef(args -> {
      checkArity("Map.keys", args, 1);
      // Maybe/Result 变体不是 Map（aster-lang-ts#134）——它们的运行期形态本身是
      // java.util.Map，不拦就会被下面的 instanceof 收下、把 _type/value 当成键来数。
      if (isVariantShaped(args[0])) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Map.keys", "Map", typeName(args[0])));
      }
      if (args[0] instanceof Map<?,?> m) {
        return new ArrayList<>(m.keySet());
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.keys", "Map", typeName(args[0])));
    }));

    register("Map.values", new BuiltinDef(args -> {
      checkArity("Map.values", args, 1);
      // Maybe/Result 变体不是 Map（aster-lang-ts#134）——它们的运行期形态本身是
      // java.util.Map，不拦就会被下面的 instanceof 收下、把 _type/value 当成键来数。
      if (isVariantShaped(args[0])) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Map.values", "Map", typeName(args[0])));
      }
      if (args[0] instanceof Map<?,?> m) {
        return new ArrayList<>(m.values());
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.values", "Map", typeName(args[0])));
    }));

    register("Map.size", new BuiltinDef(args -> {
      checkArity("Map.size", args, 1);
      // Maybe/Result 变体不是 Map（aster-lang-ts#134）——它们的运行期形态本身是
      // java.util.Map，不拦就会被下面的 instanceof 收下、把 _type/value 当成键来数。
      if (isVariantShaped(args[0])) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Map.size", "Map", typeName(args[0])));
      }
      if (args[0] instanceof Map<?,?> m) {
        return m.size();
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.size", "Map", typeName(args[0])));
    }));

    // === Result Operations (纯函数) ===
    register("Result.isOk", new BuiltinDef(args -> {
      checkArity("Result.isOk", args, 1);
      // Check for Map-based Ok
      Map<?, ?> m = asVariantMap(args[0]);
      if (m != null) {
        return "Ok".equals(m.get("__type"));
      }
      return false;
    }));

    register("Result.isErr", new BuiltinDef(args -> {
      checkArity("Result.isErr", args, 1);
      // Check for Map-based Err
      Map<?, ?> m = asVariantMap(args[0]);
      if (m != null) {
        return "Err".equals(m.get("__type"));
      }
      return false;
    }));

    register("Result.unwrap", new BuiltinDef(args -> {
      checkArity("Result.unwrap", args, 1);
      // Check for Map-based Ok
      Map<?, ?> mOk = asVariantMap(args[0]);
      if (mOk != null && "Ok".equals(mOk.get("__type"))) {
        return mOk.get("value");
      }
      throw new BuiltinException(ErrorMessages.unwrapOnUnexpectedVariant("Result.unwrap", "Err"));
    }));

    register("Result.unwrapErr", new BuiltinDef(args -> {
      checkArity("Result.unwrapErr", args, 1);
      // Check for Map-based Err
      Map<?, ?> mErr = asVariantMap(args[0]);
      if (mErr != null && "Err".equals(mErr.get("__type"))) {
        return mErr.get("value");
      }
      throw new BuiltinException(ErrorMessages.unwrapOnUnexpectedVariant("Result.unwrapErr", "Ok"));
    }));

    // === Maybe Operations (纯函数) ===
    register("Maybe.isSome", new BuiltinDef(args -> {
      checkArity("Maybe.isSome", args, 1);
      Map<?, ?> m = asVariantMap(args[0]);
      if (m != null) {
        return "Some".equals(m.get("__type"));
      }
      return false;
    }));

    register("Maybe.isNone", new BuiltinDef(args -> {
      checkArity("Maybe.isNone", args, 1);
      // Check for Map-based None
      Map<?, ?> m = asVariantMap(args[0]);
      if (m != null) {
        return "None".equals(m.get("__type"));
      }
      return false;
    }));

    // === Option Operations (alias for Maybe) ===
    register("Option.isSome", new BuiltinDef(args -> {
      checkArity("Option.isSome", args, 1);
      Map<?, ?> m = asVariantMap(args[0]);
      if (m != null) {
        return "Some".equals(m.get("__type"));
      }
      return false;
    }));

    register("Option.isNone", new BuiltinDef(args -> {
      checkArity("Option.isNone", args, 1);
      // Check for Map-based None
      Map<?, ?> m = asVariantMap(args[0]);
      if (m != null) {
        return "None".equals(m.get("__type"));
      }
      return false;
    }));

    register("Option.unwrap", new BuiltinDef(args -> {
      checkArity("Option.unwrap", args, 1);
      Map<?, ?> mSome = asVariantMap(args[0]);
      if (mSome != null && "Some".equals(mSome.get("__type"))) {
        return mSome.get("value");
      }
      throw new BuiltinException(ErrorMessages.unwrapOnUnexpectedVariant("Option.unwrap", "None"));
    }));

    register("Option.unwrapOr", new BuiltinDef(args -> {
      checkArity("Option.unwrapOr", args, 2);
      Map<?, ?> mSome = asVariantMap(args[0]);
      if (mSome != null && "Some".equals(mSome.get("__type"))) {
        return mSome.get("value");
      }
      return args[1]; // default value
    }));

    register("Maybe.unwrap", new BuiltinDef(args -> {
      checkArity("Maybe.unwrap", args, 1);
      Map<?, ?> mSome = asVariantMap(args[0]);
      if (mSome != null && "Some".equals(mSome.get("__type"))) {
        return mSome.get("value");
      }
      throw new BuiltinException(ErrorMessages.unwrapOnUnexpectedVariant("Maybe.unwrap", "None"));
    }));

    register("Maybe.unwrapOr", new BuiltinDef(args -> {
      checkArity("Maybe.unwrapOr", args, 2);
      Map<?, ?> mSome = asVariantMap(args[0]);
      if (mSome != null && "Some".equals(mSome.get("__type"))) {
        return mSome.get("value");
      }
      return args[1]; // default value
    }));

    // Alias for unwrapOr
    register("Maybe.withDefault", new BuiltinDef(args -> {
      checkArity("Maybe.withDefault", args, 2);
      // Check for Map-based Some
      Map<?, ?> mSome = asVariantMap(args[0]);
      if (mSome != null && "Some".equals(mSome.get("__type"))) {
        return mSome.get("value");
      }
      return args[1]; // default value
    }));

    register("Maybe.map", new BuiltinDef(args -> {
      checkArity("Maybe.map", args, 2);

      // If None, return None
      Map<?, ?> mNone = asVariantMap(args[0]);
      if (mNone != null && "None".equals(mNone.get("__type"))) {
        return java.util.Map.of("__type", "None");
      }

      // If Some, apply function
      Map<?, ?> mSome = asVariantMap(args[0]);
      if (mSome != null && "Some".equals(mSome.get("__type"))) {
        if (!(args[1] instanceof LambdaValue lambda)) {
          throw new BuiltinException(ErrorMessages.operationExpectedType("Maybe.map", "Lambda", typeName(args[1])));
        }

        CallTarget callTarget = lambda.getCallTarget();
        if (callTarget == null) {
          throw new BuiltinException(ErrorMessages.lambdaMissingCallTarget("Maybe.map"));
        }

        Object value = mSome.get("value");
        Object[] capturedValues = lambda.getCapturedValues();
        Object[] callArgs = new Object[1 + capturedValues.length];
        callArgs[0] = value;
        System.arraycopy(capturedValues, 0, callArgs, 1, capturedValues.length);

        Object mapped = callTarget.call(callArgs);

        // Return Some(mapped)。红队 P2-I：LinkedHashMap 固定 _type→value 插入序，
        // 防 HashMap 键序不定破坏 Map.keys 可复现 / 双引擎 parity。
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("__type", "Some");
        result.put("value", mapped);
        return result;
      }

      throw new BuiltinException(ErrorMessages.operationExpectedType("Maybe.map", "Maybe (Some or None)", typeName(args[0])));
    }));

    // Option.map 是 Maybe.map 的别名（aster-lang-ts#122）。
    // ★不复制实现体：TS 侧这两个名字共用同一个 `case`，若此处另写一份，
    // 两个引擎之间、乃至同引擎的两个名字之间都会随时间漂移。直接复用同一个
    // BuiltinDef 实例，行为天然一致。
    // 注：用 Option.map 触发的错误信息里会显示 "Maybe.map"（复用同一个 def），
    // 这是刻意接受的取舍——名字指向真正的实现，好过维护两份会漂移的副本。
    BuiltinDef maybeMap = REGISTRY.get("Maybe.map");
    if (maybeMap == null) {
      // 大声失败：若 Maybe.map 被改名/挪位，这里静默注册 null 会让 Option.map
      // 在调用时才以难以定位的方式炸掉。
      throw new IllegalStateException("Option.map 别名注册失败：Maybe.map 尚未注册");
    }
    register("Option.map", maybeMap);

    register("Result.mapOk", new BuiltinDef(args -> {
      checkArity("Result.mapOk", args, 2);

      // Check for Map-based Err - return unchanged
      Map<?, ?> mErr = asVariantMap(args[0]);
      if (mErr != null && "Err".equals(mErr.get("__type"))) {
        return args[0];
      }

      // Apply function to Ok value
      if (!(args[1] instanceof LambdaValue lambda)) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Result.mapOk", "Lambda", typeName(args[1])));
      }

      CallTarget callTarget = lambda.getCallTarget();
      if (callTarget == null) {
        throw new BuiltinException(ErrorMessages.lambdaMissingCallTarget("Result.mapOk"));
      }

      Object value;
      Map<?, ?> mOk = asVariantMap(args[0]);
      if (mOk != null && "Ok".equals(mOk.get("__type"))) {
        value = mOk.get("value");
      } else {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Result.mapOk", "Result (Ok or Err)", typeName(args[0])));
      }

      Object[] capturedValues = lambda.getCapturedValues();
      Object[] callArgs = new Object[1 + capturedValues.length];
      callArgs[0] = value;
      System.arraycopy(capturedValues, 0, callArgs, 1, capturedValues.length);

      Object mapped = callTarget.call(callArgs);

      // Return Ok(mapped)。红队 P2-I：LinkedHashMap 固定键序（可复现 / parity）。
      Map<String, Object> result = new java.util.LinkedHashMap<>();
      result.put("__type", "Ok");
      result.put("value", mapped);
      return result;
    }));

    register("Result.mapErr", new BuiltinDef(args -> {
      checkArity("Result.mapErr", args, 2);

      // Check for Map-based Ok - return unchanged
      Map<?, ?> mOk = asVariantMap(args[0]);
      if (mOk != null && "Ok".equals(mOk.get("__type"))) {
        return args[0];
      }

      // Apply function to Err value
      if (!(args[1] instanceof LambdaValue lambda)) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Result.mapErr", "Lambda", typeName(args[1])));
      }

      CallTarget callTarget = lambda.getCallTarget();
      if (callTarget == null) {
        throw new BuiltinException(ErrorMessages.lambdaMissingCallTarget("Result.mapErr"));
      }

      Object value;
      Map<?, ?> mErr = asVariantMap(args[0]);
      if (mErr != null && "Err".equals(mErr.get("__type"))) {
        value = mErr.get("value");
      } else {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Result.mapErr", "Result (Ok or Err)", typeName(args[0])));
      }

      Object[] capturedValues = lambda.getCapturedValues();
      Object[] callArgs = new Object[1 + capturedValues.length];
      callArgs[0] = value;
      System.arraycopy(capturedValues, 0, callArgs, 1, capturedValues.length);

      Object mapped = callTarget.call(callArgs);

      // Return Err(mapped)。红队 P2-I：LinkedHashMap 固定键序（可复现 / parity）。
      Map<String, Object> result = new java.util.LinkedHashMap<>();
      result.put("__type", "Err");
      result.put("value", mapped);
      return result;
    }));

    register("Result.tapError", new BuiltinDef(args -> {
      checkArity("Result.tapError", args, 2);

      // Check for Map-based Ok - return unchanged
      Map<?, ?> mOk = asVariantMap(args[0]);
      if (mOk != null && "Ok".equals(mOk.get("__type"))) {
        return args[0];
      }

      // Apply function to Err value for side effects, then return original Err
      if (!(args[1] instanceof LambdaValue lambda)) {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Result.tapError", "Lambda", typeName(args[1])));
      }

      CallTarget callTarget = lambda.getCallTarget();
      if (callTarget == null) {
        throw new BuiltinException(ErrorMessages.lambdaMissingCallTarget("Result.tapError"));
      }

      Object value;
      Map<?, ?> mErr = asVariantMap(args[0]);
      if (mErr != null && "Err".equals(mErr.get("__type"))) {
        value = mErr.get("value");
      } else {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Result.tapError", "Result (Ok or Err)", typeName(args[0])));
      }

      // Call lambda for side effects (discard return value)
      Object[] capturedValues = lambda.getCapturedValues();
      Object[] callArgs = new Object[1 + capturedValues.length];
      callArgs[0] = value;
      System.arraycopy(capturedValues, 0, callArgs, 1, capturedValues.length);
      callTarget.call(callArgs);

      // Return original Err unchanged
      return args[0];
    }));

    // === IO Operations (需要 IO effect) ===
    // 每个 IO.* builtin 先做 effect 校验（IO effect 必须在当前 AsterContext 的允许
    // 列表里，见 AsterContext.isEffectAllowed），未授权时抛出清晰的 guest 错误
    // （"effect not permitted"），让 effect 注解真正生效——而不是无论是否授权都
    // 直接抛 UnsupportedOperationException。授权后，由于 Truffle backend 不实现
    // 真实 IO，再抛 UnsupportedOperationException（effect 校验先于其执行）。
    register("IO.print", new BuiltinDef(args -> {
      requireEffect("IO.print", "IO");
      checkArity("IO.print", args, 1);
      throw new UnsupportedOperationException(ioNotSupportedMessage("IO.print"));
    }, Set.of("IO")));

    register("IO.readLine", new BuiltinDef(args -> {
      requireEffect("IO.readLine", "IO");
      checkArity("IO.readLine", args, 0);
      throw new UnsupportedOperationException(ioNotSupportedMessage("IO.readLine"));
    }, Set.of("IO")));

    register("IO.readFile", new BuiltinDef(args -> {
      requireEffect("IO.readFile", "IO");
      checkArity("IO.readFile", args, 1);
      throw new UnsupportedOperationException(ioNotSupportedMessage("IO.readFile"));
    }, Set.of("IO")));

    register("IO.writeFile", new BuiltinDef(args -> {
      requireEffect("IO.writeFile", "IO");
      checkArity("IO.writeFile", args, 2);
      throw new UnsupportedOperationException(ioNotSupportedMessage("IO.writeFile"));
    }, Set.of("IO")));

    // === Async Operations (需要 Async effect) ===
    register("await", new BuiltinDef(args -> {
      checkArity("await", args, 1);
      // 当前简化实现：直接返回值（与AwaitNode语义一致）
      return args[0];
    }, Set.of("Async")));
  }

  /**
   * 注册builtin函数
   */
  public static void register(String name, BuiltinDef def) {
    REGISTRY.put(name, def);
  }

  /**
   * 调用builtin函数
   * @param name 函数名
   * @param args 参数
   * @return 返回值，如果不存在返回null
   */
  public static Object call(String name, Object[] args) throws BuiltinException {
    BuiltinDef def = REGISTRY.get(canonicalName(name));
    if (def == null) return null;
    return def.impl.call(args);
  }

  /**
   * 检查builtin是否存在
   */
  public static boolean has(String name) {
    return REGISTRY.containsKey(canonicalName(name));
  }

  /**
   * 是否存在以 {@code ns} 为前缀的 builtin 命名空间（如 "Text"、"List"、"Maybe"）。
   *
   * <p>用于把「命名空间函数不存在」与「宿主对象成员访问」区分开（#99）：
   * `Text.nosuch(x)` 应报 Undefined function，而 `someVar.field(x)` 这类
   * 动态调用不该被误拦——判据是首段是否为**已注册的命名空间**。
   *
   * <p>不缓存：注册表在 static 初始化后不再变动，且本方法只在**编译期**
   * （Loader 建树）调用，不在热路径上。
   */
  public static boolean hasNamespace(String ns) {
    if (ns == null || ns.isEmpty()) return false;
    String prefix = ns + ".";
    for (String key : REGISTRY.keySet()) {
      if (key.startsWith(prefix)) return true;
    }
    return false;
  }

  /**
   * 取出已注册的 BuiltinDef 实例本身（不调用它）。
   *
   * <p>仅供测试断言**别名共享同一实例**用：`Option.map` 之类的别名必须复用
   * 被别名者的同一个 def，而不是复制一份实现体——复制出来的副本行为一开始一致、
   * 却会随时间漂移，那正是 TS/JVM 分叉的成因。
   * 只断言"两者都存在且行为相同"锁不住这一点（复制体同样能通过），
   * 必须比较实例同一性。
   *
   * <p>public 是因为测试在 `aster.truffle` 包、本类在 `aster.truffle.runtime`；
   * 同类的 `register` 也是 public。返回的是不可变的函数对象，不泄漏可变状态。
   */
  public static BuiltinDef defOf(String name) {
    return REGISTRY.get(canonicalName(name));
  }

  /**
   * 获取builtin函数所需的effects
   * @param name 函数名
   * @return effects集合，如果不存在返回null
   */
  public static Set<String> getEffects(String name) {
    BuiltinDef def = REGISTRY.get(canonicalName(name));
    return def != null ? def.requiredEffects : null;
  }

  /**
   * 把 parser / Core IR 里的运算符拼写归一化为 runtime builtin 名。
   *
   * <p>TS 引擎把 {@code x plus y} 降级为 {@code Call target Name "+"}（运算符
   * 符号），Java 这边 builtin 注册名是 {@code add}。归一化前，{@code "+"} 在
   * BuiltinCallNode 的 guard 里匹配不到 {@code "add"}，落到 CallNode 的
   * unknown-call 分支返回 null —— 随后 GraalVM 把这个 guest null 结果转成 host
   * Value 时 NPE（"arg2Value is null"）。归一化让这些调用在结果离开 guest
   * 函数前就解析成功。覆盖符号（+ - * / %、比较）与英文拼写两种形式。
   */
  public static String canonicalName(String name) {
    if (name == null) {
      return null;
    }
    String normalized = name.toLowerCase(java.util.Locale.ROOT).replaceAll("\\s+", " ").trim();
    return switch (normalized) {
      case "+", "plus" -> "add";
      case "-", "minus" -> "sub";
      case "*", "times", "multiplied by" -> "mul";
      case "/", "divide", "divided by" -> "div";
      case "//", "integer divide", "integer divided by" -> "intdiv";
      case "%", "modulo" -> "mod";
      case "==", "=", "equals", "equals to", "is" -> "eq";
      case "!=", "not equals", "not equal to" -> "ne";
      case "<", "less than", "under" -> "lt";
      case ">", "greater than", "more than", "over" -> "gt";
      case "<=", "less than or equal to", "at most" -> "lte";
      case ">=", "greater than or equal to", "at least" -> "gte";
      default -> name;
    };
  }

  /** guard 辅助：name 归一化后是否等于给定 canonical builtin 名。 */
  public static boolean isCanonicalName(String name, String canonical) {
    return canonical.equals(canonicalName(name));
  }

  // ===  辅助方法 ===

  private static void checkArity(String name, Object[] args, int expected) {
    if (args.length != expected) {
      throw new BuiltinException(
          ErrorMessages.operationExpectedType(name, expected + " args", args.length + " args"));
    }
  }

  private static void checkArity(String name, Object[] args, int min, int max) {
    if (args.length < min || args.length > max) {
      throw new BuiltinException(
          ErrorMessages.operationExpectedType(name, min + "-" + max + " args", args.length + " args"));
    }
  }

  /**
   * Effect 守卫：在执行带 effect 的 builtin 前，校验该 effect 是否在当前
   * {@link aster.truffle.AsterContext} 的允许列表中。未授权时抛出
   * {@link BuiltinException}（"effect not permitted"），由 CallNode 转为 guest
   * 运行时错误。这样 effect 注解才真正生效，而非到副作用执行时才晚晚失败。
   */
  private static void requireEffect(String operation, String effect) {
    aster.truffle.AsterContext context = aster.truffle.AsterLanguage.getContext();
    if (context == null || !context.isEffectAllowed(effect)) {
      throw new BuiltinException(
          String.format("%s 需要 '%s' effect，但当前未被授权（effect not permitted）", operation, effect));
    }
  }

  private static String ioNotSupportedMessage(String operation) {
    return String.format(
        "IO 操作 '%s' 在 Truffle backend 中不受支持。" +
            "\n\n原因：Truffle backend 专为纯计算任务设计，不提供 I/O 功能。" +
            "\n\n替代方案：" +
            "\n  - 使用 Java backend（支持完整 IO）" +
            "\n  - 使用 TypeScript backend（支持完整 IO）" +
            "\n\n了解更多：请参阅文档 'docs/runtime/backend-comparison.md'",
        operation);
  }

  /**
   * 真值判定的<b>唯一</b>实现（2026-08-17 审计后收敛）。
   *
   * <p>此前 {@code Exec.toBool} 是第二份拷贝，且与本实现在三处不一致：
   * 不 unwrap PII（导致包装的 {@code false} 走 {@code o != null} 恒为真，
   * 静默反转控制流）、把字符串 {@code "false"} 判为假（TS 判真）、
   * {@code toLowerCase()} 未指定 Locale（土耳其 I 问题）。
   *
   * <p>后果是 Truffle **内部**就不自洽：{@code If} 走 Exec 那份，
   * {@code and/or/not} 走本份。现由 Exec 委托到这里，公开可见性即为此。
   *
   * <p>语义与 TS 的 {@code isTruthy} 逐条对齐：
   * null→false · boolean 原值 · number≠0 · string 非空 · 其余 true。
   */
  public static boolean toBool(Object o) {
    Object value = unwrap(o);
    if (value instanceof Boolean b) return b;
    if (value == null) return false;
    if (value instanceof Number n) return n.doubleValue() != 0.0;
    if (value instanceof String s) return !s.isEmpty();
    return true;
  }

  private static int toInt(Object o) {
    Object value = unwrap(o);
    if (value instanceof Number n) return n.intValue();
    if (value instanceof String s) return Integer.parseInt(s);
    throw new BuiltinException(ErrorMessages.typeExpectedGot("Int", typeName(o)));
  }

  // ── Date.* 纯整数 proleptic Gregorian（与 ts interpreter 逐位一致；不用 java.time 路径）。 ──
  // 注：DATE_MIN_EPOCH/DATE_MAX_EPOCH 常量已上移到 static 注册块之前（static 块的 Date.addDays
  // 引用它们，Java 禁止 static 初始化器前向引用后声明的 static 字段）。
  private static boolean isLeapYear(int y) {
    return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
  }

  /** 严格 YYYY-MM-DD 解析 + 闰年/月日校验 → epoch-day。非法抛 Date.InvalidISODate。 */
  private static int dateFromISO(String s) {
    if (s == null || !s.matches("\\d{4}-\\d{2}-\\d{2}")) {
      throw new RuntimeException("Date.InvalidISODate: " + quoteIso(s));
    }
    int y = Integer.parseInt(s.substring(0, 4));
    int mo = Integer.parseInt(s.substring(5, 7));
    int d = Integer.parseInt(s.substring(8, 10));
    if (y < 1 || y > 9999 || mo < 1 || mo > 12 || d < 1) {
      throw new RuntimeException("Date.InvalidISODate: " + quoteIso(s));
    }
    int[] dim = {31, isLeapYear(y) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (d > dim[mo - 1]) throw new RuntimeException("Date.InvalidISODate: " + quoteIso(s));
    // Howard Hinnant days-from-civil（纯整数）。
    int yy = mo <= 2 ? y - 1 : y;
    int era = Math.floorDiv(yy >= 0 ? yy : yy - 399, 400);
    int yoe = yy - era * 400;
    int doy = (153 * (mo + (mo > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
  }

  /** epoch-day → [year, month, day]（civil-from-days，纯整数）。范围外抛 Date.OutOfRange。 */
  private static int[] dateToCivil(int epochDay) {
    if (epochDay < DATE_MIN_EPOCH || epochDay > DATE_MAX_EPOCH) {
      throw new RuntimeException("Date.OutOfRange: epoch-day " + epochDay);
    }
    int z = epochDay + 719468;
    int era = Math.floorDiv(z >= 0 ? z : z - 146096, 146097);
    int doe = z - era * 146097;
    int yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int y = yoe + era * 400;
    int doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    int mp = (5 * doy + 2) / 153;
    int d = doy - (153 * mp + 2) / 5 + 1;
    int m = mp + (mp < 10 ? 3 : -9);
    return new int[]{m <= 2 ? y + 1 : y, m, d};
  }

  private static String quoteIso(String s) {
    return s == null ? "null" : "\"" + s + "\"";
  }

  // === 通用集合 stdlib 辅助（ADR 0024 受控扩展）===

  /** 取列表，非列表抛标准类型错。 */
  private static List<Object> requireList(String op, Object o) {
    List<Object> l = asList(o);
    if (l == null) throw new BuiltinException(ErrorMessages.operationExpectedType(op, "List", typeName(o)));
    return l;
  }

  /** 取非空列表（min/max/minBy/maxBy 对空列表无意义，明确抛错而非返回哨兵）。 */
  private static List<Object> requireNonEmpty(String op, Object o) {
    List<Object> l = requireList(op, o);
    if (l.isEmpty()) throw new BuiltinException(ErrorMessages.operationExpectedType(op, "non-empty List", "empty List"));
    return l;
  }

  /** 取 LambdaValue，非 lambda 抛标准类型错。 */
  private static LambdaValue requireLambda(String op, Object o) {
    if (o instanceof LambdaValue lv) return lv;
    throw new BuiltinException(ErrorMessages.operationExpectedType(op, "Lambda", typeName(o)));
  }

  /** 以单参调用 lambda（拼接 captures），返回结果。复用既有 captures 调用约定。 */
  private static Object callLambda1(LambdaValue lambda, Object arg) {
    com.oracle.truffle.api.CallTarget callTarget = lambda.getCallTarget();
    if (callTarget == null) throw new BuiltinException(ErrorMessages.lambdaMissingCallTarget("lambda"));
    Object[] captured = lambda.getCapturedValues();
    Object[] callArgs = new Object[1 + captured.length];
    callArgs[0] = arg;
    System.arraycopy(captured, 0, callArgs, 1, captured.length);
    return callTarget.call(callArgs);
  }

  private static long toLong(Object o) {
    Object value = unwrap(o);
    if (value instanceof Number n) return n.longValue();
    if (value instanceof String s) return Long.parseLong(s);
    throw new BuiltinException(ErrorMessages.typeExpectedGot("Int", typeName(o)));
  }

  private static double toDouble(Object o) {
    Object value = unwrap(o);
    if (value instanceof Number n) return n.doubleValue();
    if (value instanceof String s) return Double.parseDouble(s);
    throw new BuiltinException(ErrorMessages.typeExpectedGot("Number", typeName(o)));
  }

  /**
   * 数值是否为浮点（Double/Float）。与 TS 统一 number 不同，Java 区分 Int/Double；
   * 只要任一操作数是浮点，算术结果就应保持浮点，避免 int+double 丢失小数。
   */
  private static boolean isFractional(Object o) {
    Object value = unwrap(o);
    return value instanceof Double || value instanceof Float;
  }

  private static boolean isNumber(Object o) {
    return unwrap(o) instanceof Number;
  }

  /**
   * Decimal 一等公民（ADR 0025）：运行时值是 guest {@link AsterDecimalValue}（包装
   * BigDecimal）。沙箱下不能用裸 BigDecimal（非 TruffleObject）。
   */
  private static boolean isDecimal(Object o) {
    return unwrap(o) instanceof AsterDecimalValue;
  }

  /**
   * 把操作数转成 BigDecimal 用于精确运算。AsterDecimalValue → 解包；Int/Long → 精确提升
   * （BigDecimal.valueOf）；Double/Float → 禁止混算（ADR 0025：Double 是二进制浮点，与
   * Decimal 混算会引入误差，破坏可证明性），抛 deterministic error。与 TS interpreter 的
   * toDec 行为逐位一致（TS 用 Number.isInteger 判定）。
   */
  private static java.math.BigDecimal toDecimal(Object o) {
    Object v = unwrap(o);
    if (v instanceof AsterDecimalValue d) return d.decimal();
    if (v instanceof Integer i) return java.math.BigDecimal.valueOf(i.longValue());
    if (v instanceof Long l) return java.math.BigDecimal.valueOf(l);
    if (v instanceof Double || v instanceof Float) {
      throw new BuiltinException("Cannot combine Decimal and Double; convert explicitly (ADR 0025)");
    }
    throw new BuiltinException(ErrorMessages.typeExpectedGot("Decimal", typeName(o)));
  }

  /**
   * 精确运算结果 BigDecimal → guest AsterDecimalValue（canonical 化：去尾零、零归 ZERO，
   * 避免 "-0"/指数）。保证 `1.20m × 1.080m` 两引擎都得 "1.296"，序列化 toPlainString 相同。
   */
  private static AsterDecimalValue wrapDecimal(java.math.BigDecimal d) {
    return AsterDecimalValue.of(d);
  }

  /**
   * 舍入模式字符串 → BigDecimal RoundingMode（ADR 0025 M2）。HALF_UP（远离零）/
   * HALF_EVEN（银行家，向偶）/ DOWN（截断，朝零）。与 TS decimal.js 同名同义逐位一致。
   */
  private static java.math.RoundingMode decimalRoundingMode(Object mode) {
    String m = textValue(mode);
    switch (m) {
      case "HALF_UP": return java.math.RoundingMode.HALF_UP;
      case "HALF_EVEN": return java.math.RoundingMode.HALF_EVEN;
      case "DOWN": return java.math.RoundingMode.DOWN;
      default:
        throw new BuiltinException(
            "Decimal: unknown rounding mode \"" + m + "\"; use \"HALF_UP\", \"HALF_EVEN\" or \"DOWN\".");
    }
  }

  /** scale 参数校验：必须是 0..18 的整数（v1 上限 scale 18，与 ADR 0025 一致）。 */
  /**
   * 校验并取出 Decimal 的 scale——与 TS {@code interpreter.ts} 的 {@code decimalScale} 对齐。
   *
   * <p>此前用 {@code toInt(scale)}，而 {@code toInt} 走 {@code Number.intValue()}：
   * {@code 2.7} 被<b>静默截断</b>成 {@code 2}，于是 {@code Decimal.round(x, 2.7, ...)}
   * 在 Java 上安静地按 2 位舍入，在 TS 上直接报错——错误消息里那句
   * "scale must be an integer" 描述的行为<b>并未真正被强制</b>。
   *
   * <p>对合规引擎而言，舍入精度写错应当<b>响亮地失败</b>，而不是被悄悄改写成另一个精度。
   */
  private static int decimalScale(Object scale) {
    Object value = unwrap(scale);
    double d;
    if (value instanceof Number n) {
      d = n.doubleValue();
    } else if (value instanceof String s) {
      // 保留原有的数字字符串路径（此前走 Integer.parseInt），TS 侧 Number("2") 同样接受。
      //
      // ★但必须先用严格数字正则筛一道（issue #74）：Double.parseDouble 会吃掉
      //   Java 的类型后缀（"2d"/"2f"/"2D"）与 "0x1p3" 之类十六进制浮点，
      //   而 TS 的 Number("2d") 得 NaN → 拒绝。实测此前 Java 把 "2d" 静默接受为 2，
      //   与本方法「响亮失败」的立意相悖，也与 TS 分叉。
      String t = s.trim();
      // ★无歧义写法（非捕获组 + 整数/小数分支不重叠），避免 ReDoS：
      //   原先的 (\d+\.?\d*|\.\d+) 对 "000…0!" 会灾难性回溯——
      //   实测 2 万个 '0' 需 **101 秒**（TS 侧同一模式被 CodeQL 判 high：
      //   js/polynomial-redos）。scale 可由宿主传入，属不可控输入。
      //   改写后语义完全一致，2 万字符降到毫秒级。
      if (!t.matches("[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?")) {
        throw new BuiltinException(
            "Decimal: scale must be an integer in [0, 18], got " + formatScale(value) + ".");
      }
      try {
        d = Double.parseDouble(t);
      } catch (NumberFormatException e) {
        throw new BuiltinException(
            "Decimal: scale must be an integer in [0, 18], got " + formatScale(value) + ".");
      }
    } else {
      throw new BuiltinException(
          "Decimal: scale must be an integer in [0, 18], got " + formatScale(value) + ".");
    }
    if (d != Math.floor(d) || Double.isInfinite(d) || Double.isNaN(d)) {
      throw new BuiltinException(
          "Decimal: scale must be an integer in [0, 18], got " + formatScale(value) + ".");
    }
    if (d < 0 || d > 18) {
      throw new BuiltinException(
          "Decimal: scale must be an integer in [0, 18], got " + formatScale(value) + ".");
    }
    return (int) d;
  }

  /** scale 错误消息里的值渲染：整数值不带 .0，与 TS 的 JSON.stringify(scale) 对齐。 */
  private static String formatScale(Object value) {
    if (value instanceof Double dv && dv == Math.floor(dv) && !dv.isInfinite()) {
      return String.valueOf(dv.longValue());
    }
    if (value instanceof Float fv && fv == Math.floor(fv) && !fv.isInfinite()) {
      return String.valueOf(fv.longValue());
    }
    return String.valueOf(value);
  }

  // 数值算术：任一操作数为浮点 → double 结果；否则 int。结果若为整数值，
  // 由调用方序列化层（CoreIrEvalCli.valueToJson 的 fitsInInt）收敛回 int，
  // 与 TS 的 JSON 序列化逐位一致。
  // Decimal（ADR 0025）：任一操作数是 Decimal → 精确加减乘（不舍入），结果包回
  // AsterDecimalValue。除法/取模对 Decimal 禁用（走 Decimal.divide builtin=M2）。
  // #43：整数算术提升到 long（match TS 统一 number；原 toInt 返回 int，|结果| > 2^31
  // 时静默溢出回绕，与已修为 long 的 intdiv 不一致）。CoreIrEvalCli.valueToJson 的
  // fitsInInt 会把可容纳的 long 值收敛回 int，与 TS JSON 序列化逐位一致。
  private static Object numericAdd(Object a, Object b) {
    if (isDecimal(a) || isDecimal(b)) return wrapDecimal(toDecimal(a).add(toDecimal(b)));
    if (isFractional(a) || isFractional(b)) return toDouble(a) + toDouble(b);
    return toLong(a) + toLong(b);
  }

  private static Object numericSub(Object a, Object b) {
    if (isDecimal(a) || isDecimal(b)) return wrapDecimal(toDecimal(a).subtract(toDecimal(b)));
    if (isFractional(a) || isFractional(b)) return toDouble(a) - toDouble(b);
    return toLong(a) - toLong(b);
  }

  private static Object numericMul(Object a, Object b) {
    if (isDecimal(a) || isDecimal(b)) return wrapDecimal(toDecimal(a).multiply(toDecimal(b)));
    if (isFractional(a) || isFractional(b)) return toDouble(a) * toDouble(b);
    return toLong(a) * toLong(b);
  }

  private static String textValue(Object value) {
    Object inner = unwrap(value);
    return String.valueOf(inner);
  }

  private static Object unwrap(Object value) {
    return hostUnwrap(AsterPiiValue.unwrap(value));
  }

  /**
   * 穿透 GraalVM 的宿主对象包装，取回其中的 Java 值。
   *
   * <p>★为什么需要（aster-lang-ts#138）：宿主经 polyglot 传进来的
   * {@code Map}，到达 builtin 时是 {@code com.oracle.truffle.host.HostObject}
   * 包装，{@code instanceof Map} 为 **false**。于是
   * {@code Maybe.withDefault({__type:"Some",value:7}, 0)} 会落到兜底分支返回
   * {@code 0}——**静默错答案**，而 TS 侧同样输入返回 7。
   *
   * <p>这一层极难自查：{@code Builtins.typeName} 走 InteropLibrary 能穿透、
   * 直接 {@code Builtins.call} 传的是裸 Java 对象、{@code Value.as(Object.class)}
   * 会自动解包——三种常用观察手段都显示"值是对的"，只有在
   * {@code BuiltinCallNode} 实际调用点插桩才能看到 HostObject。
   *
   * <p>用 {@code Env.isHostObject/asHostObject} 而非 {@code instanceof HostObject}：
   * 后者是 {@code com.oracle.truffle.host} 包内类型，不对外暴露。
   *
   * <p>本方法**绝不抛异常**：它位于所有值判定的公共路径上，自己炸会把
   * 「类型判定不准」升级成「任何调用都炸」。识别不了就原样返回。
   */
  /**
   * 取出变体的底层 map：**先穿透宿主包装**，再判定是否为 map 形态。
   *
   * <p>所有 Maybe/Result 的变体判定都必须走这里，不能直接写
   * {@code args[0] instanceof Map<?,?>}——宿主经 polyglot 传进来的 map 是
   * {@code HostObject} 包装，那个 instanceof 恒为 false，会让
   * {@code Maybe.withDefault(Some(7), 0)} 静默返回兜底值 0（aster-lang-ts#138）。
   *
   * <p>与 {@link #isVariantShaped} 共用同一条穿透逻辑：那边**拒绝**变体
   * （{@code Map.*} 不接受 Maybe/Result），这边**接受**变体。二者必须对同一个值
   * 给出一致判断，否则会出现「Map.size 说它不是变体所以当普通 map 收下、
   * Maybe.map 说它是变体所以接受」的两头堵。
   *
   * @return 变体的底层 map；若不是 map 形态则返回 null
   */
  private static Map<?, ?> asVariantMap(Object value) {
    Object v = unwrap(value);
    return v instanceof Map<?, ?> m ? m : null;
  }

  @com.oracle.truffle.api.CompilerDirectives.TruffleBoundary
  private static Object hostUnwrap(Object value) {
    if (value == null) {
      return null;
    }
    try {
      com.oracle.truffle.api.TruffleLanguage.Env env =
          aster.truffle.AsterLanguage.getContext().getEnv();
      if (env != null && env.isHostObject(value)) {
        return env.asHostObject(value);
      }
    } catch (Exception | LinkageError ignored) {
      // 识别失败按原值处理——见上方「绝不抛异常」。
    }
    return value;
  }

  /**
   * 把一个值转成 {@link List}（若可能），否则返回 null。既接受引擎内部产生的原生
   * {@code java.util.List}，也接受 guest 互操作列表 {@link AsterListValue}（例如 CLI/宿主
   * 经 {@link AsterInteropAdapter} 注入的数组）。统一在此处归一，避免每个 List.* builtin
   * 各写一遍类型判断。
   */
  public static List<Object> asList(Object o) {
    Object v = unwrap(o);
    if (v instanceof AsterListValue alv) {
      return alv.elements();
    }
    if (v instanceof List<?> l) {
      @SuppressWarnings("unchecked")
      List<Object> cast = (List<Object>) l;
      return cast;
    }
    return null;
  }

  /**
   * 集合语义的<b>值相等</b>——与 TS {@code interpreter.ts} 的 {@code valueEquals} 对齐。
   *
   * <p>此前 Java 侧用 {@code Objects.equals} / {@code List.contains}，即 Java 的
   * {@code equals}。而 {@code AsterListValue} / {@code AsterMapValue} /
   * {@code AsterDataValue} <b>都没有覆写 equals</b>，落回引用相等。于是同样的值，
   * 换个载体结论就不同：
   *
   * <pre>
   *   List.contains([[1, 2]], [1, 2])   // 原生嵌套 → true；guest 嵌套 → false
   *   List.distinct([[1], [1]])         // TS → [[1]]；Java（guest 载体）→ [[1], [1]]
   * </pre>
   *
   * <p>对合规引擎而言「同一个值去重不掉 / 查不到」会直接改变判定结果，且不报错。
   * TS 侧一直是深度结构相等（数组逐元素、对象逐键、Decimal 按值），故归一到 TS。
   *
   * <p>先 unwrap 再比较：PII 包装与否不应改变值相等性。
   */
  public static boolean valueEquals(Object a, Object b) {
    return valueEquals(a, b, 0);
  }

  /** 递归深度上限：防自引用/环形结构把 StackOverflowError（Error，非 Exception）抛穿 builtin。 */
  private static final int VALUE_EQUALS_MAX_DEPTH = 100;

  private static boolean valueEquals(Object a, Object b, int depth) {
    if (depth > VALUE_EQUALS_MAX_DEPTH) {
      // ★issue #74：实测自引用列表 a=[1,a] 比较会抛 StackOverflowError。
      //   它是 Error 不是 Exception，会穿透 builtin 的异常处理直达调用栈顶。
      //   改为抛域内 BuiltinException，让它像其它 builtin 错误一样被处理。
      throw new BuiltinException(
          "valueEquals: 比较深度超过 " + VALUE_EQUALS_MAX_DEPTH + " 层（疑似环形结构）");
    }
    Object x = unwrap(a);
    Object y = unwrap(b);
    if (x == y) {
      return true;
    }
    if (x == null || y == null) {
      return false;
    }
    // 列表：逐元素递归（原生 List 与 guest AsterListValue 一视同仁）
    List<Object> xl = asList(x);
    List<Object> yl = asList(y);
    if (xl != null && yl != null) {
      if (xl.size() != yl.size()) {
        return false;
      }
      for (int i = 0; i < xl.size(); i++) {
        if (!valueEquals(xl.get(i), yl.get(i), depth + 1)) {
          return false;
        }
      }
      return true;
    }
    if (xl != null || yl != null) {
      return false; // 一侧是列表另一侧不是
    }
    // 映射：逐键递归
    Map<String, Object> xm = asMap(x);
    Map<String, Object> ym = asMap(y);
    if (xm != null && ym != null) {
      if (xm.size() != ym.size()) {
        return false;
      }
      for (Map.Entry<String, Object> e : xm.entrySet()) {
        if (!ym.containsKey(e.getKey())
            || !valueEquals(e.getValue(), ym.get(e.getKey()), depth + 1)) {
          return false;
        }
      }
      return true;
    }
    if (xm != null || ym != null) {
      return false;
    }
    // 结构体：类型名 + 逐字段递归
    if (x instanceof AsterDataValue xd && y instanceof AsterDataValue yd) {
      if (!java.util.Objects.equals(xd.getTypeName(), yd.getTypeName())
          || xd.fieldCount() != yd.fieldCount()) {
        return false;
      }
      for (int i = 0; i < xd.fieldCount(); i++) {
        if (!java.util.Objects.equals(xd.fieldName(i), yd.fieldName(i))
            || !valueEquals(xd.fieldValue(i), yd.fieldValue(i), depth + 1)) {
          return false;
        }
      }
      return true;
    }
    // ★数值按**值**相等，跨 Java 数值类型（2026-08-18 对抗性审查发现）。
    //
    //   TS 只有一种 number，`1 === 1` 恒真；Java 却区分 Integer/Long/Double。
    //   实测可达路径：`List.range` 产 Integer，而算术 `numericAdd` 走 toLong 产 Long
    //   → `List.contains(List.range(0,3), 1+1)` 在 Java 是 **false**、TS 是 true；
    //   `List.distinct` 同理会把同一个 2 留下两份。
    //   这是既有分歧，但把 contains 改用 valueEquals 时若不一并处理，
    //   等于把它原样带进新路径并伪装成「已对齐 TS」。
    //
    //   Decimal 不在此列：它是精确十进制类型，与 Double 混算本就被显式禁止
    //   （见 numericAdd 的 ADR 0025 注释），故仍走 equals 保持类型区分。
    if (x instanceof Number nx && y instanceof Number ny
        && !isDecimal(x) && !isDecimal(y)) {
      if (isFractional(x) || isFractional(y)) {
        return nx.doubleValue() == ny.doubleValue();
      }
      return nx.longValue() == ny.longValue();
    }
    return java.util.Objects.equals(x, y);
  }

  /**
   * Map 键归一化：<b>唯一</b>的键规则，get/put/remove/contains 四处必须共用。
   *
   * <p>Aster 的 Map 键是文本键（{@code asMap} 一直按 {@code Map<String,Object>} 处理，
   * TS 侧则用 JS object / GuestMap，键天然是字符串）。TS 在 get/put/remove/contains
   * 四处<b>一致地</b>做 {@code String(k)}；Java 此前只在 get/contains 归一，
   * put/remove 存的是原始对象——于是非文本键上：
   *
   * <pre>
   *   Let m be Map.put(Map.empty(), 1, "one").
   *   Return Map.get(m, 1).      // TS → "one"；Java → null（键存成 Integer 1，查的是 "1"）
   * </pre>
   *
   * <p>即「刚写进去就读不出来」，且<b>不报错</b>——Map 看上去凭空丢数据。
   * 归一到 TS 的行为（而非反过来让 TS 保留原始键），因为 TS 侧的 GuestMap
   * 底层就是字符串键，改 TS 才是破坏性变更。
   *
   * <p>先 {@code unwrap} 再取字符串：Truffle 有运行期 PII 包装（{@code AsterPiiValue}），
   * TS 侧则没有——PII 在 TS 是纯编译期概念，到 {@code String(k)} 的值本就是裸值。
   * 故不 unwrap 会让包装值按包装器的 toString 成键，反而与 TS 分叉；
   * unwrap 才是与 TS 对齐的那一侧。同时也避免同一逻辑键因包装与否落到两个槽位。
   */
  /** JS 的 Number.MAX_SAFE_INTEGER（2^53）：超出后 double 无法精确表示整数。 */
  private static final double MAX_SAFE_INTEGER = 9007199254740992.0;

  /**
   * 是否为 null 键：宿主裸 null，或 guest 侧 {@code isNull()==true} 的互操作对象。
   *
   * <p>与 {@code MatchNode.isGuestNull} 同源判据——只问「是不是 null」，
   * 不认具体类，故 {@code AsterNullValue} 之外的实现同样被覆盖。
   *
   * <p>本方法**不抛异常**：它在构造错误路径上被调用，自己炸了会把
   * 「拒绝 null 键」变成「报错时又炸一次」。识别失败即视为非 null，交由下游处理。
   */
  @com.oracle.truffle.api.CompilerDirectives.TruffleBoundary
  private static boolean isNullKey(Object k) {
    if (k == null) return true;
    try {
      return com.oracle.truffle.api.interop.InteropLibrary.getUncached().isNull(k);
    } catch (Exception | LinkageError ignored) {
      return false;
    }
  }

  /** 构造 Maybe 的 None —— 形状与 ResultNodes.createResult("None") 一致。 */
  private static Map<String, Object> maybeNone() {
    java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
    m.put("__type", "None");
    return m;
  }

  /** 构造 Maybe 的 Some(v) —— 形状与 ResultNodes.createResult("Some", v) 一致。 */
  private static Map<String, Object> maybeSome(Object value) {
    java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
    m.put("__type", "Some");
    m.put("value", value);
    return m;
  }

  public static Object mapKey(Object key) {
    Object k = unwrap(key);
    // ★null 键显式拒绝（ADR 0035 档位 A / issue #74 第 1 项）。
    //
    //   此前 String.valueOf(null) 得 "null"，于是 Map.put(m, null, a) 与
    //   Map.put(m, "null", b) **塌陷到同一个槽位**——后者静默覆盖前者，
    //   size 仍是 1。这不是「null 泄漏出来」，而是「两个不同的逻辑键被悄悄合并」，
    //   属于**静默丢数据**：调用方拿不到任何提示。
    //
    //   对合规决策引擎，静默丢数据比抛错危险得多，故这里响亮失败。
    //   TS 侧同样塌陷（String(null)==="null"），已同步拒绝以维持双引擎一致。
    //
    //   两种 null 形态都要拦：宿主裸 null，以及 guest 侧 isNull()==true 的
    //   互操作对象（如 AsterNullValue）。判据与 MatchNode.isGuestNull 同源，
    //   只认「是不是 null」而不认具体类，避免漏掉别的 TruffleObject 实现。
    if (isNullKey(k)) {
      throw new BuiltinException(
          "Map key must not be null: null 与字符串 \"null\" 会塌陷到同一个键、"
              + "导致静默覆盖。请改用非 null 的键，或用 Maybe/Option 表达「缺失」。");
    }
    // ★整数值的浮点必须与整数落到同一个键（2026-08-18 对抗性审查发现）。
    //
    //   Java 的 String.valueOf(1.0) = "1.0"，而 JS 的 String(1.0) = "1"。
    //   仅用 String.valueOf 会让 `Map.put(m, 1.0, v)` 存成键 "1.0"、
    //   `Map.get(m, 1)` 查 "1" → **null**，即本次修复声称要消灭的
    //   「刚写进去就读不出来」在浮点键上原样残留。TS 侧因 JS 只有一种 number，
    //   1 与 1.0 天然同键，两侧于是分叉。
    //
    //   故整数值的 Double/Float 先降成 long 再取字符串；非整数值（2.5）、
    //   NaN/Infinity 保持 String.valueOf——后者与 JS 的 "NaN"/"Infinity" 一致。
    //
    //   ★上界取 JS 的 MAX_SAFE_INTEGER（2^53 = 9007199254740992）——复评发现
    //   此前拍的 1e15 保守了 6 个数量级：实测 mapKey(1e15) 得 "1.0E15"，
    //   而 JS String(1e15)="1000000000000000"，于是 [1e15, 2^53] 区间仍与 TS 分叉
    //   （与本 PR 上一轮被退回的理由是同一条，只是分叉点右移）。
    //   超过 2^53 后 double 本就无法精确表示整数，long 转换会失真，故止于此；
    //   JS 直到 1e21 才转科学计数法，[2^53, 1e21) 的格式差异另开 issue 跟踪。
    if (k instanceof Double d) {
      if (!d.isNaN() && !d.isInfinite() && d == Math.rint(d)
          && Math.abs(d) <= MAX_SAFE_INTEGER) {
        return String.valueOf(d.longValue());
      }
    } else if (k instanceof Float f) {
      if (!f.isNaN() && !f.isInfinite() && f == Math.rint(f)
          && Math.abs(f) <= MAX_SAFE_INTEGER) {
        return String.valueOf(f.longValue());
      }
    }
    return String.valueOf(k);
  }

  /**
   * 把一个值转成 {@code Map<String,Object>}（若可能），否则返回 null。既接受原生
   * {@code java.util.Map}，也接受 guest 互操作映射 {@link AsterMapValue}。
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> asMap(Object o) {
    Object v = unwrap(o);
    if (isVariantShaped(v)) {
      return null;
    }
    if (v instanceof AsterMapValue amv) {
      return amv.entries();
    }
    if (v instanceof Map<?, ?> m) {
      return (Map<String, Object>) m;
    }
    return null;
  }

  /**
   * 判断一个值是否是 Maybe/Result 的运行期形态（{@code {_type: Some|None|Ok|Err}}）。
   *
   * <p>★为什么需要（aster-lang-ts#134）：这些变体的运行期表示**本身就是
   * {@code java.util.Map}**（见 {@link #maybeNone()}），于是 {@code Map.*} 的
   * {@code instanceof Map} 会把它们当成普通 Map 收下，把 {@code _type}/{@code value}
   * 当成键来数——{@code Map.size(Some(1))} 返回 2、{@code Map.size(None)} 返回 1。
   *
   * <p>这是**静默错答案**：不报错，而是给出一个看起来完全合理的数字。二维矩阵
   * （6 个 Map.* × 6 种输入）实测确认 18 格受影响；其中 None 那 6 格还与 TS 侧
   * 分叉（TS 拒、此处放行）。
   *
   * <p>Maybe/Result 不是 Map，理应被 {@code Map.*} 拒绝——两引擎同步收紧。
   */
  private static boolean isVariantShaped(Object v) {
    // ★与 asVariantMap 共用同一条穿透逻辑（aster-lang-ts#138）：
    // 宿主经 polyglot 传进来的 map 是 HostObject 包装，裸 instanceof 认不出。
    // 若这里不穿透而 Maybe.* 那边穿透，同一个值会被两处判成不同结论——
    // Map.size 当它是普通 map 收下（回到 #134 修掉的静默错答案）、
    // Maybe.map 当它是变体接受，两头不一致。
    Map<?, ?> m = asVariantMap(v);
    if (m == null) {
      return false;
    }
    Object t = m.get("__type");
    return "Some".equals(t) || "None".equals(t) || "Ok".equals(t) || "Err".equals(t);
  }

  /**
   * 获取对象的类型名称，用于错误消息生成
   *
   * @param o 待检查的对象
   * @return 类型名称字符串
   */
  public static String typeName(Object o) {
    if (o == null) return "null";
    if (o instanceof AsterPiiValue) return "PII";
    if (o instanceof AsterDataValue dataValue) return dataValue.getTypeName();
    if (o instanceof AsterEnumValue enumValue) return enumValue.getQualifiedName();
    if (o instanceof Map<?,?> m) {
      Object t = m.get("__type");
      if (t instanceof String s) return s;
      return "Map";
    }
    // ★宿主 Map 经 Polyglot 传进来是 HostObject 包装，上面的 instanceof Map 抓不到，
    //   于是错误信息会写成「实际 HostObject」——那是 GraalVM 的内部类名，对用户
    //   毫无意义，且把排查方向引向引擎配置。最常见的真因其实是：context 的键名与
    //   规则参数名对不上，整个 map 被当作那一个标量参数传了进来（api#244）。
    //
    //   与 MemberAccessNode.describeAvailableKeys 同一认知：宿主 Map 在 Polyglot 里
    //   暴露为 **hash 条目**而非成员，故必须查 hasHashEntries；只看 hasMembers 会漏。
    String interopName = interopTypeName(o);
    if (interopName != null) return interopName;
    return o.getClass().getSimpleName();
  }

  /**
   * 用 InteropLibrary 判定 guest/宿主对象的**用户可理解**类型名。
   *
   * <p>只在 {@link #typeName} 的常规分支都不命中时兜底。返回 null 表示识别不出，
   * 此时调用方回退到类名。
   *
   * <p>本方法**绝不抛异常**：它只在构造错误消息的路径上被调用，若它自己炸了，
   * 就会把「错误信息不好读」升级成「报错时又炸一次」，比原问题更糟。
   */
  @com.oracle.truffle.api.CompilerDirectives.TruffleBoundary
  private static String interopTypeName(Object o) {
    try {
      com.oracle.truffle.api.interop.InteropLibrary interop =
          com.oracle.truffle.api.interop.InteropLibrary.getUncached(o);
      if (interop.hasHashEntries(o) || interop.hasMembers(o)) return "Map";
      if (interop.hasArrayElements(o)) return "List";
      if (interop.isString(o)) return "Text";
      if (interop.isNumber(o)) return "Number";
      if (interop.isBoolean(o)) return "Bool";
      if (interop.isNull(o)) return "null";
    } catch (Exception | LinkageError ignored) {
      // 识别失败就回退类名——见上方「绝不抛异常」。
    }
    return null;
  }
}
