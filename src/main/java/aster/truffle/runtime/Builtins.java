package aster.truffle.runtime;

import aster.truffle.nodes.LambdaValue;
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

  static {
    // === Arithmetic Operations (纯函数，无副作用) ===
    register("add", new BuiltinDef(args -> {
      checkArity("add", args, 2);
      return toInt(args[0]) + toInt(args[1]);
    }));

    register("sub", new BuiltinDef(args -> {
      checkArity("sub", args, 2);
      return toInt(args[0]) - toInt(args[1]);
    }));

    register("mul", new BuiltinDef(args -> {
      checkArity("mul", args, 2);
      return toInt(args[0]) * toInt(args[1]);
    }));

    register("div", new BuiltinDef(args -> {
      checkArity("div", args, 2);
      int divisor = toInt(args[1]);
      if (divisor == 0) throw new BuiltinException(ErrorMessages.arithmeticDivisionByZero());
      return toInt(args[0]) / divisor;
    }));

    register("mod", new BuiltinDef(args -> {
      checkArity("mod", args, 2);
      return toInt(args[0]) % toInt(args[1]);
    }));

    // === Comparison Operations (纯函数) ===
    register("eq", new BuiltinDef(args -> {
      checkArity("eq", args, 2);
      return Objects.equals(args[0], args[1]);
    }));

    register("ne", new BuiltinDef(args -> {
      checkArity("ne", args, 2);
      return !Objects.equals(args[0], args[1]);
    }));

    register("lt", new BuiltinDef(args -> {
      checkArity("lt", args, 2);
      return toInt(args[0]) < toInt(args[1]);
    }));

    register("lte", new BuiltinDef(args -> {
      checkArity("lte", args, 2);
      return toInt(args[0]) <= toInt(args[1]);
    }));

    register("gt", new BuiltinDef(args -> {
      checkArity("gt", args, 2);
      return toInt(args[0]) > toInt(args[1]);
    }));

    register("gte", new BuiltinDef(args -> {
      checkArity("gte", args, 2);
      return toInt(args[0]) >= toInt(args[1]);
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

    register("Text.toUpper", new BuiltinDef(args -> {
      checkArity("Text.toUpper", args, 1);
      return textValue(args[0]).toUpperCase();
    }));

    register("Text.toLower", new BuiltinDef(args -> {
      checkArity("Text.toLower", args, 1);
      return textValue(args[0]).toLowerCase();
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
      if (args[0] instanceof List<?> l) return l.size();
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.length", "List", typeName(args[0])));
    }));

    register("List.get", new BuiltinDef(args -> {
      checkArity("List.get", args, 2);
      if (args[0] instanceof List<?> l) {
        int idx = toInt(args[1]);
        if (idx < 0 || idx >= l.size()) throw new BuiltinException(ErrorMessages.collectionIndexOutOfBounds(idx, l.size()));
        return l.get(idx);
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.get", "List", typeName(args[0])));
    }));

    register("List.append", new BuiltinDef(args -> {
      checkArity("List.append", args, 2);
      if (args[0] instanceof List<?> l) {
        @SuppressWarnings("unchecked")
        List<Object> mutable = new ArrayList<>((List<Object>)l);
        mutable.add(args[1]);
        return mutable;
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.append", "List", typeName(args[0])));
    }));

    register("List.concat", new BuiltinDef(args -> {
      checkArity("List.concat", args, 2);
      if (args[0] instanceof List<?> l1 && args[1] instanceof List<?> l2) {
        @SuppressWarnings("unchecked")
        List<Object> result = new ArrayList<>((List<Object>)l1);
        @SuppressWarnings("unchecked")
        List<Object> l2Cast = (List<Object>)l2;
        result.addAll(l2Cast);
        return result;
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.concat", "List, List", typeName(args[0]) + ", " + typeName(args[1])));
    }));

    register("List.contains", new BuiltinDef(args -> {
      checkArity("List.contains", args, 2);
      if (args[0] instanceof List<?> l) {
        return l.contains(args[1]);
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.contains", "List", typeName(args[0])));
    }));

    register("List.slice", new BuiltinDef(args -> {
      checkArity("List.slice", args, 2, 3);
      if (args[0] instanceof List<?> l) {
        int start = toInt(args[1]);
        int end = args.length == 3 ? toInt(args[2]) : l.size();
        @SuppressWarnings("unchecked")
        List<Object> lCast = (List<Object>)l;
        return new ArrayList<>(lCast.subList(start, end));
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("List.slice", "List", typeName(args[0])));
    }));

    register("List.map", new BuiltinDef(args -> {
      checkArity("List.map", args, 2);
      if (!(args[0] instanceof List<?> l)) {
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
      if (!(args[0] instanceof List<?> l)) {
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
      if (!(args[0] instanceof List<?> l)) {
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

    // === Map Operations (纯函数) ===
    register("Map.empty", new BuiltinDef(args -> {
      checkArity("Map.empty", args, 0);
      return new HashMap<>();
    }));

    register("Map.get", new BuiltinDef(args -> {
      checkArity("Map.get", args, 2);
      if (args[0] instanceof Map<?,?> m) {
        return m.get(args[1]);
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.get", "Map", typeName(args[0])));
    }));

    register("Map.put", new BuiltinDef(args -> {
      checkArity("Map.put", args, 3);
      if (args[0] instanceof Map<?,?> m) {
        @SuppressWarnings("unchecked")
        Map<Object,Object> mutable = new HashMap<>((Map<Object,Object>)m);
        mutable.put(args[1], args[2]);
        return mutable;
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.put", "Map", typeName(args[0])));
    }));

    register("Map.remove", new BuiltinDef(args -> {
      checkArity("Map.remove", args, 2);
      if (args[0] instanceof Map<?,?> m) {
        @SuppressWarnings("unchecked")
        Map<Object,Object> mutable = new HashMap<>((Map<Object,Object>)m);
        mutable.remove(args[1]);
        return mutable;
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.remove", "Map", typeName(args[0])));
    }));

    register("Map.contains", new BuiltinDef(args -> {
      checkArity("Map.contains", args, 2);
      if (args[0] instanceof Map<?,?> m) {
        return m.containsKey(args[1]);
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.contains", "Map", typeName(args[0])));
    }));

    register("Map.keys", new BuiltinDef(args -> {
      checkArity("Map.keys", args, 1);
      if (args[0] instanceof Map<?,?> m) {
        return new ArrayList<>(m.keySet());
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.keys", "Map", typeName(args[0])));
    }));

    register("Map.values", new BuiltinDef(args -> {
      checkArity("Map.values", args, 1);
      if (args[0] instanceof Map<?,?> m) {
        return new ArrayList<>(m.values());
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.values", "Map", typeName(args[0])));
    }));

    register("Map.size", new BuiltinDef(args -> {
      checkArity("Map.size", args, 1);
      if (args[0] instanceof Map<?,?> m) {
        return m.size();
      }
      throw new BuiltinException(ErrorMessages.operationExpectedType("Map.size", "Map", typeName(args[0])));
    }));

    // === Result Operations (纯函数) ===
    register("Result.isOk", new BuiltinDef(args -> {
      checkArity("Result.isOk", args, 1);
      // Check for Map-based Ok
      if (args[0] instanceof Map<?,?> m) {
        return "Ok".equals(m.get("_type"));
      }
      return false;
    }));

    register("Result.isErr", new BuiltinDef(args -> {
      checkArity("Result.isErr", args, 1);
      // Check for Map-based Err
      if (args[0] instanceof Map<?,?> m) {
        return "Err".equals(m.get("_type"));
      }
      return false;
    }));

    register("Result.unwrap", new BuiltinDef(args -> {
      checkArity("Result.unwrap", args, 1);
      // Check for Map-based Ok
      if (args[0] instanceof Map<?,?> m && "Ok".equals(m.get("_type"))) {
        return m.get("value");
      }
      throw new BuiltinException(ErrorMessages.unwrapOnUnexpectedVariant("Result.unwrap", "Err"));
    }));

    register("Result.unwrapErr", new BuiltinDef(args -> {
      checkArity("Result.unwrapErr", args, 1);
      // Check for Map-based Err
      if (args[0] instanceof Map<?,?> m && "Err".equals(m.get("_type"))) {
        return m.get("value");
      }
      throw new BuiltinException(ErrorMessages.unwrapOnUnexpectedVariant("Result.unwrapErr", "Ok"));
    }));

    // === Maybe Operations (纯函数) ===
    register("Maybe.isSome", new BuiltinDef(args -> {
      checkArity("Maybe.isSome", args, 1);
      if (args[0] instanceof Map<?,?> m) {
        return "Some".equals(m.get("_type"));
      }
      return false;
    }));

    register("Maybe.isNone", new BuiltinDef(args -> {
      checkArity("Maybe.isNone", args, 1);
      // Check for Map-based None
      if (args[0] instanceof Map<?,?> m) {
        return "None".equals(m.get("_type"));
      }
      return false;
    }));

    // === Option Operations (alias for Maybe) ===
    register("Option.isSome", new BuiltinDef(args -> {
      checkArity("Option.isSome", args, 1);
      if (args[0] instanceof Map<?,?> m) {
        return "Some".equals(m.get("_type"));
      }
      return false;
    }));

    register("Option.isNone", new BuiltinDef(args -> {
      checkArity("Option.isNone", args, 1);
      // Check for Map-based None
      if (args[0] instanceof Map<?,?> m) {
        return "None".equals(m.get("_type"));
      }
      return false;
    }));

    register("Option.unwrap", new BuiltinDef(args -> {
      checkArity("Option.unwrap", args, 1);
      if (args[0] instanceof Map<?,?> m && "Some".equals(m.get("_type"))) {
        return m.get("value");
      }
      throw new BuiltinException(ErrorMessages.unwrapOnUnexpectedVariant("Option.unwrap", "None"));
    }));

    register("Option.unwrapOr", new BuiltinDef(args -> {
      checkArity("Option.unwrapOr", args, 2);
      if (args[0] instanceof Map<?,?> m && "Some".equals(m.get("_type"))) {
        return m.get("value");
      }
      return args[1]; // default value
    }));

    register("Maybe.unwrap", new BuiltinDef(args -> {
      checkArity("Maybe.unwrap", args, 1);
      if (args[0] instanceof Map<?,?> m && "Some".equals(m.get("_type"))) {
        return m.get("value");
      }
      throw new BuiltinException(ErrorMessages.unwrapOnUnexpectedVariant("Maybe.unwrap", "None"));
    }));

    register("Maybe.unwrapOr", new BuiltinDef(args -> {
      checkArity("Maybe.unwrapOr", args, 2);
      if (args[0] instanceof Map<?,?> m && "Some".equals(m.get("_type"))) {
        return m.get("value");
      }
      return args[1]; // default value
    }));

    // Alias for unwrapOr
    register("Maybe.withDefault", new BuiltinDef(args -> {
      checkArity("Maybe.withDefault", args, 2);
      // Check for Map-based Some
      if (args[0] instanceof Map<?,?> m && "Some".equals(m.get("_type"))) {
        return m.get("value");
      }
      return args[1]; // default value
    }));

    register("Maybe.map", new BuiltinDef(args -> {
      checkArity("Maybe.map", args, 2);

      // If None, return None
      if (args[0] instanceof Map<?,?> m && "None".equals(m.get("_type"))) {
        return java.util.Map.of("_type", "None");
      }

      // If Some, apply function
      if (args[0] instanceof Map<?,?> m && "Some".equals(m.get("_type"))) {
        if (!(args[1] instanceof LambdaValue lambda)) {
          throw new BuiltinException(ErrorMessages.operationExpectedType("Maybe.map", "Lambda", typeName(args[1])));
        }

        CallTarget callTarget = lambda.getCallTarget();
        if (callTarget == null) {
          throw new BuiltinException(ErrorMessages.lambdaMissingCallTarget("Maybe.map"));
        }

        Object value = m.get("value");
        Object[] capturedValues = lambda.getCapturedValues();
        Object[] callArgs = new Object[1 + capturedValues.length];
        callArgs[0] = value;
        System.arraycopy(capturedValues, 0, callArgs, 1, capturedValues.length);

        Object mapped = callTarget.call(callArgs);

        // Return Some(mapped)
        Map<String, Object> result = new HashMap<>();
        result.put("_type", "Some");
        result.put("value", mapped);
        return result;
      }

      throw new BuiltinException(ErrorMessages.operationExpectedType("Maybe.map", "Maybe (Some or None)", typeName(args[0])));
    }));

    register("Result.mapOk", new BuiltinDef(args -> {
      checkArity("Result.mapOk", args, 2);

      // Check for Map-based Err - return unchanged
      if (args[0] instanceof Map<?,?> m && "Err".equals(m.get("_type"))) {
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
      if (args[0] instanceof Map<?,?> m && "Ok".equals(m.get("_type"))) {
        value = m.get("value");
      } else {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Result.mapOk", "Result (Ok or Err)", typeName(args[0])));
      }

      Object[] capturedValues = lambda.getCapturedValues();
      Object[] callArgs = new Object[1 + capturedValues.length];
      callArgs[0] = value;
      System.arraycopy(capturedValues, 0, callArgs, 1, capturedValues.length);

      Object mapped = callTarget.call(callArgs);

      // Return Ok(mapped)
      Map<String, Object> result = new HashMap<>();
      result.put("_type", "Ok");
      result.put("value", mapped);
      return result;
    }));

    register("Result.mapErr", new BuiltinDef(args -> {
      checkArity("Result.mapErr", args, 2);

      // Check for Map-based Ok - return unchanged
      if (args[0] instanceof Map<?,?> m && "Ok".equals(m.get("_type"))) {
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
      if (args[0] instanceof Map<?,?> m && "Err".equals(m.get("_type"))) {
        value = m.get("value");
      } else {
        throw new BuiltinException(ErrorMessages.operationExpectedType("Result.mapErr", "Result (Ok or Err)", typeName(args[0])));
      }

      Object[] capturedValues = lambda.getCapturedValues();
      Object[] callArgs = new Object[1 + capturedValues.length];
      callArgs[0] = value;
      System.arraycopy(capturedValues, 0, callArgs, 1, capturedValues.length);

      Object mapped = callTarget.call(callArgs);

      // Return Err(mapped)
      Map<String, Object> result = new HashMap<>();
      result.put("_type", "Err");
      result.put("value", mapped);
      return result;
    }));

    register("Result.tapError", new BuiltinDef(args -> {
      checkArity("Result.tapError", args, 2);

      // Check for Map-based Ok - return unchanged
      if (args[0] instanceof Map<?,?> m && "Ok".equals(m.get("_type"))) {
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
      if (args[0] instanceof Map<?,?> m && "Err".equals(m.get("_type"))) {
        value = m.get("value");
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
    register("IO.print", new BuiltinDef(args -> {
      checkArity("IO.print", args, 1);
      throw new UnsupportedOperationException(ioNotSupportedMessage("IO.print"));
    }, Set.of("IO")));

    register("IO.readLine", new BuiltinDef(args -> {
      checkArity("IO.readLine", args, 0);
      throw new UnsupportedOperationException(ioNotSupportedMessage("IO.readLine"));
    }, Set.of("IO")));

    register("IO.readFile", new BuiltinDef(args -> {
      checkArity("IO.readFile", args, 1);
      throw new UnsupportedOperationException(ioNotSupportedMessage("IO.readFile"));
    }, Set.of("IO")));

    register("IO.writeFile", new BuiltinDef(args -> {
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
    BuiltinDef def = REGISTRY.get(name);
    if (def == null) return null;
    return def.impl.call(args);
  }

  /**
   * 检查builtin是否存在
   */
  public static boolean has(String name) {
    return REGISTRY.containsKey(name);
  }

  /**
   * 获取builtin函数所需的effects
   * @param name 函数名
   * @return effects集合，如果不存在返回null
   */
  public static Set<String> getEffects(String name) {
    BuiltinDef def = REGISTRY.get(name);
    return def != null ? def.requiredEffects : null;
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

  private static boolean toBool(Object o) {
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

  private static String textValue(Object value) {
    Object inner = unwrap(value);
    return String.valueOf(inner);
  }

  private static Object unwrap(Object value) {
    return AsterPiiValue.unwrap(value);
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
      Object t = m.get("_type");
      if (t instanceof String s) return s;
      return "Map";
    }
    return o.getClass().getSimpleName();
  }
}
