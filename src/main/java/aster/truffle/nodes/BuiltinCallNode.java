package aster.truffle.nodes;

import aster.truffle.nodes.parallel.ParallelListMapNode;
import aster.truffle.purity.PurityAnalyzer;
import aster.truffle.runtime.Builtins;
import aster.truffle.runtime.ErrorMessages;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.dsl.Bind;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Cached.Exclusive;
import com.oracle.truffle.api.dsl.Idempotent;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * BuiltinCallNode - 内联常用 builtin 函数的优化节点
 *
 * 通过 Truffle DSL @Specialization 实现类型特化，直接内联算术、比较、逻辑、文本、集合运算，
 * 消除 CallTarget 调用开销。仅处理已知的 builtin，其他情况 fallback 到 Builtins.call。
 *
 * Phase 2A：算术运算 add/sub/mul/div/mod、比较运算 eq/lt/gt/lte/gte、
 *           逻辑运算 and/or/not (共 13 个 builtin)
 * Phase 2B Batch 1：文本运算 Text.concat/Text.length (新增 2 个 builtin，累计 15 个)
 * Phase 2B Batch 2：集合运算 List.length/List.append (新增 2 个 builtin，累计 17 个)
 * Phase 3B：高阶函数优化 List.map/List.filter (引入 InvokeNode + DirectCallNode 缓存，累计 19 个)
 */
public abstract class BuiltinCallNode extends AsterExpressionNode {
  @CompilationFinal protected final String builtinName;
  @Children protected final AsterExpressionNode[] argNodes;
  @Child private ParallelListMapNode parallelListMapNode;

  protected BuiltinCallNode(String builtinName, AsterExpressionNode[] argNodes) {
    this.builtinName = builtinName;
    this.argNodes = argNodes;
  }

  /**
   * Guards helper methods - 标记为 @Idempotent 因为结果仅依赖于 @CompilationFinal 字段
   */
  @Idempotent
  protected boolean isAdd() {
    return "add".equals(builtinName);
  }

  @Idempotent
  protected boolean isSub() {
    return "sub".equals(builtinName);
  }

  @Idempotent
  protected boolean isMul() {
    return "mul".equals(builtinName);
  }

  @Idempotent
  protected boolean isDiv() {
    return "div".equals(builtinName);
  }

  @Idempotent
  protected boolean isMod() {
    return "mod".equals(builtinName);
  }

  @Idempotent
  protected boolean isEq() {
    return "eq".equals(builtinName);
  }

  @Idempotent
  protected boolean isLt() {
    return "lt".equals(builtinName);
  }

  @Idempotent
  protected boolean isGt() {
    return "gt".equals(builtinName);
  }

  @Idempotent
  protected boolean isLte() {
    return "lte".equals(builtinName);
  }

  @Idempotent
  protected boolean isGte() {
    return "gte".equals(builtinName);
  }

  @Idempotent
  protected boolean isAnd() {
    return "and".equals(builtinName);
  }

  @Idempotent
  protected boolean isOr() {
    return "or".equals(builtinName);
  }

  @Idempotent
  protected boolean isNot() {
    return "not".equals(builtinName);
  }

  @Idempotent
  protected boolean isTextConcat() {
    return "Text.concat".equals(builtinName);
  }

  @Idempotent
  protected boolean isTextLength() {
    return "Text.length".equals(builtinName);
  }

  @Idempotent
  protected boolean isListLength() {
    return "List.length".equals(builtinName);
  }

  @Idempotent
  protected boolean isListAppend() {
    return "List.append".equals(builtinName);
  }

  @Idempotent
  protected boolean isListMap() {
    return "List.map".equals(builtinName);
  }

  @Idempotent
  protected boolean isListFilter() {
    return "List.filter".equals(builtinName);
  }

  @Idempotent
  protected boolean hasTwoArgs() {
    return argNodes.length == 2;
  }

  /**
   * Phase 3C P1-1: 小列表判断（size <= 10）
   * 用于快速路径守卫，避免小列表使用 InvokeNode 缓存
   */
  protected boolean isSmallList(VirtualFrame frame) {
    if (argNodes.length < 1) {
      return false;
    }
    Object listObj = argNodes[0].executeGeneric(frame);
    return listObj instanceof List<?> list && list.size() <= 10;
  }

  @Idempotent
  protected boolean hasOneArg() {
    return argNodes.length == 1;
  }

  /**
   * 内联 add (int + int)
   */
  @Specialization(guards = {"isAdd()", "hasTwoArgs()"})
  protected int doAddInt(VirtualFrame frame) {
    Profiler.inc("builtin_add_inlined");

    try {
      int a = argNodes[0].executeInt(frame);
      int b = argNodes[1].executeInt(frame);
      return a + b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Integer) {
        return (int) result;
      }
      throw new RuntimeException(
          "Builtin 'add' expected int result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 sub (int - int)
   */
  @Specialization(guards = {"isSub()", "hasTwoArgs()"})
  protected int doSubInt(VirtualFrame frame) {
    Profiler.inc("builtin_sub_inlined");

    try {
      int a = argNodes[0].executeInt(frame);
      int b = argNodes[1].executeInt(frame);
      return a - b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Integer) {
        return (int) result;
      }
      throw new RuntimeException(
          "Builtin 'sub' expected int result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 mul (int * int)
   */
  @Specialization(guards = {"isMul()", "hasTwoArgs()"})
  protected int doMulInt(VirtualFrame frame) {
    Profiler.inc("builtin_mul_inlined");

    try {
      int a = argNodes[0].executeInt(frame);
      int b = argNodes[1].executeInt(frame);
      return a * b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Integer) {
        return (int) result;
      }
      throw new RuntimeException(
          "Builtin 'mul' expected int result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 div (int / int)，检查除数为零
   */
  @Specialization(guards = {"isDiv()", "hasTwoArgs()"})
  protected int doDivInt(VirtualFrame frame) {
    Profiler.inc("builtin_div_inlined");

    try {
      int a = argNodes[0].executeInt(frame);
      int b = argNodes[1].executeInt(frame);
      return a / b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Integer) {
        return (int) result;
      }
      throw new RuntimeException(
          "Builtin 'div' expected int result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 mod (int % int)，检查除数为零
   */
  @Specialization(guards = {"isMod()", "hasTwoArgs()"})
  protected int doModInt(VirtualFrame frame) {
    Profiler.inc("builtin_mod_inlined");

    try {
      int a = argNodes[0].executeInt(frame);
      int b = argNodes[1].executeInt(frame);
      return a % b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Integer) {
        return (int) result;
      }
      throw new RuntimeException(
          "Builtin 'mod' expected int result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 eq (int == int)
   */
  @Specialization(guards = {"isEq()", "hasTwoArgs()"})
  protected boolean doEqInt(VirtualFrame frame) {
    Profiler.inc("builtin_eq_inlined");

    try {
      int a = argNodes[0].executeInt(frame);
      int b = argNodes[1].executeInt(frame);
      return a == b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Boolean) {
        return (boolean) result;
      }
      throw new RuntimeException(
          "Builtin 'eq' expected boolean result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 lt (int < int)
   */
  @Specialization(guards = {"isLt()", "hasTwoArgs()"})
  protected boolean doLtInt(VirtualFrame frame) {
    Profiler.inc("builtin_lt_inlined");

    try {
      int a = argNodes[0].executeInt(frame);
      int b = argNodes[1].executeInt(frame);
      return a < b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Boolean) {
        return (boolean) result;
      }
      throw new RuntimeException(
          "Builtin 'lt' expected boolean result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 gt (int > int)
   */
  @Specialization(guards = {"isGt()", "hasTwoArgs()"})
  protected boolean doGtInt(VirtualFrame frame) {
    Profiler.inc("builtin_gt_inlined");

    try {
      int a = argNodes[0].executeInt(frame);
      int b = argNodes[1].executeInt(frame);
      return a > b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Boolean) {
        return (boolean) result;
      }
      throw new RuntimeException(
          "Builtin 'gt' expected boolean result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 lte (int <= int)
   */
  @Specialization(guards = {"isLte()", "hasTwoArgs()"})
  protected boolean doLteInt(VirtualFrame frame) {
    Profiler.inc("builtin_lte_inlined");

    try {
      int a = argNodes[0].executeInt(frame);
      int b = argNodes[1].executeInt(frame);
      return a <= b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Boolean) {
        return (boolean) result;
      }
      throw new RuntimeException(
          "Builtin 'lte' expected boolean result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 gte (int >= int)
   */
  @Specialization(guards = {"isGte()", "hasTwoArgs()"})
  protected boolean doGteInt(VirtualFrame frame) {
    Profiler.inc("builtin_gte_inlined");

    try {
      int a = argNodes[0].executeInt(frame);
      int b = argNodes[1].executeInt(frame);
      return a >= b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Boolean) {
        return (boolean) result;
      }
      throw new RuntimeException(
          "Builtin 'gte' expected boolean result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 and (boolean && boolean)
   */
  @Specialization(guards = {"isAnd()", "hasTwoArgs()"})
  protected boolean doAndBoolean(VirtualFrame frame) {
    Profiler.inc("builtin_and_inlined");

    try {
      boolean a = argNodes[0].executeBoolean(frame);
      boolean b = argNodes[1].executeBoolean(frame);
      return a && b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Boolean) {
        return (boolean) result;
      }
      throw new RuntimeException(
          "Builtin 'and' expected boolean result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 or (boolean || boolean)
   */
  @Specialization(guards = {"isOr()", "hasTwoArgs()"})
  protected boolean doOrBoolean(VirtualFrame frame) {
    Profiler.inc("builtin_or_inlined");

    try {
      boolean a = argNodes[0].executeBoolean(frame);
      boolean b = argNodes[1].executeBoolean(frame);
      return a || b;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object arg1 = argNodes[1].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0, arg1});
      if (result instanceof Boolean) {
        return (boolean) result;
      }
      throw new RuntimeException(
          "Builtin 'or' expected boolean result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 not (!boolean)
   * 注意：not 只需要 1 个参数
   */
  @Specialization(guards = {"isNot()", "hasOneArg()"})
  protected boolean doNotBoolean(VirtualFrame frame) {
    Profiler.inc("builtin_not_inlined");

    try {
      boolean a = argNodes[0].executeBoolean(frame);
      return !a;
    } catch (Exception e) {
      // Fallback 到通用路径
      Object arg0 = argNodes[0].executeGeneric(frame);
      Object result = Builtins.call(builtinName, new Object[]{arg0});
      if (result instanceof Boolean) {
        return (boolean) result;
      }
      throw new RuntimeException(
          "Builtin 'not' expected boolean result, got: " +
          (result == null ? "null" : result.getClass().getSimpleName()));
    }
  }

  /**
   * 内联 Text.concat (String + String)
   * 快速路径: 两个参数都是 String，直接拼接
   * Fallback: UnexpectedResultException 时回退到 Builtins.call（支持 String.valueOf）
   */
  @Specialization(guards = {"isTextConcat()", "hasTwoArgs()"})
  protected String doTextConcat(VirtualFrame frame) {
    Profiler.inc("builtin_text_concat_inlined");

    try {
      String left = argNodes[0].executeString(frame);
      String right = argNodes[1].executeString(frame);
      return left + right;
    } catch (Exception e) {
      // Fallback 到通用路径
      return (String) doGeneric(frame);
    }
  }

  /**
   * 内联 Text.length (String.length())
   * 快速路径: 参数是 String，直接返回长度
   * Fallback: UnexpectedResultException 时回退到 Builtins.call（支持 String.valueOf）
   */
  @Specialization(guards = {"isTextLength()", "hasOneArg()"})
  protected int doTextLength(VirtualFrame frame) {
    Profiler.inc("builtin_text_length_inlined");

    try {
      String text = argNodes[0].executeString(frame);
      return text.length();
    } catch (Exception e) {
      // Fallback 到通用路径
      return (int) doGeneric(frame);
    }
  }

  /**
   * 内联 List.length (List.size())
   * 无类型特化执行方法，使用 executeGeneric() + instanceof 检查
   * 类型不匹配时直接抛出 RuntimeException（保持异常透明性）
   */
  @Specialization(guards = {"isListLength()", "hasOneArg()"})
  protected int doListLength(VirtualFrame frame) {
    Profiler.inc("builtin_list_length_inlined");

    Object list = argNodes[0].executeGeneric(frame);
    if (list instanceof List<?> l) {
      return l.size();
    }

    throw new RuntimeException(
      ErrorMessages.operationExpectedType("List.length", "List",
        list == null ? "null" : list.getClass().getSimpleName())
    );
  }

  /**
   * 内联 List.append (list + element)
   * 使用 executeGeneric() + instanceof 模式，涉及对象分配 (new ArrayList)
   * 类型不匹配时直接抛出 RuntimeException（保持异常透明性）
   */
  @Specialization(guards = {"isListAppend()", "hasTwoArgs()"})
  @SuppressWarnings("unchecked")
  protected List<Object> doListAppend(VirtualFrame frame) {
    Profiler.inc("builtin_list_append_inlined");

    Object listObj = argNodes[0].executeGeneric(frame);
    Object element = argNodes[1].executeGeneric(frame);

    if (listObj instanceof List<?>) {
      List<Object> mutable = new ArrayList<>((List<Object>)listObj);
      mutable.add(element);
      return mutable;
    }

    throw new RuntimeException(
      ErrorMessages.operationExpectedType("List.append", "List",
        listObj == null ? "null" : listObj.getClass().getSimpleName())
    );
  }

  /**
   * 小列表快速路径：List.map (list, lambda) - Phase 3C P1-1
   *
   * 当列表大小 <= 10 时使用简化逻辑，直接调用 CallTarget.call()
   * 避免 InvokeNode 缓存开销，修复小列表 JIT 性能回归。
   *
   * 守卫优先级：此特化必须在通用 doListMap 之前，确保小列表优先匹配快速路径。
   *
   * @param frame VirtualFrame 提供执行上下文
   * @return 映射后的列表
   */
  @Specialization(guards = {"isListMap()", "hasTwoArgs()", "isSmallList(frame)"})
  protected List<Object> doListMapSmall(VirtualFrame frame) {
    Profiler.inc("builtin_list_map_small");

    // 执行参数节点
    Object listObj = argNodes[0].executeGeneric(frame);
    Object lambdaObj = argNodes[1].executeGeneric(frame);

    // 类型检查
    if (!(listObj instanceof List<?> list)) {
      throw new RuntimeException(
        ErrorMessages.operationExpectedType("List.map", "List",
          listObj == null ? "null" : listObj.getClass().getSimpleName())
      );
    }

    if (!(lambdaObj instanceof LambdaValue lambda)) {
      throw new RuntimeException(
        "List.map expects lambda as second argument, got: " +
        (lambdaObj == null ? "null" : lambdaObj.getClass().getSimpleName())
      );
    }

    // 循环外提取不变量
    CallTarget callTarget = lambda.getCallTarget();
    if (callTarget == null) {
      throw new RuntimeException("List.map: lambda has no call target");
    }

    Object[] capturedValues = lambda.getCapturedValues();

    // 小列表快速路径：直接调用 CallTarget.call()，无缓存开销
    // Phase 3C P1-2: 循环外预分配参数数组，消除每次迭代分配开销
    List<Object> result = new ArrayList<>(list.size());
    Object[] packedArgs = new Object[1 + capturedValues.length];
    for (Object item : list) {
      packedArgs[0] = item;
      System.arraycopy(capturedValues, 0, packedArgs, 1, capturedValues.length);

      // 直接调用，避免 InvokeNode 缓存查找开销
      Object mapped = callTarget.call(packedArgs);
      result.add(mapped);
    }

    return result;
  }

  /**
   * 内联 List.map (list, lambda) - Phase 3B 性能优化 + Phase 3C P1-1 小列表排除
   *
   * 使用 @Cached InvokeNode 替代裸 CallTarget.call()，引入 DirectCallNode 缓存机制，
   * 消除 76% CallTarget 间接调用开销。循环外提取 capturedValues 消除 8% 重复读取开销。
   *
   * Phase 3C P1-1: 现在仅处理大列表（size > 10），小列表由 doListMapSmall 处理
   *
   * 优化目标：大列表 (1000 元素) 从 1.309ms 降至 <0.6ms (2-3x 性能提升)
   *
   * @param frame VirtualFrame 提供执行上下文
   * @param node Node 绑定当前节点，传递给 InvokeNode
   * @param invokeNode InvokeNode 提供 DirectCallNode 缓存 (limit=3 单态缓存)
   * @return 映射后的列表
   */
  @SuppressWarnings({"truffle-static-method", "truffle-unused", "truffle-sharing"})
  @Specialization(guards = {"isListMap()", "hasTwoArgs()", "!isSmallList(frame)"})
  protected List<Object> doListMap(
      VirtualFrame frame,
      @Bind("$node") Node node,
      @Cached(inline = true) InvokeNode invokeNode) {

    Profiler.inc("builtin_list_map_node");

    // 执行参数节点
    Object listObj = argNodes[0].executeGeneric(frame);
    Object lambdaObj = argNodes[1].executeGeneric(frame);

    // 类型检查
    if (!(listObj instanceof List<?> list)) {
      throw new RuntimeException(
        ErrorMessages.operationExpectedType("List.map", "List",
          listObj == null ? "null" : listObj.getClass().getSimpleName())
      );
    }

    if (!(lambdaObj instanceof LambdaValue lambda)) {
      throw new RuntimeException(
        "List.map expects lambda as second argument, got: " +
        (lambdaObj == null ? "null" : lambdaObj.getClass().getSimpleName())
      );
    }

    // 循环外提取不变量，消除重复读取开销
    CallTarget callTarget = lambda.getCallTarget();
    if (callTarget == null) {
      throw new RuntimeException("List.map: lambda has no call target");
    }

    boolean pureLambda = PurityAnalyzer.isPure(callTarget);
    ParallelListMapNode parallelNode = getParallelListMapNode();
    if (pureLambda && parallelNode.shouldParallelize(list.size())) {
      return parallelNode.execute(list, lambda);
    }

    Object[] capturedValues = lambda.getCapturedValues();

    // Map 循环：每次迭代使用 InvokeNode 执行 lambda，享受 DirectCallNode 缓存
    List<Object> result = new ArrayList<>(list.size());
    for (Object item : list) {
      // 参数打包顺序：[item, ...captures]，与 CallNode.java:63-68 一致
      Object[] packedArgs = new Object[1 + capturedValues.length];
      packedArgs[0] = item;
      System.arraycopy(capturedValues, 0, packedArgs, 1, capturedValues.length);

      // 通过 InvokeNode 调用，触发 DirectCallNode 单态缓存 (limit=3)
      Object mapped = invokeNode.execute(node, callTarget, packedArgs);
      result.add(mapped);
    }

    return result;
  }

  /**
   * 小列表快速路径：List.filter (list, predicate) - Phase 3C P1-1
   *
   * 当列表大小 <= 10 时使用简化逻辑，直接调用 CallTarget.call()
   * 避免 InvokeNode 缓存开销，修复小列表 JIT 性能回归。
   *
   * 守卫优先级：此特化必须在通用 doListFilter 之前，确保小列表优先匹配快速路径。
   *
   * @param frame VirtualFrame 提供执行上下文
   * @return 过滤后的列表
   */
  @Specialization(guards = {"isListFilter()", "hasTwoArgs()", "isSmallList(frame)"})
  protected List<Object> doListFilterSmall(VirtualFrame frame) {
    Profiler.inc("builtin_list_filter_small");

    // 执行参数节点
    Object listObj = argNodes[0].executeGeneric(frame);
    Object predicateObj = argNodes[1].executeGeneric(frame);

    // 类型检查
    if (!(listObj instanceof List<?> list)) {
      throw new RuntimeException(
        ErrorMessages.operationExpectedType("List.filter", "List",
          listObj == null ? "null" : listObj.getClass().getSimpleName())
      );
    }

    if (!(predicateObj instanceof LambdaValue predicate)) {
      throw new RuntimeException(
        "List.filter expects lambda as second argument, got: " +
        (predicateObj == null ? "null" : predicateObj.getClass().getSimpleName())
      );
    }

    // 循环外提取不变量
    CallTarget callTarget = predicate.getCallTarget();
    if (callTarget == null) {
      throw new RuntimeException("List.filter: predicate has no call target");
    }

    Object[] capturedValues = predicate.getCapturedValues();

    // 小列表快速路径：直接调用 CallTarget.call()，无缓存开销
    // Phase 3C P1-2: 循环外预分配参数数组，消除每次迭代分配开销
    List<Object> result = new ArrayList<>();
    Object[] packedArgs = new Object[1 + capturedValues.length];
    for (Object item : list) {
      packedArgs[0] = item;
      System.arraycopy(capturedValues, 0, packedArgs, 1, capturedValues.length);

      // 直接调用，避免 InvokeNode 缓存查找开销
      Object testResult = callTarget.call(packedArgs);
      if (Boolean.TRUE.equals(testResult)) {
        result.add(item);
      }
    }

    return result;
  }

  /**
   * 内联 List.filter (list, predicate) - Phase 3B 性能优化 + Phase 3C P1-1 小列表排除
   *
   * 使用 @Cached InvokeNode 替代裸 CallTarget.call()，引入 DirectCallNode 缓存机制。
   * 循环外提取 capturedValues，循环内执行谓词并仅保留 Boolean.TRUE 的元素。
   *
   * Phase 3C P1-1: 现在仅处理大列表（size > 10），小列表由 doListFilterSmall 处理
   *
   * 优化目标：与 List.map 相同的 DirectCallNode 缓存收益
   *
   * @param frame VirtualFrame 提供执行上下文
   * @param node Node 绑定当前节点，传递给 InvokeNode
   * @param invokeNode InvokeNode 提供 DirectCallNode 缓存 (limit=3 单态缓存)
   * @return 过滤后的列表
   */
  @SuppressWarnings({"truffle-static-method", "truffle-unused", "truffle-sharing"})
  @Specialization(guards = {"isListFilter()", "hasTwoArgs()", "!isSmallList(frame)"})
  protected List<Object> doListFilter(
      VirtualFrame frame,
      @Bind("$node") Node node,
      @Cached(inline = true) InvokeNode invokeNode) {

    Profiler.inc("builtin_list_filter_node");

    // 执行参数节点
    Object listObj = argNodes[0].executeGeneric(frame);
    Object predicateObj = argNodes[1].executeGeneric(frame);

    // 类型检查
    if (!(listObj instanceof List<?> list)) {
      throw new RuntimeException(
        ErrorMessages.operationExpectedType("List.filter", "List",
          listObj == null ? "null" : listObj.getClass().getSimpleName())
      );
    }

    if (!(predicateObj instanceof LambdaValue predicate)) {
      throw new RuntimeException(
        "List.filter expects lambda as second argument, got: " +
        (predicateObj == null ? "null" : predicateObj.getClass().getSimpleName())
      );
    }

    // 循环外提取不变量，消除重复读取开销
    CallTarget callTarget = predicate.getCallTarget();
    if (callTarget == null) {
      throw new RuntimeException("List.filter: lambda has no call target");
    }
    boolean purePredicate = PurityAnalyzer.isPure(callTarget);
    ParallelListMapNode parallelNode = getParallelListMapNode();
    if (purePredicate && parallelNode.shouldParallelize(list.size())) {
      List<Object> predicateResults = parallelNode.execute(list, predicate);
      List<Object> filtered = new ArrayList<>();
      for (int i = 0; i < list.size(); i++) {
        if (Boolean.TRUE.equals(predicateResults.get(i))) {
          filtered.add(list.get(i));
        }
      }
      Profiler.inc("builtin_list_filter_parallel");
      return filtered;
    }

    Object[] capturedValues = predicate.getCapturedValues();

    // Filter 循环：每次迭代使用 InvokeNode 执行谓词，仅保留 Boolean.TRUE 的元素
    List<Object> result = new ArrayList<>();
    for (Object item : list) {
      // 参数打包顺序：[item, ...captures]，与 CallNode.java:63-68 一致
      Object[] packedArgs = new Object[1 + capturedValues.length];
      packedArgs[0] = item;
      System.arraycopy(capturedValues, 0, packedArgs, 1, capturedValues.length);

      // 通过 InvokeNode 调用，触发 DirectCallNode 单态缓存 (limit=3)
      Object predicateResult = invokeNode.execute(node, callTarget, packedArgs);

      // 谓词判断：使用 Boolean.TRUE.equals() 避免 null 或非 Boolean 类型错误
      if (Boolean.TRUE.equals(predicateResult)) {
        result.add(item);
      }
    }

    return result;
  }

  /**
   * Fallback: 调用 Builtins.call（通用路径）
   * 处理所有未内联的 builtin 或类型不匹配的情况
   * Phase 3C P1-1: 添加 doListMapSmall, doListFilterSmall 到 replaces 列表
   */
  @Specialization(replaces = {"doAddInt", "doSubInt", "doMulInt", "doDivInt", "doModInt",
                               "doEqInt", "doLtInt", "doGtInt", "doLteInt", "doGteInt",
                               "doAndBoolean", "doOrBoolean", "doNotBoolean",
                               "doTextConcat", "doTextLength", "doListLength", "doListAppend",
                               "doListMapSmall", "doListMap", "doListFilterSmall", "doListFilter"})
  protected Object doGeneric(VirtualFrame frame) {
    Profiler.inc("builtin_call_generic");

    // 执行参数节点
    Object[] args = new Object[argNodes.length];
    for (int i = 0; i < argNodes.length; i++) {
      args[i] = argNodes[i].executeGeneric(frame);
    }

    // 直接调用 Builtins.call，不包装异常以保持原始错误消息
    return Builtins.call(builtinName, args);
  }

  @Override
  public String toString() {
    return "BuiltinCallNode(" + builtinName + ", " + argNodes.length + " args)";
  }

  private ParallelListMapNode getParallelListMapNode() {
    if (parallelListMapNode == null) {
      CompilerDirectives.transferToInterpreterAndInvalidate();
      parallelListMapNode = insert(ParallelListMapNode.create());
    }
    return parallelListMapNode;
  }
}
