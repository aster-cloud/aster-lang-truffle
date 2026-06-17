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
import com.oracle.truffle.api.nodes.UnexpectedResultException;

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
  // 算术 add/sub/mul/div/mod 无 int 快路径（统一走 doGeneric → Builtins.*，
  // 已做 int/double 提升 + 字符串拼接），故无 isAdd/isSub/isMul/isDiv/isMod guard。

  @Idempotent
  protected boolean isEq() {
    return Builtins.isCanonicalName(builtinName, "eq");
  }

  @Idempotent
  protected boolean isLt() {
    return Builtins.isCanonicalName(builtinName, "lt");
  }

  @Idempotent
  protected boolean isGt() {
    return Builtins.isCanonicalName(builtinName, "gt");
  }

  @Idempotent
  protected boolean isLte() {
    return Builtins.isCanonicalName(builtinName, "lte");
  }

  @Idempotent
  protected boolean isGte() {
    return Builtins.isCanonicalName(builtinName, "gte");
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
    List<Object> list = Builtins.asList(listObj);
    return list != null && list.size() <= 10;
  }

  @Idempotent
  protected boolean hasOneArg() {
    return argNodes.length == 1;
  }

  // 算术 add/sub/mul/div/mod 不再用 int 快路径（@Specialization 返回 int）。
  // 原因：`/` 改为浮点后会产生 double，int 与 double 混算（如 subtotal(int) +
  // tax(double)）经 int-specialization 的 rewriteOn / executeInt 重特化路径会
  // 错误返回 UnexpectedResultException 携带的单个操作数值（实测 100 - 10.0 → 10）。
  // 统一走 doGeneric → Builtins.*（已做 int/double 数值提升 + 字符串拼接双语义），
  // 由 CoreIrEvalCli.valueToJson 的 fitsInInt 把整数值 double 收敛回 int，
  // 与 TS（统一 number）逐位一致。算术非热点循环，正确性优先于内联。

  // ---------------------------------------------------------------------------
  // 类型特化快速路径（#14 修复）
  //
  // 关键约束：参数节点只能 executeGeneric 一次，绝不能重复求值（参数可能有副作用，
  // 如 `return`、列表 append 等）。同时，参数求值期间抛出的 ControlFlowException
  // （如 ReturnNode.ReturnException，它 extends ControlFlowException extends
  // RuntimeException）必须原样向上传播，绝不能被吞掉。
  //
  // 因此这些快速路径**不再**调用 executeInt/executeBoolean/executeString（它们
  // 内部先 executeGeneric 再 throw UnexpectedResultException——一旦后一个参数
  // 的求值已经发生，重新跑通用路径就会二次求值），而是显式 executeGeneric 一次、
  // 缓存结果，再就缓存值做 instanceof 类型判断。类型不匹配时把已求值的参数直接
  // 交给 Builtins.call（doGenericWithArgs），不重新执行任何参数节点。
  // ControlFlowException 自然从 executeGeneric 透传，不被任何 catch 拦截。
  // ---------------------------------------------------------------------------

  /**
   * 内联 eq (int == int)
   */
  @Specialization(guards = {"isEq()", "hasTwoArgs()"})
  protected boolean doEqInt(VirtualFrame frame) {
    Profiler.inc("builtin_eq_inlined");
    Object a = argNodes[0].executeGeneric(frame);
    Object b = argNodes[1].executeGeneric(frame);
    if (a instanceof Integer ai && b instanceof Integer bi) {
      return ai.intValue() == bi.intValue();
    }
    return expectBoolean(doGenericWithArgs(a, b));
  }

  /**
   * 内联 lt (int < int)
   */
  @Specialization(guards = {"isLt()", "hasTwoArgs()"})
  protected boolean doLtInt(VirtualFrame frame) {
    Profiler.inc("builtin_lt_inlined");
    Object a = argNodes[0].executeGeneric(frame);
    Object b = argNodes[1].executeGeneric(frame);
    if (a instanceof Integer ai && b instanceof Integer bi) {
      return ai.intValue() < bi.intValue();
    }
    return expectBoolean(doGenericWithArgs(a, b));
  }

  /**
   * 内联 gt (int > int)
   */
  @Specialization(guards = {"isGt()", "hasTwoArgs()"})
  protected boolean doGtInt(VirtualFrame frame) {
    Profiler.inc("builtin_gt_inlined");
    Object a = argNodes[0].executeGeneric(frame);
    Object b = argNodes[1].executeGeneric(frame);
    if (a instanceof Integer ai && b instanceof Integer bi) {
      return ai.intValue() > bi.intValue();
    }
    return expectBoolean(doGenericWithArgs(a, b));
  }

  /**
   * 内联 lte (int <= int)
   */
  @Specialization(guards = {"isLte()", "hasTwoArgs()"})
  protected boolean doLteInt(VirtualFrame frame) {
    Profiler.inc("builtin_lte_inlined");
    Object a = argNodes[0].executeGeneric(frame);
    Object b = argNodes[1].executeGeneric(frame);
    if (a instanceof Integer ai && b instanceof Integer bi) {
      return ai.intValue() <= bi.intValue();
    }
    return expectBoolean(doGenericWithArgs(a, b));
  }

  /**
   * 内联 gte (int >= int)
   */
  @Specialization(guards = {"isGte()", "hasTwoArgs()"})
  protected boolean doGteInt(VirtualFrame frame) {
    Profiler.inc("builtin_gte_inlined");
    Object a = argNodes[0].executeGeneric(frame);
    Object b = argNodes[1].executeGeneric(frame);
    if (a instanceof Integer ai && b instanceof Integer bi) {
      return ai.intValue() >= bi.intValue();
    }
    return expectBoolean(doGenericWithArgs(a, b));
  }

  /**
   * 内联 and (boolean && boolean)
   */
  @Specialization(guards = {"isAnd()", "hasTwoArgs()"})
  protected boolean doAndBoolean(VirtualFrame frame) {
    Profiler.inc("builtin_and_inlined");
    Object a = argNodes[0].executeGeneric(frame);
    Object b = argNodes[1].executeGeneric(frame);
    if (a instanceof Boolean ab && b instanceof Boolean bb) {
      return ab && bb;
    }
    return expectBoolean(doGenericWithArgs(a, b));
  }

  /**
   * 内联 or (boolean || boolean)
   */
  @Specialization(guards = {"isOr()", "hasTwoArgs()"})
  protected boolean doOrBoolean(VirtualFrame frame) {
    Profiler.inc("builtin_or_inlined");
    Object a = argNodes[0].executeGeneric(frame);
    Object b = argNodes[1].executeGeneric(frame);
    if (a instanceof Boolean ab && b instanceof Boolean bb) {
      return ab || bb;
    }
    return expectBoolean(doGenericWithArgs(a, b));
  }

  /**
   * 内联 not (!boolean)
   * 注意：not 只需要 1 个参数
   */
  @Specialization(guards = {"isNot()", "hasOneArg()"})
  protected boolean doNotBoolean(VirtualFrame frame) {
    Profiler.inc("builtin_not_inlined");
    Object a = argNodes[0].executeGeneric(frame);
    if (a instanceof Boolean ab) {
      return !ab;
    }
    return expectBoolean(doGenericWithArgs(a));
  }

  /**
   * 内联 Text.concat (String + String)
   * 快速路径: 两个参数都是 String，直接拼接
   * Fallback: 非 String 时交给 Builtins.call（支持 String.valueOf），不二次求值参数
   */
  @Specialization(guards = {"isTextConcat()", "hasTwoArgs()"})
  protected String doTextConcat(VirtualFrame frame) {
    Profiler.inc("builtin_text_concat_inlined");
    Object a = argNodes[0].executeGeneric(frame);
    Object b = argNodes[1].executeGeneric(frame);
    if (a instanceof String left && b instanceof String right) {
      return left + right;
    }
    return (String) doGenericWithArgs(a, b);
  }

  /**
   * 内联 Text.length (String.length())
   * 快速路径: 参数是 String，直接返回长度
   * Fallback: 非 String 时交给 Builtins.call（支持 String.valueOf），不二次求值参数
   */
  @Specialization(guards = {"isTextLength()", "hasOneArg()"})
  protected int doTextLength(VirtualFrame frame) {
    Profiler.inc("builtin_text_length_inlined");
    Object a = argNodes[0].executeGeneric(frame);
    if (a instanceof String text) {
      return text.length();
    }
    return (int) doGenericWithArgs(a);
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
    List<Object> l = Builtins.asList(list);
    if (l != null) {
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

    List<Object> src = Builtins.asList(listObj);
    if (src != null) {
      List<Object> mutable = new ArrayList<>(src);
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

    // 类型检查（asList 兼容 guest AsterListValue 与原生 java.util.List）
    List<Object> list = Builtins.asList(listObj);
    if (list == null) {
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

    // 类型检查（asList 兼容 guest AsterListValue 与原生 java.util.List）
    List<Object> list = Builtins.asList(listObj);
    if (list == null) {
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

    // 类型检查（asList 兼容 guest AsterListValue 与原生 java.util.List）
    List<Object> list = Builtins.asList(listObj);
    if (list == null) {
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

    // 类型检查（asList 兼容 guest AsterListValue 与原生 java.util.List）
    List<Object> list = Builtins.asList(listObj);
    if (list == null) {
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
  @Specialization(replaces = {"doEqInt", "doLtInt", "doGtInt", "doLteInt", "doGteInt",
                               "doAndBoolean", "doOrBoolean", "doNotBoolean",
                               "doTextConcat", "doTextLength", "doListLength", "doListAppend",
                               "doListMapSmall", "doListMap", "doListFilterSmall", "doListFilter"})
  protected Object doGeneric(VirtualFrame frame) {
    Profiler.inc("builtin_call_generic");

    // 执行参数节点（每个参数恰好求值一次；ControlFlowException 自然透传）
    Object[] args = new Object[argNodes.length];
    for (int i = 0; i < argNodes.length; i++) {
      args[i] = argNodes[i].executeGeneric(frame);
    }

    return callBuiltinChecked(args);
  }

  /**
   * 用已经求值好的参数调用 Builtins.call（不重新执行任何参数节点）。
   * 快速路径类型判断失败时复用，确保副作用参数不会被二次求值。
   */
  private Object doGenericWithArgs(Object... args) {
    Profiler.inc("builtin_call_generic");
    return callBuiltinChecked(args);
  }

  /**
   * 调用 Builtins.call 并区分「不存在该 builtin」与「builtin 合法返回 null」。
   * 镜像 CallNode.java 的守卫：未知 builtin 在 guest 内显式失败，避免把 null
   * 透传到 asGuestValue 触发难以诊断的 NPE。BuiltinCallNode 仅在 Builtins.has()
   * 为真时才由 Loader 构造，故这里 null 只可能是「合法返回 null」——但仍保留
   * 显式守卫，防御未来注册表变动或运算符规范化的边界。
   */
  private Object callBuiltinChecked(Object[] args) {
    if (!Builtins.has(builtinName)) {
      // 未知 builtin：显式失败而非返回 null（mirror CallNode:100-106）
      throw new RuntimeException("Unknown builtin: " + builtinName);
    }
    // 合法返回值（含 null，如 Map.get / Option.unwrapOr 的 sentinel）直接返回
    return Builtins.call(builtinName, args);
  }

  /**
   * 把通用路径的返回值收敛为 boolean，给比较/逻辑 builtin 的快速路径回退用。
   */
  private boolean expectBoolean(Object result) {
    if (result instanceof Boolean) {
      return (boolean) result;
    }
    throw new RuntimeException(
        "Builtin '" + builtinName + "' expected boolean result, got: " +
        (result == null ? "null" : result.getClass().getSimpleName()));
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
