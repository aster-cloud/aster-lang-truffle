package aster.truffle;

import aster.truffle.core.CoreModel;
import aster.truffle.nodes.*;
import aster.truffle.runtime.AsterEnumValue;
import aster.truffle.runtime.Builtins;
import aster.truffle.runtime.FrameSlotBuilder;
import aster.truffle.runtime.LexicalSlots;
import aster.truffle.runtime.PiiSupport;
import com.fasterxml.jackson.databind.*;
import com.oracle.truffle.api.nodes.Node;
import java.io.*;
import java.util.List;

public final class Loader {
  public static final class Program {
    public final Node root; public final Env env; public final List<CoreModel.Param> params; public final String entry; public final java.util.List<String> effects;
    public Program(Node root, Env env, List<CoreModel.Param> params, String entry, java.util.List<String> effects) { this.root = root; this.env = env; this.params = params; this.entry = entry; this.effects = effects; }
  }

  private final ObjectMapper mapper = new ObjectMapper().configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private final AsterLanguage language;

  public Loader(AsterLanguage language) {
    this.language = language;
  }

  public Node buildFromJson(File f) throws IOException { return buildProgram(f, null, null).root; }

  public Program buildProgram(File f, String funcName) throws IOException { return buildProgram(f, funcName, null); }

  public Program buildProgram(File f, String funcName, java.util.List<String> rawArgs) throws IOException {
    var mod = mapper.readValue(f, CoreModel.Module.class);
    return buildProgramInternal(mod, funcName, rawArgs);
  }

  public Program buildProgram(String jsonContent, String funcName, java.util.List<String> rawArgs) throws IOException {
    var mod = mapper.readValue(jsonContent, CoreModel.Module.class);
    return buildProgramInternal(mod, funcName, rawArgs);
  }

  private Program buildProgramInternal(CoreModel.Module mod, String funcName, java.util.List<String> rawArgs) throws IOException {
    this.frames.clear();
    // Import 声明在 Core 阶段已展开依赖，这里直接消费合并后的 Module，无需额外处理。
    if (mod.decls != null) {
      for (var d : mod.decls) {
        if (d instanceof CoreModel.Func fn) {
          ensureWorkflowEffects(fn);
        }
      }
    }
    this.dataTypeIndex = new java.util.LinkedHashMap<>();
    if (mod.decls != null) {
      for (var d : mod.decls) {
        if (d instanceof CoreModel.Data data) {
          dataTypeIndex.put(data.name, data);
        }
      }
    }
    // collect enum variants mapping
    this.enumVariantToEnum = new java.util.HashMap<>();
    this.enumVariants = new java.util.HashMap<>();
    if (mod.decls != null) for (var d : mod.decls) if (d instanceof CoreModel.Enum en) {
      for (var v : en.variants) enumVariantToEnum.put(v, en.name);
      enumVariants.put(en.name, java.util.Set.copyOf(en.variants));
    }
    // Group functions by name for possible overloading
    java.util.Map<String, java.util.List<CoreModel.Func>> funcGroups = new java.util.LinkedHashMap<>();
    if (mod.decls != null) for (var d : mod.decls) if (d instanceof CoreModel.Func fn) funcGroups.computeIfAbsent(fn.name, k -> new java.util.ArrayList<>()).add(fn);
    // 注册到字段，让 buildExpr 在分发 Call 时可查（用户函数屏蔽同名 builtin）
    this.userFunctionNames = java.util.Set.copyOf(funcGroups.keySet());
    // Resolve entry function with simple overload selection if needed
    CoreModel.Func entry = null;
    if (funcName != null && !funcName.isEmpty()) {
      var list = funcGroups.get(funcName);
      if (list != null && !list.isEmpty()) entry = selectOverload(list, rawArgs);
    }
    if (entry == null && funcGroups.containsKey("main")) entry = selectOverload(funcGroups.get("main"), rawArgs);
    if (entry == null) {
      // fallback to first available function
      for (var e : funcGroups.entrySet()) { if (!e.getValue().isEmpty()) { entry = selectOverload(e.getValue(), rawArgs); break; } }
    }
    if (entry == null) throw new IOException("No function in module");

    // Build env and predefine all functions as lambdas to enable cross-calls
    this.env = new Env();
    java.util.Map<String, CoreModel.Func> funcs = new java.util.LinkedHashMap<>();
    for (var e : funcGroups.entrySet()) {
      // 【重载处理说明】
      // 当前实现使用"最佳匹配"策略：选择参数数量最多的重载作为默认版本
      // 这是因为 Env 是简单的 name -> value 映射，无法存储多个同名函数
      //
      // 重载解析在以下阶段完成：
      // 1. 入口函数：selectOverload() 根据实际参数选择最佳匹配
      // 2. 内部调用：Core IR 阶段已完成重载解析
      //
      // 未来可改进为运行时动态分发（需要修改 Env 和 CallNode）
      var overloads = e.getValue();
      CoreModel.Func bestMatch = overloads.get(0);
      for (var fn : overloads) {
        int fnArity = fn.params == null ? 0 : fn.params.size();
        int bestArity = bestMatch.params == null ? 0 : bestMatch.params.size();
        if (fnArity > bestArity) {
          bestMatch = fn;
        }
      }
      funcs.put(e.getKey(), bestMatch);
    }
    // First pass: reserve names
    for (var name : funcs.keySet()) env.set(name, null);
    // Second pass: build lambda values and set into env
    for (var e : funcs.entrySet()) {
      var fn = e.getValue();
      java.util.List<String> params = new java.util.ArrayList<>();
      if (fn.params != null) for (var p : fn.params) params.add(p.name);
      java.util.Map<String,Object> captured = java.util.Map.of();

      if (language != null) {
        // New CallTarget-based approach
        // 形参先占槽位 0..N-1；局部变量（Let、模式绑定）在构建函数体时按词法作用域分配，
        // 故 FrameDescriptor 在函数体构建完成后再生成。
        FrameSlotBuilder slotBuilder = new FrameSlotBuilder();
        for (String param : params) {
          slotBuilder.addParameter(param);
        }
        Node body = withFrame(slotBuilder, () -> buildFunctionBody(fn));
        com.oracle.truffle.api.frame.FrameDescriptor frameDescriptor = slotBuilder.build();

        // Create LambdaRootNode for this function (no captures for top-level functions)
        String lambdaFuncName = "func_" + e.getKey();
        LambdaRootNode rootNode = new LambdaRootNode(
            language,
            frameDescriptor,
            lambdaFuncName,
            params.size(),
            0,  // captureCount = 0 for top-level functions
            body,
            extractParamTypes(fn.params)
        );

        // Get CallTarget
        com.oracle.truffle.api.CallTarget callTarget = rootNode.getCallTarget();

        // 从 Core IR 函数声明中提取 effects（如 ["IO", "Async"]）
        java.util.Set<String> requiredEffects = fn.effects != null ? new java.util.HashSet<>(fn.effects) : java.util.Set.of();

        // Set LambdaValue with CallTarget and effects into env
        env.set(e.getKey(), new aster.truffle.nodes.LambdaValue(params, List.of(), new Object[0], callTarget, requiredEffects));
      }
    }
    // 如果入口函数有参数，直接返回 LambdaValue（让调用者传参执行）
    // 否则构建立即调用的 CallNode（无参函数可以直接执行）
    Node root;
    if (entry.params != null && !entry.params.isEmpty()) {
      // 有参函数：返回可执行的 lambda，GoldenTestAdapter 会调用 program.execute(args)
      root = new NameNodeEnv(env, entry.name);
    } else {
      // 无参函数：构建立即调用节点，context.eval() 会直接执行
      Node target = new NameNodeEnv(env, entry.name);
      root = CallNode.create(target, new java.util.ArrayList<>());
    }
    return new Program(root, env, entry.params, entry.name, entry.effects);
  }

  private CoreModel.Func selectOverload(java.util.List<CoreModel.Func> funcs, java.util.List<String> rawArgs) {
    if (funcs == null || funcs.isEmpty()) return null;
    if (rawArgs == null || rawArgs.isEmpty()) return funcs.get(0);
    int bestScore = Integer.MIN_VALUE;
    CoreModel.Func best = null;
    for (var fn : funcs) {
      int arity = fn.params == null ? 0 : fn.params.size();
      if (arity != rawArgs.size()) continue;
      int s = 0;
      for (int i = 0; i < arity; i++) {
        String raw = rawArgs.get(i);
        CoreModel.Type ty = fn.params.get(i).type;
        s += score(raw, ty);
      }
      if (s > bestScore) { bestScore = s; best = fn; }
    }
    return best != null ? best : funcs.get(0);
  }

  private static int score(String raw, CoreModel.Type ty) {
    if (raw == null) return 0;
    String t = raw.trim();
    if (ty instanceof CoreModel.PiiType pii) {
      return score(t, pii.baseType);
    }
    if (ty instanceof CoreModel.TypeVar) {
      // 泛型参数宽松匹配，始终赋予最低优先级
      return 1;
    }
    if (ty instanceof CoreModel.TypeApp app) {
      int baseScore = (app.base != null) ? score(t, app.base) : 0;
      if (baseScore > 0) {
        return baseScore;
      }
      if (app.args != null && !app.args.isEmpty()) {
        int best = 0;
        for (CoreModel.Type arg : app.args) {
          best = Math.max(best, score(t, arg));
        }
        return best;
      }
      return 0;
    }
    if (ty instanceof CoreModel.FuncType funcType) {
      String lower = t.toLowerCase(java.util.Locale.ROOT);
      if (lower.contains("lambda") || lower.contains("function") || lower.contains("->") || lower.contains("=>")) {
        return 3;
      }
      return 0;
    }
    if (ty instanceof CoreModel.TypeName tn) {
      String n = tn.name;
      if ("Int".equals(n)) return looksInt(t) ? 3 : 0;
      if ("Bool".equals(n) || "Boolean".equals(n)) return looksBool(t) ? 3 : 0;
      return 1; // Text/String or others
    }
    if (ty instanceof CoreModel.Option opt || ty instanceof CoreModel.Maybe mb) {
      if ("null".equalsIgnoreCase(t) || "none".equalsIgnoreCase(t)) return 2;
      CoreModel.Type inner = (ty instanceof CoreModel.Option) ? ((CoreModel.Option)ty).type : ((CoreModel.Maybe)ty).type;
      return 1 + score(t, inner);
    }
    if (ty instanceof CoreModel.ListT) {
      if (t.startsWith("[") && t.endsWith("]")) return 3;
      if (t.contains(",") || t.contains(";") || t.contains("|")) return 2;
      return 1;
    }
    if (ty instanceof CoreModel.MapT) {
      if (t.startsWith("{") && t.endsWith("}")) return 3;
      if (t.contains(":")) return 2;
      return 0;
    }
    if (ty instanceof CoreModel.Result) {
      if (t.startsWith("{") || t.startsWith("Ok(") || t.startsWith("Err(")) return 2;
      return 0;
    }
    return 0;
  }

  private static boolean looksInt(String s) {
    int i = (s.startsWith("+") || s.startsWith("-")) ? 1 : 0;
    if (i >= s.length()) return false;
    for (; i < s.length(); i++) if (!Character.isDigit(s.charAt(i))) return false;
    return true;
  }
  private static boolean looksBool(String s) { return "true".equalsIgnoreCase(s) || "false".equalsIgnoreCase(s); }

  private Env env;
  private java.util.Map<String,String> enumVariantToEnum;
  /** 枚举名 → 变体集合；限定写法 E.M 按此判定，不受不同枚举同名变体覆盖影响。 */
  private java.util.Map<String, java.util.Set<String>> enumVariants = java.util.Map.of();
  /** 正在构建的 frame（规则体 / lambda 体）栈，栈顶为当前 frame 的词法作用域链。 */
  private final java.util.Deque<LexicalSlots> frames = new java.util.ArrayDeque<>();
  private final java.util.Deque<CoreModel.Type> returnTypeStack = new java.util.ArrayDeque<>();
  private java.util.Map<String, CoreModel.Data> dataTypeIndex;
  /**
   * 用户定义的函数名集合。Loader 在 buildProgramInternal 第一阶段填入，
   * buildExpr 在判断 Call 是否走 BuiltinCallNode 时**先**检查此集合 ——
   * 用户定义函数优先于同名 builtin（如 user 的 `Rule add` 应屏蔽内置
   * arithmetic add）。
   *
   * 若不做这个屏蔽，用户 `add(request)` 会被识别为 builtin add(a,b)，
   * 收到 1 个 Map 参数后 arity 错误，但 fallback 路径在 DSL 重写中
   * 被吞掉返回 0 —— 这正是导致 calculator 案例返回 0 的根因。
   */
  private java.util.Set<String> userFunctionNames = java.util.Set.of();

  /**
   * 把二元 {@code and}/{@code or} 调用构建成短路节点；其它调用返回 null 交由常规路径。
   * 中缀与前缀写法在 Core IR 中同为 {@code Call{Name "and"|"or"}}，此处一并覆盖。
   * 参数个数不为 2 时仍走 builtin 路径，由 builtin 的 arity 检查报错。
   */
  private AsterExpressionNode buildShortCircuit(CoreModel.Call c) {
    if (!(c.target instanceof CoreModel.Name n) || c.args == null || c.args.size() != 2) return null;
    if (userFunctionNames.contains(n.name)) return null;
    boolean isAnd = "and".equals(n.name);
    if (!isAnd && !"or".equals(n.name)) return null;
    AsterExpressionNode left = buildExpr(c.args.get(0));
    AsterExpressionNode right = buildExpr(c.args.get(1));
    return isAnd
        ? new aster.truffle.nodes.ShortCircuitNode.AndNode(left, right)
        : new aster.truffle.nodes.ShortCircuitNode.OrNode(left, right);
  }

  private Node buildBlock(CoreModel.Block b) {
    if (b == null || b.statements == null || b.statements.isEmpty()) return LiteralNode.create(null);
    return buildStatements(b.statements);
  }

  /** 在当前词法作用域内依次构建语句；Block / If 分支不开新层，与所在块共享作用域。 */
  private Node buildStatements(java.util.List<CoreModel.Stmt> statements) {
    var list = new java.util.ArrayList<Node>();
    for (var s : statements) {
      if (s instanceof CoreModel.Return r) {
        AsterExpressionNode returnExpr = buildExpr(r.expr);
        returnExpr = maybeWrapForType(returnExpr, currentReturnType());
        list.add(new ReturnNode(returnExpr, originLine(r.origin)));
      } else if (s instanceof CoreModel.If iff) {
        list.add(IfNode.create(buildExpr(iff.cond), buildBlock(iff.thenBlock),
            buildBlock(iff.elseBlock), originLine(iff.origin)));
      } else if (s instanceof CoreModel.Let let) {
        // 先构建右侧（`Let x be x plus 1` 读的是外层 x），再在当前层声明 x
        AsterExpressionNode valueNode = buildExpr(let.expr);
        LexicalSlots frame = currentFrame();
        if (frame != null) {
          list.add(LetNodeGen.create(let.name, frame.declare(let.name), valueNode));
        } else {
          list.add(new LetNodeEnv(let.name, valueNode, env));
        }
      } else if (s instanceof CoreModel.Match mm) {
        list.add(buildMatch(mm));
      } else if (s instanceof CoreModel.Scope sc) {
        list.add(buildScope(sc));
      } else if (s instanceof CoreModel.Block nested) {
        list.add(buildBlock(nested));
      } else if (s instanceof CoreModel.Set set) {
        // 写到词法作用域链上最近的那层绑定（ADR 0052 §2.1）；不是局部名时回退到 Env
        AsterExpressionNode valueNode = buildExpr(set.expr);
        Integer slot = localSlot(set.name);
        if (slot != null) {
          list.add(SetNodeGen.create(set.name, slot, valueNode));
        } else {
          list.add(new SetNodeEnv(set.name, valueNode, env));
        }
      } else if (s instanceof CoreModel.Start st) {
        list.add(new StartNode(env, st.name, buildExpr(st.expr)));
      } else if (s instanceof CoreModel.Wait wt) {
        list.add(new WaitNode(env, ((wt.names != null) ? wt.names : java.util.List.<String>of()).toArray(new String[0])));
      } else if (s instanceof CoreModel.Workflow wf) {
        list.add(buildWorkflow(wf));
      }
    }
    return BlockNode.create(list);
  }

  private Node buildWorkflow(CoreModel.Workflow wf) {
    if (wf == null || wf.steps == null || wf.steps.isEmpty()) {
      return LiteralNode.create(null);
    }
    java.util.List<CoreModel.Step> steps = wf.steps;
    Node[] stepBodies = new Node[steps.size()];
    Node[] compensateBodies = new Node[steps.size()];  // 补偿代码块数组
    String[] stepNames = new String[steps.size()];
    // 依赖按 step 下标登记（与 WorkflowNode 的 idByIndex 同一约定）：Core IR 中
    // Step.dependencies 与 Step.name 相互独立，未命名 step 的依赖声明同样有效，
    // 若以 name 为键登记就只能丢弃它们，导致该 step 不等待前置 step、依赖拼错也不报错。
    java.util.List<java.util.Set<String>> dependencies = new java.util.ArrayList<>(steps.size());
    boolean hasAnyCompensation = false;  // 跟踪是否存在任何补偿逻辑

    for (int i = 0; i < steps.size(); i++) {
      CoreModel.Step step = steps.get(i);
      if (step == null) {
        stepBodies[i] = LiteralNode.create(null);
        compensateBodies[i] = null;
        stepNames[i] = null;
        dependencies.add(java.util.Set.of());
        continue;
      }
      stepBodies[i] = buildBlock(step.body);
      stepNames[i] = step.name;

      // 构建补偿代码块（如果存在）
      if (step.compensate != null && step.compensate.statements != null && !step.compensate.statements.isEmpty()) {
        compensateBodies[i] = buildBlock(step.compensate);
        hasAnyCompensation = true;
      } else {
        compensateBodies[i] = null;
      }

      java.util.LinkedHashSet<String> depSet = new java.util.LinkedHashSet<>();
      if (step.dependencies != null) {
        for (String dep : step.dependencies) {
          if (dep != null && !dep.isEmpty()) {
            depSet.add(dep);
          }
        }
      }
      dependencies.add(depSet);
    }

    long timeoutMs = 0L;
    if (wf.timeout != null) {
      timeoutMs = wf.timeout.milliseconds;
    }

    return new WorkflowNode(env, stepBodies,
        hasAnyCompensation ? compensateBodies : null,
        stepNames, dependencies, timeoutMs);
  }

  private AsterExpressionNode buildExpr(CoreModel.Expr e) {
    if (e instanceof CoreModel.StringE s) return LiteralNode.create(s.value);
    if (e instanceof CoreModel.Bool b) return LiteralNode.create(b.value);
    if (e instanceof CoreModel.IntE i) return LiteralNode.create(Integer.valueOf(i.value));
    if (e instanceof CoreModel.LongE l) return LiteralNode.create(Long.valueOf(l.value));
    if (e instanceof CoreModel.DoubleE d) return LiteralNode.create(Double.valueOf(d.value));
    // Decimal 字面量（ADR 0025）：value 是 canonical 十进制字符串 → guest AsterDecimalValue
    // （包装 BigDecimal）。沙箱下不能用裸 BigDecimal（非 TruffleObject 会在 interop 边界抛
    // ClassCastException）。
    if (e instanceof CoreModel.DecimalE dec) {
      return LiteralNode.create(
          aster.truffle.runtime.interop.AsterDecimalValue.of(new java.math.BigDecimal(dec.value)));
    }
    if (e instanceof CoreModel.NullE) return LiteralNode.create(null);
    if (e instanceof CoreModel.AwaitE aw) return aster.truffle.nodes.AwaitNode.create(buildExpr(aw.expr));
    if (e instanceof CoreModel.Lambda lam) {
      java.util.List<String> params = new java.util.ArrayList<>();
      if (lam.params != null) for (var p : lam.params) params.add(p.name);
      java.util.List<String> caps = lam.captures != null ? lam.captures : java.util.List.of();

      if (language != null) {
        // New CallTarget-based approach with closure capture
        // Build FrameDescriptor: parameters first, then captures, then locals
        FrameSlotBuilder slotBuilder = new FrameSlotBuilder();

        // Add parameter slots (0..paramCount-1)
        for (String param : params) {
          slotBuilder.addParameter(param);
        }

        // Add capture slots (paramCount..paramCount+captureCount-1)
        for (String captureName : caps) {
          slotBuilder.addLocal(captureName);
        }

        // lambda 体是独立的 frame：局部变量在构建 lambda 体时分配，之后再生成 FrameDescriptor
        Node body = withReturnType(lam.ret, () -> withFrame(slotBuilder, () -> buildBlock(lam.body)));
        com.oracle.truffle.api.frame.FrameDescriptor frameDescriptor = slotBuilder.build();

        // Create LambdaRootNode
        String lambdaName = "lambda@" + System.identityHashCode(lam);
        LambdaRootNode rootNode = new LambdaRootNode(
            language,
            frameDescriptor,
            lambdaName,
            params.size(),
            caps.size(),
            body,
            extractParamTypes(lam.params)
        );

        // Get CallTarget
        com.oracle.truffle.api.CallTarget callTarget = rootNode.getCallTarget();

        // Create nodes to evaluate captured values at runtime
        AsterExpressionNode[] captureExprs = new AsterExpressionNode[caps.size()];
        for (int i = 0; i < caps.size(); i++) {
          // Build expression to read the captured variable at Lambda creation time
          captureExprs[i] = buildName(caps.get(i));
        }

        // Return LambdaNode that will create LambdaValue with captured values at runtime
        return aster.truffle.nodes.LambdaNode.create(language, env, params, caps, captureExprs, callTarget);
      } else {
        // Legacy Loader (Runner without AsterLanguage) 不支持 Lambda
        // 这是有意的设计决策,因为:
        // 1. Lambda 需要 AsterLanguage 来创建 CallTarget
        // 2. Legacy Runner 已标记为 deprecated
        // 3. 用户应迁移到 Polyglot API
        throw new UnsupportedOperationException(
            "Lambda 需要 AsterLanguage 支持。\n" +
            "请使用 Polyglot API 运行代码: Context.create(\"aster\").eval(...)\n" +
            "Legacy Runner 不再支持 Lambda 特性。"
        );
      }
    }
    if (e instanceof CoreModel.Name n) return buildName(n.name);
    if (e instanceof CoreModel.Call c) {
      // and/or 必须短路，不能走先求值全部参数的 BuiltinCallNode
      AsterExpressionNode shortCircuit = buildShortCircuit(c);
      if (shortCircuit != null) return shortCircuit;
      // 检测 builtin 调用：如果 target 是 Name 且是已注册的 builtin，使用 BuiltinCallNode 优化
      // **但**用户定义的函数（如 `Rule add ...`）必须优先于同名 builtin —— 不然
      // user 的 `add(request)` 会被识别为算术 add(a,b) 然后 arity 失配。
      if (c.target instanceof CoreModel.Name targetName) {
        String name = targetName.name;
        if (Builtins.has(name) && !userFunctionNames.contains(name)) {
          // 创建 BuiltinCallNode（内联优化）
          var argNodes = new java.util.ArrayList<aster.truffle.nodes.AsterExpressionNode>();
          if (c.args != null) {
            for (var a : c.args) {
              Node argNode = buildExpr(a);
              if (argNode instanceof aster.truffle.nodes.AsterExpressionNode exprNode) {
                argNodes.add(exprNode);
              } else {
                throw new RuntimeException("Builtin argument must be expression node: " + argNode.getClass());
              }
            }
          }
          return aster.truffle.nodes.BuiltinCallNodeGen.create(
              name,
              argNodes.toArray(new aster.truffle.nodes.AsterExpressionNode[0])
          );
        }
      }

      // ★命名空间函数不存在时，直接报「未定义」，别退化成成员访问（#99）。
      //
      //   `Foo.bar(n)` 在 Core IR 里是 Call{target: Name("Foo.bar")}。若 Foo.bar
      //   既不是 builtin 也不是用户规则，下面的 buildExpr 会把它当普通表达式解析，
      //   最终走到 MemberAccessNode，抛出：
      //       无法访问成员：对象类型 java.lang.String 不支持成员访问，成员：bar
      //       （若为宿主对象，请确认 polyglot Context 配置了恰当的 HostAccess…）
      //
      //   这条消息把排查引向 polyglot/HostAccess 配置——一整个无关子系统；
      //   而真实原因只是**函数名写错或 builtin 不存在**。报的类型也是错的
      //   （Foo 并非 String）。TS 侧对同一输入给的是 `Undefined function 'Foo.bar'`。
      //
      //   判据用「含点号 + 首段是已知 builtin 命名空间」而非无条件拦截：
      //   合法的宿主对象成员访问（`applicant.age`）在 Core IR 里是 Member 节点、
      //   不是 Call{Name}，本就走不到这里；而形如 `someVar.field(x)` 的动态调用
      //   首段不会是命名空间名，故不受影响。
      if (c.target instanceof CoreModel.Name nsName
          && nsName.name != null
          && nsName.name.indexOf('.') > 0
          && !userFunctionNames.contains(nsName.name)
          && !Builtins.has(nsName.name)
          && Builtins.hasNamespace(nsName.name.substring(0, nsName.name.indexOf('.')))) {
        throw new RuntimeException("Undefined function '" + nsName.name + "'");
      }

      // 普通函数调用：使用 CallNode
      Node target = buildExpr(c.target);
      var args = new java.util.ArrayList<Node>();
      if (c.args != null) for (var a : c.args) args.add(buildExpr(a));
      return CallNode.create(target, args);
    }
    if (e instanceof CoreModel.Ok ok) return new aster.truffle.nodes.ResultNodes.OkNode(buildExpr(ok.expr));
    if (e instanceof CoreModel.Err er) return new aster.truffle.nodes.ResultNodes.ErrNode(buildExpr(er.expr));
    if (e instanceof CoreModel.Some sm) return new aster.truffle.nodes.ResultNodes.SomeNode(buildExpr(sm.expr));
    if (e instanceof CoreModel.NoneE) return new aster.truffle.nodes.ResultNodes.NoneNode();
    if (e instanceof CoreModel.Construct cons) return buildConstruct(cons);
    if (e instanceof CoreModel.IfE ifx) {
      // ADR 0019 G2b：表达式级 if → IfExprNode（三分支都是表达式节点，求值产出值）。
      return aster.truffle.nodes.IfExprNode.create(
          buildExpr(ifx.cond), buildExpr(ifx.thenE), buildExpr(ifx.elseE),
          originLine(ifx.origin));
    }
    if (e instanceof CoreModel.ListE list) {
      // ADR 0024 C0：列表字面量 → ListLiteralNode（逐元素求值成 ArrayList，
      // 与 List.* builtin 的运行时列表表示一致）。
      var elems = list.elements == null ? java.util.List.<CoreModel.Expr>of() : list.elements;
      AsterExpressionNode[] elNodes = new AsterExpressionNode[elems.size()];
      for (int i = 0; i < elems.size(); i++) elNodes[i] = buildExpr(elems.get(i));
      return aster.truffle.nodes.ListLiteralNode.create(elNodes);
    }
    return LiteralNode.create(null);
  }

  private Node buildMatch(CoreModel.Match mm) {
    var patCases = new java.util.ArrayList<aster.truffle.nodes.MatchNode.CaseNode>();
    if (mm.cases != null) {
      for (var c : mm.cases) {
        // 每个 case 是一层词法作用域：模式绑定与分支体内的 Let 只遮蔽外层（ADR 0052 §2.1）
        LexicalSlots frame = currentFrame();
        if (frame != null) frame.enter();
        try {
          patCases.add(new aster.truffle.nodes.MatchNode.CaseNode(buildPatternNode(c.pattern, false), buildCaseBody(c.body)));
        } finally {
          if (frame != null) frame.exit();
        }
      }
    }
    return aster.truffle.nodes.MatchNode.create(env, buildExpr(mm.expr), patCases,
        originLine(mm.origin));
  }

  /**
   * 从 Core IR 的 {@code Origin} 取起始行号（1-based），取不到返回 0。
   *
   * <p>用于给步骤级 trace 打标签，使漏斗聚合能区分同类型的不同节点。
   */
  private static int originLine(CoreModel.Origin origin) {
    return (origin != null && origin.start != null) ? origin.start.line : 0;
  }

  /** case 分支体：Scope 再开一层，Block 与单语句在 case 这一层构建。 */
  private Node buildCaseBody(CoreModel.Stmt body) {
    if (body instanceof CoreModel.Scope sc) return buildScope(sc);
    if (body instanceof CoreModel.Block block) return buildBlock(block);
    if (body != null) return buildStatements(java.util.List.of(body));
    return LiteralNode.create(null);
  }

  /**
   * 构建模式节点，并在当前层为绑定名分配槽位。顶层名字模式按大小写区分变体与绑定（见 PatNameNode）；
   * 构造器模式内的名字一律绑定（PatCtorNode 按位置绑定字段值）。
   */
  private aster.truffle.nodes.MatchNode.PatternNode buildPatternNode(CoreModel.Pattern p, boolean nested) {
    if (p instanceof CoreModel.PatNull) return new aster.truffle.nodes.MatchNode.PatNullNode();
    if (p instanceof CoreModel.PatName pn) {
      boolean binds = nested
          ? pn.name != null && !pn.name.isEmpty() && !"_".equals(pn.name)
          : aster.truffle.nodes.MatchNode.PatNameNode.binds(pn.name);
      return new aster.truffle.nodes.MatchNode.PatNameNode(pn.name, binds ? declareLocal(pn.name) : -1);
    }
    if (p instanceof CoreModel.PatInt pi) return new aster.truffle.nodes.MatchNode.PatIntNode(pi.value);
    if (p instanceof CoreModel.PatCtor pc) {
      java.util.List<aster.truffle.nodes.MatchNode.PatternNode> args = new java.util.ArrayList<>();
      if (pc.args != null) for (var a : pc.args) args.add(buildPatternNode(a, true));
      int[] bindSlots = null;
      if (pc.names != null) {
        bindSlots = new int[pc.names.size()];
        for (int i = 0; i < bindSlots.length; i++) {
          String bn = pc.names.get(i);
          bindSlots[i] = (bn == null || bn.isEmpty() || "_".equals(bn)) ? -1 : declareLocal(bn);
        }
      }
      return new aster.truffle.nodes.MatchNode.PatCtorNode(pc.typeName, pc.names, bindSlots, args);
    }
    return new aster.truffle.nodes.MatchNode.PatNameNode("_");
  }

  /** 在当前层声明局部名并返回槽位；不在 frame 内（无槽位）时返回 -1，由调用方回退到 Env。 */
  private int declareLocal(String name) {
    LexicalSlots frame = currentFrame();
    return frame == null ? -1 : frame.declare(name);
  }

  /** name 在当前 frame 词法作用域链上最近的绑定槽位；不是局部名时为 null。 */
  private Integer localSlot(String name) {
    LexicalSlots frame = currentFrame();
    return frame == null ? null : frame.lookup(name);
  }

  /**
   * Scope 开一层词法作用域：块内 Let 离开后消失，对外层绑定的 Set 写回外层（ADR 0052 §2.1）。
   * 子 Env 只供 Start / Wait 的任务名与非局部名查找使用；局部变量都在 frame 槽位里。
   */
  private Node buildScope(CoreModel.Scope sc) {
    if (sc.statements == null || sc.statements.isEmpty()) return BlockNode.create(new java.util.ArrayList<>());
    Env previousEnv = this.env;
    LexicalSlots frame = currentFrame();
    this.env = (previousEnv != null) ? previousEnv.createChild() : new Env();
    if (frame != null) frame.enter();
    try {
      return buildStatements(sc.statements);
    } finally {
      if (frame != null) frame.exit();
      this.env = previousEnv;
    }
  }

  private AsterExpressionNode buildConstruct(CoreModel.Construct cons) {
    CoreModel.Data dataDefinition = requireDataDefinition(cons.typeName);
    java.util.LinkedHashMap<String, AsterExpressionNode> orderedFields = prepareDataFields(cons, dataDefinition);
    return ConstructNode.create(cons.typeName, orderedFields, dataDefinition);
  }

  private AsterExpressionNode buildName(String name) {
    AsterEnumValue enumValue = enumMember(name);
    if (enumValue != null) {
      return LiteralNode.create(enumValue);
    }
    // If name contains '.', build member access chain
    if (name.contains(".")) {
      String[] parts = name.split("\\.");
      if (parts.length >= 2) {
        // 构建基础变量节点
        AsterExpressionNode base = buildSimpleName(parts[0]);
        // 构建成员访问链
        String[] members = new String[parts.length - 1];
        System.arraycopy(parts, 1, members, 0, members.length);
        return MemberAccessNode.buildChain(base, members);
      }
    }
    return buildSimpleName(name);
  }

  /**
   * 名称若指枚举成员则返回其值，否则 null。
   * 裸成员 M 与限定成员 E.M（ADR 0051 R18：解析保留原写法）求值为同一枚举值；
   * E 为当前作用域的局部 / 参数时让位于字段访问，未知的 E.X 仍按成员访问链处理。
   */
  private AsterEnumValue enumMember(String name) {
    String bareEnum = enumVariantToEnum == null ? null : enumVariantToEnum.get(name);
    if (bareEnum != null) return new AsterEnumValue(bareEnum, name);
    int dot = name.indexOf('.');
    if (dot <= 0 || dot != name.lastIndexOf('.')) return null;
    String enumName = name.substring(0, dot);
    String variant = name.substring(dot + 1);
    if (!enumVariants.getOrDefault(enumName, java.util.Set.of()).contains(variant)) return null;
    if (localSlot(enumName) != null) return null;
    return new AsterEnumValue(enumName, variant);
  }

  private AsterExpressionNode buildSimpleName(String name) {
    Integer slot = localSlot(name);
    if (slot != null) return NameNodeGen.create(name, slot);
    return new NameNodeEnv(env, name);
  }

  private Node buildFunctionBody(CoreModel.Func fn) {
    if (fn == null) return LiteralNode.create(null);
    return withReturnType(fn.ret, () -> buildBlock(fn.body));
  }

  /**
   * 在新 frame 内构建：最外层绑定为 slots 中已分配的形参 / 捕获变量。
   * 嵌套 lambda 各自一个 frame，不会解析到外层 frame 的槽位（捕获变量经 captures 传入）。
   */
  private <T> T withFrame(FrameSlotBuilder slots, java.util.function.Supplier<T> supplier) {
    frames.push(new LexicalSlots(slots, slots.getSymbolTable()));
    try {
      return supplier.get();
    } finally {
      frames.pop();
    }
  }

  private LexicalSlots currentFrame() {
    return frames.peek();
  }

  private <T> T withReturnType(CoreModel.Type type, java.util.function.Supplier<T> supplier) {
    if (type == null) {
      return supplier.get();
    }
    returnTypeStack.push(type);
    try {
      return supplier.get();
    } finally {
      returnTypeStack.pop();
    }
  }

  private CoreModel.Type currentReturnType() {
    return returnTypeStack.isEmpty() ? null : returnTypeStack.peek();
  }

  private void ensureWorkflowEffects(CoreModel.Func fn) {
    if (fn == null) return;
    if (!blockContainsWorkflow(fn.body)) return;
    if (fn.effects == null) {
      fn.effects = new java.util.ArrayList<>();
    } else if (!(fn.effects instanceof java.util.ArrayList)) {
      fn.effects = new java.util.ArrayList<>(fn.effects);
    }
    if (!fn.effects.contains("Async")) {
      fn.effects.add("Async");
    }
  }

  private boolean blockContainsWorkflow(CoreModel.Block block) {
    if (block == null || block.statements == null) return false;
    for (var stmt : block.statements) {
      if (stmtContainsWorkflow(stmt)) {
        return true;
      }
    }
    return false;
  }

  private boolean stmtContainsWorkflow(CoreModel.Stmt stmt) {
    if (stmt == null) return false;
    if (stmt instanceof CoreModel.Workflow) return true;
    if (stmt instanceof CoreModel.Block block) return blockContainsWorkflow(block);
    if (stmt instanceof CoreModel.If iff) {
      if (blockContainsWorkflow(iff.thenBlock) || blockContainsWorkflow(iff.elseBlock)) return true;
    } else if (stmt instanceof CoreModel.Scope sc && sc.statements != null) {
      for (var nested : sc.statements) {
        if (stmtContainsWorkflow(nested)) return true;
      }
    } else if (stmt instanceof CoreModel.Match mm && mm.cases != null) {
      for (var c : mm.cases) {
        if (stmtContainsWorkflow(c.body)) return true;
      }
    }
    return false;
  }

  private static CoreModel.Type[] extractParamTypes(java.util.List<CoreModel.Param> params) {
    if (params == null || params.isEmpty()) {
      return new CoreModel.Type[0];
    }
    CoreModel.Type[] types = new CoreModel.Type[params.size()];
    for (int i = 0; i < params.size(); i++) {
      types[i] = params.get(i).type;
    }
    return types;
  }

  private AsterExpressionNode maybeWrapForType(AsterExpressionNode node, CoreModel.Type type) {
    if (node == null || type == null) {
      return node;
    }
    if (!PiiSupport.containsPii(type)) {
      return node;
    }
    return PiiWrapNode.create(node, type);
  }

  private CoreModel.Data requireDataDefinition(String typeName) {
    if (typeName == null || typeName.isBlank()) {
      throw new IllegalArgumentException("Construct 缺少数据类型名称");
    }
    if (dataTypeIndex == null) {
      throw new IllegalStateException("Data 类型索引未初始化");
    }
    CoreModel.Data data = dataTypeIndex.get(typeName);
    if (data == null) {
      throw new IllegalArgumentException("未定义的数据类型：" + typeName);
    }
    return data;
  }

  private java.util.LinkedHashMap<String, AsterExpressionNode> prepareDataFields(CoreModel.Construct cons, CoreModel.Data dataDefinition) {
    java.util.LinkedHashMap<String, CoreModel.Field> declared = new java.util.LinkedHashMap<>();
    if (dataDefinition.fields != null) {
      for (CoreModel.Field field : dataDefinition.fields) {
        if (field != null && field.name != null) {
          declared.put(field.name, field);
        }
      }
    }
    java.util.LinkedHashMap<String, AsterExpressionNode> provided = new java.util.LinkedHashMap<>();
    java.util.LinkedHashSet<String> seen = new java.util.LinkedHashSet<>();
    if (cons.fields != null) {
      for (CoreModel.FieldInit init : cons.fields) {
        if (init == null) continue;
        if (!declared.containsKey(init.name)) {
          throw new IllegalArgumentException("数据类型 " + dataDefinition.name + " 不存在字段：" + init.name);
        }
        if (!seen.add(init.name)) {
          throw new IllegalArgumentException("数据类型 " + dataDefinition.name + " 字段重复：" + init.name);
        }
        AsterExpressionNode valueNode = buildExpr(init.expr);
        CoreModel.Field declaredField = declared.get(init.name);
        valueNode = maybeWrapForType(valueNode, declaredField != null ? declaredField.type : null);
        provided.put(init.name, valueNode);
      }
    }
    if (declared.size() != provided.size()) {
      java.util.ArrayList<String> missing = new java.util.ArrayList<>();
      for (String fieldName : declared.keySet()) {
        if (!provided.containsKey(fieldName)) {
          missing.add(fieldName);
        }
      }
      if (!missing.isEmpty()) {
        throw new IllegalArgumentException("数据类型 " + dataDefinition.name + " 缺少字段：" + String.join(", ", missing));
      }
    }
    java.util.LinkedHashMap<String, AsterExpressionNode> ordered = new java.util.LinkedHashMap<>();
    for (String fieldName : declared.keySet()) {
      ordered.put(fieldName, provided.get(fieldName));
    }
    return ordered;
  }
}
