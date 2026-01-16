package aster.truffle;

import aster.truffle.core.CoreModel;
import aster.truffle.nodes.*;
import aster.truffle.runtime.AsterEnumValue;
import aster.truffle.runtime.Builtins;
import aster.truffle.runtime.FrameSlotBuilder;
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
    this.entryFunction = null;
    this.entryParamSlots = null;
    this.paramSlotStack.clear();
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
    if (mod.decls != null) for (var d : mod.decls) if (d instanceof CoreModel.Enum en) for (var v : en.variants) enumVariantToEnum.put(v, en.name);
    // Group functions by name for possible overloading
    java.util.Map<String, java.util.List<CoreModel.Func>> funcGroups = new java.util.LinkedHashMap<>();
    if (mod.decls != null) for (var d : mod.decls) if (d instanceof CoreModel.Func fn) funcGroups.computeIfAbsent(fn.name, k -> new java.util.ArrayList<>()).add(fn);
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
    this.entryFunction = entry;
    if (entry != null) {
      FrameSlotBuilder slotBuilder = new FrameSlotBuilder();
      // Add entry function parameters
      if (entry.params != null) {
        for (var p : entry.params) slotBuilder.addParameter(p.name);
      }
      // Collect local variables from entry function body (Let statements)
      java.util.Set<String> entryLocals = new java.util.LinkedHashSet<>();
      collectLocalVariables(entry.body, entryLocals);
      for (String local : entryLocals) {
        if (!slotBuilder.hasVariable(local)) {  // 避免与参数重复
          slotBuilder.addLocal(local);
        }
      }
      this.entryParamSlots = slotBuilder.getSymbolTable();
    } else {
      this.entryParamSlots = null;
    }

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
        // Build FrameDescriptor for function parameters and local variables
        FrameSlotBuilder slotBuilder = new FrameSlotBuilder();
        for (String param : params) {
          slotBuilder.addParameter(param);
        }

        // Collect local variables from function body (Let statements)
        java.util.Set<String> bodyLocals = new java.util.LinkedHashSet<>();
        collectLocalVariables(fn.body, bodyLocals);
        for (String local : bodyLocals) {
          if (!slotBuilder.hasVariable(local)) {  // 避免与参数重复
            slotBuilder.addLocal(local);
          }
        }

        com.oracle.truffle.api.frame.FrameDescriptor frameDescriptor = slotBuilder.build();

        // Build function body with parameter slots in scope
        java.util.Map<String,Integer> funcParamSlots = slotBuilder.getSymbolTable();
        Node body = withParamSlots(funcParamSlots, () -> buildFunctionBody(fn));

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
  private CoreModel.Func entryFunction;
  private java.util.Map<String,Integer> entryParamSlots;
  private final java.util.Deque<java.util.Map<String,Integer>> paramSlotStack = new java.util.ArrayDeque<>();
  private final java.util.Deque<CoreModel.Type> returnTypeStack = new java.util.ArrayDeque<>();
  private java.util.Map<String, CoreModel.Data> dataTypeIndex;

  private Node buildBlock(CoreModel.Block b) {
    if (b == null || b.statements == null || b.statements.isEmpty()) return LiteralNode.create(null);
    var list = new java.util.ArrayList<Node>();
    java.util.Map<String,Integer> slots = currentParamSlots();  // 获取当前 frame slots

    for (var s : b.statements) {
      if (s instanceof CoreModel.Return r) {
        AsterExpressionNode returnExpr = buildExpr(r.expr);
        returnExpr = maybeWrapForType(returnExpr, currentReturnType());
        list.add(new ReturnNode(returnExpr));
      } else if (s instanceof CoreModel.If iff) {
        list.add(IfNode.create(buildExpr(iff.cond), buildBlock(iff.thenBlock), buildBlock(iff.elseBlock)));
      } else if (s instanceof CoreModel.Let let) {
        // 优先使用 frame-based LetNode，回退到 Env-based
        AsterExpressionNode valueNode = buildExpr(let.expr);
        if (slots != null && slots.containsKey(let.name)) {
          list.add(LetNodeGen.create(let.name, slots.get(let.name), valueNode));
        } else {
          list.add(new LetNodeEnv(let.name, valueNode, env));
        }
      } else if (s instanceof CoreModel.Match mm) {
        list.add(buildMatch(mm));
      } else if (s instanceof CoreModel.Scope sc) {
        list.add(buildScope(sc));
      } else if (s instanceof CoreModel.Set set) {
        // 优先使用 frame-based SetNode，回退到 Env-based
        AsterExpressionNode valueNode = buildExpr(set.expr);
        if (slots != null && slots.containsKey(set.name)) {
          list.add(SetNodeGen.create(set.name, slots.get(set.name), valueNode));
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
    java.util.Map<String, java.util.Set<String>> dependencies = new java.util.LinkedHashMap<>();
    boolean hasAnyCompensation = false;  // 跟踪是否存在任何补偿逻辑

    for (int i = 0; i < steps.size(); i++) {
      CoreModel.Step step = steps.get(i);
      if (step == null) {
        stepBodies[i] = LiteralNode.create(null);
        compensateBodies[i] = null;
        stepNames[i] = null;
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

      java.util.List<String> deps = step.dependencies;
      if (deps != null && !deps.isEmpty() && step.name != null) {
        java.util.LinkedHashSet<String> depSet = new java.util.LinkedHashSet<>();
        for (String dep : deps) {
          if (dep != null && !dep.isEmpty()) {
            depSet.add(dep);
          }
        }
        if (!depSet.isEmpty()) {
          dependencies.put(step.name, depSet);
        }
      }
    }

    long timeoutMs = 0L;
    if (wf.timeout != null) {
      timeoutMs = wf.timeout.milliseconds;
    }

    // 只有存在补偿逻辑时才传递补偿数组，否则使用旧构造器（优化内存）
    if (hasAnyCompensation) {
      return new WorkflowNode(env, stepBodies, compensateBodies, stepNames, dependencies, timeoutMs);
    } else {
      return new WorkflowNode(env, stepBodies, stepNames, dependencies, timeoutMs);
    }
  }

  private AsterExpressionNode buildExpr(CoreModel.Expr e) {
    if (e instanceof CoreModel.StringE s) return LiteralNode.create(s.value);
    if (e instanceof CoreModel.Bool b) return LiteralNode.create(b.value);
    if (e instanceof CoreModel.IntE i) return LiteralNode.create(Integer.valueOf(i.value));
    if (e instanceof CoreModel.LongE l) return LiteralNode.create(Long.valueOf(l.value));
    if (e instanceof CoreModel.DoubleE d) return LiteralNode.create(Double.valueOf(d.value));
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

        // Collect local variables from lambda body (Let statements)
        java.util.Set<String> bodyLocals = new java.util.LinkedHashSet<>();
        collectLocalVariables(lam.body, bodyLocals);
        for (String local : bodyLocals) {
          if (!slotBuilder.hasVariable(local)) {  // 避免与参数/捕获变量重复
            slotBuilder.addLocal(local);
          }
        }

        com.oracle.truffle.api.frame.FrameDescriptor frameDescriptor = slotBuilder.build();

        // Build body node with parameter and capture slots in scope
        java.util.Map<String,Integer> lambdaSlots = slotBuilder.getSymbolTable();
        Node body = withReturnType(lam.ret, () -> withParamSlots(lambdaSlots, () -> buildBlock(lam.body)));

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
      // 检测 builtin 调用：如果 target 是 Name 且是已注册的 builtin，使用 BuiltinCallNode 优化
      if (c.target instanceof CoreModel.Name targetName) {
        String name = targetName.name;
        if (Builtins.has(name)) {
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
    return LiteralNode.create(null);
  }

  private Node buildMatch(CoreModel.Match mm) {
    var patCases = new java.util.ArrayList<aster.truffle.nodes.MatchNode.CaseNode>();
    if (mm.cases != null) {
      for (var c : mm.cases) {
        aster.truffle.nodes.MatchNode.PatternNode pn = buildPatternNode(c.pattern);
        Node body;
        if (c.body instanceof CoreModel.Scope sc) {
          body = buildScope(sc);
        } else if (c.body != null) {
          // 将所有非 Scope 的语句包装为单语句 Block,确保正确处理 Let/Set/Start/Wait 等
          CoreModel.Block singleStmtBlock = new CoreModel.Block();
          singleStmtBlock.statements = java.util.List.of(c.body);
          body = buildBlock(singleStmtBlock);
        } else {
          body = LiteralNode.create(null);
        }
        patCases.add(new aster.truffle.nodes.MatchNode.CaseNode(pn, body));
      }
    }
    return aster.truffle.nodes.MatchNode.create(env, buildExpr(mm.expr), patCases);
  }

  private aster.truffle.nodes.MatchNode.PatternNode buildPatternNode(CoreModel.Pattern p) {
    if (p instanceof CoreModel.PatNull) return new aster.truffle.nodes.MatchNode.PatNullNode();
    if (p instanceof CoreModel.PatName pn) return new aster.truffle.nodes.MatchNode.PatNameNode(pn.name);
    if (p instanceof CoreModel.PatInt pi) return new aster.truffle.nodes.MatchNode.PatIntNode(pi.value);
    if (p instanceof CoreModel.PatCtor pc) {
      java.util.List<aster.truffle.nodes.MatchNode.PatternNode> args = new java.util.ArrayList<>();
      if (pc.args != null) for (var a : pc.args) args.add(buildPatternNode(a));
      return new aster.truffle.nodes.MatchNode.PatCtorNode(pc.typeName, pc.names, args);
    }
    return new aster.truffle.nodes.MatchNode.PatNameNode("_");
  }

  private Node buildScope(CoreModel.Scope sc) {
    java.util.ArrayList<Node> list = new java.util.ArrayList<>();
    Env previousEnv = this.env;
    Env scopeEnv = (previousEnv != null) ? previousEnv.createChild() : new Env();
    java.util.Map<String,Integer> slots = currentParamSlots();
    java.util.Set<String> scopeLocals = new java.util.LinkedHashSet<>();
    this.env = scopeEnv;
    try {
      if (sc.statements != null) for (var s : sc.statements) {
        if (s instanceof CoreModel.Return r) {
          AsterExpressionNode returnExpr = buildExpr(r.expr);
          returnExpr = maybeWrapForType(returnExpr, currentReturnType());
          list.add(new ReturnNode(returnExpr));
        }
        else if (s instanceof CoreModel.Let let) {
          scopeLocals.add(let.name);
          list.add(new LetNodeEnv(let.name, buildExpr(let.expr), scopeEnv));
        }
        else if (s instanceof CoreModel.If iff) list.add(IfNode.create(buildExpr(iff.cond), buildBlock(iff.thenBlock), buildBlock(iff.elseBlock)));
        else if (s instanceof CoreModel.Match match) {
          list.add(buildMatch(match));
        }
        else if (s instanceof CoreModel.Scope nestedScope) {
          list.add(buildScope(nestedScope));
        }
        else if (s instanceof CoreModel.Set set) {
          AsterExpressionNode valueNode = buildExpr(set.expr);
          if (!scopeLocals.contains(set.name) && slots != null && slots.containsKey(set.name)) {
            list.add(SetNodeGen.create(set.name, slots.get(set.name), valueNode));
          } else {
            list.add(new SetNodeEnv(set.name, valueNode, scopeEnv));
          }
        }
        else if (s instanceof CoreModel.Start st) list.add(new StartNode(scopeEnv, st.name, buildExpr(st.expr)));
        else if (s instanceof CoreModel.Wait wt) list.add(new WaitNode(scopeEnv, ((wt.names != null) ? wt.names : java.util.List.<String>of()).toArray(new String[0])));
        else if (s instanceof CoreModel.Workflow wf) list.add(buildWorkflow(wf));
      }
    } finally {
      this.env = previousEnv;
    }
    return BlockNode.create(list);
  }

  private AsterExpressionNode buildConstruct(CoreModel.Construct cons) {
    CoreModel.Data dataDefinition = requireDataDefinition(cons.typeName);
    java.util.LinkedHashMap<String, AsterExpressionNode> orderedFields = prepareDataFields(cons, dataDefinition);
    return ConstructNode.create(cons.typeName, orderedFields, dataDefinition);
  }

  private AsterExpressionNode buildName(String name) {
    // If name is an enum variant, return an enum value object
    if (enumVariantToEnum != null) {
      String en = enumVariantToEnum.get(name);
      if (en != null) {
        return LiteralNode.create(new AsterEnumValue(en, name));
      }
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

  private AsterExpressionNode buildSimpleName(String name) {
    java.util.Map<String,Integer> slots = currentParamSlots();
    if (slots != null && slots.containsKey(name)) {
      return NameNodeGen.create(name, slots.get(name));
    }
    return new NameNodeEnv(env, name);
  }

  private Node buildFunctionBody(CoreModel.Func fn) {
    if (fn == null) return LiteralNode.create(null);
    return withReturnType(fn.ret, () -> {
      if (fn == entryFunction && entryParamSlots != null && !entryParamSlots.isEmpty()) {
        return withParamSlots(entryParamSlots, () -> buildBlock(fn.body));
      }
      return buildBlock(fn.body);
    });
  }

  private <T> T withParamSlots(java.util.Map<String,Integer> slots, java.util.function.Supplier<T> supplier) {
    if (slots == null || slots.isEmpty()) return supplier.get();
    paramSlotStack.push(slots);
    try {
      return supplier.get();
    } finally {
      paramSlotStack.pop();
    }
  }

  private java.util.Map<String,Integer> currentParamSlots() {
    return paramSlotStack.isEmpty() ? null : paramSlotStack.peek();
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

  /**
   * 收集 Block 中所有 Let 语句声明的局部变量名。
   *
   * 递归遍历所有语句，包括 If/Match/Scope 中嵌套的 Block。
   * 用于在构建 FrameDescriptor 前预先分配所有局部变量的 slots。
   *
   * @param block 要扫描的 Block
   * @param locals 用于收集变量名的 Set
   */
  private void collectLocalVariables(CoreModel.Block block, java.util.Set<String> locals) {
    if (block == null || block.statements == null) return;

    for (var stmt : block.statements) {
      if (stmt instanceof CoreModel.Let let) {
        // Let 语句声明新的局部变量
        locals.add(let.name);
      } else if (stmt instanceof CoreModel.If iff) {
        // If 语句的两个分支可能声明局部变量
        collectLocalVariables(iff.thenBlock, locals);
        collectLocalVariables(iff.elseBlock, locals);
      } else if (stmt instanceof CoreModel.Scope) {
        // Scope 块内声明的变量不逃逸到外层作用域，不纳入 frame slots
        continue;
      } else if (stmt instanceof CoreModel.Match match) {
        // Match 语句的每个 case 可能有局部变量
        if (match.cases != null) {
          for (var c : match.cases) {
            if (c.body instanceof CoreModel.Scope scopeBody) {
              CoreModel.Block caseBlock = new CoreModel.Block();
              caseBlock.statements = scopeBody.statements;
              collectLocalVariables(caseBlock, locals);
            } else if (c.body != null) {
              CoreModel.Block caseBlock = new CoreModel.Block();
              caseBlock.statements = java.util.List.of(c.body);
              collectLocalVariables(caseBlock, locals);
            }
          }
        }
      }
      // Set/Return/Wait/Start 不声明新变量，跳过
    }
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
