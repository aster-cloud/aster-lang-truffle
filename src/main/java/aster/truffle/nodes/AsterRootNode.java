package aster.truffle.nodes;

import aster.truffle.AsterContext;
import aster.truffle.AsterLanguage;
import aster.truffle.core.CoreModel;
import aster.truffle.runtime.FrameSlotBuilder;
import aster.truffle.runtime.interop.AsterInteropAdapter;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.RootNode;

import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Map;
public final class AsterRootNode extends RootNode {
  private final Node body;
  private final Env globalEnv;
  private final List<CoreModel.Param> params;
  private final FrameDescriptor frameDescriptor;
  private final Map<String, Integer> symbolTable;
  private final List<String> effects;

  public AsterRootNode(AsterLanguage lang, Node body, Env globalEnv, List<CoreModel.Param> params, List<String> effects) {
    this(lang, body, globalEnv, params, effects, initFrame(params));
  }

  private AsterRootNode(
      AsterLanguage lang,
      Node body,
      Env globalEnv,
      List<CoreModel.Param> params,
      List<String> effects,
      FrameInit frameInit) {
    super(lang, frameInit.descriptor);
    this.body = body;
    this.globalEnv = globalEnv;
    this.params = params;
    this.effects = effects;
    this.frameDescriptor = frameInit.descriptor;
    this.symbolTable = frameInit.symbolTable;
  }

  /**
   * 顶层程序入口，直接委托给 Loader 生成的节点树执行。
   * 捕获 ReturnException 以兼容旧运行时语义。
   * 在执行前设置入口函数的 effect 权限。
   */
  @Override
  public Object execute(VirtualFrame frame) {
    AsterContext context = AsterLanguage.getContext();
    // 审计 #43 High：顶层 eval 边界**总是**设置 effect 权限（空 effects → 空集=deny-all），
    // 不能像旧代码那样"空就跳过 set"。effectPermissions 现为 ThreadLocal——若跳过，同一宿主/
    // 池线程上一次 eval 遗留的权限会被本次无 effect 的程序继承（fail-open 跨顺序 eval 越权）。
    // 保存 previous 并在 finally 恢复，保证本次 eval 的权限视图不外泄到线程后续使用。
    Set<String> previousEffects = context.getAllowedEffects();
    context.setAllowedEffects(effects == null ? Set.of() : new HashSet<>(effects));

    bindArgumentsToFrame(frame);
    bindArgumentsToEnv(frame);
    // 入口函数返回值跨宿主边界：null 须规整为 guest-null（toInteropValue），否则
    // 裸 null 经 asGuestValue 触发 NPE/契约违例。adapt 仍只做集合/结构归一，保留
    // 嵌套 raw null（底层 Map/List 内部消费依赖它）。
    try {
      Object result = Exec.exec(body, frame);
      Object adapted = AsterInteropAdapter.adapt(result);
      return context.getEnv().asGuestValue(
          aster.truffle.runtime.interop.InteropValues.toInteropValue(adapted));
    } catch (ReturnNode.ReturnException rex) {
      Object adapted = AsterInteropAdapter.adapt(rex.value);
      return context.getEnv().asGuestValue(
          aster.truffle.runtime.interop.InteropValues.toInteropValue(adapted));
    } finally {
      // 恢复本次 eval 之前的线程权限视图，防止残留跨顺序 eval 泄漏（fail-closed）。
      context.setAllowedEffects(previousEffects);
    }
  }

  public Env getGlobalEnv() {
    return globalEnv;
  }

  private void bindArgumentsToFrame(VirtualFrame frame) {
    if (params == null || params.isEmpty()) return;
    Object[] args = frame.getArguments();
    int count = Math.min(params.size(), args != null ? args.length : 0);
    for (int i = 0; i < count; i++) {
      frame.setObject(i, args[i]);
    }
  }

  private void bindArgumentsToEnv(VirtualFrame frame) {
    if (params == null || params.isEmpty()) return;
    Object[] args = frame.getArguments();
    int count = Math.min(params.size(), args != null ? args.length : 0);
    for (int i = 0; i < count; i++) {
      CoreModel.Param param = params.get(i);
      globalEnv.set(param.name, args[i]);
    }
  }

  public Map<String, Integer> getSymbolTable() {
    return symbolTable;
  }

  private static FrameInit initFrame(List<CoreModel.Param> params) {
    FrameSlotBuilder builder = new FrameSlotBuilder();
    if (params != null) {
      for (CoreModel.Param param : params) {
        builder.addParameter(param.name);
      }
    }
    FrameDescriptor descriptor = builder.build();
    return new FrameInit(descriptor, builder.getSymbolTable());
  }

  private record FrameInit(FrameDescriptor descriptor, Map<String, Integer> symbolTable) {}
}
