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
    // 设置入口函数的 effect 权限
    AsterContext context = AsterLanguage.getContext();
    if (effects != null && !effects.isEmpty()) {
      context.setAllowedEffects(new HashSet<>(effects));
    }

    bindArgumentsToFrame(frame);
    bindArgumentsToEnv(frame);
    try {
      Object result = Exec.exec(body, frame);
      Object adapted = AsterInteropAdapter.adapt(result);
      return context.getEnv().asGuestValue(adapted);
    } catch (ReturnNode.ReturnException rex) {
      Object adapted = AsterInteropAdapter.adapt(rex.value);
      return context.getEnv().asGuestValue(adapted);
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
