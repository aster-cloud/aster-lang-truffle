package aster.truffle.types;

import aster.truffle.nodes.LambdaValue;
import com.oracle.truffle.api.dsl.ImplicitCast;
import com.oracle.truffle.api.dsl.TypeSystem;
import java.util.Map;

/**
 * Aster 语言的类型系统定义
 *
 * 定义了 Aster 运行时的核心值类型和隐式类型转换规则。
 * Truffle DSL 处理器会自动生成类型检查和转换方法。
 */
@TypeSystem({
    int.class,
    long.class,
    double.class,
    boolean.class,
    String.class,
    Map.class,
    LambdaValue.class
})
public abstract class AsterTypes {
  // 构造函数必须是 protected，以便 Truffle DSL 生成的子类可以访问
  protected AsterTypes() {}

  // 数值类型的隐式提升链：int → long → double

  @ImplicitCast
  public static long castIntToLong(int value) {
    return value;
  }

  @ImplicitCast
  public static double castIntToDouble(int value) {
    return value;
  }

  @ImplicitCast
  public static double castLongToDouble(long value) {
    return value;
  }
}
