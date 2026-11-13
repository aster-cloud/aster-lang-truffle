package aster.truffle.core;

import com.fasterxml.jackson.annotation.*;
import java.util.*;

/**
 * Truffle 侧的 Core IR v2 DTO（ADR 0055）。
 *
 * <p>契约由 core jar 中的 schema 副本 {@code /core-ir/core-ir.v2.schema.json} 定义；{@link aster.truffle.Loader}
 * 先调用 core 的 {@code CoreIrJson.validate} 做版本、schema 与不变式检查，再用严格 mapper
 * （{@code FAIL_ON_UNKNOWN_PROPERTIES=true}）绑定到本类。严格绑定要求每个类声明 schema 中的全部字段：
 * 解释器用不到的字段（注解、约束、类型推断标记等）照样声明，只接收不读取。
 * {@code TruffleDtoSchemaShapeTest} 逐类核对属性名集合与 schema {@code $defs} 一致。
 */
public final class CoreModel {
  /** 源码位置（1-based）。 */
  public static final class Position { public int line; public int col; }

  /**
   * 源码范围。每个带 kind 的节点都必带 origin（ADR 0055 #17）；{@code written} / {@code role}
   * 是 ADR 0051 的原写法与解析角色，解释器不读取。
   */
  public static final class Origin {
    public String file;
    public Position start;
    public Position end;
    public String written;
    public String role;
  }

  /** 编译单元。{@code kind} 恒为 Module，{@code schemaVersion} 恒为 2（由 core 校验）。 */
  public static final class Module {
    public String kind;
    public int schemaVersion;
    public String name;
    public String profile;
    public List<Decl> decls;
    public Origin origin;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = Import.class, name = "Import"),
    @JsonSubTypes.Type(value = Func.class, name = "Func"),
    @JsonSubTypes.Type(value = Enum.class, name = "Enum"),
    @JsonSubTypes.Type(value = Data.class, name = "Data")
  })
  public sealed interface Decl permits Import, Func, Enum, Data {}

  @JsonTypeName("Import")
  public static final class Import implements Decl { public String path; public String alias; public Integer version; public Origin origin; }
  @JsonTypeName("Func")
  public static final class Func implements Decl {
    public String name;
    public List<String> typeParams;
    public List<Param> params;
    public Type ret;
    public boolean retTypeInferred;
    public List<Annotation> annotations;
    public List<Annotation> retAnnotations;
    /** 只含小写的授权型效应 io / cpu 与已声明的效应变量（ADR 0055 #5）。 */
    public List<String> effects;
    public List<String> effectParams;
    public List<String> effectCaps;
    public boolean effectCapsExplicit;
    public List<String> inputRefs;
    public Block body;
    public Origin origin;
  }
  @JsonTypeName("Enum")
  public static final class Enum implements Decl { public String name; public List<String> variants; public Origin origin; }
  @JsonTypeName("Data")
  public static final class Data implements Decl { public String name; public List<Field> fields; public Origin origin; }
  public static final class Field {
    public String name;
    public Type type;
    public List<Annotation> annotations;
    public List<Constraint> constraints;
    public boolean typeInferred;
  }
  public static final class Param {
    public String name;
    public Type type;
    public List<Annotation> annotations;
    public List<Constraint> constraints;
    public boolean typeInferred;
  }

  /** 注解：args 按源码顺序（ADR 0055 #3）。解释器不读取注解。 */
  public static final class Annotation { public String name; public List<AnnotationArg> args; }
  /** 注解参数；value 只能是 string / Int32 / boolean。 */
  public static final class AnnotationArg { public String name; public Object value; }

  /** 字段 / 形参约束（ADR 0055 #4）。解释器不读取约束。 */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = Required.class, name = "Required"),
    @JsonSubTypes.Type(value = Range.class, name = "Range"),
    @JsonSubTypes.Type(value = RegexpConstraint.class, name = "Pattern")
  })
  public sealed interface Constraint permits Required, Range, RegexpConstraint {}
  @JsonTypeName("Required") public static final class Required implements Constraint { public Origin origin; }
  /** min / max 是规范十进制串。 */
  @JsonTypeName("Range") public static final class Range implements Constraint { public String min; public String max; public Origin origin; }
  /** 正则约束（schema 名 Pattern，Java 侧改名以免与匹配模式 {@link Pattern} 冲突）。 */
  @JsonTypeName("Pattern") public static final class RegexpConstraint implements Constraint { public String regexp; public Origin origin; }

  /**
   * 语句块。既是 Func / If / Step 的 body，也是 Match 分支体与语句列表中的嵌套块。
   * 作为已声明为 Block 的位置时由 defaultImpl 绑定；schema 仍要求 kind 存在。
   */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind", defaultImpl = Block.class)
  @JsonTypeName("Block")
  public static final class Block implements Stmt { public List<Stmt> statements; public Origin origin; }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = Return.class, name = "Return"),
    @JsonSubTypes.Type(value = If.class, name = "If"),
    @JsonSubTypes.Type(value = Match.class, name = "Match"),
    @JsonSubTypes.Type(value = Scope.class, name = "Scope"),
    @JsonSubTypes.Type(value = Block.class, name = "Block"),
    @JsonSubTypes.Type(value = Let.class, name = "Let"),
    @JsonSubTypes.Type(value = Set.class, name = "Set"),
    @JsonSubTypes.Type(value = Start.class, name = "Start"),
    @JsonSubTypes.Type(value = Wait.class, name = "Wait"),
    @JsonSubTypes.Type(value = Workflow.class, name = "workflow")
  })
  public sealed interface Stmt permits Return, If, Match, Scope, Block, Let, Set, Start, Wait, Workflow {}
  @JsonTypeName("Return") public static final class Return implements Stmt { public Expr expr; public Origin origin; }
  @JsonTypeName("If") public static final class If implements Stmt { public Expr cond; public Block thenBlock; public Block elseBlock; public Origin origin; }
  @JsonTypeName("Let") public static final class Let implements Stmt { public String name; public Expr expr; public Origin origin; }
  @JsonTypeName("Set") public static final class Set implements Stmt { public String name; public Expr expr; public Origin origin; }
  @JsonTypeName("Start") public static final class Start implements Stmt { public String name; public Expr expr; public Origin origin; }
  @JsonTypeName("Wait") public static final class Wait implements Stmt { public List<String> names; public Origin origin; }
  @JsonTypeName("Match") public static final class Match implements Stmt { public Expr expr; public List<Case> cases; public Origin origin; }
  @JsonTypeName("Scope") public static final class Scope implements Stmt { public List<Stmt> statements; public Origin origin; }
  /** 匹配分支（非多态位置，kind 恒为 Case）。 */
  public static final class Case { public String kind; public Pattern pattern; public Stmt body; public Origin origin; }
  @JsonTypeName("workflow")
  public static final class Workflow implements Stmt {
    public List<Step> steps;
    public List<String> effectCaps;
    public RetryPolicy retry;
    public Timeout timeout;
    public Origin origin;
  }

  /** 工作流步骤（非多态位置，kind 恒为 step）。 */
  public static final class Step {
    public String kind;
    public String name;
    public Block body;
    public Block compensate;
    public List<String> dependencies;
    public List<String> effectCaps;
    public Origin origin;
  }

  public static final class RetryPolicy {
    public int maxAttempts;
    public String backoff;
  }

  public static final class Timeout {
    public long milliseconds;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = StringE.class, name = "String"),
    @JsonSubTypes.Type(value = Bool.class, name = "Bool"),
    @JsonSubTypes.Type(value = Name.class, name = "Name"),
    @JsonSubTypes.Type(value = Call.class, name = "Call"),
    @JsonSubTypes.Type(value = IntE.class, name = "Int"),
    @JsonSubTypes.Type(value = LongE.class, name = "Long"),
    @JsonSubTypes.Type(value = DoubleE.class, name = "Double"),
    @JsonSubTypes.Type(value = DecimalE.class, name = "Decimal"),
    @JsonSubTypes.Type(value = NullE.class, name = "Null"),
    @JsonSubTypes.Type(value = Ok.class, name = "Ok"),
    @JsonSubTypes.Type(value = Err.class, name = "Err"),
    @JsonSubTypes.Type(value = Some.class, name = "Some"),
    @JsonSubTypes.Type(value = NoneE.class, name = "None"),
    @JsonSubTypes.Type(value = Construct.class, name = "Construct"),
    @JsonSubTypes.Type(value = Lambda.class, name = "Lambda"),
    @JsonSubTypes.Type(value = AwaitE.class, name = "Await"),
    @JsonSubTypes.Type(value = IfE.class, name = "IfExpr"),
    @JsonSubTypes.Type(value = ListE.class, name = "ListLit")
  })
  public sealed interface Expr permits StringE, Bool, Name, Call, IntE, LongE, DoubleE, DecimalE, NullE, Ok, Err, Some, NoneE, Construct, Lambda, AwaitE, IfE, ListE {}
  @JsonTypeName("String") public static final class StringE implements Expr { public String value; public Origin origin; }
  @JsonTypeName("Bool") public static final class Bool implements Expr { public boolean value; public Origin origin; }
  @JsonTypeName("Name") public static final class Name implements Expr { public String name; public Origin origin; }
  @JsonTypeName("Call") public static final class Call implements Expr { public Expr target; public List<Expr> args; public Origin origin; }
  @JsonTypeName("Int") public static final class IntE implements Expr { public int value; public Origin origin; }
  /** 64 位整数以十进制字符串承载，Loader 求值时再解析为 long。 */
  @JsonTypeName("Long") public static final class LongE implements Expr { public String value; public Origin origin; }
  /**
   * 浮点字面量：value 是规范十进制串（ADR 0055 #18，如 {@code "0.0000001"}），
   * 只在 Loader 构建字面量节点时才转换为 double。
   */
  @JsonTypeName("Double") public static final class DoubleE implements Expr { public String value; public Origin origin; }
  // Decimal 字面量（ADR 0025）：value 是 canonical 十进制字符串，运行时 BigDecimal。
  @JsonTypeName("Decimal") public static final class DecimalE implements Expr { public String value; public Origin origin; }
  @JsonTypeName("Null") public static final class NullE implements Expr { public Origin origin; }
  @JsonTypeName("Await") public static final class AwaitE implements Expr { public Expr expr; public Origin origin; }
  @JsonTypeName("Ok") public static final class Ok implements Expr { public Expr expr; public Origin origin; }
  @JsonTypeName("Err") public static final class Err implements Expr { public Expr expr; public Origin origin; }
  @JsonTypeName("Some") public static final class Some implements Expr { public Expr expr; public Origin origin; }
  @JsonTypeName("None") public static final class NoneE implements Expr { public Origin origin; }
  @JsonTypeName("Construct") public static final class Construct implements Expr { public String typeName; public List<FieldInit> fields; public Origin origin; }
  public static final class FieldInit { public String name; public Expr expr; }
  @JsonTypeName("Lambda") public static final class Lambda implements Expr { public List<Param> params; public Type ret; public Block body; public List<String> captures; public Origin origin; }
  // ADR 0019 G2b：表达式级 if（kind="IfExpr"）。
  @JsonTypeName("IfExpr") public static final class IfE implements Expr { public Expr cond; public Expr thenE; public Expr elseE; public Origin origin; }
  // ADR 0024 C0：列表字面量（kind="ListLit"）。
  @JsonTypeName("ListLit") public static final class ListE implements Expr { public List<Expr> elements; public Origin origin; }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = TypeName.class, name = "TypeName"),
    @JsonSubTypes.Type(value = TypeVar.class, name = "TypeVar"),
    @JsonSubTypes.Type(value = TypeApp.class, name = "TypeApp"),
    @JsonSubTypes.Type(value = Maybe.class, name = "Maybe"),
    @JsonSubTypes.Type(value = Option.class, name = "Option"),
    @JsonSubTypes.Type(value = Result.class, name = "Result"),
    @JsonSubTypes.Type(value = ListT.class, name = "List"),
    @JsonSubTypes.Type(value = MapT.class, name = "Map"),
    @JsonSubTypes.Type(value = FuncType.class, name = "FuncType"),
    @JsonSubTypes.Type(value = PiiType.class, name = "PiiType")
  })
  public sealed interface Type permits TypeName, TypeVar, TypeApp, Maybe, Option, Result, ListT, MapT, FuncType, PiiType {}
  @JsonTypeName("TypeName") public static final class TypeName implements Type { public String name; public Origin origin; }
  @JsonTypeName("TypeVar") public static final class TypeVar implements Type { public String name; public Origin origin; }
  /** 类型应用：base 是被应用的类型名（如 {@code "Box"}）。 */
  @JsonTypeName("TypeApp") public static final class TypeApp implements Type { public String base; public List<Type> args; public Origin origin; }
  @JsonTypeName("Maybe") public static final class Maybe implements Type { public Type type; public Origin origin; }
  @JsonTypeName("Option") public static final class Option implements Type { public Type type; public Origin origin; }
  @JsonTypeName("Result") public static final class Result implements Type { public Type ok; public Type err; public Origin origin; }
  // Name ListT/MapT to avoid clashing with java.util.List/Map in annotations
  @JsonTypeName("List") public static final class ListT implements Type { public Type type; public Origin origin; }
  @JsonTypeName("Map") public static final class MapT implements Type { public Type key; public Type val; public Origin origin; }
  @JsonTypeName("FuncType") public static final class FuncType implements Type { public List<Type> params; public Type ret; public Origin origin; }
  @JsonTypeName("PiiType") public static final class PiiType implements Type {
    public Type baseType;
    public String sensitivity;
    public String category;
    public Origin origin;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = PatNull.class, name = "PatNull"),
    @JsonSubTypes.Type(value = PatCtor.class, name = "PatCtor"),
    @JsonSubTypes.Type(value = PatName.class, name = "PatName"),
    @JsonSubTypes.Type(value = PatInt.class, name = "PatInt")
  })
  public sealed interface Pattern permits PatNull, PatCtor, PatName, PatInt {}
  @JsonTypeName("PatNull") public static final class PatNull implements Pattern { public Origin origin; }
  /** 构造器 / 枚举变体模式：0 参变体为 {@code args:[]}（ADR 0055 #15、#16）。 */
  @JsonTypeName("PatCtor") public static final class PatCtor implements Pattern { public String typeName; public List<Pattern> args; public Origin origin; }
  /** 绑定模式：只用于绑定，不再按首字母大小写当成变体（ADR 0055 #16）。 */
  @JsonTypeName("PatName") public static final class PatName implements Pattern { public String name; public Origin origin; }
  @JsonTypeName("PatInt") public static final class PatInt implements Pattern { public int value; public Origin origin; }
}
