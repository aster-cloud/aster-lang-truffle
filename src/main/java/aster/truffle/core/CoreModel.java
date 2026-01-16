package aster.truffle.core;

import com.fasterxml.jackson.annotation.*;
import java.util.*;

public final class CoreModel {
  public static final class Module { public String name; public List<Decl> decls; }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = Import.class, name = "Import"),
    @JsonSubTypes.Type(value = Func.class, name = "Func"),
    @JsonSubTypes.Type(value = Enum.class, name = "Enum"),
    @JsonSubTypes.Type(value = Data.class, name = "Data")
  })
  public sealed interface Decl permits Import, Func, Enum, Data {}

  @JsonTypeName("Import")
  public static final class Import implements Decl { public String path; public String alias; }
  @JsonTypeName("Func")
  public static final class Func implements Decl {
    public String name;
    public java.util.List<String> typeParams;
    public List<Param> params;
    public Type ret;
    public java.util.List<String> effects; // Effect enum serialized as strings
    public Block body;
  }
  @JsonTypeName("Enum")
  public static final class Enum implements Decl { public String name; public java.util.List<String> variants; }
  @JsonTypeName("Data")
  public static final class Data implements Decl { public String name; public java.util.List<Field> fields; }
  public static final class Field { public String name; public Type type; }
  public static final class Param { public String name; public Type type; }

  public static final class Block { public List<Stmt> statements; }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = Return.class, name = "Return"),
    @JsonSubTypes.Type(value = If.class, name = "If"),
    @JsonSubTypes.Type(value = Match.class, name = "Match"),
    @JsonSubTypes.Type(value = Scope.class, name = "Scope"),
    @JsonSubTypes.Type(value = Let.class, name = "Let"),
    @JsonSubTypes.Type(value = Set.class, name = "Set"),
    @JsonSubTypes.Type(value = Start.class, name = "Start"),
    @JsonSubTypes.Type(value = Wait.class, name = "Wait"),
    @JsonSubTypes.Type(value = Workflow.class, name = "workflow")
  })
  public sealed interface Stmt permits Return, If, Match, Scope, Let, Set, Start, Wait, Workflow {}
  @JsonTypeName("Return") public static final class Return implements Stmt { public Expr expr; }
  @JsonTypeName("If") public static final class If implements Stmt { public Expr cond; public Block thenBlock; public Block elseBlock; }
  @JsonTypeName("Let") public static final class Let implements Stmt { public String name; public Expr expr; }
  @JsonTypeName("Set") public static final class Set implements Stmt { public String name; public Expr expr; }
  @JsonTypeName("Start") public static final class Start implements Stmt { public String name; public Expr expr; }
  @JsonTypeName("Wait") public static final class Wait implements Stmt { public java.util.List<String> names; }
  @JsonTypeName("Match") public static final class Match implements Stmt { public Expr expr; public java.util.List<Case> cases; }
  @JsonTypeName("Scope") public static final class Scope implements Stmt { public java.util.List<Stmt> statements; }
  @JsonTypeName("Case") public static final class Case { public Pattern pattern; public Stmt body; }
  @JsonTypeName("workflow")
  public static final class Workflow implements Stmt {
    public List<Step> steps;
    public List<String> effectCaps;
    public RetryPolicy retry;
    public Timeout timeout;
  }

  public static final class Step {
    public String kind;
    public String name;
    public Block body;
    public Block compensate;
    public List<String> dependencies;
    public List<String> effectCaps;
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
    @JsonSubTypes.Type(value = NullE.class, name = "Null"),
    @JsonSubTypes.Type(value = Ok.class, name = "Ok"),
    @JsonSubTypes.Type(value = Err.class, name = "Err"),
    @JsonSubTypes.Type(value = Some.class, name = "Some"),
    @JsonSubTypes.Type(value = NoneE.class, name = "None"),
    @JsonSubTypes.Type(value = Construct.class, name = "Construct"),
    @JsonSubTypes.Type(value = Lambda.class, name = "Lambda"),
    @JsonSubTypes.Type(value = AwaitE.class, name = "Await")
  })
  public sealed interface Expr permits StringE, Bool, Name, Call, IntE, LongE, DoubleE, NullE, Ok, Err, Some, NoneE, Construct, Lambda, AwaitE {}
  @JsonTypeName("String") public static final class StringE implements Expr { public String value; }
  @JsonTypeName("Bool") public static final class Bool implements Expr { public boolean value; }
  @JsonTypeName("Name") public static final class Name implements Expr { public String name; }
  @JsonTypeName("Call") public static final class Call implements Expr { public Expr target; public java.util.List<Expr> args; }
  @JsonTypeName("Int") public static final class IntE implements Expr { public int value; }
  @JsonTypeName("Long") public static final class LongE implements Expr { public long value; }
  @JsonTypeName("Double") public static final class DoubleE implements Expr { public double value; }
  @JsonTypeName("Null") public static final class NullE implements Expr {}
  @JsonTypeName("Await") public static final class AwaitE implements Expr { public Expr expr; }
  @JsonTypeName("Ok") public static final class Ok implements Expr { public Expr expr; }
  @JsonTypeName("Err") public static final class Err implements Expr { public Expr expr; }
  @JsonTypeName("Some") public static final class Some implements Expr { public Expr expr; }
  @JsonTypeName("None") public static final class NoneE implements Expr {}
  @JsonTypeName("Construct") public static final class Construct implements Expr { public String typeName; public java.util.List<FieldInit> fields; }
  public static final class FieldInit { public String name; public Expr expr; }
  @JsonTypeName("Lambda") public static final class Lambda implements Expr { public java.util.List<Param> params; public Type ret; public Block body; public java.util.List<String> captures; }

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
  @JsonTypeName("TypeName") public static final class TypeName implements Type { public String name; }
  @JsonTypeName("TypeVar")
  public static final class TypeVar implements Type {
    public String kind = "TypeVar";
    public String name;
  }
  @JsonTypeName("TypeApp")
  public static final class TypeApp implements Type {
    public String kind = "TypeApp";
    public Type base;
    public java.util.List<Type> args;
  }
  @JsonTypeName("Maybe") public static final class Maybe implements Type { public Type type; }
  @JsonTypeName("Option")
  public static final class Option implements Type {
    public String kind = "Option";
    public Type type;
  }
  @JsonTypeName("Result") public static final class Result implements Type { public Type ok; public Type err; }
  // Name ListT/MapT to avoid clashing with java.util.List/Map in annotations
  @JsonTypeName("List") public static final class ListT implements Type { public Type type; }
  @JsonTypeName("Map") public static final class MapT implements Type { public Type key; public Type val; }
  @JsonTypeName("FuncType")
  public static final class FuncType implements Type {
    public String kind = "FuncType";
    public java.util.List<Type> params;
    public Type returnType;
    public java.util.List<String> effects;
  }
  @JsonTypeName("PiiType") public static final class PiiType implements Type {
    public Type baseType;
    public String sensitivity;
    public String category;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = PatNull.class, name = "PatNull"),
    @JsonSubTypes.Type(value = PatCtor.class, name = "PatCtor"),
    @JsonSubTypes.Type(value = PatName.class, name = "PatName"),
    @JsonSubTypes.Type(value = PatInt.class, name = "PatInt")
  })
  public sealed interface Pattern permits PatNull, PatCtor, PatName, PatInt {}
  @JsonTypeName("PatNull") public static final class PatNull implements Pattern {}
  @JsonTypeName("PatCtor") public static final class PatCtor implements Pattern { public String typeName; public java.util.List<String> names; public java.util.List<Pattern> args; }
  @JsonTypeName("PatName") public static final class PatName implements Pattern { public String name; }
  @JsonTypeName("PatInt") public static final class PatInt implements Pattern { public int value; }
}
