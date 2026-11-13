package aster.truffle;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;

/**
 * 测试用 Core IR v2 JSON 拼装工具（ADR 0055）。
 *
 * <p>Loader 只接受通过 schema 的 v2 IR：所有字段恒输出、每个带 kind 的节点必带 origin。手写完整 JSON
 * 冗长易错，本类按 schema 补齐全部字段，origin 统一用第 1 行第 1 列（测试不关心位置时）。
 * 只负责「写出合法 v2」，不做任何 v1 兼容；产出照样经 Loader 的严格读入校验。
 */
public final class IrV2 {
  private static final ObjectMapper JSON = new ObjectMapper();

  private IrV2() {}

  /** 第 {@code line} 行的合成 origin。 */
  public static String origin(int line) {
    return "{\"file\":null,\"start\":{\"line\":" + line + ",\"col\":1},\"end\":{\"line\":" + line
        + ",\"col\":1},\"written\":null,\"role\":null}";
  }

  public static final String ORIGIN = origin(1);

  /** 把节点自身（最外层、即末尾）的 origin 改为第 {@code line} 行，供需要真实行号的测试使用。 */
  public static String at(String node, int line) {
    int i = node.lastIndexOf(ORIGIN);
    if (i < 0 || i + ORIGIN.length() != node.length() - 1) {
      throw new IllegalArgumentException("节点末尾不是合成 origin: " + node);
    }
    return node.substring(0, i) + origin(line) + "}";
  }

  /** JSON 字符串字面量（带引号与转义）。 */
  public static String q(String s) {
    try {
      return JSON.writeValueAsString(s);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static String arr(List<String> items) {
    return "[" + String.join(",", items) + "]";
  }

  private static String strs(List<String> items) {
    return arr(items.stream().map(IrV2::q).toList());
  }

  // ---------- 模块与声明 ----------

  public static String module(String name, String... decls) {
    return "{\"kind\":\"Module\",\"schemaVersion\":2,\"name\":" + q(name) + ",\"profile\":null,\"decls\":"
        + arr(Arrays.asList(decls)) + ",\"origin\":" + ORIGIN + "}";
  }

  /** 规则 / 函数。{@code effects} 为小写效应名（io / cpu）。 */
  public static String func(String name, List<String> params, String ret, List<String> effects, String body) {
    return "{\"kind\":\"Func\",\"name\":" + q(name) + ",\"typeParams\":[],\"params\":" + arr(params)
        + ",\"ret\":" + ret + ",\"retTypeInferred\":false,\"annotations\":[],\"retAnnotations\":[],\"effects\":"
        + strs(effects) + ",\"effectParams\":[],\"effectCaps\":[],\"effectCapsExplicit\":false,\"inputRefs\":null,"
        + "\"body\":" + body + ",\"origin\":" + ORIGIN + "}";
  }

  public static String param(String name, String type) {
    return "{\"name\":" + q(name) + ",\"type\":" + type
        + ",\"annotations\":[],\"constraints\":[],\"typeInferred\":false}";
  }

  public static String enumDecl(String name, String... variants) {
    return "{\"kind\":\"Enum\",\"name\":" + q(name) + ",\"variants\":" + strs(Arrays.asList(variants))
        + ",\"origin\":" + ORIGIN + "}";
  }

  /** 字段列表中每项为 {@link #field}。 */
  public static String data(String name, String... fields) {
    return "{\"kind\":\"Data\",\"name\":" + q(name) + ",\"fields\":" + arr(Arrays.asList(fields))
        + ",\"origin\":" + ORIGIN + "}";
  }

  public static String field(String name, String type) {
    return "{\"name\":" + q(name) + ",\"type\":" + type
        + ",\"annotations\":[],\"constraints\":[],\"typeInferred\":false}";
  }

  // ---------- 类型 ----------

  public static String typeName(String name) {
    return "{\"kind\":\"TypeName\",\"name\":" + q(name) + ",\"origin\":" + ORIGIN + "}";
  }

  // ---------- 语句 ----------

  public static String block(String... stmts) {
    return "{\"kind\":\"Block\",\"statements\":" + arr(Arrays.asList(stmts)) + ",\"origin\":" + ORIGIN + "}";
  }

  public static String scope(String... stmts) {
    return "{\"kind\":\"Scope\",\"statements\":" + arr(Arrays.asList(stmts)) + ",\"origin\":" + ORIGIN + "}";
  }

  public static String ret(String expr) {
    return "{\"kind\":\"Return\",\"expr\":" + expr + ",\"origin\":" + ORIGIN + "}";
  }

  public static String let(String name, String expr) {
    return "{\"kind\":\"Let\",\"name\":" + q(name) + ",\"expr\":" + expr + ",\"origin\":" + ORIGIN + "}";
  }

  public static String set(String name, String expr) {
    return "{\"kind\":\"Set\",\"name\":" + q(name) + ",\"expr\":" + expr + ",\"origin\":" + ORIGIN + "}";
  }

  public static String ifStmt(String cond, String thenBlock, String elseBlock) {
    return "{\"kind\":\"If\",\"cond\":" + cond + ",\"thenBlock\":" + thenBlock + ",\"elseBlock\":"
        + (elseBlock == null ? "null" : elseBlock) + ",\"origin\":" + ORIGIN + "}";
  }

  public static String match(String expr, String... cases) {
    return "{\"kind\":\"Match\",\"expr\":" + expr + ",\"cases\":" + arr(Arrays.asList(cases))
        + ",\"origin\":" + ORIGIN + "}";
  }

  /** 匹配分支；{@code body} 为 Return 或 Block。 */
  public static String caseOf(String pattern, String body) {
    return "{\"kind\":\"Case\",\"pattern\":" + pattern + ",\"body\":" + body + ",\"origin\":" + ORIGIN + "}";
  }

  // ---------- 模式 ----------

  public static String patName(String name) {
    return "{\"kind\":\"PatName\",\"name\":" + q(name) + ",\"origin\":" + ORIGIN + "}";
  }

  /** 构造器 / 枚举变体模式；0 参变体不传 args。 */
  public static String patCtor(String typeName, String... args) {
    return "{\"kind\":\"PatCtor\",\"typeName\":" + q(typeName) + ",\"args\":" + arr(Arrays.asList(args))
        + ",\"origin\":" + ORIGIN + "}";
  }

  public static String patNull() {
    return "{\"kind\":\"PatNull\",\"origin\":" + ORIGIN + "}";
  }

  public static String patInt(int value) {
    return "{\"kind\":\"PatInt\",\"value\":" + value + ",\"origin\":" + ORIGIN + "}";
  }

  // ---------- 表达式 ----------

  public static String name(String name) {
    return "{\"kind\":\"Name\",\"name\":" + q(name) + ",\"origin\":" + ORIGIN + "}";
  }

  public static String intLit(int value) {
    return "{\"kind\":\"Int\",\"value\":" + value + ",\"origin\":" + ORIGIN + "}";
  }

  public static String str(String value) {
    return "{\"kind\":\"String\",\"value\":" + q(value) + ",\"origin\":" + ORIGIN + "}";
  }

  public static String bool(boolean value) {
    return "{\"kind\":\"Bool\",\"value\":" + value + ",\"origin\":" + ORIGIN + "}";
  }

  /** 浮点字面量，{@code canonical} 为规范十进制串（如 {@code "0.1"}）。 */
  public static String doubleLit(String canonical) {
    return "{\"kind\":\"Double\",\"value\":" + q(canonical) + ",\"origin\":" + ORIGIN + "}";
  }

  public static String nullLit() {
    return "{\"kind\":\"Null\",\"origin\":" + ORIGIN + "}";
  }

  public static String call(String target, String... args) {
    return "{\"kind\":\"Call\",\"target\":" + target + ",\"args\":" + arr(Arrays.asList(args))
        + ",\"origin\":" + ORIGIN + "}";
  }

  /** 构造表达式；{@code fieldInits} 中每项为 {@link #fieldInit}。 */
  public static String construct(String typeName, String... fieldInits) {
    return "{\"kind\":\"Construct\",\"typeName\":" + q(typeName) + ",\"fields\":" + arr(Arrays.asList(fieldInits))
        + ",\"origin\":" + ORIGIN + "}";
  }

  public static String fieldInit(String name, String expr) {
    return "{\"name\":" + q(name) + ",\"expr\":" + expr + "}";
  }

  public static String listLit(String... elements) {
    return "{\"kind\":\"ListLit\",\"elements\":" + arr(Arrays.asList(elements)) + ",\"origin\":" + ORIGIN + "}";
  }
}
