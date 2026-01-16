package aster.truffle.runtime;

/**
 * 错误消息统一生成工具。
 *
 * <p>为了提升运行时错误的可读性与可恢复性，所有错误消息均提供中英文双语描述并附带恢复提示。
 * 工具类方法返回格式化后的字符串，确保与既有英文关键字兼容，避免破坏黄金测试基准。</p>
 */
public final class ErrorMessages {

  private ErrorMessages() {
    // 禁止实例化工具类
  }

  /**
   * 构造双语消息，保持英文关键字用于兼容既有测试。
   *
   * @param zh 中文描述
   * @param en 英文描述（保留原有关键词）
   * @return 按照“中文 (English)”格式拼接的字符串
   */
  public static String bilingual(String zh, String en) {
    return zh + " (" + en + ")";
  }

  /**
   * 为消息附加恢复提示，提示部分同样采用中英文双语。
   *
   * @param message 主体消息
   * @param hintZh 中文提示
   * @param hintEn 英文提示
   * @return 包含提示信息的完整消息文本
   */
  public static String withHint(String message, String hintZh, String hintEn) {
    return message + "\n提示：" + hintZh + " (Hint: " + hintEn + ")";
  }

  /**
   * 构造除零算术错误消息。
   *
   * @return 带有恢复建议的除零错误描述
   */
  public static String arithmeticDivisionByZero() {
    String message = bilingual("算术错误：除数为 0", "division by zero");
    return withHint(message, "检查输入参数，确保除数非 0", "Check the divisor and ensure it is non-zero");
  }

  /**
   * 构造集合索引越界错误消息。
   *
   * @param index 用户访问的索引
   * @param size 集合实际大小
   * @return 带有恢复建议的集合索引越界描述
   */
  public static String collectionIndexOutOfBounds(int index, int size) {
    String english = "index out of bounds: " + index + " (size=" + size + ")";
    String message = bilingual("集合访问越界：索引 " + index + "，长度 " + size, english);
    return withHint(message, "确认索引满足 0 <= index < size", "Ensure 0 <= index < size");
  }

  /**
   * 构造字符串索引为负数的错误消息。
   *
   * @param index 用户访问的索引
   * @return 带有恢复建议的字符串索引错误描述
   */
  public static String stringIndexNegative(int index) {
    String english = "string index must be non-negative: " + index;
    String message = bilingual("字符串索引不能为负数：" + index, english);
    return withHint(message, "调整起始位置，避免使用负数索引", "Adjust start position to avoid negative index");
  }

  /**
   * 构造类型期望与实际不符的错误消息。
   *
   * @param expected 期望类型描述
   * @param actual 实际类型描述
   * @return 带有恢复建议的类型错误描述
   */
  public static String typeExpectedGot(String expected, String actual) {
    String english = "Expected " + expected + ", got " + actual;
    String message = bilingual("类型不匹配：期望 " + expected + "，实际 " + actual, english);
    return withHint(message, "检查数据来源或转换逻辑，确保类型一致", "Review data source or conversion to ensure types match");
  }

  /**
   * 构造变量未初始化的错误消息。
   *
   * @param name 未初始化的变量名称
   * @return 带有恢复建议的变量初始化错误描述
   */
  public static String variableNotInitialized(String name) {
    String english = "Variable not initialized: " + name;
    String message = bilingual("变量未初始化：" + name, english);
    return withHint(message, "确保在首次读取前进行赋值", "Assign the variable before first read");
  }

  /**
   * 构造模运算除零的错误消息。
   *
   * @return 带有恢复建议的模运算除零描述
   */
  public static String arithmeticModuloByZero() {
    String english = "modulo by zero";
    String message = bilingual("算术错误：模运算除数为 0", english);
    return withHint(message, "校验模运算分母，避免为 0", "Validate the modulo divisor to avoid zero");
  }

  /**
   * 构造空集合访问的错误消息。
   *
   * @param operation 触发错误的集合操作名称
   * @return 带有恢复建议的集合访问错误描述
   */
  public static String collectionEmptyAccess(String operation) {
    String english = operation + ": collection is empty";
    String message = bilingual("集合为空，无法执行操作：" + operation, english);
    return withHint(message, "在调用前确认集合非空或提供默认值", "Ensure the collection is not empty or provide a default");
  }

  /**
   * 构造文本编码无效的错误消息。
   *
   * @param encoding 无法识别的编码名称
   * @return 带有恢复建议的编码错误描述
   */
  public static String textInvalidEncoding(String encoding) {
    String english = "invalid text encoding: " + encoding;
    String message = bilingual("文本编码无效：" + encoding, english);
    return withHint(message, "选择受支持的编码或确认输入来源", "Pick a supported encoding or verify the input source");
  }

  /**
   * 构造异步任务缺失的错误消息。
   *
   * @param taskId 未找到的任务标识
   * @return 带有恢复建议的异步任务错误描述
   */
  public static String asyncTaskNotFound(String taskId) {
    String english = "Async task not found: " + taskId;
    String message = bilingual("异步任务不存在：" + taskId, english);
    return withHint(message, "确认任务已经提交并记录正确 ID", "Ensure the task was scheduled and the ID is correct");
  }

  /**
   * 构造异步操作权限拒绝的错误消息。
   *
   * @param operation 被拒绝的操作名称
   * @return 带有恢复建议的权限错误描述
   */
  public static String asyncPermissionDenied(String operation) {
    String english = operation + ": permission denied";
    String message = bilingual("异步操作权限被拒绝：" + operation, english);
    return withHint(message, "检查任务上下文或授予所需权限", "Review task context or grant the required permission");
  }

  /**
   * 构造操作类型不匹配的错误消息。
   *
   * @param operation 操作名称
   * @param expected 期望的类型描述
   * @param actual 实际类型描述
   * @return 带有恢复建议的类型错误描述
   */
  public static String operationExpectedType(String operation, String expected, String actual) {
    String english = operation + ": expected " + expected + ", got " + actual;
    String message = bilingual("操作 " + operation + " 期望类型 " + expected + "，实际为 " + actual, english);
    return withHint(message, "核对参数或调用结果，确保类型匹配", "Verify arguments or results to ensure type compatibility");
  }

  /**
   * 构造 Lambda 缺少 CallTarget 的错误消息。
   *
   * @param operation 操作名称
   * @return 带有恢复建议的 Lambda 错误描述
   */
  public static String lambdaMissingCallTarget(String operation) {
    String english = operation + ": lambda must have CallTarget";
    String message = bilingual("操作 " + operation + " 使用的 Lambda 缺少 CallTarget", english);
    return withHint(message, "通过编译器或适配器生成可调用的 Lambda", "Ensure the lambda is compiled with an invocable CallTarget");
  }

  /**
   * 构造 Result/Option 解包错误消息。
   *
   * @param operation 操作名称
   * @param variant 实际变体名称
   * @return 带有恢复建议的解包错误描述
   */
  public static String unwrapOnUnexpectedVariant(String operation, String variant) {
    String english = operation + ": unexpected variant " + variant;
    String message = bilingual("操作 " + operation + " 解包时遇到意外变体：" + variant, english);
    return withHint(message, "在调用前判断变体或使用 match 语句", "Inspect the variant before unwrapping or use pattern matching");
  }
}
