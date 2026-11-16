package aster.truffle.embed;

/**
 * 宿主 ↔ guest 值边界上的形态违例（见 {@link PolyglotValues}）。
 *
 * <p>aster-api 原版继承 {@code DynamicCnlExecutor.DynamicExecutionException}（执行器层原样上抛）；本副本不依赖执行器，
 * 改为直接继承 {@link RuntimeException}，其余不变（ADR 0059 P1 T7）。REST 层据 {@link #code()} 回稳定错误码。入参违例是调用方可修正的问题（400），
 * 出参违例说明策略产出了无法表示的结果（422）。
 */
public final class ValueShapeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public static final String INPUT_TOO_DEEP = "input_too_deep";
    public static final String INPUT_CYCLIC = "input_cyclic";
    public static final String INPUT_INVALID_KEY = "input_invalid_key";
    public static final String INPUT_UNSUPPORTED_VALUE = "input_unsupported_value";
    public static final String OUTPUT_TOO_DEEP = "output_too_deep";
    public static final String OUTPUT_CYCLIC = "output_cyclic";
    public static final String OUTPUT_INVALID_KEY = "output_invalid_key";

    private final String code;
    private final boolean inputError;

    ValueShapeException(String code, boolean inputError, String message) {
        super(message);
        this.code = code;
        this.inputError = inputError;
    }

    /** 机器可读错误码，REST 响应的 {@code code} 字段原样使用。 */
    public String code() {
        return code;
    }

    /** true=入参违例（调用方可修正，400）；false=出参违例（策略产出不可表示，422）。 */
    public boolean inputError() {
        return inputError;
    }

    /** 建议的 HTTP 状态码。 */
    public int httpStatus() {
        return inputError ? 400 : 422;
    }
}
