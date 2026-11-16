package aster.truffle.embed;

/**
 * {@link SandboxedExecutor#execute} 的结果。
 *
 * <p>{@link Failed#code()} 取值固定为 {@link #EVAL_ERROR}、{@link #ENTRY_NOT_FOUND}、{@link #TIMEOUT}、
 * {@link #RESOURCE_EXHAUSTED}（与包格式 v1 的 {@code exit-codes.json} 同名）；{@link Failed#message()} 是给 stderr 的诊断，
 * 不进输出信封（ADR 0059 修订 2 R1）。
 */
public sealed interface Outcome permits Outcome.Ok, Outcome.Failed {

    String EVAL_ERROR = "EVAL_ERROR";
    String ENTRY_NOT_FOUND = "ENTRY_NOT_FOUND";
    String TIMEOUT = "TIMEOUT";
    String RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED";

    /** 求值成功；{@code hostValue} 为 {@link PolyglotValues#toHost} 的结果（可为 null）。 */
    record Ok(Object hostValue) implements Outcome {}

    /** 求值失败。 */
    record Failed(String code, String message) implements Outcome {}
}
