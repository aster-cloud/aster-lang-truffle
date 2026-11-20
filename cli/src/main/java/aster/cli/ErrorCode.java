package aster.cli;

/**
 * 包格式 v1 的错误码、退出码与固定 message（aster-lang-test {@code conformance/package-v1/exit-codes.json}）。
 *
 * <p>信封的 {@code error.message} 逐字等于这里的 message（ADR 0059 修订 2 R1），诊断细节只写 stderr。
 * {@code VectorsTest} 守住本表与真相源（{@code codes} ∪ {@code codesSince} 的各次版本）逐项相同。
 */
public enum ErrorCode {
    COMPILE_ERROR(4, "source failed to compile"),
    DISPLAY_AMBIGUOUS(2, "edited display text maps onto the package layout in more than one way"),
    DISPLAY_REJECTED(2, "edited display text would hide a value from readers"),
    DISPLAY_UNMAPPABLE(2, "edited display text does not map onto the package layout"),
    ENTRY_NOT_FOUND(2, "entry function not found or not runnable"),
    EVAL_ERROR(1, "policy evaluation failed"),
    INPUT_INVALID(2, "input does not match the entry's input schema"),
    PKG_DISPLAY_INVALID(4, "package display layout is not valid"),
    PKG_INTEGRITY(3, "package integrity check failed"),
    PKG_INVALID_MANIFEST(4, "manifest is not valid"),
    PKG_IR_DRIFT(4, "locally compiled IR differs from the package; use an A package or the CLI version recorded in runtime.cli.version"),
    PKG_SIGNATURE(3, "manifest signature is invalid"),
    PKG_UNSIGNED(3, "package is unsigned; pass --allow-unsigned to accept it"),
    PKG_UNSUPPORTED_FORMAT(4, "package format version is not supported"),
    PKG_UNSUPPORTED_IR(4, "Core IR version is not supported"),
    PKG_UNTRUSTED_KID(3, "signing key id is not in the trust anchor"),
    PROBE_MISMATCH(1, "a probe result differs from its expected output"),
    RESOURCE_EXHAUSTED(5, "evaluation exhausted a resource limit"),
    TIMEOUT(5, "evaluation timed out"),
    USAGE(2, "invalid command line");

    private final int exit;
    private final String message;

    ErrorCode(int exit, String message) {
        this.exit = exit;
        this.message = message;
    }

    public int exit() {
        return exit;
    }

    public String message() {
        return message;
    }
}
