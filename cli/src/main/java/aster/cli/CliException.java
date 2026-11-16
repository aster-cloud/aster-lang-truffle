package aster.cli;

/**
 * 某一检查阶段的失败：{@link #code()} 决定信封与退出码，{@link #getMessage()} 只写 stderr。
 */
public final class CliException extends Exception {

    private static final long serialVersionUID = 1L;

    private final ErrorCode code;

    public CliException(ErrorCode code, String detail) {
        super(detail);
        this.code = code;
    }

    public CliException(ErrorCode code, String detail, Throwable cause) {
        super(detail, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
