package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Issue #16 / Task 2: IO.* builtins declare an {@code IO} effect, so the effect
 * check must run first. When the entry function does not request the IO effect,
 * calling an IO builtin must fail with a clear "effect not permitted" guest error
 * — not a late {@code UnsupportedOperationException} that bypasses the effect gate.
 */
class IoEffectCheckTest {

    /**
     * Program whose entry ({@code main}) declares the given effects list and whose
     * body calls {@code IO.print("hi")}.
     */
    private static String ioPrintProgram(java.util.List<String> effects) {
        return IrV2.module("test.io.effect",
            IrV2.func("main", java.util.List.of(), IrV2.typeName("Int"), effects,
                IrV2.block(IrV2.ret(IrV2.call(IrV2.name("IO.print"), IrV2.str("hi"))))));
    }

    @Test
    @DisplayName("Task 2: IO builtin with IO effect disallowed -> clear effect error")
    void ioBuiltinWithoutEffectFailsWithEffectError() throws IOException {
        try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
            // Entry declares no effects -> IO is not permitted. The default entry
            // (main) runs during eval, so the effect error surfaces there.
            Source source = Source.newBuilder("aster", ioPrintProgram(java.util.List.of()), "io-disallowed.json").build();

            PolyglotException ex = assertThrows(PolyglotException.class, () -> context.eval(source));
            String msg = ex.getMessage();
            assertTrue(msg.contains("effect not permitted"),
                "expected an effect-not-permitted error, got: " + msg);
        }
    }

    @Test
    @DisplayName("Task 2: IO builtin with IO effect permitted -> passes effect gate, then unsupported")
    void ioBuiltinWithEffectReachesUnsupported() throws IOException {
        try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
            // Entry declares the IO effect -> effect gate passes; Truffle backend
            // has no real IO, so it then fails with the unsupported-operation message.
            // The default entry (main) runs during eval.
            Source source = Source.newBuilder("aster", ioPrintProgram(java.util.List.of("io")), "io-allowed.json").build();

            PolyglotException ex = assertThrows(PolyglotException.class, () -> context.eval(source));
            String msg = ex.getMessage();
            assertTrue(!msg.contains("effect not permitted"),
                "effect gate should pass when IO is permitted, got: " + msg);
            assertTrue(msg.contains("IO.print") || msg.contains("不受支持"),
                "expected an unsupported-IO error after the effect gate, got: " + msg);
        }
    }

    /**
     * v2 IR 的效应名一律小写（ADR 0055 #5）：{@code effects:["io"]} 必须授权 IO builtin。
     * 此前 Builtins 按 {@code "IO"} 比较，小写的 {@code io} 会被误判为未授权。
     */
    @Test
    void io_effect_lowercase_permitted() throws IOException {
        try (Context context = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Source source = Source.newBuilder("aster", ioPrintProgram(java.util.List.of("io")), "io-lower.json").build();
            PolyglotException ex = assertThrows(PolyglotException.class, () -> context.eval(source));
            assertFalse(ex.getMessage().contains("effect not permitted"), "小写 io 必须通过效应校验: " + ex.getMessage());
            assertTrue(ex.getMessage().contains("不受支持"), "通过校验后应报 Truffle 不支持 IO: " + ex.getMessage());
        }
    }
}
