package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 测试 AsterLanguage 的基本功能
 */
public class AsterLanguageTest {
    private Context context;

    @BeforeEach
    public void setUp() {
        context = Context.newBuilder("aster")
                .allowAllAccess(true)
                .build();
    }

    @AfterEach
    public void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    public void testSimpleLiteral() {
        // 测试最简单的字面量程序
        String coreJson = createSimpleProgram("42");
        Value result = context.eval("aster", coreJson);
        assertNotNull(result);
        // 暂时只验证不抛异常，具体值验证后续添加
    }

    @Test
    public void testFunctionCall() {
        // 测试简单的函数调用
        // 需要构造一个包含函数定义和调用的 Core JSON
        String coreJson = createFunctionProgram();
        Value result = context.eval("aster", coreJson);
        assertNotNull(result);
    }

    /**
     * 辅助方法：创建简单的 Core IR v2 程序（main 返回整数字面量）
     */
    private String createSimpleProgram(String literalValue) {
        return IrV2.module("test.simple.literal", IrV2.func("main", java.util.List.of(), IrV2.typeName("Int"),
            java.util.List.of(), IrV2.block(IrV2.ret(IrV2.intLit(Integer.parseInt(literalValue))))));
    }

    /** helper 返回字面量，main 调用 helper。 */
    private String createFunctionProgram() {
        return IrV2.module("test.simple.function",
            IrV2.func("helper", java.util.List.of(), IrV2.typeName("Int"), java.util.List.of(),
                IrV2.block(IrV2.ret(IrV2.intLit(100)))),
            IrV2.func("main", java.util.List.of(), IrV2.typeName("Int"), java.util.List.of(),
                IrV2.block(IrV2.ret(IrV2.call(IrV2.name("helper"))))));
    }
}
