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
     * 辅助方法：创建简单的 Core JSON 程序
     */
    private String createSimpleProgram(String literalValue) {
        // 根据 CoreModel 期望的结构输出基础模块
        return "{" +
                "\"name\":\"test.simple.literal\"," +
                "\"decls\":[{" +
                "\"kind\":\"Func\"," +
                "\"name\":\"main\"," +
                "\"params\":[]," +
                "\"ret\":{\"kind\":\"TypeName\",\"name\":\"Int\"}," +
                "\"effects\":[]," +
                "\"body\":{" +
                "\"kind\":\"Block\"," +
                "\"statements\":[{" +
                "\"kind\":\"Return\"," +
                "\"expr\":{\"kind\":\"Int\",\"value\":" + literalValue + "}" +
                "}]" +
                "}" +
                "}]" +
                "}";
    }

    private String createFunctionProgram() {
        // 构造一个包含辅助函数并在 main 中调用的 Core JSON
        return "{" +
                "\"name\":\"test.simple.function\"," +
                "\"decls\":[" +
                // helper 函数直接返回字面量
                "{" +
                "\"kind\":\"Func\"," +
                "\"name\":\"helper\"," +
                "\"params\":[]," +
                "\"ret\":{\"kind\":\"TypeName\",\"name\":\"Int\"}," +
                "\"effects\":[]," +
                "\"body\":{" +
                "\"kind\":\"Block\"," +
                "\"statements\":[{" +
                "\"kind\":\"Return\"," +
                "\"expr\":{\"kind\":\"Int\",\"value\":100}" +
                "}]" +
                "}" +
                "}," +
                // main 调用 helper
                "{" +
                "\"kind\":\"Func\"," +
                "\"name\":\"main\"," +
                "\"params\":[]," +
                "\"ret\":{\"kind\":\"TypeName\",\"name\":\"Int\"}," +
                "\"effects\":[]," +
                "\"body\":{" +
                "\"kind\":\"Block\"," +
                "\"statements\":[{" +
                "\"kind\":\"Return\"," +
                "\"expr\":{" +
                "\"kind\":\"Call\"," +
                "\"target\":{\"kind\":\"Name\",\"name\":\"helper\"}," +
                "\"args\":[]" +
                "}" +
                "}]" +
                "}" +
                "}" +
                "]" +
                "}";
    }
}
