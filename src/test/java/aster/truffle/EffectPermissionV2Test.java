package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.junit.jupiter.api.Test;

import java.util.List;

import static aster.truffle.IrV2.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Core IR v2 下的运行时 effect 权限（ADR 0055 #5）：IR 只携带小写 io / cpu 与效应变量。
 * async 不是效应：Start / workflow 是并发构造，不需要任何声明即可执行，但其中的 IO 仍需要 io；
 * 效应变量不授权任何具体效应。
 */
class EffectPermissionV2Test {

    private static String start(String name, String expr) {
        return "{\"kind\":\"Start\",\"name\":" + q(name) + ",\"expr\":" + expr + ",\"origin\":" + ORIGIN + "}";
    }

    private static String waitFor(String name) {
        return "{\"kind\":\"Wait\",\"names\":[" + q(name) + "],\"origin\":" + ORIGIN + "}";
    }

    private static String workflow(String stepBody) {
        String step = "{\"kind\":\"step\",\"name\":\"s\",\"body\":" + stepBody + ",\"compensate\":null,"
            + "\"dependencies\":[],\"effectCaps\":[],\"origin\":" + ORIGIN + "}";
        return "{\"kind\":\"workflow\",\"steps\":[" + step + "],\"effectCaps\":[],\"retry\":null,\"timeout\":null,"
            + "\"origin\":" + ORIGIN + "}";
    }

    private static String main(List<String> effects, String... stmts) {
        return module("m", func("main", List.of(), typeName("Int"), effects, block(stmts)));
    }

    private static String ioPrint() {
        return call(name("IO.print"), str("hi"));
    }

    private static int eval(String json) throws Exception {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            return ctx.eval(Source.newBuilder("aster", json, "effects.json").build()).asInt();
        }
    }

    private static String failure(String json) {
        return assertThrows(PolyglotException.class, () -> eval(json)).getMessage();
    }

    @Test
    void startRunsWithoutAnyDeclaredEffect() throws Exception {
        assertEquals(0, eval(main(List.of(), start("t", intLit(1)), waitFor("t"), ret(intLit(0)))));
    }

    @Test
    void workflowRunsWithoutAnyDeclaredEffect() throws Exception {
        assertEquals(0, eval(main(List.of(), workflow(block(ret(intLit(1)))), ret(intLit(0)))));
    }

    @Test
    void ioInsideWorkflowStillRequiresIo() {
        // 去掉 async 门不等于放开效应：step 体内的 IO 内建照样按 io 权限校验
        String msg = failure(main(List.of(), workflow(block(ret(ioPrint()))), ret(intLit(0))));
        assertTrue(msg.contains("effect not permitted"), msg);
    }

    @Test
    void effectVariableGrantsNoConcreteEffect() {
        // 入口 main<E> performs E 调用 IO 内建：效应变量在运行时没有实例化来源，按 fail-closed 拒绝
        String json = main(List.of("E"), ret(ioPrint())).replace("\"effectParams\":[]", "\"effectParams\":[\"E\"]");
        String msg = failure(json);
        assertTrue(msg.contains("effect not permitted"), msg);
    }
}
