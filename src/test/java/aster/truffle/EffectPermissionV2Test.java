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
 * Core IR v2 下的运行时 effect 权限（ADR 0055 #5）：IR 只携带小写 io / cpu 与效应变量，
 * async 由 Loader 按模块内容授予，效应变量不授权任何具体效应。
 */
class EffectPermissionV2Test {

    private static String start(String name, String expr) {
        return "{\"kind\":\"Start\",\"name\":" + q(name) + ",\"expr\":" + expr + ",\"origin\":" + ORIGIN + "}";
    }

    private static String lambda(String body) {
        return "{\"kind\":\"Lambda\",\"params\":[],\"ret\":" + typeName("Int") + ",\"body\":" + body
            + ",\"captures\":[],\"origin\":" + ORIGIN + "}";
    }

    private static Object eval(String json) throws Exception {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            return ctx.eval(Source.newBuilder("aster", json, "effects.json").build()).asInt();
        }
    }

    @Test
    void startInsideLambdaBodyIsGrantedAsync() throws Exception {
        // Start 只出现在 Lambda 体内（表达式位置）：扫描必须下探表达式，否则执行时报 requires async effect
        String json = module("m", func("main", List.of(), typeName("Int"), List.of(),
            block(let("f", lambda(block(start("t", intLit(1)), ret(intLit(0))))),
                ret(call(name("f"))))));
        assertEquals(0, eval(json));
    }

    @Test
    void startInHelperIsGrantedForEntry() throws Exception {
        // 权限只在入口设置一次：辅助函数里的 Start 也需要入口拿到 async
        String json = module("m",
            func("main", List.of(), typeName("Int"), List.of(), block(ret(call(name("helper"))))),
            func("helper", List.of(), typeName("Int"), List.of(), block(start("t", intLit(1)), ret(intLit(7)))));
        assertEquals(7, eval(json));
    }

    @Test
    void effectVariableGrantsNoConcreteEffect() {
        // 入口 main<E> performs E 调用 IO 内建：效应变量在运行时没有实例化来源，按 fail-closed 拒绝
        String json = module("m", func("main", List.of(), typeName("Int"), List.of("E"),
            block(ret(call(name("IO.print"), str("hi")))))).replace("\"effectParams\":[]", "\"effectParams\":[\"E\"]");
        PolyglotException e = assertThrows(PolyglotException.class, () -> eval(json));
        assertTrue(e.getMessage().contains("effect not permitted"), e.getMessage());
    }
}
