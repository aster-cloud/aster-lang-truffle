package aster.truffle;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static aster.truffle.IrV2.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Core IR v2 的匹配模式语义（ADR 0055 #15、#16）。
 *
 * <p>0 参枚举变体由 lowering 显式产出 {@code PatCtor{typeName, args:[]}}；{@code PatName} 只用于绑定。
 * 此前 Truffle 按「首字母 ASCII 大写」把 PatName 当成变体（ADR 0051 R55），这对没有大小写的文字
 * （中文、印地文）必然失效，v2 删除了这条启发式。
 */
class PatternV2Test {

    /** 入口 entry(n: Int)：Match n，唯一分支为给定模式，未命中返回 -1。 */
    private static String matchOnParam(String pattern, String body, String... extraDecls) {
        String[] decls = new String[extraDecls.length + 1];
        System.arraycopy(extraDecls, 0, decls, 0, extraDecls.length);
        decls[extraDecls.length] = func("entry", List.of(param("n", typeName("Int"))), typeName("Int"), List.of(),
            block(match(name("n"), caseOf(pattern, ret(body))), ret(intLit(-1))));
        return module("m", decls);
    }

    /** 无参入口 main：Match 枚举成员 {@code subject}，各变体分支依次返回 1、2…，未命中返回 -1。 */
    private static String matchOnVariant(String enumName, List<String> variants, String subject) {
        String[] cases = new String[variants.size()];
        for (int i = 0; i < variants.size(); i++) {
            cases[i] = caseOf(patCtor(variants.get(i)), ret(intLit(i + 1)));
        }
        return module("m",
            enumDecl(enumName, variants.toArray(new String[0])),
            func("main", List.of(), typeName("Int"), List.of(),
                block(match(name(subject), cases), ret(intLit(-1)))));
    }

    private static int runWith(String json, int arg) throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            Value program = ctx.eval(Source.newBuilder("aster", json, "pat.json").build());
            return program.execute(arg).asInt();
        }
    }

    private static int run(String json) throws IOException {
        try (Context ctx = Context.newBuilder("aster").allowAllAccess(true).build()) {
            return ctx.eval(Source.newBuilder("aster", json, "pat.json").build()).asInt();
        }
    }

    @Test
    void patctor_zero_arg_matches_variant() throws IOException {
        assertEquals(1, run(matchOnVariant("Color", List.of("Red", "Green"), "Red")));
        assertEquals(2, run(matchOnVariant("Color", List.of("Red", "Green"), "Green")));
    }

    @Test
    void patctor_zero_arg_matches_caseless_variant() throws IOException {
        // 中文变体名没有大小写：只有显式的 PatCtor 才能表达「这是变体」
        assertEquals(2, run(matchOnVariant("决定", List.of("批准", "拒绝"), "拒绝")));
    }

    @Test
    void patname_always_binds() throws IOException {
        // PatName{"Red"} 是绑定：任何非 null 主语都匹配并绑定，不再因首字母大写而按变体名比较
        assertEquals(7, runWith(matchOnParam(patName("Red"), name("Red")), 7));
    }

    @Test
    void nonAsciiUppercaseInitialBindsTheScrutinee() throws IOException {
        assertEquals(7, runWith(matchOnParam(patName("Ärger"), name("Ärger")), 7));
    }

    @Test
    void bindingNamedLikeAnEnumMemberIsReadAsTheBinding() throws IOException {
        // 整数主语下 `bei Ärger,` 是绑定（R12）；分支体读 Ärger 取绑定值，局部名优先于同名枚举成员（ADR 0051 §6.2）
        assertEquals(7, runWith(matchOnParam(patName("Ärger"), name("Ärger"), enumDecl("Laune", "Übel", "Ärger")), 7));
    }

    @Test
    void caselessInitialBinds() throws IOException {
        assertEquals(5, runWith(matchOnParam(patName("其他"), name("其他")), 5));
    }
}
