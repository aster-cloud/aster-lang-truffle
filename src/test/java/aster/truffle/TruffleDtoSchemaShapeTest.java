package aster.truffle;

import aster.core.ir.CoreIrJson;
import aster.truffle.core.CoreModel;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Truffle DTO 与 Core IR v2 schema 的形状核对（ADR 0055 R1）。
 *
 * <p>Loader 的绑定是严格的（{@code FAIL_ON_UNKNOWN_PROPERTIES=true}），所以每个 DTO 类必须声明 schema
 * 对应 {@code $defs} 的全部属性：少一个，合法 IR 就绑定失败；多一个，说明 DTO 引入了 schema 没有的字段。
 * schema 取 core jar 中的副本（core 的 sibling parity 测试保证它与 locales 源文件一致）。
 */
class TruffleDtoSchemaShapeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** schema 中每个对象类 {@code $defs} 对应的 Truffle DTO 类；联合类型（Expr、Stmt…）不在此列。 */
    private static final Map<String, Class<?>> DEF_TO_CLASS = new LinkedHashMap<>();
    static {
        DEF_TO_CLASS.put("Position", CoreModel.Position.class);
        DEF_TO_CLASS.put("Origin", CoreModel.Origin.class);
        DEF_TO_CLASS.put("Module", CoreModel.Module.class);
        DEF_TO_CLASS.put("Import", CoreModel.Import.class);
        DEF_TO_CLASS.put("Data", CoreModel.Data.class);
        DEF_TO_CLASS.put("Field", CoreModel.Field.class);
        DEF_TO_CLASS.put("Enum", CoreModel.Enum.class);
        DEF_TO_CLASS.put("Func", CoreModel.Func.class);
        DEF_TO_CLASS.put("Param", CoreModel.Param.class);
        DEF_TO_CLASS.put("Annotation", CoreModel.Annotation.class);
        DEF_TO_CLASS.put("AnnotationArg", CoreModel.AnnotationArg.class);
        DEF_TO_CLASS.put("Required", CoreModel.Required.class);
        DEF_TO_CLASS.put("Range", CoreModel.Range.class);
        DEF_TO_CLASS.put("Pattern", CoreModel.RegexpConstraint.class);
        DEF_TO_CLASS.put("TypeName", CoreModel.TypeName.class);
        DEF_TO_CLASS.put("TypeVar", CoreModel.TypeVar.class);
        DEF_TO_CLASS.put("TypeApp", CoreModel.TypeApp.class);
        DEF_TO_CLASS.put("Result", CoreModel.Result.class);
        DEF_TO_CLASS.put("Maybe", CoreModel.Maybe.class);
        DEF_TO_CLASS.put("Option", CoreModel.Option.class);
        DEF_TO_CLASS.put("List", CoreModel.ListT.class);
        DEF_TO_CLASS.put("Map", CoreModel.MapT.class);
        DEF_TO_CLASS.put("FuncType", CoreModel.FuncType.class);
        DEF_TO_CLASS.put("PiiType", CoreModel.PiiType.class);
        DEF_TO_CLASS.put("Let", CoreModel.Let.class);
        DEF_TO_CLASS.put("Set", CoreModel.Set.class);
        DEF_TO_CLASS.put("Return", CoreModel.Return.class);
        DEF_TO_CLASS.put("If", CoreModel.If.class);
        DEF_TO_CLASS.put("Match", CoreModel.Match.class);
        DEF_TO_CLASS.put("Case", CoreModel.Case.class);
        DEF_TO_CLASS.put("Scope", CoreModel.Scope.class);
        DEF_TO_CLASS.put("Block", CoreModel.Block.class);
        DEF_TO_CLASS.put("Start", CoreModel.Start.class);
        DEF_TO_CLASS.put("Wait", CoreModel.Wait.class);
        DEF_TO_CLASS.put("Workflow", CoreModel.Workflow.class);
        DEF_TO_CLASS.put("Step", CoreModel.Step.class);
        DEF_TO_CLASS.put("RetryPolicy", CoreModel.RetryPolicy.class);
        DEF_TO_CLASS.put("Timeout", CoreModel.Timeout.class);
        DEF_TO_CLASS.put("PatNull", CoreModel.PatNull.class);
        DEF_TO_CLASS.put("PatCtor", CoreModel.PatCtor.class);
        DEF_TO_CLASS.put("PatName", CoreModel.PatName.class);
        DEF_TO_CLASS.put("PatInt", CoreModel.PatInt.class);
        DEF_TO_CLASS.put("Name", CoreModel.Name.class);
        DEF_TO_CLASS.put("Bool", CoreModel.Bool.class);
        DEF_TO_CLASS.put("Int", CoreModel.IntE.class);
        DEF_TO_CLASS.put("Long", CoreModel.LongE.class);
        DEF_TO_CLASS.put("Double", CoreModel.DoubleE.class);
        DEF_TO_CLASS.put("Decimal", CoreModel.DecimalE.class);
        DEF_TO_CLASS.put("String", CoreModel.StringE.class);
        DEF_TO_CLASS.put("Null", CoreModel.NullE.class);
        DEF_TO_CLASS.put("Ok", CoreModel.Ok.class);
        DEF_TO_CLASS.put("Err", CoreModel.Err.class);
        DEF_TO_CLASS.put("Some", CoreModel.Some.class);
        DEF_TO_CLASS.put("None", CoreModel.NoneE.class);
        DEF_TO_CLASS.put("Construct", CoreModel.Construct.class);
        DEF_TO_CLASS.put("FieldInit", CoreModel.FieldInit.class);
        DEF_TO_CLASS.put("Call", CoreModel.Call.class);
        DEF_TO_CLASS.put("Lambda", CoreModel.Lambda.class);
        DEF_TO_CLASS.put("Await", CoreModel.AwaitE.class);
        DEF_TO_CLASS.put("IfExpr", CoreModel.IfE.class);
        DEF_TO_CLASS.put("ListLit", CoreModel.ListE.class);
    }

    private static JsonNode schemaDefs() throws Exception {
        try (InputStream in = CoreIrJson.class.getResourceAsStream(CoreIrJson.SCHEMA_RESOURCE)) {
            assertNotNull(in, "core jar 中缺少 schema 副本 " + CoreIrJson.SCHEMA_RESOURCE);
            return MAPPER.readTree(in).path("$defs");
        }
    }

    /** 联合类型只有 kind 一个属性，靠 if/then 按 kind 分派；其余对象类 $defs 都要有 DTO。 */
    private static boolean isObjectDef(JsonNode def) {
        JsonNode props = def.path("properties");
        boolean union = def.has("allOf") && props.size() == 1 && props.has("kind");
        return "object".equals(def.path("type").asText()) && props.isObject() && !union;
    }

    /** DTO 可绑定的 JSON 属性名；多态子类的 kind 由类型标识承载（@JsonTypeName）。 */
    private static Set<String> dtoProperties(Class<?> type) {
        BeanDescription desc = MAPPER.getDeserializationConfig().introspect(MAPPER.constructType(type));
        Set<String> names = new TreeSet<>();
        for (BeanPropertyDefinition p : desc.findProperties()) {
            if (p.hasSetter() || p.hasField() || p.hasConstructorParameter()) names.add(p.getName());
        }
        if (type.isAnnotationPresent(JsonTypeName.class)) names.add("kind");
        return names;
    }

    @Test
    void every_object_def_has_a_dto() throws Exception {
        Set<String> objectDefs = new TreeSet<>();
        for (Iterator<Map.Entry<String, JsonNode>> it = schemaDefs().fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            if (isObjectDef(e.getValue())) objectDefs.add(e.getKey());
        }
        assertEquals(objectDefs, new TreeSet<>(DEF_TO_CLASS.keySet()), "schema 对象类 $defs 与 DTO 映射表不一致");
    }

    @Test
    void every_dto_matches_schema_properties() throws Exception {
        JsonNode defs = schemaDefs();
        for (Map.Entry<String, Class<?>> e : DEF_TO_CLASS.entrySet()) {
            Set<String> expected = new TreeSet<>();
            defs.path(e.getKey()).path("properties").fieldNames().forEachRemaining(expected::add);
            assertEquals(expected, dtoProperties(e.getValue()), "DTO " + e.getValue().getSimpleName() + " ↔ $defs/" + e.getKey());
        }
    }

    @Test
    void every_dto_class_is_mapped() {
        // CoreModel 中的每个具体 DTO 类都必须出现在映射表里，避免新增类绕开形状核对
        Set<String> mapped = new TreeSet<>();
        DEF_TO_CLASS.values().forEach(c -> mapped.add(c.getSimpleName()));
        Set<String> declared = new TreeSet<>();
        for (Class<?> c : CoreModel.class.getDeclaredClasses()) {
            if (!c.isInterface()) declared.add(c.getSimpleName());
        }
        assertEquals(declared, mapped);
    }
}
