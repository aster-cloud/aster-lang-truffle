package aster.truffle.nodes;

import aster.truffle.runtime.AsterDataValue;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.InvalidArrayIndexException;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.interop.UnknownKeyException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.nodes.NodeInfo;

import java.util.Map;

/**
 * 成员访问节点
 *
 * 用于访问对象的字段，例如：用户.名字
 * 支持 AsterDataValue、Map 和 Polyglot 互操作对象
 *
 * <p>对于宿主（host）包装对象（{@code HostObject}/{@code HostProxy}），统一通过
 * {@link InteropLibrary} 的标准消息（{@code hasMembers}/{@code readMember}、
 * {@code hasHashEntries}/{@code readHashValue}、{@code hasArrayElements} 等）访问。
 * 这要求 polyglot {@code Context} 配置了恰当的 {@code HostAccess}（例如
 * {@code HostAccess.ALL}、{@code allowAllAccess(true)}，或显式
 * {@code allowMapAccess/allowListAccess/allowPublicAccess}）。配置好后宿主
 * {@code Map} 暴露为 hash 条目、宿主 POJO 暴露为可读成员、宿主 {@code List}/数组
 * 暴露为数组元素——无需任何反射解包。
 */
@NodeInfo(shortName = "memberAccess")
public final class MemberAccessNode extends AsterExpressionNode {

    @Child private AsterExpressionNode baseNode;
    @Child private InteropLibrary interop;
    private final String memberName;

    public MemberAccessNode(AsterExpressionNode baseNode, String memberName) {
        this.baseNode = baseNode;
        this.memberName = memberName;
        this.interop = InteropLibrary.getFactory().createDispatched(3);
    }

    @Override
    public Object executeGeneric(VirtualFrame frame) {
        Object base = baseNode.executeGeneric(frame);
        return accessMember(base, memberName);
    }

    private Object accessMember(Object base, String member) {
        if (base == null) {
            throw new RuntimeException("无法访问 null 对象的成员：" + member);
        }

        // 处理 AsterDataValue
        if (base instanceof AsterDataValue dataValue) {
            if (dataValue.hasField(member)) {
                return dataValue.getField(member);
            }
            throw new RuntimeException("类型 " + dataValue.getTypeName() + " 不存在字段：" + member);
        }

        // 处理 Map（用于 JSON 上下文参数）
        if (base instanceof Map<?, ?> map) {
            if (map.containsKey(member)) {
                return map.get(member);
            }
            // 模糊匹配：编译后的字段名可能包含 umlaut（如 reqüstedLimit），
            // 而 Map 键是原始形式（如 requestedLimit）
            String denormalized = denormalizeUmlauts(member);
            if (!denormalized.equals(member) && map.containsKey(denormalized)) {
                return map.get(denormalized);
            }
            // 反向尝试：Map 键可能包含 umlaut，而成员名是 ASCII 形式
            for (Object key : map.keySet()) {
                if (key instanceof String keyStr && denormalizeUmlauts(keyStr).equals(denormalized)) {
                    return map.get(key);
                }
            }
            throw new RuntimeException("Map 中不存在键：" + member);
        }

        // 优先使用 Polyglot InteropLibrary 处理 TruffleObject
        // 这是处理 Polyglot 包装的 Java 对象（如 HostObject 包装的 Map）的标准方式，
        // 比反射解包更可靠
        try {
            // 首先检查是否为 hash 类型（Map 在 Polyglot 中表现为 hash）
            if (interop.hasHashEntries(base)) {
                if (interop.isHashEntryExisting(base, member) && interop.isHashEntryReadable(base, member)) {
                    Object value = interop.readHashValue(base, member);
                    return unboxInteropValue(value);
                }
                // 模糊匹配：尝试 umlaut 还原后的键名
                String denormalized = denormalizeUmlauts(member);
                if (!denormalized.equals(member) && interop.isHashEntryExisting(base, denormalized) && interop.isHashEntryReadable(base, denormalized)) {
                    Object value = interop.readHashValue(base, denormalized);
                    return unboxInteropValue(value);
                }
            }

            // 然后尝试成员访问（用于普通对象属性）
            if (interop.hasMembers(base)) {
                if (interop.isMemberReadable(base, member)) {
                    Object value = interop.readMember(base, member);
                    return unboxInteropValue(value);
                }
                // 模糊匹配
                String denormalized = denormalizeUmlauts(member);
                if (!denormalized.equals(member) && interop.isMemberReadable(base, denormalized)) {
                    Object value = interop.readMember(base, denormalized);
                    return unboxInteropValue(value);
                }
            }

            // 尝试通过数组索引访问（如果 member 是数字）
            if (interop.hasArrayElements(base)) {
                try {
                    long index = Long.parseLong(member);
                    if (interop.isArrayElementReadable(base, index)) {
                        return interop.readArrayElement(base, index);
                    }
                } catch (NumberFormatException ignored) {
                    // member 不是数字，跳过
                }
            }
        } catch (UnsupportedMessageException | UnknownIdentifierException | UnknownKeyException | InvalidArrayIndexException e) {
            throw new RuntimeException("无法访问成员 " + member + "：" + e.getMessage(), e);
        }

        throw new RuntimeException("无法访问成员：对象类型 " + base.getClass().getName()
            + " 不支持成员访问，成员：" + member
            + "（若为宿主对象，请确认 polyglot Context 配置了恰当的 HostAccess，"
            + "使其成员/条目/元素可经 InteropLibrary 访问）");
    }

    /**
     * 将 Polyglot 互操作返回的值转换为 Java 原始类型
     *
     * InteropLibrary 的 readHashValue/readMember 返回的值可能是 Polyglot 包装的对象，
     * 需要解包为 Java 原始类型（String、Integer 等），以便下游节点（如 PatNameNode）
     * 能正确使用 instanceof 进行类型匹配。
     */
    private Object unboxInteropValue(Object value) {
        if (value == null) return null;
        // 已经是 Java 原始类型，直接返回
        if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        // 尝试通过 InteropLibrary 解包
        try {
            if (interop.isString(value)) {
                return interop.asString(value);
            }
            if (interop.isBoolean(value)) {
                return interop.asBoolean(value);
            }
            if (interop.isNumber(value)) {
                if (interop.fitsInInt(value)) {
                    return interop.asInt(value);
                }
                if (interop.fitsInLong(value)) {
                    return interop.asLong(value);
                }
                if (interop.fitsInDouble(value)) {
                    return interop.asDouble(value);
                }
            }
        } catch (UnsupportedMessageException e) {
            // 解包失败，返回原值
        }
        return value;
    }

    /**
     * 将 umlaut 字符还原为 ASCII 等价形式
     *
     * 德语 canonicalization 将 ue→ü, oe→ö, ae→ä，
     * 此方法反向还原，用于模糊匹配 Map 键。
     */
    private static String denormalizeUmlauts(String s) {
        return s.replace("ü", "ue")
                .replace("ö", "oe")
                .replace("ä", "ae")
                .replace("ß", "ss")
                .replace("Ü", "Ue")
                .replace("Ö", "Oe")
                .replace("Ä", "Ae");
    }

    /**
     * 创建成员访问链
     *
     * 例如：buildChain(baseNode, ["a", "b", "c"]) 生成 baseNode.a.b.c
     */
    public static AsterExpressionNode buildChain(AsterExpressionNode base, String[] members) {
        AsterExpressionNode current = base;
        for (String member : members) {
            current = new MemberAccessNode(current, member);
        }
        return current;
    }
}
