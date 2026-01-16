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
            throw new RuntimeException("Map 中不存在键：" + member);
        }

        // 尝试解包 HostObject（Polyglot 包装的 Java 对象）
        Object unwrapped = unwrapHostObject(base);
        if (unwrapped != base) {
            return accessMember(unwrapped, member);
        }

        // 使用 Polyglot InteropLibrary 处理 TruffleObject
        try {
            // 首先检查是否为 hash 类型（Map 在 Polyglot 中表现为 hash）
            if (interop.hasHashEntries(base)) {
                if (interop.isHashEntryExisting(base, member) && interop.isHashEntryReadable(base, member)) {
                    return interop.readHashValue(base, member);
                }
            }

            // 然后尝试成员访问（用于普通对象属性）
            if (interop.hasMembers(base)) {
                if (interop.isMemberReadable(base, member)) {
                    return interop.readMember(base, member);
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

        throw new RuntimeException("无法访问成员：对象类型 " + base.getClass().getName() + " 不支持成员访问，成员：" + member);
    }

    /**
     * 尝试解包 Polyglot HostObject 获取底层 Java 对象
     */
    private Object unwrapHostObject(Object obj) {
        if (obj == null) return null;

        String className = obj.getClass().getName();

        // 检查是否为 Truffle HostObject
        if (className.contains("HostObject") || className.contains("HostProxy")) {
            try {
                // 尝试通过反射获取 obj 字段
                java.lang.reflect.Field objField = findField(obj.getClass(), "obj");
                if (objField != null) {
                    objField.setAccessible(true);
                    return objField.get(obj);
                }

                // 尝试 delegate 字段
                java.lang.reflect.Field delegateField = findField(obj.getClass(), "delegate");
                if (delegateField != null) {
                    delegateField.setAccessible(true);
                    return delegateField.get(obj);
                }
            } catch (Exception e) {
                // 反射失败，返回原对象
            }
        }

        return obj;
    }

    private java.lang.reflect.Field findField(Class<?> clazz, String name) {
        while (clazz != null) {
            try {
                return clazz.getDeclaredField(name);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        return null;
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
