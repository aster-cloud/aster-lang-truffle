package aster.truffle.nodes;

import aster.truffle.runtime.AsterDataValue;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.InvalidArrayIndexException;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.interop.UnknownKeyException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.nodes.NodeInfo;

import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

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

        // 最后尝试解包 HostObject（仅作为兜底）
        Object unwrapped = unwrapHostObject(base);
        if (unwrapped != base) {
            return accessMember(unwrapped, member);
        }

        throw new RuntimeException("无法访问成员：对象类型 " + base.getClass().getName() + " 不支持成员访问，成员：" + member);
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
     * 尝试解包 Polyglot HostObject 获取底层 Java 对象。
     *
     * <p>该路径仅在 InteropLibrary 标准 API（hasMembers/hasHashEntries 等）
     * 都无法访问目标对象时作为兜底使用。Truffle 内部的 HostObject 类不暴露
     * 公开 API，故只能反射获取 {@code obj}/{@code delegate} 字段。
     *
     * <p>安全收紧：
     * <ul>
     *   <li>{@link CompilerDirectives.TruffleBoundary} 标注：阻止反射进入 PE 编译。</li>
     *   <li>失败异常按类别分类计数（NoSuchField / InaccessibleObject / IllegalAccess）。
     *       命中 JDK 17+ 模块访问限制时，第一次会以 WARNING 级别记录全栈，
     *       避免静默吞错；后续命中只递增计数器，避免污染日志。</li>
     *   <li>JDK 17+ {@link InaccessibleObjectException} 单独识别 —— 这是
     *       native-image / 严格模块场景下最常见的失败原因，对运维更友好。</li>
     * </ul>
     */
    // TODO(#14): replace this reflective HostObject/HostProxy field read with an
    // InteropLibrary-only access path (or register these Graal-internal classes for
    // native-image), so member access on host-wrapped objects doesn't throw
    // InaccessibleObjectException under native image. Deferred from PR #14.
    @CompilerDirectives.TruffleBoundary
    private Object unwrapHostObject(Object obj) {
        if (obj == null) return null;

        String className = obj.getClass().getName();
        if (!(className.contains("HostObject") || className.contains("HostProxy"))) {
            return obj;
        }

        Field objField = findField(obj.getClass(), "obj");
        if (objField != null) {
            Object unwrapped = tryReflectiveRead(objField, obj);
            if (unwrapped != null) return unwrapped;
        }

        Field delegateField = findField(obj.getClass(), "delegate");
        if (delegateField != null) {
            Object unwrapped = tryReflectiveRead(delegateField, obj);
            if (unwrapped != null) return unwrapped;
        }

        return obj;
    }

    private static Object tryReflectiveRead(Field field, Object obj) {
        try {
            field.setAccessible(true);
            return field.get(obj);
        } catch (InaccessibleObjectException e) {
            logUnwrapFailure("inaccessible-object", field, e);
        } catch (IllegalAccessException e) {
            logUnwrapFailure("illegal-access", field, e);
        } catch (SecurityException e) {
            logUnwrapFailure("security-manager", field, e);
        } catch (RuntimeException e) {
            // 仅捕获 RuntimeException（保留 Error 让 JVM 处理）
            logUnwrapFailure("runtime", field, e);
        }
        return null;
    }

    private static final AtomicInteger UNWRAP_FAILURE_COUNT = new AtomicInteger(0);
    private static final int UNWRAP_FAILURE_LOG_LIMIT = 5;
    private static final Logger LOG = Logger.getLogger(MemberAccessNode.class.getName());

    private static void logUnwrapFailure(String reason, Field field, Throwable t) {
        int n = UNWRAP_FAILURE_COUNT.incrementAndGet();
        if (n <= UNWRAP_FAILURE_LOG_LIMIT) {
            LOG.log(Level.WARNING,
                String.format("HostObject 反射解包失败（%s）field=%s 第%d次（前%d次会记录详细堆栈）",
                    reason, field.getName(), n, UNWRAP_FAILURE_LOG_LIMIT),
                t);
        }
    }

    private static Field findField(Class<?> clazz, String name) {
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
