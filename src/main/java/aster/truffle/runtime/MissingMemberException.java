package aster.truffle.runtime;

import aster.truffle.runtime.interop.AsterInteropAdapter;
import aster.truffle.runtime.interop.AsterListValue;
import aster.truffle.runtime.interop.AsterNullValue;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.exception.AbstractTruffleException;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.Node;

import java.util.List;
import java.util.Map;

/**
 * 成员访问失败（读不到键 / 字段 / 成员，或对象根本不支持成员访问）时抛出的结构化 guest 异常。
 *
 * <p>为什么需要：宿主（aster-api）要判断失败是不是「调用方输入缺字段」并给出字段路径。只有文案时，
 * 宿主只能按成员名在输入里猜，规则内部 Map / None / Ok / Err 上的取键失败会被误判成输入缺字段，
 * 输入里多处缺失时也可能报错位置。本异常携带<b>被访问对象的原引用</b>，宿主按对象身份在自己传入的
 * guest 参数里定位：找到即准确路径，找不到即规则内部错误。
 *
 * <p>宿主读取方式：{@code PolyglotException.isGuestException()} 为真时取 {@code getGuestObject()}，
 * 读成员 {@code member}（成员名）、{@code base}（被访问对象）、{@code declaredType}（结构体类型名，
 * 无则为 null）。{@code base} 若本就是合法 interop 值（{@code AsterMapValue}、{@code AsterListValue}、
 * 宿主对象等）则原样返回，宿主可用 {@code Context.asValue(参数).equals(base)} 判断身份；
 * 引擎内部的裸 Java 集合会转换成 interop 值（必然不是调用方输入，身份不会命中）。
 *
 * <p>message 与此前的 {@code RuntimeException} 文案逐字一致，旧的文案匹配消费方不受影响；
 * 继承链仍是 {@link RuntimeException}，引擎内既有的 {@code catch (RuntimeException)} 行为不变。
 */
@ExportLibrary(InteropLibrary.class)
public final class MissingMemberException extends AbstractTruffleException {

    private static final long serialVersionUID = 1L;

    static final String MEMBER = "member";
    static final String BASE = "base";
    static final String DECLARED_TYPE = "declaredType";

    private final String member;
    private final transient Object base;
    private final String declaredType;

    public MissingMemberException(String message, String member, Object base, String declaredType, Node location) {
        super(message, location);
        this.member = member;
        this.base = base;
        this.declaredType = declaredType;
    }

    /** 读取失败的成员名（编译后的形式，未做 umlaut 还原）。 */
    public String getMember() {
        return member;
    }

    /** 被访问对象的原引用；对 null 读成员时为 null。 */
    public Object getBase() {
        return base;
    }

    /** 被访问对象是结构体时的声明类型名，否则为 null。 */
    public String getDeclaredType() {
        return declaredType;
    }

    @ExportMessage
    boolean hasMembers() {
        return true;
    }

    @ExportMessage
    Object getMembers(@SuppressWarnings("unused") boolean includeInternal) {
        return new AsterListValue(List.of(MEMBER, BASE, DECLARED_TYPE));
    }

    @ExportMessage
    boolean isMemberReadable(String name) {
        return MEMBER.equals(name) || BASE.equals(name) || DECLARED_TYPE.equals(name);
    }

    @ExportMessage
    @TruffleBoundary
    Object readMember(String name) throws UnknownIdentifierException {
        return switch (name) {
            case MEMBER -> member;
            case BASE -> interopBase(base);
            case DECLARED_TYPE -> declaredType == null ? AsterNullValue.INSTANCE : declaredType;
            default -> throw UnknownIdentifierException.create(name);
        };
    }

    /**
     * 把 base 规整为合法 interop 值：已是 interop 值的原样返回以保留身份；引擎内部的裸 Java 集合
     * （如 None/Ok/Err 的 HashMap）经 {@link AsterInteropAdapter} 转换；其它无法表示的对象退化为字符串。
     */
    private static Object interopBase(Object value) {
        if (value == null) {
            return AsterNullValue.INSTANCE;
        }
        if (value instanceof TruffleObject || value instanceof String || value instanceof Boolean
                || value instanceof Character || value instanceof Integer || value instanceof Long
                || value instanceof Double || value instanceof Float || value instanceof Short
                || value instanceof Byte) {
            return value;
        }
        if (value instanceof Map<?, ?> || value instanceof List<?>) {
            return AsterInteropAdapter.adapt(value);
        }
        return String.valueOf(value);
    }
}
