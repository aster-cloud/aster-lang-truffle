package aster.cli;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 命令行语法（README「命令行」；参照实现 aster-lang-test {@code scripts/lib/package-v1/argv.mjs} 的 {@code parseCommand}）。
 *
 * <p>不用 picocli：它的缺省语法与包格式 v1 的规则在多处不同（{@code -x} 当作未知选项而非位置参数、开关接受 {@code --flag=true}、
 * {@code @file} 展开、短选项聚合……），逐项关掉之后剩下的只是一个与参照实现逐行对应的小解析器，两个 CLI 才能对同一 argv 给出
 * 同一结论。规则：
 * <ul>
 *   <li>子命令决定认识的选项；未知选项、同一选项两次为 USAGE；</li>
 *   <li>带值选项写作 {@code --opt value} 或 {@code --opt=value}；{@code --opt=} 值为空、{@code --opt ''}、缺值、下一个参数是本子命令认识的
 *       选项（含 {@code --known=…}）或 {@code --} 都是 USAGE；</li>
 *   <li>开关不带值（{@code --switch=…} 为 USAGE）；</li>
 *   <li>{@code --} 之后一律是位置参数；不以 {@code --} 开头的参数（含 {@code -}、{@code -x}）是位置参数。</li>
 * </ul>
 */
final class Argv {

    private record Spec(Set<String> valued, Set<String> switches, int positional) {}

    private static final Map<String, Spec> SPECS = Map.of(
        "run", new Spec(Set.of("--entry", "--input", "--timeout", "--display", "--jwks", "--pubkey"), Set.of("--allow-unsigned"), 1),
        "verify", new Spec(Set.of("--timeout", "--jwks", "--pubkey"), Set.of("--allow-unsigned", "--recompile", "--probes"), 1),
        "inspect", new Spec(Set.of("--jwks", "--pubkey"), Set.of("--allow-unsigned"), 1),
        "compile", new Spec(Set.of("--locale", "--alias-set", "--vocabulary", "--out", "--jwks", "--pubkey"),
            Set.of("--allow-unsigned", "--allow-structural"), 1),
        "version", new Spec(Set.of(), Set.of(), 0),
        // ADR 0059 修订 4 D6：show 打印显示文本（--canonical 打印规范源码，--from-display 打印还原出的规范源码）
        "show", new Spec(Set.of("--from-display", "--jwks", "--pubkey"), Set.of("--allow-unsigned", "--canonical"), 1));

    final String command;
    final List<String> positional;
    private final Map<String, String> options;

    private Argv(String command, List<String> positional, Map<String, String> options) {
        this.command = command;
        this.positional = positional;
        this.options = options;
    }

    /** 带值选项的值；未给出为 null。 */
    String value(String name) {
        return options.get(name);
    }

    /** 开关是否给出。 */
    boolean flag(String name) {
        return options.containsKey(name);
    }

    static Argv parse(String[] args) throws CliException {
        if (args.length == 0) {
            throw usage("缺少子命令");
        }
        String command = args[0];
        Spec spec = SPECS.get(command);
        if (spec == null) {
            throw usage("未知子命令 " + command);
        }
        Map<String, String> options = new HashMap<>();
        List<String> positional = new ArrayList<>();
        boolean endOfOptions = false;
        for (int i = 1; i < args.length; i++) {
            String a = args[i];
            if (endOfOptions || !a.startsWith("--")) {
                positional.add(a);
                continue;
            }
            if (a.equals("--")) {
                endOfOptions = true;
                continue;
            }
            int eq = a.indexOf('=');
            String name = eq >= 0 ? a.substring(0, eq) : a;
            if (!spec.valued().contains(name) && !spec.switches().contains(name)) {
                throw usage("未知选项 " + name);
            }
            if (options.containsKey(name)) {
                throw usage("选项重复 " + name);
            }
            if (spec.switches().contains(name)) {
                if (eq >= 0) {
                    throw usage("开关 " + name + " 不带值");
                }
                options.put(name, "");
                continue;
            }
            String value;
            if (eq >= 0) {
                value = a.substring(eq + 1);
                if (value.isEmpty()) {
                    throw usage(name + "= 的值为空");
                }
            } else {
                if (i + 1 >= args.length || args[i + 1].equals("--")) {
                    throw usage(name + " 缺值");
                }
                String next = args[i + 1];
                int nextEq = next.indexOf('=');
                String nextName = nextEq >= 0 ? next.substring(0, nextEq) : next;
                if (spec.valued().contains(nextName) || spec.switches().contains(nextName)) {
                    throw usage(name + " 的值是另一个选项");
                }
                if (next.isEmpty()) {
                    throw usage(name + " 的值为空");
                }
                value = next;
                i++;
            }
            options.put(name, value);
        }
        if (positional.size() != spec.positional()) {
            throw usage("位置参数个数应为 " + spec.positional());
        }
        if (command.equals("run") && !options.containsKey("--input")) {
            throw usage("run 缺 --input");
        }
        if (command.equals("show") && options.containsKey("--canonical") && options.containsKey("--from-display")) {
            throw usage("--canonical 与 --from-display 互斥");
        }
        return new Argv(command, positional, options);
    }

    private static CliException usage(String why) {
        return new CliException(ErrorCode.USAGE, why);
    }
}
