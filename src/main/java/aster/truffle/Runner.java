package aster.truffle;
import aster.truffle.runtime.AsterConfig;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

public final class Runner {
  public static void main(String[] args) throws Exception {
    if (args.length > 0) {
      // Parse optional function selection flags from args after the JSON path
      String funcName = AsterConfig.DEFAULT_FUNCTION;
      java.util.List<String> argList = new java.util.ArrayList<>(java.util.Arrays.asList(args));
      java.io.File f = new java.io.File(argList.get(0));

      // Parse flags and collect function arguments
      java.util.List<String> functionArgs = new java.util.ArrayList<>();
      boolean collectingArgs = false;

      // Support --func=<name>, --fn=<name>, --entry=<name>, or --func <name>
      // Support -- separator to mark start of function arguments
      for (int i = 1; i < argList.size(); ) {
        String a = argList.get(i);

        // -- separator marks start of function arguments
        if ("--".equals(a)) {
          argList.remove(i);
          collectingArgs = true;
          continue;
        }

        // If collecting args, everything goes to functionArgs
        if (collectingArgs) {
          functionArgs.add(a);
          argList.remove(i);
          continue;
        }

        // Parse function name flags
        String name = null;
        if (a.startsWith("--func=")) name = a.substring("--func=".length());
        else if (a.startsWith("--fn=")) name = a.substring("--fn=".length());
        else if (a.startsWith("--entry=")) name = a.substring("--entry=".length());
        else if ("--func".equals(a) || "--fn".equals(a) || "--entry".equals(a)) {
          if (i+1 < argList.size()) { name = argList.get(i+1); argList.remove(i+1); }
        }
        if (name != null && !name.isEmpty()) {
          funcName = name; argList.remove(i); continue;
        }
        i++;
      }

      if (!f.isAbsolute() && !f.exists()) {
        // Gradle runs in the ':truffle' project dir; allow paths relative to repo root
        java.io.File root = new java.io.File(System.getProperty("user.dir")).getParentFile();
        if (root != null) {
          java.io.File f2 = new java.io.File(root, args[0]);
          if (f2.exists()) f = f2;
        }
      }

      if (AsterConfig.DEBUG) {
        System.err.println("DEBUG: input=" + f.getAbsolutePath());
        System.err.println("DEBUG: funcName=" + funcName);
      }

      // Create Polyglot context
      try (Context context = Context.newBuilder("aster")
          .allowAllAccess(true)
          .build()) {

        // Load and parse the Aster source file
        Source source = Source.newBuilder("aster", f).build();

        // Evaluate the source and execute with optional arguments
        Value result;
        if (functionArgs.isEmpty()) {
          // No arguments - eval executes the program directly
          result = context.eval(source);
        } else {
          // With arguments - need to get the function and execute with args
          Value program = context.eval(source);
          if (program.canExecute()) {
            Object[] execArgs = convertArguments(functionArgs);
            result = program.execute(execArgs);
          } else {
            // If program is not executable, it's the result itself
            result = program;
          }
        }

        if (AsterConfig.DEBUG) {
          System.err.println("DEBUG: result=" + result);
        }

        // Print result if available
        if (result != null && !result.isNull()) {
          if (result.isNumber()) {
            System.out.println(result.asInt());
          } else if (result.isString()) {
            System.out.println(result.asString());
          } else if (result.isBoolean()) {
            System.out.println(result.asBoolean());
          } else {
            System.out.println(result);
          }
        }
      }

      if (AsterConfig.PROFILE) {
        System.out.print(aster.truffle.nodes.Profiler.dump());
      }
      return;
    }

    // Fallback: print usage
    System.err.println("Usage: Runner <file.json> [--func=<name>] [-- <args...>]");
    System.err.println("  --func=<name>   Specify entry function (default: " + AsterConfig.DEFAULT_FUNCTION + ")");
    System.err.println("  --fn=<name>     Same as --func");
    System.err.println("  --entry=<name>  Same as --func");
    System.err.println("  --              Separator before function arguments");
    System.err.println("");
    System.err.println("Examples:");
    System.err.println("  Runner program.json");
    System.err.println("  Runner program.json --func=greet -- Alice");
    System.err.println("  Runner program.json -- 42 true hello");
  }

  /**
   * 将命令行字符串参数转换为适当的类型。
   * 尝试解析为数字或布尔值，否则保持为字符串。
   */
  private static Object[] convertArguments(java.util.List<String> args) {
    Object[] result = new Object[args.size()];
    for (int i = 0; i < args.size(); i++) {
      String arg = args.get(i);
      result[i] = parseArgument(arg);
    }
    return result;
  }

  private static Object parseArgument(String arg) {
    // 尝试解析为整数
    try {
      return Integer.parseInt(arg);
    } catch (NumberFormatException e) {
      // 不是整数，继续尝试其他类型
    }

    // 尝试解析为长整数
    try {
      return Long.parseLong(arg);
    } catch (NumberFormatException e) {
      // 不是长整数，继续尝试其他类型
    }

    // 尝试解析为双精度浮点数
    try {
      return Double.parseDouble(arg);
    } catch (NumberFormatException e) {
      // 不是浮点数，继续尝试其他类型
    }

    // 尝试解析为布尔值
    if ("true".equalsIgnoreCase(arg)) return true;
    if ("false".equalsIgnoreCase(arg)) return false;

    // 默认作为字符串返回
    return arg;
  }
}
