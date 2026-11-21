# Aster Truffle

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Aster Language 的 GraalVM Truffle 实现。

## 概述

本项目是从 [aster-lang](https://github.com/wontlost/aster-lang) monorepo 提取的独立模块，提供基于 GraalVM Truffle 框架的高性能语言运行时。

## 依赖

- `cloud.aster-lang:aster-lang-core:0.0.1` - 解析器和 Core IR
- `cloud.aster-lang:aster-lang-runtime:0.0.1` - 运行时支持

## 构建要求

- Java 25+
- GraalVM 25.0.1+
- Gradle 9.2+

## 快速开始

### 安装依赖

首先需要将 aster-lang-core 和 aster-lang-runtime 发布到本地 Maven 仓库：

```bash
# 在 aster-lang 目录下执行
cd ../aster-lang
./gradlew :aster-lang-core:publishToMavenLocal :aster-lang-runtime:publishToMavenLocal
```

### 编译项目

```bash
./gradlew build
```

### 运行测试

```bash
# 运行所有测试
./gradlew test

# 排除耗时测试（CI 模式）
./gradlew test -PexcludeBenchmarks=true
```

### 构建 Native Image

```bash
# 普通构建
./gradlew nativeCompile

# 使用 PGO 优化
./gradlew nativeCompile -PpgoMode=instrument
# 运行采集 profile
./build/native/nativeCompile/aster <workload>
./gradlew nativeCompile -PpgoMode=default.iprof

# 启用大小优化
./gradlew nativeCompile -PpgoMode=default.iprof -PsizeOptimization=true
```

## 开发模式

### 依赖解析与 Composite Build

默认构建解析**已发布**的 `cloud.aster-lang:aster-lang-core` / `aster-lang-runtime`（版本取自 `asterLibs` 平台 catalog，
来源 `settings.gradle.kts`；本地开发从 `mavenLocal()` 优先解析，CI 只验证已发布制品）。

联调尚未发布的 core 改动时，显式指定 core 源码目录，`settings.gradle.kts` 会把 `cloud.aster-lang:aster-lang-core`
替换为该目录的构建：

```bash
./gradlew test -PasterCoreDir=../aster-lang-core
# 或
ASTER_CORE_DIR=../aster-lang-core ./gradlew test
```

不按「兄弟目录存在」自动启用 core 替换：兄弟目录常停在与 catalog 版本不一致的分支上，静默替换会让构建结果不可复现。
`asterCoreDir` 不是目录时构建直接失败。

语言包不同：只要兄弟目录 `../aster-lang-locales` 存在，就会自动 `includeBuild`，把
`cloud.aster-lang:aster-lang-locales-{en,zh,de}` 替换为其 `:en` / `:zh` / `:de` 子项目；该目录不存在时（含 CI）从
Maven Local / 已发布制品解析。

#### 依赖新 core API 的分支必须等 core 发版

本仓依赖 core 的 API（例如 ADR 0051 之后的 `NameResolution` / `ResolutionContext`，见 `CnlCompiler.compileToCore`）。
没有 `-PasterCoreDir` 时，构建只能对**已发布**的 core 编译：

- 分支改动依赖尚未发布的 core API：本地用 `-PasterCoreDir` 联调；合入前必须等 core 发版并 bump `asterLibs` catalog
  版本（`settings.gradle.kts` 中的 `aster-lang-platform:<版本>`），否则 CI 无法编译。
- 本机缺对应版本制品（`cloud.aster-lang:aster-lang-core:<版本>` 等）时，在对应仓库执行 `./gradlew publishToMavenLocal`
  发布到本地 Maven 仓库，不要降级版本。

## JVM aster CLI（运行导出的策略包）

`cli` 子项目是 ADR 0059 的 JVM `aster` CLI，与 Node CLI（`@aster-cloud/aster-lang-ts` 的 `aster`）同一命令契约：
`compile`、`run`、`verify`、`inspect`、`version`。行为以 aster-lang-test `conformance/package-v1/README.md` 为准。

```bash
./gradlew :cli:shadowJar                 # → cli/build/libs/aster-cli-<ver>.jar 与 .sha256（Release 资产同名）
./gradlew :cli:cliLaunchers              # → cli/build/libs/aster、aster.cmd 与各自 .sha256（Release 资产同名）
java -jar cli/build/libs/aster-cli-<ver>.jar version
java -jar aster-cli-<ver>.jar run <包目录或 zip> --input input.json --jwks jwks.json
```

需要 Java 25。stdout 恒为一行 JSON 信封（`compile` 输出 IR），诊断只写 stderr。

**JDK 25 的 stderr 警告与启动脚本**：求值时 Truffle 自身（`TStringUnsafe`、`NodeClassImpl`、`InlineSupport`、`FrameWithoutBoxing` 等）调用
`sun.misc.Unsafe` 的内存访问方法，JDK 24+（JEP 498）在首次调用时向 stderr 打四行 `WARNING:`（JDK 26 仍是同样的警告）。它们不影响 stdout 与退出码。
截至 GraalVM / Truffle 25.x 的全部发布版都还依赖这些调用（oracle/graal#12782，上游给出的处理就是下面这个选项），jar 的 manifest 又无法设置它，
所以 GitHub Release 在 jar 旁附带启动脚本 `aster`（POSIX sh）与 `aster.cmd`（Windows），连同各自的 `.sha256`。脚本与 jar 放在同一目录，
等价于 `java --sun-misc-unsafe-memory-access=allow -jar aster-cli-<ver>.jar "$@"`：其余参数、stdin / stdout / stderr 与退出码原样透传。

```bash
sha256sum -c aster-cli-<ver>.jar.sha256 aster.sha256  # macOS 无 sha256sum 时用 shasum -a 256 -c
chmod +x aster                                        # Release 下载的资产不带可执行位；也可以 sh aster …
./aster run <包目录或 zip> --input input.json --jwks jwks.json
```

Windows 下用 `aster.cmd`，校验时把 `Get-FileHash -Algorithm SHA256 aster-cli-<ver>.jar, aster.cmd`（PowerShell）的结果与
`aster-cli-<ver>.jar.sha256`、`aster.cmd.sha256` 中的十六进制值比对：

```bat
aster.cmd run <包目录或 zip> --input input.json --jwks jwks.json
```

- `JAVA_HOME` 已设置时用 `$JAVA_HOME/bin/java`，否则用 `PATH` 上的 `java`；`ASTER_CLI_JAR` 可指向别处的 jar；`aster` 可软链到 `PATH` 上的目录。
- 直接 `java -jar aster-cli-<ver>.jar …` 仍然可用，只是求值时会出现上述警告；自己写命令行时加 `--sun-misc-unsafe-memory-access=allow` 即可。
- 也可以用环境变量 `JDK_JAVA_OPTIONS=--sun-misc-unsafe-memory-access=allow`，但 `java` 启动器会在 stderr 打一行
  `NOTE: Picked up JDK_JAVA_OPTIONS: …`（`JAVA_TOOL_OPTIONS` 同理），且对该 shell 里所有 Java 程序生效。

JEP 498 计划在后续 JDK 中把默认值改为 `deny`、最终移除这些方法；届时需要 Truffle 换掉 `sun.misc.Unsafe`，生态升级 GraalVM 版本
（`aster-lang-platform` catalog 的 `asterLibs.graalvm.*`）后可删去启动脚本里的这个选项。

### 原生可执行文件（ADR 0063）

同一个 CLI 也可以用 GraalVM Native Image 编译成单个原生可执行文件（单次调用约 25 ms，jar 约 640 ms）。它只是安装渠道，行为以同一个
package-v1 一致性门为准；求值只走 Truffle 解释器（镜像内不带 JIT）。需要 GraalVM 25（`native-image`），由 `GRAALVM_HOME` 指定：

```bash
GRAALVM_HOME=<graalvm-25> ./gradlew :cli:nativeCompile        # → cli/build/native/nativeCompile/aster（Windows 为 aster.exe）
GRAALVM_HOME=<graalvm-25> ./gradlew :cli:nativeSmoke          # 原生 version 与 fat jar 逐字节相同
GRAALVM_HOME=<graalvm-25> ./gradlew :cli:nativeConformanceTest # conformance/package-v1 全部用例以原生可执行文件运行
# 发布给其他机器的 x64 产物加 -PnativeMarch=compatibility（缺省按构建机 CPU 特性生成）
```

原生镜像的配置都在 jar 里，`native-image -cp …` 手工构建也会带上：

- `src/main/resources/META-INF/native-image/cloud.aster-lang/aster-lang-truffle/`：类初始化策略（`native-image.properties`）与
  Truffle / core 资源的元数据；
- `build/generated/native-image-model/…/aster-lang-truffle-model/`：`generateNativeModelMetadata` 按 classpath 生成的 IR 模型类反射元数据
  （`aster.truffle.core.CoreModel$*`、`aster.core.ir.CoreModel$*`、`aster.core.identifier.VocabularyLoader$*` 全部成员），不手写；
- `cli/src/main/resources/META-INF/native-image/cloud.aster-lang/aster-cli/`：CLI 的运行参数（`--enable-url-protocols=https`、
  `-R:StackSize=2m`）与 tracing agent 采集的其余元数据。

依赖升级后若原生一致性门失败（典型症状是 `this appears to be a native image` 或缺资源），重新采集：以
`-agentlib:native-image-agent=config-output-dir=<dir>/{pid}` 运行 fat jar 跑一遍 aster-lang-test 的 `package-conformance.mjs`
（PATH 前放一个把 `java` 换成带 agent 的 java 的垫片），删掉被杀进程留下的 `.lock`，用 `native-image-configure generate` 合并，
再把新增条目并入上面的 `aster-cli/reachability-metadata.json`（IR 模型类不用手加）。

## 项目结构

```
aster-lang-truffle/
├── src/
│   ├── main/java/aster/truffle/
│   │   ├── AsterLanguage.java     # Truffle 语言入口
│   │   ├── AsterContext.java      # 执行上下文
│   │   ├── CnlCompiler.java       # CNL 编译器
│   │   ├── Runner.java            # 命令行运行器
│   │   ├── nodes/                 # Truffle 节点实现
│   │   └── runtime/               # 运行时工具
│   └── main/resources/
│       └── META-INF/native-image/ # Native Image 配置（cloud.aster-lang/aster-lang-truffle/）
├── cli/                           # JVM aster CLI（ADR 0059）
├── build.gradle.kts
├── settings.gradle.kts
└── gradle.properties
```

## 许可证

Apache License 2.0
