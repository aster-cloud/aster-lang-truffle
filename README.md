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
│       └── META-INF/native-image/ # Native Image 配置
├── build.gradle.kts
├── settings.gradle.kts
└── gradle.properties
```

## 许可证

Apache License 2.0
