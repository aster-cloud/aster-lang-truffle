# Aster Truffle

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

### Composite Build（推荐）

如需同时修改 aster-lang-core/aster-lang-runtime，编辑 `settings.gradle.kts` 取消注释 composite build 配置：

```kotlin
includeBuild("../aster-lang") {
    dependencySubstitution {
        substitute(module("io.aster:aster-lang-core")).using(project(":aster-lang-core"))
        substitute(module("io.aster:aster-lang-runtime")).using(project(":aster-lang-runtime"))
    }
}
```

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
