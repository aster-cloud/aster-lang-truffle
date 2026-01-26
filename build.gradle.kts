plugins {
    application
    java
    id("org.graalvm.buildtools.native") version "0.11.3"
}

group = "io.aster"
version = "0.0.1"

dependencies {
    // Maven 坐标依赖（从 mavenLocal 或 mavenCentral 解析）
    implementation("cloud.aster-lang:aster-lang-core:0.0.1")
    implementation("cloud.aster-lang:aster-lang-runtime:0.0.1")

    // GraalVM Truffle 框架
    implementation("org.graalvm.truffle:truffle-api:25.0.1")
    annotationProcessor("org.graalvm.truffle:truffle-dsl-processor:25.0.1")
    implementation("org.graalvm.sdk:graal-sdk:25.0.1")

    // JSON 序列化（与 aster-core 2.18.2 保持一致）
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")

    // Quarkus 核心（运行时集成）
    implementation("io.quarkus:quarkus-core:3.30.2")

    // 测试依赖
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("org.graalvm.truffle:truffle-api:25.0.1")
    testRuntimeOnly("org.graalvm.truffle:truffle-runtime:25.0.1")
    testRuntimeOnly("org.graalvm.truffle:truffle-compiler:25.0.1")
    testRuntimeOnly("org.graalvm.compiler:compiler:25.0.1")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.1")
}

application {
    mainClass.set("aster.truffle.Runner")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

tasks.withType<JavaCompile>().configureEach {
    // Truffle DSL 和 Jackson 注解处理器会产生一些警告，这里忽略
    options.compilerArgs.remove("-Werror")
    options.isDeprecation = true
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showStandardStreams = true
    }
    jvmArgs("--add-opens", "java.base/java.lang=ALL-UNNAMED")
    jvmArgs("--add-opens", "java.base/java.util=ALL-UNNAMED")
    jvmArgs("-Xss10m")  // 增加栈大小以支持深度递归
    jvmArgs("-da")      // 禁用断言以避免 Truffle 内部断言失败

    // Profiler 支持
    systemProperty("aster.profiler.enabled", System.getProperty("aster.profiler.enabled", "false"))

    // CI 模式：通过 -PexcludeBenchmarks=true 排除耗时测试
    val excludeBenchmarks: String? by project
    if (excludeBenchmarks == "true") {
        filter {
            excludeTestsMatching("aster.truffle.GraalVMJitBenchmark")
            excludeTestsMatching("aster.truffle.CrossBackendBenchmark")
            excludeTestsMatching("aster.truffle.BenchmarkTest")
            excludeTestsMatching("aster.truffle.ChaosSchedulerTest")
            excludeTestsMatching("aster.truffle.ExecutionTestSuite.testWorkflowError")
        }
        println("[CI Mode] Excluding slow/flaky tests")
    }
}

// Native Image Agent 配置生成任务
tasks.register<JavaExec>("generateNativeConfig") {
    group = "native"
    description = "Generate Native Image configuration using Agent"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("aster.truffle.Runner")

    val configOutputDir = "${projectDir}/src/main/resources/META-INF/native-image"
    jvmArgs = listOf(
        "-agentlib:native-image-agent=config-output-dir=${configOutputDir}"
    )

    // 运行测试资源收集元数据
    args = listOf(
        "${projectDir}/src/test/resources/simple-literal.json"
    )
}

// GraalVM Native Image 配置
graalvmNative {
    metadataRepository {
        enabled.set(false)
    }
    binaries {
        named("main") {
            imageName.set("aster")
            mainClass.set("aster.truffle.Runner")
            buildArgs.add("--no-fallback")
            buildArgs.add("-H:+ReportExceptionStackTraces")
            buildArgs.add("--initialize-at-build-time=")
            buildArgs.add("-H:+UnlockExperimentalVMOptions")
            buildArgs.add("--initialize-at-build-time=aster.truffle.AsterLanguage")
            buildArgs.add("--initialize-at-build-time=aster.truffle.runtime.AsterConfig")

            // PGO 支持
            val pgoMode: String? by project
            if (pgoMode != null) {
                when {
                    pgoMode == "instrument" -> buildArgs.add("--pgo-instrument")
                    pgoMode!!.endsWith(".iprof") -> buildArgs.add("--pgo=$pgoMode")
                    else -> println("Warning: Unknown PGO mode: $pgoMode")
                }
            }

            // 二进制大小优化
            val sizeOptimization: String? by project
            if (sizeOptimization == "true") {
                buildArgs.add("-O3")
                buildArgs.add("--gc=serial")
                buildArgs.add("-H:+StripDebugInfo")
                buildArgs.add("-H:-AddAllCharsets")
                buildArgs.add("-H:+RemoveUnusedSymbols")
                println("[Size Optimization] Enabled additional size optimization flags")
            }
        }
    }
    agent {
        defaultMode.set("standard")
        builtinCallerFilter = true
        builtinHeuristicFilter = true
        enableExperimentalPredefinedClasses = false
        enableExperimentalUnsafeAllocationTracing = false
        trackReflectionMetadata = true
        modes {
            standard {}
        }
    }
}

tasks.named("nativeCompile") {
    notCompatibleWithConfigurationCache("GraalVM Native Image build requires exclusive lock")
}
