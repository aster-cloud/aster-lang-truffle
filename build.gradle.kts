plugins {
    application
    `java-library`
    `maven-publish`
    id("org.graalvm.buildtools.native") version "0.11.3"
}

group = "cloud.aster-lang"
version = "1.0.3"

// 版本统一管理 — 升级 GraalVM/Truffle 或 Quarkus 时只改这里。
// 此前 25.0.1 在 4 处 dependency 重复硬编码，升级容易半成功。
val graalvmVersion = "25.0.1"
val quarkusVersion = "3.32.2"
val junitVersion = "6.0.0"
val junitPlatformVersion = "6.0.0"  // JUnit Jupiter 6.x 配套 Platform 走同版本
val jacksonVersion = "2.18.2"

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = "aster-lang-truffle"
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/aster-cloud/${rootProject.name}")
            credentials {
                username = System.getenv("GITHUB_ACTOR") ?: ""
                password = System.getenv("GITHUB_TOKEN") ?: ""
            }
        }
    }
}

dependencies {
    // aster-lang 生态依赖：版本由共享 version catalog（aster-lang-platform）
    // 统一管理，见 settings.gradle.kts 的 asterLibs 导入（ADR 0012）。
    implementation(asterLibs.core)
    implementation(asterLibs.runtime)

    // 语言包（通过 SPI 自动发现并注册到 LexiconRegistry）
    runtimeOnly(asterLibs.bundles.locales)  // en + zh + de

    // GraalVM Truffle 框架
    implementation("org.graalvm.truffle:truffle-api:$graalvmVersion")
    annotationProcessor("org.graalvm.truffle:truffle-dsl-processor:$graalvmVersion")
    implementation("org.graalvm.sdk:graal-sdk:$graalvmVersion")

    // JSON 序列化（与 aster-lang-core 保持一致）
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")

    // Quarkus 核心（运行时集成；与 aster-api / aster-lang-runtime 对齐）
    implementation("io.quarkus:quarkus-core:$quarkusVersion")

    // 测试依赖
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testImplementation("org.graalvm.truffle:truffle-api:$graalvmVersion")
    testRuntimeOnly("org.graalvm.truffle:truffle-runtime:$graalvmVersion")
    testRuntimeOnly("org.graalvm.truffle:truffle-compiler:$graalvmVersion")
    testRuntimeOnly("org.graalvm.compiler:compiler:$graalvmVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:$junitPlatformVersion")
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

    // Forward the two parity.eval.* system properties used by
    // CoreIrEvalCli (mirrors aster-lang-core's parity.ir.* forwarding
    // for the Phase B fingerprint CLI). Scoped to the exact key names
    // so future tests don't accidentally couple to the parity runner.
    listOf("parity.eval.input", "parity.eval.output").forEach { key ->
        val v = System.getProperty(key)
        if (v != null) systemProperty(key, v)
    }

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
