import java.util.zip.ZipFile

plugins {
    application
    `java-library`
    `maven-publish`
    id("org.graalvm.buildtools.native") version "0.11.3"
}

group = "cloud.aster-lang"

// Maven 制品版本 = 共享版本目录的 asterLang（JVM 生态单一版本源，ADR 0012/0023 §9）。
// 不硬编码字面量——字面量是版本漂移的来源。从 catalog 派生让版本永远跟随 ecosystemVersion，
// 漂移面收敛到 catalog 1 处。与 core/locales/hi 同构（VersionCatalogsExtension 取 handle，
// 避免与生成的类型安全访问器 asterLibs 含 .core/.runtime 等库别名 冲突）。
version = extensions.getByType<VersionCatalogsExtension>()
    .named("asterLibs").findVersion("asterLang").get().requiredVersion

// 版本统一管理 — 升级 Quarkus 时只改这里。
// 此前 25.0.1 在 4 处 dependency 重复硬编码，升级容易半成功。
//
// ★GraalVM/Truffle 已移出本文件，改由 aster-lang-platform 版本目录统一
//   （asterLibs.graalvm.*，aster-api#193）。原因：本仓与 aster-api 各自硬编码时，
//   曾出现 api 内部 25.0.3/25.0.4 混用 + 本仓落后到 25.0.1 的跨仓 drift；
//   而 GraalVM 组件混搭的症状是 NoClassDefFoundError →
//   ExceptionInInitializerError at Engine.java:559，且**逐个升看起来都绿**。
val graalvmVersion: String = extensions.getByType<VersionCatalogsExtension>()
    .named("asterLibs").findVersion("graalvm").get().requiredVersion

// Quarkus 版本对齐（aster-lang-runtime#90）：quarkus-core / jackson / smallrye-common 等传递依赖
// 用 catalog 的 quarkus-bom（与 aster-lang-runtime 同一版本）对齐。此前靠 runtime 发布元数据泄漏的
// enforcedPlatform 对齐，它同时把 org.graalvm.sdk 强制降到 23.1.2；runtime 不再泄漏后由本仓自己声明。
// - 用 platform 而非 enforcedPlatform：BOM 只作推荐版本，graal-sdk 等更高的显式版本（catalog 的 25.x）照常胜出。
// - 挂在不可消费、不可解析的 quarkusPlatform 上，只由各项目可解析的运行类 *Classpath 继承
//   （runtime / testRuntime，以及原生插件另建、不继承 runtimeClasspath 的 nativeImage*Classpath），
//   不进 apiElements / runtimeElements，不会发布给消费方。
// - 不作用于 compile / testCompile classpath：runtime 是 implementation 依赖，从未出现在本仓编译 classpath 上，
//   原先的泄漏也只影响运行类 classpath；保持编译 classpath 不变。
val quarkusBom = extensions.getByType<VersionCatalogsExtension>()
    .named("asterLibs").findLibrary("quarkus-bom").get()
allprojects {
    val quarkusPlatform = configurations.create("quarkusPlatform") {
        isCanBeConsumed = false
        isCanBeResolved = false
    }
    dependencies.add(quarkusPlatform.name, dependencies.platform(quarkusBom))
    configurations.matching { it.isCanBeResolved && it.name.endsWith("Classpath") && !it.name.lowercase().contains("compile") }.configureEach {
        extendsFrom(quarkusPlatform)
    }
}

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
    implementation(asterLibs.graalvm.truffle.api)
    annotationProcessor(asterLibs.graalvm.truffle.dsl.processor)
    implementation(asterLibs.graalvm.sdk)

    // JSON 序列化（与 aster-lang-core 保持一致）
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")

    // Quarkus 核心（运行时集成；与 aster-api / aster-lang-runtime 对齐）
    implementation("io.quarkus:quarkus-core:$quarkusVersion")

    // 测试依赖
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testImplementation(asterLibs.graalvm.truffle.api)
    testRuntimeOnly(asterLibs.graalvm.truffle.runtime)
    testRuntimeOnly(asterLibs.graalvm.truffle.compiler)
    testRuntimeOnly(asterLibs.graalvm.compiler)
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

// 所有 Test 任务（默认 test 与 benchmark）共用的 JVM 与日志配置
tasks.withType<Test>().configureEach {
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
    listOf("parity.eval.input", "parity.eval.output", "aster.exportBuiltins.update").forEach { key ->
        val v = System.getProperty(key)
        if (v != null) systemProperty(key, v)
    }
}

// 带 @Tag("benchmark") 的用例只断言墙钟耗时，结果随机器负载漂移，不作为默认测试的门禁；
// 需要时用 ./gradlew benchmark 单独运行
tasks.test {
    useJUnitPlatform {
        excludeTags("benchmark")
    }

    // CI 模式：通过 -PexcludeBenchmarks=true 排除耗时测试
    val excludeBenchmarks: String? by project
    if (excludeBenchmarks == "true") {
        filter {
            excludeTestsMatching("aster.truffle.GraalVMJitBenchmark")
            excludeTestsMatching("aster.truffle.CrossBackendBenchmark")
            excludeTestsMatching("aster.truffle.ChaosSchedulerTest")
            excludeTestsMatching("aster.truffle.ExecutionTestSuite.testWorkflowError")
        }
        println("[CI Mode] Excluding slow/flaky tests")
    }
}

tasks.register<Test>("benchmark") {
    description = "运行带 benchmark 标签的墙钟性能基准（不参与 check）"
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("benchmark")
    }
}

// 原生镜像的 IR 模型反射元数据（生成，不手写）：Jackson 按字段 / 构造器绑定 Core IR 与词汇表 DTO，原生镜像里每个类型都要注册全部成员。
// tracing agent 只记录一致性门走到的成员，IR 节点类型随 core 版本增减，手写清单必然漂移（旧清单就停在 ADR 0055 之前）。
// 这里按 classpath 实际内容枚举 aster.truffle.core.CoreModel$*（本仓）与 aster.core.ir.CoreModel$*、
// aster.core.identifier.VocabularyLoader$*（core jar）的全部嵌套类型，写进 jar 的 META-INF/native-image。
val nativeModelTypePattern = Regex("""^(aster/truffle/core/CoreModel|aster/core/ir/CoreModel|aster/core/identifier/VocabularyLoader)((\$[A-Za-z_][A-Za-z0-9_]*)+)\.class$""")
val generateNativeModelMetadata = tasks.register("generateNativeModelMetadata") {
    description = "按 classpath 生成 IR 模型类的原生镜像反射元数据"
    group = "native"
    val classesDirs = sourceSets["main"].output.classesDirs
    val runtimeCp = configurations.named("runtimeClasspath")
    val out = layout.buildDirectory.dir("generated/native-image-model")
    dependsOn(tasks.named("compileJava"))
    inputs.files(classesDirs).withPropertyName("classes")
    inputs.files(runtimeCp).withPropertyName("runtimeClasspath").withNormalizer(ClasspathNormalizer::class.java)
    outputs.dir(out)
    doLast {
        val names = sortedSetOf<String>()
        fun consider(entry: String) {
            if (nativeModelTypePattern.matches(entry)) {
                names.add(entry.removeSuffix(".class").replace('/', '.'))
            }
        }
        (classesDirs.files + runtimeCp.get().files).filter { it.exists() }.forEach { f ->
            if (f.isDirectory) {
                f.walkTopDown().filter { it.isFile }.forEach { consider(it.relativeTo(f).invariantSeparatorsPath) }
            } else if (f.name.endsWith(".jar")) {
                ZipFile(f).use { z -> z.entries().asSequence().forEach { consider(it.name) } }
            }
        }
        listOf("aster.truffle.core.CoreModel$", "aster.core.ir.CoreModel$").forEach { prefix ->
            if (names.none { it.startsWith(prefix) }) {
                throw GradleException("classpath 上找不到 ${prefix}* 类型，IR 模型元数据会是空的")
            }
        }
        val flags = listOf("allDeclaredConstructors", "allPublicConstructors", "allDeclaredMethods", "allPublicMethods",
            "allDeclaredFields", "allPublicFields")
        val entries = names.joinToString(",\n") { n ->
            "    {\"type\": \"$n\", " + flags.joinToString(", ") { "\"$it\": true" } + "}"
        }
        val f = out.get().file("META-INF/native-image/cloud.aster-lang/aster-lang-truffle-model/reachability-metadata.json").asFile
        f.parentFile.mkdirs()
        f.writeText("{\n  \"reflection\": [\n$entries\n  ]\n}\n")
    }
}
sourceSets["main"].resources.srcDir(generateNativeModelMetadata)

// GraalVM 组件版本一致性门：每个项目运行时 classpath 上的 org.graalvm.* 必须全部等于 catalog 的 graalvm 版本
// （aster-lang-runtime 曾泄漏 enforcedPlatform(quarkus-bom)，把 23.1.2 的 SDK 混入并阻断原生构建，
// 见 aster-lang-runtime#90）。check 依赖它。
allprojects {
    plugins.withId("java") {
        val checkGraalvmVersions = tasks.register("checkGraalvmVersions") {
            description = "核对运行时 classpath 上的 org.graalvm.* 版本与 catalog 一致"
            group = "verification"
            val root = configurations.named("runtimeClasspath").flatMap { it.incoming.resolutionResult.rootComponent }
            val expected = graalvmVersion
            val path = project.path
            doLast {
                val bad = sortedSetOf<String>()
                val seen = mutableSetOf<org.gradle.api.artifacts.result.ResolvedComponentResult>()
                val queue = ArrayDeque(listOf(root.get()))
                while (queue.isNotEmpty()) {
                    val c = queue.removeFirst()
                    if (!seen.add(c)) continue
                    val id = c.moduleVersion
                    if (id != null && id.group.startsWith("org.graalvm") && id.version != expected) {
                        bad.add("${id.group}:${id.name}:${id.version}")
                    }
                    c.dependencies.filterIsInstance<org.gradle.api.artifacts.result.ResolvedDependencyResult>()
                        .forEach { queue.addLast(it.selected) }
                }
                if (bad.isNotEmpty()) {
                    throw GradleException("$path 的 org.graalvm.* 版本须为 $expected：\n" + bad.joinToString("\n"))
                }
            }
        }
        tasks.named("check") { dependsOn(checkGraalvmVersions) }
    }
}

// Native Image Agent 配置生成任务
tasks.register<JavaExec>("generateNativeConfig") {
    group = "native"
    description = "Generate Native Image configuration using Agent"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("aster.truffle.Runner")

    // 输出到 build/：agent 的原始结果只作为整理元数据的参考，不直接覆盖手写的 META-INF/native-image 文件
    val configOutputDir = layout.buildDirectory.dir("native-agent").get().asFile.absolutePath
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
