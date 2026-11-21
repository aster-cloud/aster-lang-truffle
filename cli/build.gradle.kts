import java.security.MessageDigest

// JVM `aster` CLI（ADR 0059 P1 T8）：compile / run / verify / inspect / version，与 Node CLI 同一命令契约。
// 行为以 aster-lang-test conformance/package-v1/README.md 为准；测试跑同一组夹具（兄弟仓）。
plugins {
    application
    java
    // fat jar（ADR 0059 P1 T9）：9.2.2 在 Gradle 9.6 下实测可用（P0 结果 §1）
    id("com.gradleup.shadow") version "9.2.2"
    // 原生可执行文件（ADR 0063 原生分发）：版本由根项目的 plugins 块声明
    id("org.graalvm.buildtools.native")
}

group = "cloud.aster-lang"
version = rootProject.version

val jacksonVersion = "2.18.2"

// conformance 夹具（兄弟仓 aster-lang-test）是测试的真实输入：声明为任务输入，夹具变了测试就不会从构建缓存里复用旧结果
val conformanceDir = listOf(rootDir.resolve("aster-lang-test"), rootDir.resolve("../aster-lang-test"))
    .map { it.resolve("conformance/package-v1") }
    .firstOrNull { it.isDirectory }

fun requireConformanceDir() {
    if (conformanceDir == null) {
        throw GradleException("缺少兄弟仓 aster-lang-test 的 conformance/package-v1（找过 ${rootDir}/aster-lang-test 与 ${rootDir}/../aster-lang-test）")
    }
}

dependencies {
    implementation(project(":"))
    implementation(asterLibs.core)
    implementation(asterLibs.graalvm.sdk)
    // 与 aster-lang-core 的 CoreIrJson 同一版本（core 传递依赖 2.0.1；计划原写 1.5.x，会被冲突解析升到 2.x）
    implementation("com.networknt:json-schema-validator:2.0.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    // networknt 经 slf4j-api 打日志；无 provider 时每次启动在 stderr 打 SLF4J 警告
    runtimeOnly("org.slf4j:slf4j-nop:2.0.17")

    testImplementation("org.junit.jupiter:junit-jupiter:6.0.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.0.0")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

application {
    mainClass.set("aster.cli.AsterCli")
}

// CLI 运行时闭包排除（P0 结果 §1 决策门）：
// - io.quarkus：只经 aster-lang-runtime 的两个工作流类引用，CLI 闭包不加载；
// - org.antlr:antlr4 与 com.ibm.icu：core 的 antlr 插件把 ANTLR 工具链泄漏成运行时依赖（aster-cloud/aster-lang-core#279），
//   运行时只需 antlr4-runtime。core 侧暂不修：aster-api 的 MessagesValidator 依赖这条泄漏取得 icu4j。
// 排除作用于 runtimeClasspath 与原生构建的 nativeImageClasspath（后者由 native 插件另建、不继承排除规则），
// :cli:test、fat jar 与原生可执行文件用同一闭包。
configurations.matching { it.name == "runtimeClasspath" || it.name == "nativeImageClasspath" }.configureEach {
    exclude(group = "io.quarkus")
    exclude(group = "org.antlr", module = "antlr4")
    exclude(group = "com.ibm.icu")
}

/** fat jar 体积上限（ADR 0059 修订 1 A3：jre-a 18 MiB）。 */
val cliJarMaxBytes = 18L * 1024 * 1024

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveFileName.set("aster-cli-${project.version}.jar")
    // Shadow 9 缺省 duplicatesStrategy=EXCLUDE：同名的 META-INF/services 文件在到达合并器之前就被丢掉，只剩第一个
    // （实测只剩 en 的 LexiconPlugin，zh / de / hi 词法表全部丢失）。服务文件须 INCLUDE 才能合并。
    filesMatching("META-INF/services/**") { duplicatesStrategy = DuplicatesStrategy.INCLUDE }
    mergeServiceFiles()
    manifest {
        attributes(
            "Main-Class" to "aster.cli.AsterCli",
            "Implementation-Version" to project.version.toString(),
            // 消除 JDK 25 对 Truffle 调用受限方法（System::load 等）的 stderr 警告
            "Enable-Native-Access" to "ALL-UNNAMED",
            "Multi-Release" to "true",
        )
    }
    // 产物 SHA-256 写到同目录的 .sha256（GNU 格式），Release 资产一并上传；声明为输出，缺失时任务不会被判为 up-to-date
    outputs.file(destinationDirectory.zip(archiveFileName) { dir, name -> dir.file("$name.sha256") })
    doLast {
        val jar = archiveFile.get().asFile
        val digest = MessageDigest.getInstance("SHA-256").digest(jar.readBytes())
        val hex = digest.joinToString("") { "%02x".format(it) }
        File(jar.path + ".sha256").writeText("$hex  ${jar.name}\n")
    }
}

// 启动脚本（aster-cloud/aster-lang-truffle#188）：JDK 24+ 对 Truffle 调用 sun.misc.Unsafe 打 stderr 警告，截至 GraalVM 25.x 无发布版去掉这些调用，
// jar manifest 又无法设置 --sun-misc-unsafe-memory-access。Release 在 jar 旁附 aster（POSIX sh）与 aster.cmd，由它们在命令行传该选项。
// 模板里的 @CLI_VERSION@ 换成本版本，使脚本指向同目录的 aster-cli-<ver>.jar；.cmd 统一为 CRLF。各自写 .sha256（GNU 格式），与 jar 一并上传。
val cliLaunchers = tasks.register("cliLaunchers") {
    description = "生成 aster / aster.cmd 启动脚本（Release 资产）"
    group = "distribution"
    val ver = project.version.toString()
    val src = layout.projectDirectory.dir("src/launcher")
    val out = layout.buildDirectory.dir("libs")
    inputs.property("version", ver)
    inputs.dir(src).withPathSensitivity(PathSensitivity.RELATIVE)
    val names = listOf("aster", "aster.cmd")
    names.forEach { name ->
        outputs.file(out.map { it.file(name) })
        outputs.file(out.map { it.file("$name.sha256") })
    }
    doLast {
        names.forEach { name ->
            val text = src.file(name).asFile.readText().replace("@CLI_VERSION@", ver)
            val lf = text.replace("\r\n", "\n")
            val body = if (name.endsWith(".cmd")) lf.replace("\n", "\r\n") else lf
            val f = out.get().file(name).asFile
            f.parentFile.mkdirs()
            f.writeText(body)
            if (!name.endsWith(".cmd")) {
                f.setExecutable(true, false)
            }
            val hex = MessageDigest.getInstance("SHA-256").digest(f.readBytes()).joinToString("") { "%02x".format(it) }
            File(f.path + ".sha256").writeText("$hex  $name\n")
        }
    }
}

// 体积门：超过 18 MiB 即失败（CI 与本地同一判据）
val checkCliJarSize = tasks.register("checkCliJarSize") {
    dependsOn(tasks.named("shadowJar"))
    val jar = tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar").flatMap { it.archiveFile }
    doLast {
        val bytes = jar.get().asFile.length()
        val mib = "%.2f".format(bytes / 1024.0 / 1024.0)
        println("aster-cli jar: $bytes 字节（$mib MiB），上限 $cliJarMaxBytes 字节")
        if (bytes > cliJarMaxBytes) {
            throw GradleException("aster-cli jar $bytes 字节超过 18 MiB 上限（ADR 0059 修订 1 A3）")
        }
    }
}

// fat jar 一致性门：以子进程 java -jar 跑 conformance 全部用例（ConformanceCasesTest 的 jar 模式）与 ShadowJarSmokeTest（含启动脚本用例）；
// check 依赖它，./gradlew build 即覆盖。用例彼此独立，按类内并发执行以缩短墙钟。
val shadowJarSmokeTest = tasks.register<Test>("shadowJarSmokeTest") {
    description = "以 java -jar 运行 aster-cli fat jar：conformance 全部用例与冒烟测试"
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("aster.cli.ShadowJarSmokeTest")
        includeTestsMatching("aster.cli.ConformanceCasesTest")
    }
    dependsOn(checkCliJarSize, cliLaunchers)
    val jar = tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar").flatMap { it.archiveFile }
    val launcher = layout.buildDirectory.file("libs/aster")
    inputs.file(jar)
    inputs.file(launcher).withPropertyName("launcher")
    if (conformanceDir != null) {
        inputs.dir(conformanceDir).withPropertyName("conformance").withPathSensitivity(PathSensitivity.RELATIVE)
    }
    // 缺兄弟仓即失败，不静默去掉输入声明（否则又会从构建缓存复用旧结果）
    doFirst { requireConformanceDir() }
    doFirst { systemProperty("aster.cli.jar", jar.get().asFile.absolutePath) }
    doFirst { systemProperty("aster.cli.launcher", launcher.get().asFile.absolutePath) }
    systemProperty("aster.root.dir", rootDir.absolutePath)
    systemProperty("junit.jupiter.execution.parallel.enabled", "true")
    systemProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
    systemProperty("junit.jupiter.execution.parallel.config.strategy", "fixed")
    systemProperty("junit.jupiter.execution.parallel.config.fixed.parallelism", "4")
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
tasks.named("check") { dependsOn(shadowJarSmokeTest) }

// CLI 版本写进资源，运行时由 Version 读取（jar MANIFEST 在 IDE / 测试 classpath 下不可用）
val generateVersionResource = tasks.register("generateVersionResource") {
    val out = layout.buildDirectory.dir("generated/resources/version")
    val ver = project.version.toString()
    inputs.property("version", ver)
    outputs.dir(out)
    doLast {
        val f = out.get().file("aster/cli/version.properties").asFile
        f.parentFile.mkdirs()
        f.writeText("cliVersion=$ver\n")
    }
}
sourceSets["main"].resources.srcDir(generateVersionResource)

tasks.test {
    useJUnitPlatform()
    if (conformanceDir != null) {
        inputs.dir(conformanceDir).withPropertyName("conformance").withPathSensitivity(PathSensitivity.RELATIVE)
    }
    // 缺兄弟仓即失败，不静默去掉输入声明（否则又会从构建缓存复用旧结果）
    doFirst { requireConformanceDir() }
    // 兄弟仓 aster-lang-test 的两种布局：CI 检出到工作区内（根项目同级），本地为并列目录
    systemProperty("aster.root.dir", rootDir.absolutePath)
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

// 原生可执行文件（ADR 0063）：./gradlew :cli:nativeCompile → cli/build/native/nativeCompile/aster(.exe)。
// 只是安装渠道，行为以同一个 package-v1 一致性门为准（CI 的 native-conformance job）。构建用 runtimeClasspath 的分开 jar，
// 不用 fat jar（shadow 合并后 org.graalvm.word 的 ForceOnModulePath 声明失效）。类初始化与运行参数在各 jar 的
// META-INF/native-image/**/native-image.properties 里；这里只放构建策略。GraalVM 取自 GRAALVM_HOME（CI 用 graalvm/setup-graalvm）。
graalvmNative {
    toolchainDetection.set(false)
    metadataRepository {
        enabled.set(false)
    }
    binaries {
        named("main") {
            imageName.set("aster")
            mainClass.set("aster.cli.AsterCli")
            buildArgs.add("--no-fallback")
            buildArgs.add("-H:+ReportExceptionStackTraces")
            // 发布到其他机器的产物用 -PnativeMarch=compatibility（x64 缺省按构建机 CPU 特性生成，换机可能 SIGILL）
            providers.gradleProperty("nativeMarch").orNull?.let { buildArgs.add("-march=$it") }
        }
    }
}

// 原生冒烟：version 与 jar 的 stdout 一致（toolchainId 的 build 段相同）
tasks.register<Exec>("nativeSmoke") {
    description = "运行原生 aster version 并与 fat jar 比较"
    group = "verification"
    dependsOn("nativeCompile", "shadowJar")
    val exe = layout.buildDirectory.file(
        "native/nativeCompile/aster" + if (System.getProperty("os.name").startsWith("Windows")) ".exe" else "")
    val jar = tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar").flatMap { it.archiveFile }
    val javaLauncher = javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(25)) }
    executable = "true"
    doFirst {
        fun run(vararg cmd: String): String {
            val p = ProcessBuilder(*cmd).redirectErrorStream(false).start()
            val out = p.inputStream.readBytes().toString(Charsets.UTF_8)
            if (p.waitFor() != 0) throw GradleException("${cmd.joinToString(" ")} 退出码 ${p.exitValue()}")
            return out
        }
        val native = run(exe.get().asFile.absolutePath, "version")
        val jvm = run(javaLauncher.get().executablePath.asFile.absolutePath, "-jar", jar.get().asFile.absolutePath, "version")
        if (native != jvm) {
            throw GradleException("原生 version 与 jar 不同：\n原生 $native\njar  $jvm")
        }
        println("aster native version: ${native.trim()}")
    }
}

// 原生一致性门（ADR 0063）：以子进程运行原生可执行文件，跑 conformance/package-v1 的全部用例（与 fat jar 门同一测试类）。
// 不进 check：原生构建要 GraalVM（native-image）与 3–6 min，CI 由独立的 native job 调用。
tasks.register<Test>("nativeConformanceTest") {
    description = "以原生 aster 可执行文件运行 conformance 全部用例"
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("aster.cli.ConformanceCasesTest")
    }
    dependsOn("nativeCompile")
    val exe = layout.buildDirectory.file(
        "native/nativeCompile/aster" + if (System.getProperty("os.name").startsWith("Windows")) ".exe" else "")
    inputs.file(exe).withPropertyName("nativeExecutable")
    if (conformanceDir != null) {
        inputs.dir(conformanceDir).withPropertyName("conformance").withPathSensitivity(PathSensitivity.RELATIVE)
    }
    doFirst { requireConformanceDir() }
    doFirst { systemProperty("aster.cli.native", exe.get().asFile.absolutePath) }
    systemProperty("aster.root.dir", rootDir.absolutePath)
    systemProperty("junit.jupiter.execution.parallel.enabled", "true")
    systemProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
    systemProperty("junit.jupiter.execution.parallel.config.strategy", "fixed")
    systemProperty("junit.jupiter.execution.parallel.config.fixed.parallelism", "4")
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
