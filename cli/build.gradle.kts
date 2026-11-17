// JVM `aster` CLI（ADR 0059 P1 T8）：compile / run / verify / inspect / version，与 Node CLI 同一命令契约。
// 行为以 aster-lang-test conformance/package-v1/README.md 为准；测试跑同一组夹具（兄弟仓）。
plugins {
    application
    java
}

group = "cloud.aster-lang"
version = rootProject.version

val jacksonVersion = "2.18.2"

dependencies {
    implementation(project(":"))
    implementation(asterLibs.core)
    implementation(asterLibs.graalvm.sdk)
    // 与 aster-lang-core 的 CoreIrJson 同一版本（core 传递依赖 2.0.1；计划原写 1.5.x，会被冲突解析升到 2.x）
    implementation("com.networknt:json-schema-validator:2.0.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")

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

// CLI 版本写进资源，运行时由 Version 读取（jar MANIFEST 在 IDE / 测试 classpath 下不可用）
val generateVersionResource by tasks.registering {
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

// conformance 夹具（兄弟仓 aster-lang-test）是测试的真实输入：声明为任务输入，夹具变了测试就不会从构建缓存里复用旧结果
val conformanceDir = listOf(rootDir.resolve("aster-lang-test"), rootDir.resolve("../aster-lang-test"))
    .map { it.resolve("conformance/package-v1") }
    .firstOrNull { it.isDirectory }

tasks.test {
    useJUnitPlatform()
    if (conformanceDir != null) {
        inputs.dir(conformanceDir).withPropertyName("conformance").withPathSensitivity(PathSensitivity.RELATIVE)
    }
    // 兄弟仓 aster-lang-test 的两种布局：CI 检出到工作区内（根项目同级），本地为并列目录
    systemProperty("aster.root.dir", rootDir.absolutePath)
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
