rootProject.name = "aster-lang-truffle"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenLocal()  // 优先使用本地 Maven 仓库（aster-core, aster-runtime）
        mavenCentral()
    }
    // 共享版本目录（aster-lang-platform，ADR 0012）：aster-lang 生态依赖版本
    // 的单一来源。用 asterLibs.* 别名代替散落的 "cloud.aster-lang:...:0.0.1"。
    versionCatalogs {
        create("asterLibs") {
            from("cloud.aster-lang:aster-lang-platform:1.0.9")
        }
    }
}

// Composite Build 支持（用于本地开发调试）
// 如需同时修改 aster-core/aster-runtime，取消注释以下行：
// includeBuild("../aster-lang-core") {
//     dependencySubstitution {
//         substitute(module("cloud.aster-lang:aster-lang-core")).using(project(":"))
//     }
// }

// 语言包模块（通过 SPI 注册到 LexiconRegistry）。en/zh/de 已从已归档的
// aster-lang-{en,zh,de} 迁到多模块仓 aster-lang-locales（坐标
// cloud.aster-lang:aster-lang-locales-{en,zh,de}）；composite includeBuild 把这三个
// 坐标替换到 locales 对应 subproject。仅当兄弟目录存在时启用（本地开发）；CI 用 Maven Local。
val localesDir = file("../aster-lang-locales")
if (localesDir.isDirectory) {
    includeBuild(localesDir) {
        dependencySubstitution {
            substitute(module("cloud.aster-lang:aster-lang-locales-en")).using(project(":en"))
            substitute(module("cloud.aster-lang:aster-lang-locales-zh")).using(project(":zh"))
            substitute(module("cloud.aster-lang:aster-lang-locales-de")).using(project(":de"))
        }
    }
}
