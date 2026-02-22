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
}

// Composite Build 支持（用于本地开发调试）
// 如需同时修改 aster-core/aster-runtime，取消注释以下行：
// includeBuild("../aster-lang-core") {
//     dependencySubstitution {
//         substitute(module("cloud.aster-lang:aster-lang-core")).using(project(":"))
//     }
// }

// 语言包模块 - 独立项目（通过 SPI 注册到 LexiconRegistry）
// 仅当兄弟目录存在时启用 composite build（本地开发）；CI 使用 Maven Local
listOf("aster-lang-en", "aster-lang-zh", "aster-lang-de").forEach { name ->
    val dir = file("../$name")
    if (dir.isDirectory) {
        includeBuild(dir) {
            dependencySubstitution {
                substitute(module("cloud.aster-lang:$name")).using(project(":"))
            }
        }
    }
}
