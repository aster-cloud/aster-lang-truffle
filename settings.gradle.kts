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
// includeBuild("../aster-lang") {
//     dependencySubstitution {
//         substitute(module("io.aster:aster-core")).using(project(":aster-core"))
//         substitute(module("io.aster:aster-runtime")).using(project(":aster-runtime"))
//     }
// }
