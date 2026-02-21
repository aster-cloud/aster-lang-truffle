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
includeBuild("../aster-lang-en") {
    dependencySubstitution {
        substitute(module("cloud.aster-lang:aster-lang-en")).using(project(":"))
    }
}
includeBuild("../aster-lang-zh") {
    dependencySubstitution {
        substitute(module("cloud.aster-lang:aster-lang-zh")).using(project(":"))
    }
}
includeBuild("../aster-lang-de") {
    dependencySubstitution {
        substitute(module("cloud.aster-lang:aster-lang-de")).using(project(":"))
    }
}
