@echo off
rem aster 启动脚本（Windows，aster-cloud/aster-lang-truffle#188）：与 aster-cli-@CLI_VERSION@.jar 放在同一目录。
rem 在命令行上传 --sun-misc-unsafe-memory-access=allow，消除 JDK 24+ 因 Truffle 调用 sun.misc.Unsafe 打出的 stderr 警告；
rem 其余参数、标准流与退出码原样透传。JAVA_HOME 优先于 PATH 上的 java；ASTER_CLI_JAR 可覆盖 jar 路径。
setlocal
set "ASTER_JAR=%~dp0aster-cli-@CLI_VERSION@.jar"
if defined ASTER_CLI_JAR set "ASTER_JAR=%ASTER_CLI_JAR%"
set "ASTER_JAVA=java"
if defined JAVA_HOME set "ASTER_JAVA=%JAVA_HOME%\bin\java.exe"
"%ASTER_JAVA%" --sun-misc-unsafe-memory-access=allow -jar "%ASTER_JAR%" %*
exit /b %ERRORLEVEL%
