@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
for %%i in ("%SCRIPT_DIR%\..") do set "CLI_DIR=%%~fi"
set "TARGET_DIR=%CLI_DIR%\target"

set "JAR="
for /f "delims=" %%f in ('dir /b /a:-d "%TARGET_DIR%\*.jar" 2^>nul ^| findstr /v /i "^original-"') do (
  set "JAR=%TARGET_DIR%\%%f"
  goto :found_jar
)
echo Error: CLI jar not found under %TARGET_DIR% 1>&2
echo Build it with: mvn -q -pl cli -am package -DskipTests 1>&2
exit /b 1
:found_jar

@REM echo Running: java %JAVA_OPTS% -jar "%JAR%" %*
java %JAVA_OPTS% -jar "%JAR%" %*
