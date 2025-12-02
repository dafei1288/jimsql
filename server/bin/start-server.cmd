@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
for %%i in ("%SCRIPT_DIR%\..") do set "SERVER_DIR=%%~fi"
set "TARGET_DIR=%SERVER_DIR%\target"

set "JAR="
for /f "delims=" %%f in ('dir /b /a:-d "%TARGET_DIR%\*-jar-with-dependencies.jar" 2^>nul') do (
  set "JAR=%TARGET_DIR%\%%f"
  goto :found_jar
)
echo Error: server fat-jar not found under %TARGET_DIR% 1>&2
echo Build it with: mvn -q -pl server -am package -DskipTests 1>&2
exit /b 1
:found_jar

if not defined PORT if defined JIMSQL_PORT set "PORT=%JIMSQL_PORT%"
if not defined PORT set "PORT=8821"
if not defined HOST if defined JIMSQL_HOST set "HOST=%JIMSQL_HOST%"
if not defined HOST set "HOST=0.0.0.0"
if not defined DATADIR if defined JIMSQL_DATADIR set "DATADIR=%JIMSQL_DATADIR%"
if not defined DATADIR set "DATADIR=%SERVER_DIR%\src\main\resources\datadir"
if not defined PROTOCOL if defined JIMSQL_PROTOCOL set "PROTOCOL=%JIMSQL_PROTOCOL%"

set "JAVA_OPTS=%JAVA_OPTS%"
if defined JIMSQL_WIRELOG set "JAVA_OPTS=%JAVA_OPTS% -Djimsql.wirelog=%JIMSQL_WIRELOG%"

set "ARGS=%PORT% %HOST% \"%DATADIR%\""
if defined PROTOCOL set "ARGS=%ARGS% %PROTOCOL%"

echo Running: java %JAVA_OPTS% -jar "%JAR%" %ARGS%
java %JAVA_OPTS% -jar "%JAR%" %ARGS%
