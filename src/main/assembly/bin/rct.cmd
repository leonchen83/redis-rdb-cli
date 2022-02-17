@echo off

@setlocal

set ERROR_CODE=0

@REM ==== START VALIDATION ====
if not "%JAVA_HOME%"=="" goto OkJHome
for %%i in (java.exe) do set "JAVACMD=%%~$PATH:i"
goto checkJCmd

:OkJHome
set "JAVACMD=%JAVA_HOME%\bin\java.exe"

:checkJCmd
if exist "%JAVACMD%" goto chkMHome

echo The JAVA_HOME environment variable is not defined correctly >&2
echo This environment variable is needed to run this program >&2
echo NB: JAVA_HOME should point to a JDK not a JRE >&2
goto error

:chkMHome
set "RCT_HOME=%~dp0.."
if not "%RCT_HOME%"=="" goto stripMHome
goto error

:stripMHome
if not "_%RCT_HOME:~-1%"=="_\" goto checkMCmd
set "RCT_HOME=%RCT_HOME:~0,-1%"
goto stripMHome

:checkMCmd
if exist "%RCT_HOME%\bin\rct.cmd" goto chkVersion
goto error

:chkVersion
for /f tokens^=2-5^ delims^=.-+_^" %%j in ('java -fullversion 2^>^&1') do @set "JVER=%%j%%k%%l"

if %JVER% GEQ 180 goto init
echo java -version is less than 1.8
goto error
@REM ==== END VALIDATION ====

:init
setLocal EnableDelayedExpansion
set CLASS_PATH="
for %%i in ("%RCT_HOME%\lib\*.jar") do (
    set CLASS_PATH=!CLASS_PATH!;%%i
)
set CLASS_PATH=!CLASS_PATH!"

set LOG_DIR=%RCT_HOME%\log
set CON_DIR=%RCT_HOME%\conf
set LOG_FILE=%CON_DIR%\log4j2.xml
set CON_FILE=%CON_DIR%\redis-rdb-cli.conf
set MAIN_CLASS=com.moilioncircle.redis.rdb.cli.Rct
set RCT_OPS=-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:+ExitOnOutOfMemoryError -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -Dlog4j.configurationFile="%LOG_FILE%" -Dcli.log.path="%LOG_DIR%" -Dconf="%CON_FILE%" -Drct.home="%RCT_HOME%" -Dsun.stderr.encoding=UTF-8 -Dsun.stdout.encoding=UTF-8 -Dsun.err.encoding=UTF-8 -Dfile.encoding=UTF-8

"%JAVACMD%" %RCT_OPS% -cp %CLASS_PATH% %MAIN_CLASS% %*
if ERRORLEVEL 1 goto error
goto end

:error
set ERROR_CODE=1

:end
@endlocal & set ERROR_CODE=%ERROR_CODE%

cmd /C exit /B %ERROR_CODE%
