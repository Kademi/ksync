@echo off
setlocal enabledelayedexpansion
rem ksync3 installer for Windows CMD. Standalone, PowerShell is not needed.
rem   curl -fsSL https://raw.githubusercontent.com/Kademi/ksync/master/installers/install.cmd -o install.cmd && install.cmd && del install.cmd
rem Installs ksync3.jar and, if no Java 11+ is found, a JRE into %LOCALAPPDATA%\Programs\ksync3, and puts a ksync3 command on the user PATH.
rem Overrides: KSYNC3_HOME, KSYNC3_JAR_URL, KSYNC3_JAVA_VERSION, KSYNC3_BUNDLE_JRE=1

if not defined KSYNC3_JAR_URL set "KSYNC3_JAR_URL=https://docs.kademi.co/assets/fe0d6a30-5665-4dec-bdee-89f112c17905"
if not defined KSYNC3_HOME set "KSYNC3_HOME=%LOCALAPPDATA%\Programs\ksync3"
set "BIN_DIR=%KSYNC3_HOME%\bin"
set "JAVA_MIN=11"

rem A 32-bit cmd on 64-bit Windows reports the real machine in PROCESSOR_ARCHITEW6432
set "MACHINE=%PROCESSOR_ARCHITECTURE%"
if defined PROCESSOR_ARCHITEW6432 set "MACHINE=%PROCESSOR_ARCHITEW6432%"
set "ARCH=x64"
if /i "%MACHINE%"=="ARM64" set "ARCH=aarch64"
rem Temurin has no 25 JRE for Windows on ARM yet, so that platform gets the previous LTS
if not defined KSYNC3_JAVA_VERSION set "KSYNC3_JAVA_VERSION=25"
if /i "%ARCH%"=="aarch64" if "%KSYNC3_JAVA_VERSION%"=="25" set "KSYNC3_JAVA_VERSION=21"
if /i "%MACHINE%"=="x86" (
    echo ksync3 needs 64-bit Windows. >&2
    exit /b 1
)

curl --version >nul 2>&1 || (
    echo curl is required. It ships with Windows 10 1803 and later, or use installers\install.ps1 instead. >&2
    exit /b 1
)

if not exist "%BIN_DIR%" mkdir "%BIN_DIR%"

echo Downloading ksync3.jar ...
call :download "%KSYNC3_JAR_URL%" "%KSYNC3_HOME%\ksync3.jar.tmp" || exit /b 1
move /y "%KSYNC3_HOME%\ksync3.jar.tmp" "%KSYNC3_HOME%\ksync3.jar" >nul

set "JAVA="
if not "%KSYNC3_BUNDLE_JRE%"=="1" call :find_java
if defined JAVA (
    echo Using Java !JAVA_MAJOR! at !JAVA!
) else (
    echo No Java %JAVA_MIN%+ found, downloading Temurin JRE %KSYNC3_JAVA_VERSION% ^(windows/%ARCH%^) ...
    call :download "https://api.adoptium.net/v3/binary/latest/%KSYNC3_JAVA_VERSION%/ga/windows/%ARCH%/jre/hotspot/normal/eclipse" "%KSYNC3_HOME%\jre.zip" || exit /b 1
    if exist "%KSYNC3_HOME%\jre" rmdir /s /q "%KSYNC3_HOME%\jre"
    mkdir "%KSYNC3_HOME%\jre"
    rem tar.exe is bsdtar, it reads zip files; the archive has one top-level dir
    tar -xf "%KSYNC3_HOME%\jre.zip" -C "%KSYNC3_HOME%\jre" --strip-components=1 || (
        echo Failed to extract the JRE >&2
        exit /b 1
    )
    del "%KSYNC3_HOME%\jre.zip"
    set "JAVA=%KSYNC3_HOME%\jre\bin\java.exe"
)

rem Launcher. %% and ^ are doubled so the written file holds single ones.
> "%BIN_DIR%\ksync3.cmd" echo @echo off
>>"%BIN_DIR%\ksync3.cmd" echo rem ksync3 launcher, written by install.cmd
>>"%BIN_DIR%\ksync3.cmd" echo for %%%%I in ("%%cd%%") do title Ksync3 ^^^| %%%%~nxI
>>"%BIN_DIR%\ksync3.cmd" echo "!JAVA!" -jar "%KSYNC3_HOME%\ksync3.jar" %%*

rem Add BIN_DIR to the user PATH. reg add has no 1024-char limit, unlike setx; the setx of
rem KSYNC3_HOME afterwards is what broadcasts the change so new terminals pick it up.
set "USERPATH="
for /f "skip=2 tokens=1,2,*" %%A in ('reg query HKCU\Environment /v Path 2^>nul') do set "USERPATH=%%C"
echo ;!USERPATH!; | find /i ";%BIN_DIR%;" >nul || (
    reg add HKCU\Environment /v Path /t REG_EXPAND_SZ /d "%BIN_DIR%;!USERPATH!" /f >nul
    setx KSYNC3_HOME "%KSYNC3_HOME%" >nul
    echo Added %BIN_DIR% to your user PATH. Open a new terminal to pick it up.
)

rem Uninstaller. Delayed expansion is off here so the ! in it are written literally.
setlocal disabledelayedexpansion
set "U=%KSYNC3_HOME%\uninstall.cmd"
> "%U%" echo @echo off
>>"%U%" echo setlocal enabledelayedexpansion
>>"%U%" echo rem ksync3 uninstaller, written by the installer. Removes the PATH entry, then the install dir.
>>"%U%" echo set "HOME_DIR=%KSYNC3_HOME%"
>>"%U%" echo set "BIN_DIR=%BIN_DIR%"
>>"%U%" echo set "USERPATH="
>>"%U%" echo for /f "skip=2 tokens=1,2,*" %%%%A in ('reg query HKCU\Environment /v Path 2^^^>nul') do set "USERPATH=%%%%C"
>>"%U%" echo set "USERPATH=!USERPATH:%%BIN_DIR%%;=!"
>>"%U%" echo set "USERPATH=!USERPATH:;%%BIN_DIR%%=!"
>>"%U%" echo reg add HKCU\Environment /v Path /t REG_EXPAND_SZ /d "!USERPATH!" /f ^>nul
>>"%U%" echo reg delete HKCU\Environment /v KSYNC3_HOME /f ^>nul 2^>^&1
>>"%U%" echo cd /d "%%TEMP%%"
>>"%U%" echo echo ksync3 removed.
>>"%U%" echo (goto) 2^>nul ^& rmdir /s /q "%%HOME_DIR%%"
endlocal

echo Installed ksync3 to %BIN_DIR%\ksync3.cmd
echo Uninstall: run "%KSYNC3_HOME%\uninstall.cmd"
exit /b 0

rem ---------------------------------------------------------------- subroutines

:download
rem %1=URL %2=output. Retries with best-effort revocation checking on networks that
rem block the CRL/OCSP endpoints, where schannel fails with 0x80092012 / 0x80092013.
curl -fsSL "%~1" -o "%~2" 2>"%~2.err"
if !ERRORLEVEL! equ 0 (
    del "%~2.err"
    exit /b 0
)
findstr /i /c:"0x80092012" /c:"0x80092013" "%~2.err" >nul
if !ERRORLEVEL! equ 0 (
    echo This network blocks certificate revocation checks, retrying with best-effort checking... >&2
    curl -fsSL --ssl-revoke-best-effort "%~1" -o "%~2"
    if !ERRORLEVEL! equ 0 (
        del "%~2.err"
        exit /b 0
    )
)
type "%~2.err" >&2
del "%~2.err"
echo Download failed: %~1 >&2
exit /b 1

:find_java
rem Sets JAVA and JAVA_MAJOR from JAVA_HOME or the first java.exe on PATH, if it is JAVA_MIN or newer
if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" call :check_java "%JAVA_HOME%\bin\java.exe"
if defined JAVA exit /b 0
for %%J in (java.exe) do if not "%%~$PATH:J"=="" call :check_java "%%~$PATH:J"
exit /b 0

:check_java
rem First line of java -version is like: openjdk version "21.0.2" 2024-01-16  or  java version "1.8.0_392"
set "VLINE="
for /f "usebackq delims=" %%L in (`"%~1" -version 2^>^&1`) do if not defined VLINE set "VLINE=%%L"
if not defined VLINE exit /b 0
set "VLINE=!VLINE:"=!"
set "VER="
for /f "tokens=3" %%V in ("!VLINE!") do set "VER=%%V"
set "MAJ=" & set "MIN="
for /f "tokens=1,2 delims=._" %%A in ("!VER!") do (
    set "MAJ=%%A"
    set "MIN=%%B"
)
if "!MAJ!"=="1" set "MAJ=!MIN!"
if not defined MAJ exit /b 0
echo !MAJ!| findstr /r "^[0-9][0-9]*$" >nul || exit /b 0
if !MAJ! geq %JAVA_MIN% (
    set "JAVA=%~1"
    set "JAVA_MAJOR=!MAJ!"
)
exit /b 0
