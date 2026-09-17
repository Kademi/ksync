# ksync3 installer for Windows (PowerShell 5.1+).
#   irm https://raw.githubusercontent.com/Kademi/ksync/master/installers/install.ps1 | iex
# Installs ksync3.jar and, if no Java 11+ is found, a JRE into %LOCALAPPDATA%\Programs\ksync3, and puts a `ksync3` command on the user PATH.
# Overrides: $env:KSYNC3_HOME, $env:KSYNC3_VERSION, $env:KSYNC3_JAR_URL, $env:KSYNC3_JAVA_VERSION, $env:KSYNC3_BUNDLE_JRE=1
$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$ProgressPreference = 'SilentlyContinue'  # Invoke-WebRequest is very slow with the progress bar on

# The latest release, or KSYNC3_VERSION with or without its leading v, as in 1.8.9 or v1.8.9.
$Version     = if ($env:KSYNC3_VERSION) { $env:KSYNC3_VERSION } else { 'latest' }
$BaseUrl     = 'https://github.com/Kademi/ksync/releases/latest/download'
if ($Version -ne 'latest') {
    if ($Version -notlike 'v*') { $Version = "v$Version" }
    $BaseUrl = "https://github.com/Kademi/ksync/releases/download/$Version"
}
$JarUrl      = if ($env:KSYNC3_JAR_URL) { $env:KSYNC3_JAR_URL } else { "$BaseUrl/ksync3.jar" }
$JavaMin     = 11
$Arch        = if ($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') { 'aarch64' } else { 'x64' }
# Temurin has no 25 JRE for Windows on ARM yet, so that platform gets the previous LTS
$JavaDefault = if ($Arch -eq 'aarch64') { '21' } else { '25' }
$JavaVersion = if ($env:KSYNC3_JAVA_VERSION) { $env:KSYNC3_JAVA_VERSION } else { $JavaDefault }
$HomeDir     = if ($env:KSYNC3_HOME) { $env:KSYNC3_HOME } else { Join-Path $env:LOCALAPPDATA 'Programs\ksync3' }
$BinDir      = Join-Path $HomeDir 'bin'

# Major version of the java.exe given, or $null if it does not run.
function Get-JavaMajor($java) {
    try { $line = (& $java -version 2>&1 | Select-Object -First 1) -join '' } catch { return $null }
    if ($line -match 'version "(\d+)(?:\.(\d+))?') {
        if ($Matches[1] -eq '1') { return [int]$Matches[2] } else { return [int]$Matches[1] }
    }
}

function Find-Java {
    $candidates = @()
    if ($env:JAVA_HOME) { $candidates += Join-Path $env:JAVA_HOME 'bin\java.exe' }
    $onPath = Get-Command java.exe -ErrorAction SilentlyContinue
    if ($onPath) { $candidates += $onPath.Source }
    foreach ($j in $candidates) {
        if ((Test-Path $j) -and (Get-JavaMajor $j) -ge $JavaMin) { return $j }
    }
}

New-Item -ItemType Directory -Force -Path $HomeDir, $BinDir | Out-Null

Write-Host 'Downloading ksync3.jar ...'
Invoke-WebRequest -Uri $JarUrl -OutFile "$HomeDir\ksync3.jar.tmp"

# The checksum comes from the same release as the jar, so it catches a truncated or
# swapped download, not a compromised release. A KSYNC3_JAR_URL of your own has no
# SHA256SUMS to check it against.
if (-not $env:KSYNC3_JAR_URL) {
    # To a file, then read it back. GitHub serves release assets as application/octet-stream,
    # and Windows PowerShell hands back .Content as a byte array for anything it does not
    # consider text, so splitting that into lines found nothing and every install failed here.
    Invoke-WebRequest -Uri "$BaseUrl/SHA256SUMS" -OutFile "$HomeDir\SHA256SUMS.tmp"
    $line = Get-Content "$HomeDir\SHA256SUMS.tmp" | Where-Object { $_ -match '\s+ksync3\.jar\s*$' } | Select-Object -First 1
    Remove-Item -Force "$HomeDir\SHA256SUMS.tmp"
    if (-not $line) { throw "SHA256SUMS in the release has no line for ksync3.jar" }
    $expected = ($line -split '\s+')[0]
    $actual = (Get-FileHash "$HomeDir\ksync3.jar.tmp" -Algorithm SHA256).Hash
    if ($actual -ine $expected) {
        Remove-Item -Force "$HomeDir\ksync3.jar.tmp"
        throw "Checksum mismatch for ksync3.jar. Expected $expected, got $actual"
    }
    Write-Host 'Checksum verified'
}
Move-Item -Force "$HomeDir\ksync3.jar.tmp" "$HomeDir\ksync3.jar"

$Java = if ($env:KSYNC3_BUNDLE_JRE -eq '1') { $null } else { Find-Java }
if ($Java) {
    Write-Host "Using Java $(Get-JavaMajor $Java) at $Java"
} else {
    Write-Host "No Java $JavaMin+ found, downloading Temurin JRE $JavaVersion (windows/$Arch) ..."
    $JreUrl = "https://api.adoptium.net/v3/binary/latest/$JavaVersion/ga/windows/$Arch/jre/hotspot/normal/eclipse"
    Invoke-WebRequest -Uri $JreUrl -OutFile "$HomeDir\jre.zip"
    Remove-Item -Recurse -Force "$HomeDir\jre" -ErrorAction SilentlyContinue
    Expand-Archive "$HomeDir\jre.zip" "$HomeDir\jre.tmp"
    Move-Item (Get-ChildItem "$HomeDir\jre.tmp" | Select-Object -First 1).FullName "$HomeDir\jre"  # zip has one top-level dir
    Remove-Item -Recurse -Force "$HomeDir\jre.tmp", "$HomeDir\jre.zip"
    $Java = "$HomeDir\jre\bin\java.exe"
}

@"
@echo off
rem ksync3 launcher, written by install.ps1
for %%I in ("%cd%") do title Ksync3 ^| %%~nxI
"$Java" -jar "$HomeDir\ksync3.jar" %*
"@ | Set-Content -Encoding ASCII "$BinDir\ksync3.cmd"

$UserPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (($UserPath -split ';') -notcontains $BinDir) {
    [Environment]::SetEnvironmentVariable('Path', "$BinDir;$UserPath", 'User')
    $env:Path = "$BinDir;$env:Path"
    Write-Host "Added $BinDir to your user PATH. Open a new terminal to pick it up."
}
# Uninstaller: removes the PATH entry, then the install dir. Batch so it works from cmd and PowerShell.
@'
@echo off
setlocal enabledelayedexpansion
rem ksync3 uninstaller, written by the installer. Removes the PATH entry, then the install dir.
set "HOME_DIR=__HOME__"
set "BIN_DIR=__BIN__"
set "USERPATH="
for /f "skip=2 tokens=1,2,*" %%A in ('reg query HKCU\Environment /v Path 2^>nul') do set "USERPATH=%%C"
set "USERPATH=!USERPATH:%BIN_DIR%;=!"
set "USERPATH=!USERPATH:;%BIN_DIR%=!"
reg add HKCU\Environment /v Path /t REG_EXPAND_SZ /d "!USERPATH!" /f >nul
reg delete HKCU\Environment /v KSYNC3_HOME /f >nul 2>&1
cd /d "%TEMP%"
echo ksync3 removed.
(goto) 2>nul & rmdir /s /q "%HOME_DIR%"
'@ -replace '__HOME__', $HomeDir -replace '__BIN__', $BinDir | Set-Content -Encoding ASCII "$HomeDir\uninstall.cmd"

Write-Host "Installed ksync3 to $BinDir\ksync3.cmd"
Write-Host "Uninstall: run $HomeDir\uninstall.cmd"
