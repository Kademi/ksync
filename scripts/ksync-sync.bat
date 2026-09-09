@echo off
rem
rem Runs a ksync command on whatever directory you are standing in, with local as the authority.
rem The Windows twin of ksync-sync.sh - same commands, same defaults, same environment variables.
rem
rem ksync takes the checkout from the jvm's working directory, so there is nothing to pass in: cd
rem to the checkout and run this. The command defaults to sync; a first argument that is not a
rem flag replaces it. Everything else goes straight through, so all of these work:
rem
rem   ksync-sync                     :: sync
rem   ksync-sync pull                :: pull instead
rem   ksync-sync pull -debug
rem   ksync-sync push
rem   ksync-sync sync -url https://site/repos/myrepo   :: follow a repo's latest version
rem   ksync-sync -notray -debug      :: still a sync: flags are not commands
rem
rem PowerShell runs this the same way - no separate .ps1 needed. Both find it by name once the
rem folder is on your PATH; add-to-path.bat in this folder does that for you.
rem
rem Set KSYNC_LOCALWINS=0 to be asked about conflicts instead, which is the ksync default.
rem Set KSYNC_JAR to run a jar somewhere other than the one beside this script.
rem
rem -localwins is passed by default to the three commands that resolve conflicts. For sync and
rem push that means the local checkout overwrites the remote even where the remote has changed;
rem for pull it means a remote change to a file you have edited is dropped in favour of yours.
rem Either way nothing is prompted. That is what you want for a checkout that git manages, and it
rem is the wrong thing anywhere the server is the source of truth.

setlocal EnableExtensions

rem A flag starts with a dash and a command does not, which is the whole rule, so a bare
rem invocation and one carrying extra flags both still mean sync.
set "FIRST=%~1"
set "COMMAND=sync"
if not defined FIRST goto :haveCommand
if "%FIRST:~0,1%"=="-" goto :haveCommand
set "COMMAND=%FIRST%"
shift
:haveCommand

rem The commands ksync registers. ksync itself is the authority - if you add one there, add it
rem here too. Checked up front because ksync answers an unknown command by printing its usage and
rem exiting 0, which a caller cannot tell apart from a command that ran. Case sensitive, because
rem ksync matches the name exactly and would print its usage for SYNC.
set "KNOWN=sync pull push checkout verify login ignore publish usage"
set "VALID="
for %%c in (%KNOWN%) do if "%%c"=="%COMMAND%" set "VALID=1"
if not defined VALID (
    rem Redirection first, which reads oddly and is the only form that is safe: cmd reads the
    rem digit before a > as a file handle, so "echo ...%COMMAND%>&2" on a command ending in a
    rem digit would swallow the digit and redirect on it instead.
    >&2 echo Unknown ksync command: %COMMAND%
    >&2 echo Use one of: %KNOWN%
    exit /b 2
)

rem Rebuilt one at a time because shift does not move %*, so the arguments after the command have
rem to be collected by hand.
set ARGS=
:collect
if "%~1"=="" goto :collected
set ARGS=%ARGS% "%~1"
shift
goto :collect
:collected

rem The jar that ships beside this script, so a clone works wherever it is put and a git pull
rem updates both together. %~dp0 is this script's own folder: used to find the jar, never to
rem change directory, because the working directory is what ksync syncs.
set "JAR=%KSYNC_JAR%"
if not defined JAR set "JAR=%~dp0..\dist\ksync3.jar"
if not exist "%JAR%" (
    >&2 echo ksync jar not found at %JAR%
    >&2 echo Build one with: cd /d "%~dp0..\ksync-cli" ^&^& mvn package
    >&2 echo Or set KSYNC_JAR to an existing jar.
    exit /b 1
)

set "JAVA="
where /q java.exe && set "JAVA=java"
if not defined JAVA if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" set "JAVA=%JAVA_HOME%\bin\java.exe"
if not defined JAVA (
    >&2 echo No java on PATH, and JAVA_HOME is not set to a jdk
    exit /b 1
)

rem checkout and login are how a checkout comes into being, so for those the absence is expected.
if not exist ".ksync" if not "%COMMAND%"=="checkout" if not "%COMMAND%"=="login" >&2 echo Note: no .ksync in %CD%, so this is not a checkout yet - ksync will ask for a url.

rem Only the commands that resolve conflicts are given it. Passing it to login or verify would do
rem nothing, and saying "local wins" on the way into them would be a lie about what is happening.
set "LOCALWINS="
set "RESOLVES="
if "%COMMAND%"=="sync" set "RESOLVES=1"
if "%COMMAND%"=="push" set "RESOLVES=1"
if "%COMMAND%"=="pull" set "RESOLVES=1"
if defined RESOLVES (
    rem On unless it is explicitly turned off, so a typo in the variable name cannot silently
    rem leave the remote authoritative when this script says it is not.
    if "%KSYNC_LOCALWINS%"=="0" (
        echo Running ksync %COMMAND% in %CD% - conflicts will be asked about
    ) else (
        set "LOCALWINS=-localwins"
        echo Running ksync %COMMAND% in %CD% - local wins, remote changes are discarded
    )
) else (
    echo Running ksync %COMMAND% in %CD%
)

rem Ctrl-C reaches the jvm before cmd asks "Terminate batch job (Y/N)?", so a sync still runs its
rem shutdown hook: the status icon goes away and the final status file is written either way.
"%JAVA%" -jar "%JAR%" -command %COMMAND% %LOCALWINS% %ARGS%
exit /b %ERRORLEVEL%
