@echo off
rem
rem Puts this folder on your PATH, so ksync-sync can be run by name from any checkout.
rem
rem For anyone in cmd, or who would rather double click something than think about PowerShell
rem execution policy. It runs add-to-path.ps1, which does the work and explains itself.
rem
rem   add-to-path.bat            :: add it
rem   add-to-path.bat -Remove    :: take it off again
rem
rem -ExecutionPolicy Bypass applies to this one run of this one file. It changes no setting, and
rem is what lets a freshly cloned script run on a machine whose policy is the default Restricted.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0add-to-path.ps1" %*
exit /b %ERRORLEVEL%
