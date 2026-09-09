<#
.SYNOPSIS
    Puts this folder on your PATH, so ksync-sync can be run by name from any checkout.

.DESCRIPTION
    Adds the folder this script lives in - the one holding ksync-sync.bat - to your own PATH.
    Nothing machine wide, so it needs no administrator, and because it is the folder inside the
    clone that goes on the PATH, a git pull updates the script you are running with no re-run.

    Run it once:

        add-to-path.bat

    or, from PowerShell:

        .\add-to-path.ps1

    Then open a new terminal and "ksync-sync pull" works from any checkout.

.PARAMETER Remove
    Takes the folder off your PATH again.

.NOTES
    Deliberately not "setx PATH %PATH%;...", which is the usual advice and is a trap twice over:
    setx silently truncates anything past 1024 characters, and %PATH% is the machine PATH and
    yours already joined together, so it copies every system entry into your own PATH where it
    shadows later machine wide changes. This reads and writes only your half.
#>
[CmdletBinding()]
param(
    [switch]$Remove
)

$ErrorActionPreference = 'Stop'

$dir = $PSScriptRoot.TrimEnd('\')

# The registry rather than [Environment]::SetEnvironmentVariable, to keep entries like
# %USERPROFILE%\bin as the variables they are: reading through the Environment api expands them,
# and writing back would freeze today's expansion into someone else's PATH permanently.
$key = [Microsoft.Win32.Registry]::CurrentUser.OpenSubKey('Environment', $true)
if (-not $key) {
    throw 'Could not open HKCU\Environment, so your PATH cannot be read or written'
}
try {
    $existing = ''
    $kind = [Microsoft.Win32.RegistryValueKind]::ExpandString
    if ($key.GetValueNames() -contains 'Path') {
        $existing = $key.GetValue('Path', '', [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
        $kind = $key.GetValueKind('Path')
    }

    # Empty entries are dropped on the way out, which is how a PATH ending in a stray semicolon
    # quietly fixes itself rather than growing another one.
    $parts = @($existing -split ';' | Where-Object { $_ -ne '' })
    # -eq on strings is case insensitive, which is what Windows paths want, and a trailing
    # backslash is not a different folder
    $already = @($parts | Where-Object { $_.TrimEnd('\') -eq $dir }).Count -gt 0

    if ($Remove) {
        if (-not $already) {
            Write-Host "Not on your PATH, so nothing to remove: $dir"
            return
        }
        $parts = @($parts | Where-Object { $_.TrimEnd('\') -ne $dir })
        $key.SetValue('Path', ($parts -join ';'), $kind)
        Write-Host "Removed from your PATH: $dir"
    } elseif ($already) {
        Write-Host "Already on your PATH: $dir"
        Write-Host 'Nothing to do. If "ksync-sync" is still not found, open a new terminal.'
        return
    } else {
        $parts += $dir
        $key.SetValue('Path', ($parts -join ';'), $kind)
        Write-Host "Added to your PATH: $dir"
    }
} finally {
    $key.Dispose()
}

# This session too, so it can be tried without opening another terminal
if ($Remove) {
    $env:Path = ($env:Path -split ';' | Where-Object { $_ -ne '' -and $_.TrimEnd('\') -ne $dir }) -join ';'
} else {
    $env:Path = "$env:Path;$dir"
}

# Tell the running desktop, so a terminal opened from the Start menu sees the change. Explorer
# hands its own copy of the environment to everything it launches, and without this it keeps
# handing out the old one until you log out.
try {
    if (-not ('KsyncEnv.Native' -as [type])) {
        Add-Type -Namespace KsyncEnv -Name Native -MemberDefinition @'
[System.Runtime.InteropServices.DllImport("user32.dll", SetLastError = true, CharSet = System.Runtime.InteropServices.CharSet.Auto)]
public static extern System.IntPtr SendMessageTimeout(System.IntPtr hWnd, uint Msg, System.IntPtr wParam, string lParam, uint fuFlags, uint uTimeout, out System.IntPtr lpdwResult);
'@
    }
    $unused = [System.IntPtr]::Zero
    # HWND_BROADCAST, WM_SETTINGCHANGE, SMTO_ABORTIFHUNG, 100ms - a hung window must not hold
    # this up, the PATH is already written by now
    [void][KsyncEnv.Native]::SendMessageTimeout([System.IntPtr]0xffff, 0x1A, [System.IntPtr]::Zero, 'Environment', 2, 100, [ref]$unused)
} catch {
    Write-Verbose "Could not notify the desktop of the PATH change: $_"
}

Write-Host ''
Write-Host 'Open a new terminal, then from any ksync checkout:'
Write-Host '    ksync-sync          # sync'
Write-Host '    ksync-sync pull     # pull instead'
