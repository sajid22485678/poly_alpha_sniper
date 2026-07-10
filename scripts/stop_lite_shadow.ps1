# Stops ONLY the validated Poly Alpha Lite shadow process.
# PID alone is never trusted: mode, venv executable, exact module marker, and
# process creation identity are checked before a bounded force-stop fallback.
[CmdletBinding()]
param(
    [ValidateRange(1, 120)]
    [int]$GracePeriodSeconds = 12
)

$ErrorActionPreference = "Stop"
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$RuntimeDir = Join-Path $ProjectRoot "runtime\lite_shadow"
$LockFile = Join-Path $RuntimeDir "process.lock"
$StopRequestFile = Join-Path $RuntimeDir "stop.request"
$ExpectedPython = [System.IO.Path]::GetFullPath(
    (Join-Path $ProjectRoot ".venv\Scripts\python.exe"))
$ExpectedExecutables = @($ExpectedPython)
if (Test-Path -LiteralPath $ExpectedPython) {
    try {
        $basePython = (& $ExpectedPython -I -c "import sys; print(sys._base_executable)" 2>$null |
            Select-Object -First 1)
        if (-not [string]::IsNullOrWhiteSpace([string]$basePython)) {
            $ExpectedExecutables += [System.IO.Path]::GetFullPath(([string]$basePython).Trim())
        }
    } catch {}
}
$ModulePattern = '(?i)^\s*(?:"[^"]+"|\S+)\s+-m\s+"?lite\.lite_bot"?\s*$'

function Read-JsonSafe {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }
    try {
        $raw = Get-Content -LiteralPath $Path -Raw -ErrorAction Stop
        if ([string]::IsNullOrWhiteSpace($raw)) {
            return $null
        }
        return ($raw | ConvertFrom-Json -ErrorAction Stop)
    } catch {
        return $null
    }
}

function Test-LiteProcessIdentity {
    param(
        [int]$ProcessId,
        [string[]]$ExpectedExecutable
    )

    $process = Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $ProcessId) `
        -ErrorAction SilentlyContinue
    if ($null -eq $process) {
        return [pscustomobject]@{ Valid = $false; Reason = "process_not_found"; Process = $null }
    }

    $actualExecutable = [string]$process.ExecutablePath
    if ([string]::IsNullOrWhiteSpace($actualExecutable)) {
        return [pscustomobject]@{ Valid = $false; Reason = "executable_unknown"; Process = $process }
    }
    try {
        $actualExecutable = [System.IO.Path]::GetFullPath($actualExecutable)
    } catch {
        return [pscustomobject]@{ Valid = $false; Reason = "executable_invalid"; Process = $process }
    }
    $executableMatch = $false
    foreach ($expected in $ExpectedExecutable) {
        if ([string]::Equals($actualExecutable, $expected,
                [System.StringComparison]::OrdinalIgnoreCase)) {
            $executableMatch = $true
            break
        }
    }
    if (-not $executableMatch) {
        return [pscustomobject]@{ Valid = $false; Reason = "executable_mismatch"; Process = $process }
    }

    $commandLine = [string]$process.CommandLine
    if ([string]::IsNullOrWhiteSpace($commandLine) -or $commandLine -notmatch $ModulePattern) {
        return [pscustomobject]@{ Valid = $false; Reason = "module_marker_mismatch"; Process = $process }
    }

    return [pscustomobject]@{ Valid = $true; Reason = "ok"; Process = $process }
}

function Remove-MatchingControlFiles {
    param([int]$ProcessId)
    $currentLock = Read-JsonSafe -Path $LockFile
    $currentLockPid = 0
    if ($null -ne $currentLock -and
        [int]::TryParse([string]$currentLock.pid, [ref]$currentLockPid) -and
        $currentLockPid -eq $ProcessId) {
        Remove-Item -LiteralPath $LockFile -Force -ErrorAction SilentlyContinue
    }
    Remove-Item -LiteralPath $StopRequestFile -Force -ErrorAction SilentlyContinue
}

if (-not (Test-Path -LiteralPath $LockFile)) {
    Write-Output "Poly Alpha Lite shadow is not running (no Lite process lock)."
    exit 0
}

$lock = Read-JsonSafe -Path $LockFile
if ($null -eq $lock) {
    Write-Error "Lite process lock is empty or malformed; refusing to stop any process."
    exit 1
}

if ([string]$lock.mode -ne "lite_shadow") {
    Write-Error ("Lite lock mode is '{0}', not 'lite_shadow'; refusing to stop any process." -f $lock.mode)
    exit 1
}

$targetPid = 0
if (-not [int]::TryParse([string]$lock.pid, [ref]$targetPid) -or $targetPid -le 0) {
    Write-Error "Lite process lock has no valid PID; refusing to stop any process."
    exit 1
}

$identity = Test-LiteProcessIdentity -ProcessId $targetPid -ExpectedExecutable $ExpectedExecutables
if (-not $identity.Valid) {
    if ($identity.Reason -eq "process_not_found") {
        Remove-MatchingControlFiles -ProcessId $targetPid
        Write-Output ("Lite PID {0} is no longer alive; removed its stale control files." -f $targetPid)
        exit 0
    }
    Write-Error ("PID {0} failed Lite identity validation ({1}); refusing to stop it." -f
        $targetPid, $identity.Reason)
    exit 2
}

$initialCreationDate = [string]$identity.Process.CreationDate
$request = [ordered]@{
    requested_ts_ms = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    target_pid      = $targetPid
    mode            = "lite_shadow"
}
$requestJson = $request | ConvertTo-Json -Compress
$tempRequest = "{0}.tmp.{1}" -f $StopRequestFile, $PID
[System.IO.File]::WriteAllText(
    $tempRequest,
    $requestJson,
    (New-Object System.Text.UTF8Encoding($false)))
Move-Item -LiteralPath $tempRequest -Destination $StopRequestFile -Force

Write-Output ("Graceful stop requested for Poly Alpha Lite shadow PID {0}." -f $targetPid)
$deadline = [DateTime]::UtcNow.AddSeconds($GracePeriodSeconds)
$exited = $false
$pidReused = $false

while ([DateTime]::UtcNow -lt $deadline) {
    $current = Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $targetPid) `
        -ErrorAction SilentlyContinue
    if ($null -eq $current) {
        $exited = $true
        break
    }
    if ([string]$current.CreationDate -ne $initialCreationDate) {
        $pidReused = $true
        break
    }
    Start-Sleep -Milliseconds 250
}

if ($pidReused) {
    Write-Error ("PID {0} was reused while waiting; refusing to stop the new process." -f $targetPid)
    exit 2
}

if (-not $exited) {
    # Revalidate immediately before force-stop. This is deliberately scoped to
    # the one PID from the Lite lock; there is no process-name scan or fallback.
    $finalIdentity = Test-LiteProcessIdentity -ProcessId $targetPid -ExpectedExecutable $ExpectedExecutables
    if (-not $finalIdentity.Valid -or
        [string]$finalIdentity.Process.CreationDate -ne $initialCreationDate) {
        Write-Error ("PID {0} no longer has the validated Lite identity; refusing force-stop." -f $targetPid)
        exit 2
    }

    Write-Output ("Grace period expired; force-stopping only validated Lite PID {0}." -f $targetPid)
    Stop-Process -Id $targetPid -Force -ErrorAction Stop

    $forceDeadline = [DateTime]::UtcNow.AddSeconds(5)
    while ([DateTime]::UtcNow -lt $forceDeadline) {
        if ($null -eq (Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $targetPid) `
                    -ErrorAction SilentlyContinue)) {
            $exited = $true
            break
        }
        Start-Sleep -Milliseconds 250
    }
}

if (-not $exited) {
    Write-Error ("Validated Lite PID {0} did not exit." -f $targetPid)
    exit 1
}

Remove-MatchingControlFiles -ProcessId $targetPid
Write-Output ("Stopped Poly Alpha Lite shadow PID {0}. Advanced bot processes were not touched." -f $targetPid)
exit 0
