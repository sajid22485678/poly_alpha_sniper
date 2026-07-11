# Read-only status for Poly Alpha Lite shadow. This script never creates,
# removes, or changes runtime files and never scans/kills generic Python.
$ErrorActionPreference = "SilentlyContinue"
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$RuntimeDir = Join-Path $ProjectRoot "runtime\lite_shadow"
$LockFile = Join-Path $RuntimeDir "process.lock"
$HeartbeatFile = Join-Path $RuntimeDir "heartbeat.json"
$StateFile = Join-Path $RuntimeDir "state.json"
$ExpectedPython = [System.IO.Path]::GetFullPath(
    (Join-Path $ProjectRoot ".venv\Scripts\python.exe"))
$ExpectedExecutables = @($ExpectedPython)
if (Test-Path -LiteralPath $ExpectedPython) {
    try {
        # Windows venv python.exe is a redirector: the long-lived interpreter
        # may be sys._base_executable while retaining the exact Lite module
        # command line. Allow only that resolved base interpreter as well.
        $basePython = (& $ExpectedPython -I -c "import sys; print(sys._base_executable)" 2>$null |
            Select-Object -First 1)
        if (-not [string]::IsNullOrWhiteSpace([string]$basePython)) {
            $ExpectedExecutables += [System.IO.Path]::GetFullPath(([string]$basePython).Trim())
        }
    } catch {}
}
$ExpectedDb = Join-Path $ProjectRoot "data\poly_alpha_lite.db"
$ModulePattern = '(?i)^\s*(?:"[^"]+"|\S+)\s+-m\s+"?lite\.lite_bot"?\s*$'
$HeartbeatStaleSeconds = 15.0

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

function Get-JsonProperty {
    param([object]$Object, [string]$Name)
    if ($null -eq $Object) {
        return [pscustomobject]@{ Present = $false; Value = $null }
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return [pscustomobject]@{ Present = $false; Value = $null }
    }
    return [pscustomobject]@{ Present = $true; Value = $property.Value }
}

function Get-FirstJsonProperty {
    param([object]$Object, [string[]]$Names)
    foreach ($name in $Names) {
        $field = Get-JsonProperty -Object $Object -Name $name
        if ($field.Present) {
            return $field
        }
    }
    return [pscustomobject]@{ Present = $false; Value = $null }
}

function Format-TimeValue {
    param([object]$Value)
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) {
        return "UNKNOWN"
    }
    $milliseconds = [int64]0
    if ([int64]::TryParse([string]$Value, [ref]$milliseconds) -and
        $milliseconds -gt 100000000000) {
        try {
            return [DateTimeOffset]::FromUnixTimeMilliseconds($milliseconds).ToLocalTime().ToString("o")
        } catch {
            return [string]$Value
        }
    }
    return [string]$Value
}

function Format-StrictBoolean {
    param([object]$Field)
    if (-not $Field.Present -or -not ($Field.Value -is [bool])) {
        return "UNKNOWN"
    }
    if ([bool]$Field.Value) {
        return "true"
    }
    return "false"
}

function Test-LiteProcessIdentity {
    param([int]$ProcessId)
    if ($ProcessId -le 0) {
        return [pscustomobject]@{ Valid = $false; Reason = "invalid_pid"; Process = $null }
    }

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
    foreach ($expected in $ExpectedExecutables) {
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

$lock = Read-JsonSafe -Path $LockFile
$heartbeat = Read-JsonSafe -Path $HeartbeatFile
$state = Read-JsonSafe -Path $StateFile

$lockModeField = Get-JsonProperty -Object $lock -Name "mode"
$lockPidField = Get-JsonProperty -Object $lock -Name "pid"
$targetPid = 0
$lockPidValid = $lockPidField.Present -and
    [int]::TryParse([string]$lockPidField.Value, [ref]$targetPid) -and
    $targetPid -gt 0
$lockModeValid = $lockModeField.Present -and [string]$lockModeField.Value -eq "lite_shadow"

$processIdentity = Test-LiteProcessIdentity -ProcessId $targetPid
$exactLiteProcesses = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
    $candidate = $_
    $candidatePath = ""
    try { $candidatePath = [System.IO.Path]::GetFullPath([string]$candidate.ExecutablePath) } catch {}
    $pathMatch = @($ExpectedExecutables | Where-Object {
        [string]::Equals($candidatePath, $_, [System.StringComparison]::OrdinalIgnoreCase)
    }).Count -gt 0
    $pathMatch -and ([string]$candidate.CommandLine -match $ModulePattern)
})
$ownedLiteProcessIds = New-Object 'System.Collections.Generic.HashSet[int]'
if ($lockPidValid) {
    [void]$ownedLiteProcessIds.Add($targetPid)
    # Windows venv launchers can retain a redirector parent whose command line
    # is identical while the base interpreter child owns the runtime lock.
    # Walk only the lock owner's exact parent chain; unrelated sibling/child
    # Lite invocations remain orphans and keep status fail-closed.
    $cursor = $exactLiteProcesses | Where-Object { $_.ProcessId -eq $targetPid } |
        Select-Object -First 1
    while ($null -ne $cursor) {
        $parent = $exactLiteProcesses | Where-Object {
            $_.ProcessId -eq $cursor.ParentProcessId
        } | Select-Object -First 1
        if ($null -eq $parent -or $ownedLiteProcessIds.Contains([int]$parent.ProcessId)) {
            break
        }
        [void]$ownedLiteProcessIds.Add([int]$parent.ProcessId)
        $cursor = $parent
    }
}
$orphanLiteProcesses = @($exactLiteProcesses | Where-Object {
    -not $ownedLiteProcessIds.Contains([int]$_.ProcessId)
})
$orphanCount = $orphanLiteProcesses.Count
# A process with the exact Lite executable/module identity is running even if
# its lock mode is corrupt. Treat that as unsafe (exit 2) so start never creates
# a duplicate; do not misreport it as stopped merely because metadata is bad.
$running = $lockPidValid -and $processIdentity.Valid
$lockModeCrosscheck = "UNKNOWN"
if ($lockModeField.Present) {
    if ($lockModeValid) {
        $lockModeCrosscheck = "MATCH"
    } else {
        $lockModeCrosscheck = "MISMATCH"
    }
}

$heartbeatPidField = Get-JsonProperty -Object $heartbeat -Name "pid"
$heartbeatModeField = Get-JsonProperty -Object $heartbeat -Name "mode"
$heartbeatTsField = Get-JsonProperty -Object $heartbeat -Name "ts_ms"
$lockNonceField = Get-JsonProperty -Object $lock -Name "launch_nonce"
$heartbeatNonceField = Get-JsonProperty -Object $heartbeat -Name "launch_nonce"
$stateNonceField = Get-JsonProperty -Object $state -Name "launch_nonce"
$nonceCrosscheck = "UNKNOWN"
if ($lockNonceField.Present -and $heartbeatNonceField.Present -and $stateNonceField.Present) {
    $nonce = [string]$lockNonceField.Value
    if (-not [string]::IsNullOrWhiteSpace($nonce) -and
        $nonce -eq [string]$heartbeatNonceField.Value -and
        $nonce -eq [string]$stateNonceField.Value) {
        $nonceCrosscheck = "MATCH"
    } else {
        $nonceCrosscheck = "MISMATCH"
    }
}
if (-not $heartbeatTsField.Present) {
    $heartbeatTsField = Get-JsonProperty -Object $state -Name "heartbeat_ts_ms"
}

$heartbeatPid = 0
$heartbeatCrosscheck = "UNKNOWN"
if ($null -ne $heartbeat -and
    $heartbeatPidField.Present -and
    [int]::TryParse([string]$heartbeatPidField.Value, [ref]$heartbeatPid) -and
    $heartbeatModeField.Present) {
    if ($lockModeValid -and $lockPidValid -and $heartbeatPid -eq $targetPid -and
        [string]$heartbeatModeField.Value -eq "lite_shadow") {
        $heartbeatCrosscheck = "MATCH"
    } else {
        $heartbeatCrosscheck = "MISMATCH"
    }
}

$heartbeatAgeSeconds = $null
$heartbeatFresh = $false
$heartbeatMilliseconds = [int64]0
if ($heartbeatTsField.Present -and
    [int64]::TryParse([string]$heartbeatTsField.Value, [ref]$heartbeatMilliseconds)) {
    $heartbeatAgeSeconds = ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() -
        $heartbeatMilliseconds) / 1000.0
    $heartbeatFresh = $heartbeatAgeSeconds -ge -5.0 -and
        $heartbeatAgeSeconds -le $HeartbeatStaleSeconds
}

$modeField = Get-JsonProperty -Object $state -Name "mode"
$dryRunField = Get-JsonProperty -Object $state -Name "dry_run"
$liveEnabledField = Get-JsonProperty -Object $state -Name "live_enabled"
$stateRunningField = Get-JsonProperty -Object $state -Name "running"
$modeText = "UNKNOWN"
if ($modeField.Present -and -not [string]::IsNullOrWhiteSpace([string]$modeField.Value)) {
    $modeText = [string]$modeField.Value
}
$dryRunText = Format-StrictBoolean -Field $dryRunField
$liveEnabledText = Format-StrictBoolean -Field $liveEnabledField

$safetyStatus = "UNKNOWN"
$safetyFieldsKnown = $modeField.Present -and ($dryRunField.Value -is [bool]) -and
    ($liveEnabledField.Value -is [bool])
if ($lockModeField.Present -and -not $lockModeValid) {
    $safetyStatus = "VIOLATION"
} elseif ($lockModeValid -and $safetyFieldsKnown) {
    if ($modeText -eq "lite_shadow" -and [bool]$dryRunField.Value -and
        ($stateRunningField.Value -is [bool]) -and [bool]$stateRunningField.Value -and
        (-not [bool]$liveEnabledField.Value)) {
        $safetyStatus = "OK"
    } else {
        $safetyStatus = "VIOLATION"
    }
}

$openPositionsField = Get-FirstJsonProperty -Object $state -Names @(
    "open_positions", "open_position_count")
$openPositionsText = "UNKNOWN"
if ($openPositionsField.Present -and $null -ne $openPositionsField.Value) {
    if ($openPositionsField.Value -is [System.Array]) {
        $openPositionsText = [string]$openPositionsField.Value.Count
    } else {
        $openPositionsText = [string]$openPositionsField.Value
    }
}

$lastScanField = Get-FirstJsonProperty -Object $state -Names @(
    "last_scan_ts_ms", "last_scan_at", "last_scan")
$lastTradeField = Get-FirstJsonProperty -Object $state -Names @(
    "last_trade_ts_ms", "last_trade_at", "last_trade")
$dbPathField = Get-JsonProperty -Object $state -Name "db_path"
$dbPathText = $ExpectedDb
if ($dbPathField.Present -and -not [string]::IsNullOrWhiteSpace([string]$dbPathField.Value)) {
    $dbPathText = [string]$dbPathField.Value
}

$runningText = "false"
if ($running) { $runningText = "true" }
$pidText = "UNKNOWN"
if ($lockPidValid) { $pidText = [string]$targetPid }
$heartbeatText = "UNKNOWN"
if ($heartbeatTsField.Present) {
    $heartbeatText = Format-TimeValue -Value $heartbeatTsField.Value
}
$heartbeatAgeText = "UNKNOWN"
if ($null -ne $heartbeatAgeSeconds) {
    $heartbeatAgeText = ("{0:N1}s" -f $heartbeatAgeSeconds)
}
$heartbeatFreshText = "false"
if ($heartbeatFresh) { $heartbeatFreshText = "true" }

Write-Output "Poly Alpha Lite Shadow Status"
Write-Output "-----------------------------"
Write-Output ("running             : {0}" -f $runningText)
Write-Output ("pid                 : {0}" -f $pidText)
Write-Output ("process_identity    : {0} ({1})" -f $processIdentity.Valid, $processIdentity.Reason)
Write-Output ("exact_lite_processes: {0}" -f $exactLiteProcesses.Count)
Write-Output ("owned_lite_processes: {0}" -f $ownedLiteProcessIds.Count)
Write-Output ("orphan_lite_processes: {0}" -f $orphanCount)
Write-Output ("lock_mode_crosscheck: {0}" -f $lockModeCrosscheck)
Write-Output ("launch_nonce_check  : {0}" -f $nonceCrosscheck)
Write-Output ("heartbeat           : {0}" -f $heartbeatText)
Write-Output ("heartbeat_age       : {0}" -f $heartbeatAgeText)
Write-Output ("heartbeat_fresh     : {0}" -f $heartbeatFreshText)
Write-Output ("heartbeat_crosscheck: {0}" -f $heartbeatCrosscheck)
Write-Output ("mode                : {0}" -f $modeText)
Write-Output ("dry_run             : {0}" -f $dryRunText)
Write-Output ("live_enabled        : {0}" -f $liveEnabledText)
Write-Output ("safety              : {0}" -f $safetyStatus)
Write-Output ("open_positions      : {0}" -f $openPositionsText)
Write-Output ("last_scan           : {0}" -f (Format-TimeValue -Value $lastScanField.Value))
Write-Output ("last_trade          : {0}" -f (Format-TimeValue -Value $lastTradeField.Value))
Write-Output ("db_path             : {0}" -f $dbPathText)

# Exit 2 means a validated Lite process exists but health/safety is not fully
# verified. Start uses this to refuse a duplicate. Exit 1 means no validated
# Lite process. Only a healthy, safe Lite process returns 0.
if ($running) {
    if ($safetyStatus -eq "OK" -and $heartbeatCrosscheck -eq "MATCH" -and
        $nonceCrosscheck -eq "MATCH" -and $orphanCount -eq 0 -and $heartbeatFresh) {
        exit 0
    }
    exit 2
}
if ($exactLiteProcesses.Count -gt 0) {
    # An exact Lite module process without the authoritative lock is unsafe;
    # start must refuse to create another and status must never call it stopped.
    exit 2
}
exit 1
