# Read-only health and ownership check for ONLY lite_frequency_v4_shadow.
$ErrorActionPreference = "SilentlyContinue"
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$RuntimeDir = Join-Path $ProjectRoot "runtime\lite_frequency_v4_shadow"
$LockFile = Join-Path $RuntimeDir "process.lock"
$StateFile = Join-Path $RuntimeDir "state.json"
$HeartbeatFile = Join-Path $RuntimeDir "heartbeat.json"
# Display fallback only, used when the runtime has not published its own
# db_path.  Resolved from configuration because the database now lives on the
# SSD rather than under the repository; this status view is polled in a tight
# loop by the launcher, so it is folded into the interpreter probe below rather
# than costing a second process spawn.
$ExpectedDb = ""
$ExpectedPython = [System.IO.Path]::GetFullPath((Join-Path $ProjectRoot ".venv\Scripts\python.exe"))
$ExpectedExecutables = @($ExpectedPython)
if (Test-Path -LiteralPath $ExpectedPython) {
    try {
        $probe = @(& $ExpectedPython -I -c @"
import sys
sys.path.insert(0, r'$ProjectRoot')
print(sys._base_executable)
try:
    from lite_frequency_v4.config import V4_DB_PATH
    print(V4_DB_PATH)
except Exception:
    print('')
"@ 2>$null)
        $base = if ($probe.Count -ge 1) { $probe[0] } else { "" }
        if (-not [string]::IsNullOrWhiteSpace([string]$base)) {
            $ExpectedExecutables += [System.IO.Path]::GetFullPath(([string]$base).Trim())
        }
        if ($probe.Count -ge 2 -and -not [string]::IsNullOrWhiteSpace([string]$probe[1])) {
            $ExpectedDb = ([string]$probe[1]).Trim()
        }
    } catch {}
}
if (-not $ExpectedDb) {
    $ExpectedDb = Join-Path $ProjectRoot "data\poly_alpha_frequency_v4.db"
}
$ModulePattern = '(?i)^\s*(?:"[^"]+"|\S+)\s+-m\s+"?lite_frequency_v4\.bot"?\s*$'

function Read-JsonSafe([string]$Path) {
    try {
        if (-not (Test-Path -LiteralPath $Path)) { return $null }
        $value = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -ErrorAction Stop
        return $value
    } catch { return $null }
}

function Test-V4Process([int]$ProcessId) {
    if ($ProcessId -le 0) { return [pscustomobject]@{Valid=$false;Reason="invalid_pid";Process=$null} }
    $process = Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $ProcessId) -ErrorAction SilentlyContinue
    if ($null -eq $process) { return [pscustomobject]@{Valid=$false;Reason="process_not_found";Process=$null} }
    try { $path = [System.IO.Path]::GetFullPath([string]$process.ExecutablePath) } catch { $path = "" }
    $pathOk = @($ExpectedExecutables | Where-Object { [string]::Equals($path,$_,[System.StringComparison]::OrdinalIgnoreCase) }).Count -gt 0
    $moduleOk = [string]$process.CommandLine -match $ModulePattern
    if (-not $pathOk) { return [pscustomobject]@{Valid=$false;Reason="executable_mismatch";Process=$process} }
    if (-not $moduleOk) { return [pscustomobject]@{Valid=$false;Reason="module_mismatch";Process=$process} }
    return [pscustomobject]@{Valid=$true;Reason="ok";Process=$process}
}

$lock = Read-JsonSafe $LockFile
$state = Read-JsonSafe $StateFile
$heartbeat = Read-JsonSafe $HeartbeatFile
$pidValue = 0
$pidOk = $null -ne $lock -and [int]::TryParse([string]$lock.pid,[ref]$pidValue) -and $pidValue -gt 0
$identity = Test-V4Process $pidValue
$exact = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
    try { $candidatePath=[System.IO.Path]::GetFullPath([string]$_.ExecutablePath) } catch { $candidatePath="" }
    @($ExpectedExecutables | Where-Object { [string]::Equals($candidatePath,$_,[System.StringComparison]::OrdinalIgnoreCase) }).Count -gt 0 -and [string]$_.CommandLine -match $ModulePattern
})
$owned = New-Object 'System.Collections.Generic.HashSet[int]'
if ($pidOk) {
    [void]$owned.Add($pidValue)
    $cursor = $exact | Where-Object ProcessId -eq $pidValue | Select-Object -First 1
    while ($null -ne $cursor) {
        $parent = $exact | Where-Object ProcessId -eq $cursor.ParentProcessId | Select-Object -First 1
        if ($null -eq $parent -or $owned.Contains([int]$parent.ProcessId)) { break }
        [void]$owned.Add([int]$parent.ProcessId); $cursor=$parent
    }
}
$orphans = @($exact | Where-Object { -not $owned.Contains([int]$_.ProcessId) })
$nonceMatch = $null -ne $lock -and $null -ne $state -and $null -ne $heartbeat -and -not [string]::IsNullOrWhiteSpace([string]$lock.launch_nonce) -and [string]$lock.launch_nonce -eq [string]$state.launch_nonce -and [string]$lock.launch_nonce -eq [string]$heartbeat.launch_nonce
$pidMatch = $pidOk -and [int]$state.pid -eq $pidValue -and [int]$heartbeat.pid -eq $pidValue
$modeMatch = [string]$lock.mode -eq "lite_frequency_v4_shadow" -and [string]$state.mode -eq "lite_frequency_v4_shadow" -and [string]$heartbeat.mode -eq "lite_frequency_v4_shadow"
$safety = $modeMatch -and ($state.dry_run -is [bool]) -and [bool]$state.dry_run -and ($state.live_enabled -is [bool]) -and -not [bool]$state.live_enabled -and ($state.real_orders_possible -is [bool]) -and -not [bool]$state.real_orders_possible -and ($state.live_adapter_present -is [bool]) -and -not [bool]$state.live_adapter_present -and ($state.kill_switch_engaged -is [bool]) -and [bool]$state.kill_switch_engaged -and [double]$state.fixed_shares -eq 5.0
$ageSeconds = $null
try {
    $heartbeatMs = 0L
    if ($null -ne $heartbeat -and
        [int64]::TryParse([string]$heartbeat.ts_ms,[ref]$heartbeatMs) -and
        $heartbeatMs -gt 0) {
        $ageSeconds=([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()-$heartbeatMs)/1000.0
    }
} catch {}
$fresh = $null -ne $ageSeconds -and $ageSeconds -ge -5.0 -and $ageSeconds -le 15.0
$running = $pidOk -and $identity.Valid
$healthy = $running -and $nonceMatch -and $pidMatch -and $safety -and $fresh -and $orphans.Count -eq 0

Write-Output "Poly Alpha Lite Frequency V4 Shadow Status"
Write-Output "------------------------------------------"
Write-Output ("running              : {0}" -f $running.ToString().ToLowerInvariant())
Write-Output ("pid                  : {0}" -f $(if($pidOk){$pidValue}else{"UNKNOWN"}))
Write-Output ("process_identity     : {0} ({1})" -f $identity.Valid,$identity.Reason)
Write-Output ("exact_v4_processes   : {0}" -f $exact.Count)
Write-Output ("owned_v4_processes   : {0}" -f $owned.Count)
Write-Output ("orphan_v4_processes  : {0}" -f $orphans.Count)
Write-Output ("launch_nonce_check   : {0}" -f $(if($nonceMatch){"MATCH"}else{"MISMATCH"}))
Write-Output ("heartbeat_age        : {0}" -f $(if($null -ne $ageSeconds){"{0:N1}s" -f $ageSeconds}else{"UNKNOWN"}))
Write-Output ("heartbeat_fresh      : {0}" -f $fresh.ToString().ToLowerInvariant())
Write-Output ("mode                 : {0}" -f [string]$state.mode)
Write-Output ("dry_run              : {0}" -f [string]$state.dry_run)
Write-Output ("live_enabled         : {0}" -f [string]$state.live_enabled)
Write-Output ("real_orders_possible : {0}" -f [string]$state.real_orders_possible)
Write-Output ("live_adapter_present : {0}" -f [string]$state.live_adapter_present)
Write-Output ("kill_switch_engaged  : {0}" -f [string]$state.kill_switch_engaged)
Write-Output ("fixed_shares         : {0}" -f [string]$state.fixed_shares)
Write-Output ("safety               : {0}" -f $(if($safety){"OK"}else{"VIOLATION"}))
Write-Output ("current_commit       : {0}" -f [string]$state.current_commit)
Write-Output ("open_positions       : {0}" -f [string]$state.open_positions)
Write-Output ("db_path              : {0}" -f $(if($state.db_path){[string]$state.db_path}else{$ExpectedDb}))

if ($healthy) { exit 0 }
if ($running -or $exact.Count -gt 0) { exit 2 }
exit 1
