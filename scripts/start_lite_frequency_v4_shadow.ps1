# Starts only the isolated lite_frequency_v4.bot module and verifies its nonce-correlated safety heartbeat.
[CmdletBinding()]
param([ValidateRange(5,60)][int]$ReadyTimeoutSeconds = 20)
$ErrorActionPreference = "Stop"
$root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$python = Join-Path $root ".venv\Scripts\python.exe"
$statusScript = Join-Path $PSScriptRoot "status_lite_frequency_v4_shadow.ps1"
if (-not (Test-Path -LiteralPath $python)) { throw "V4 Python not found: $python" }
if (-not (Test-Path -LiteralPath (Join-Path $root "lite_frequency_v4\bot.py"))) { throw "V4 entrypoint is missing" }
& $statusScript *> $null
$pre = $LASTEXITCODE
if ($pre -eq 0) { Write-Output "Frequency V4 is already healthy."; & $statusScript; exit $LASTEXITCODE }
if ($pre -eq 2) { Write-Error "An exact V4 process exists but is unsafe; refusing a duplicate."; & $statusScript; exit 2 }

$runtime = Join-Path $root "runtime\lite_frequency_v4_shadow"
$logDir = Join-Path $root "logs\lite_frequency_v4_shadow"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stdout = Join-Path $logDir "frequency_v4.stdout.log"
$stderr = Join-Path $logDir "frequency_v4.stderr.log"
$nonce = [Guid]::NewGuid().ToString("N")
$startedMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$env:POLY_ALPHA_FREQUENCY_V4_LAUNCH_NONCE = $nonce
try {
    $launcher = Start-Process -FilePath $python -ArgumentList @("-m","lite_frequency_v4.bot") -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
} finally { Remove-Item Env:POLY_ALPHA_FREQUENCY_V4_LAUNCH_NONCE -ErrorAction SilentlyContinue }
Write-Output ("Started V4 launcher PID {0}; verifying safety heartbeat." -f $launcher.Id)
$deadline=[DateTime]::UtcNow.AddSeconds($ReadyTimeoutSeconds)
$ready=$false; $ownedPid=0
while([DateTime]::UtcNow -lt $deadline) {
    try {
        $lock=Get-Content -LiteralPath (Join-Path $runtime "process.lock") -Raw | ConvertFrom-Json
        $state=Get-Content -LiteralPath (Join-Path $runtime "state.json") -Raw | ConvertFrom-Json
        $heartbeat=Get-Content -LiteralPath (Join-Path $runtime "heartbeat.json") -Raw | ConvertFrom-Json
        $ownedPid=[int]$lock.pid
        $p=Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $ownedPid) -ErrorAction SilentlyContinue
        $moduleOk=$null -ne $p -and [string]$p.CommandLine -match '(?i)-m\s+"?lite_frequency_v4\.bot"?\s*$'
        $ready=$moduleOk -and [string]$lock.launch_nonce -eq $nonce -and [string]$state.launch_nonce -eq $nonce -and [string]$heartbeat.launch_nonce -eq $nonce -and [int64]$lock.started_ts_ms -ge $startedMs -and [string]$state.mode -eq "lite_frequency_v4_shadow" -and [bool]$state.running -and [bool]$state.dry_run -and -not [bool]$state.live_enabled -and -not [bool]$state.real_orders_possible -and -not [bool]$state.live_adapter_present -and [bool]$state.kill_switch_engaged -and [double]$state.fixed_shares -eq 5.0
    } catch { $ready=$false }
    if($ready){break}; Start-Sleep -Milliseconds 250
}
if(-not $ready) {
    if($ownedPid -gt 0) {
        try { $p=Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $ownedPid); if([string]$p.CommandLine -match 'lite_frequency_v4\.bot'){Stop-Process -Id $ownedPid -Force} } catch {}
    }
    if(-not $launcher.HasExited){Stop-Process -Id $launcher.Id -Force -ErrorAction SilentlyContinue}
    Write-Error "V4 failed to publish the required shadow-only heartbeat. See $stdout and $stderr"
    exit 1
}
Write-Output ("Verified Frequency V4 runtime PID {0}." -f $ownedPid)
& $statusScript
exit $LASTEXITCODE
