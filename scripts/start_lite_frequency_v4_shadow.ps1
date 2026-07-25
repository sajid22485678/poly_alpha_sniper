# Starts only the isolated lite_frequency_v4.bot module and verifies its
# nonce-correlated shadow-only heartbeat after startup integrity verification.
[CmdletBinding()]
param(
    [ValidateRange(5,1800)]
    [int]$ReadyTimeoutSeconds = 600
)

$ErrorActionPreference = "Stop"

$root = [System.IO.Path]::GetFullPath(
    (Join-Path $PSScriptRoot "..")
)
$python = Join-Path $root ".venv\Scripts\python.exe"
$statusScript = Join-Path $PSScriptRoot "status_lite_frequency_v4_shadow.ps1"
$runtime = Join-Path $root "runtime\lite_frequency_v4_shadow"
$expectedDb = Join-Path $root "data\poly_alpha_frequency_v4.db"

if (-not (Test-Path -LiteralPath $python)) {
    throw "V4 Python not found: $python"
}

if (-not (
    Test-Path -LiteralPath (
        Join-Path $root "lite_frequency_v4\bot.py"
    )
)) {
    throw "V4 entrypoint is missing"
}

$expectedCommit = (
    git -C $root rev-parse HEAD |
        Out-String
).Trim()

& $statusScript *> $null
$pre = $LASTEXITCODE

if ($pre -eq 0) {
    Write-Output "Frequency V4 is already healthy."
    & $statusScript
    exit $LASTEXITCODE
}

if ($pre -eq 2) {
    Write-Error `
        "An exact V4 process exists but is unsafe; refusing a duplicate." `
        -ErrorAction Continue
    & $statusScript
    exit 2
}

$logDir = Join-Path $root "logs\lite_frequency_v4_shadow"
New-Item -ItemType Directory -Force -Path $logDir |
    Out-Null

$stdout = Join-Path $logDir "frequency_v4.stdout.log"
$stderr = Join-Path $logDir "frequency_v4.stderr.log"

$nonce = [Guid]::NewGuid().ToString("N")
$startedMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()

$env:POLY_ALPHA_FREQUENCY_V4_LAUNCH_NONCE = $nonce

try {
    $launcher = Start-Process `
        -FilePath $python `
        -ArgumentList @("-m","lite_frequency_v4.bot") `
        -WorkingDirectory $root `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru
}
finally {
    Remove-Item `
        Env:POLY_ALPHA_FREQUENCY_V4_LAUNCH_NONCE `
        -ErrorAction SilentlyContinue
}

Write-Output (
    "Started V4 launcher PID {0}; waiting up to {1}s for " +
    "post-integrity shadow readiness."
) -f $launcher.Id, $ReadyTimeoutSeconds

$deadline = [DateTimeOffset]::UtcNow.AddSeconds(
    $ReadyTimeoutSeconds
)

$ready = $false
$ownershipObserved = $false
$ownedPid = 0
$lastPhase = "waiting_for_process_lock"

while ([DateTimeOffset]::UtcNow -lt $deadline) {
    try {
        $lock = Get-Content `
            -LiteralPath (Join-Path $runtime "process.lock") `
            -Raw |
            ConvertFrom-Json

        $candidatePid = [int]$lock.pid

        $process = Get-CimInstance `
            Win32_Process `
            -Filter ("ProcessId={0}" -f $candidatePid) `
            -ErrorAction SilentlyContinue

        $moduleOk = (
            $null -ne $process -and
            [string]$process.CommandLine -match (
                '(?i)-m\s+"?lite_frequency_v4\.bot"?\s*$'
            )
        )

        $lockMatches = (
            $moduleOk -and
            [string]$lock.launch_nonce -eq $nonce -and
            [int64]$lock.started_ts_ms -ge $startedMs
        )

        if ($lockMatches) {
            $ownershipObserved = $true
            $ownedPid = $candidatePid
            $lastPhase = "pre-heartbeat integrity scan or engine startup"

            try {
                $state = Get-Content `
                    -LiteralPath (Join-Path $runtime "state.json") `
                    -Raw |
                    ConvertFrom-Json

                $heartbeat = Get-Content `
                    -LiteralPath (Join-Path $runtime "heartbeat.json") `
                    -Raw |
                    ConvertFrom-Json

                $heartbeatTs = [int64]$heartbeat.ts_ms
                $nowMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
                $heartbeatAgeMs = $nowMs - $heartbeatTs

                $nonceMatches = (
                    [string]$state.launch_nonce -eq $nonce -and
                    [string]$heartbeat.launch_nonce -eq $nonce
                )

                $pidMatches = (
                    [int]$state.pid -eq $candidatePid -and
                    [int]$heartbeat.pid -eq $candidatePid
                )

                $identityOk = (
                    [string]$state.mode -eq "lite_frequency_v4_shadow" -and
                    [string]$state.db_path -eq $expectedDb -and
                    [string]$state.current_commit -eq $expectedCommit -and
                    [string]$heartbeat.current_commit -eq $expectedCommit
                )

                $stateSafetyOk = (
                    [bool]$state.running -and
                    [bool]$state.integrity_ok -and
                    [bool]$state.dry_run -and
                    -not [bool]$state.live_enabled -and
                    -not [bool]$state.real_orders_possible -and
                    -not [bool]$state.live_adapter_present -and
                    -not [bool]$state.authenticated_trading_client -and
                    -not [bool]$state.real_order_placement -and
                    -not [bool]$state.real_order_cancellation -and
                    -not [bool]$state.real_wallet_signing -and
                    [bool]$state.kill_switch_engaged -and
                    [double]$state.fixed_shares -eq 5.0
                )

                $heartbeatSafetyOk = (
                    [bool]$heartbeat.dry_run -and
                    -not [bool]$heartbeat.live_enabled -and
                    -not [bool]$heartbeat.real_orders_possible -and
                    -not [bool]$heartbeat.live_adapter_present -and
                    -not [bool]$heartbeat.authenticated_trading_client -and
                    -not [bool]$heartbeat.real_order_placement -and
                    -not [bool]$heartbeat.real_order_cancellation -and
                    -not [bool]$heartbeat.real_wallet_signing -and
                    [bool]$heartbeat.kill_switch_engaged -and
                    [double]$heartbeat.fixed_shares -eq 5.0
                )

                $heartbeatFresh = (
                    $heartbeatTs -ge $startedMs -and
                    $heartbeatAgeMs -ge -5000 -and
                    $heartbeatAgeMs -le 15000
                )

                $ready = (
                    $lockMatches -and
                    $nonceMatches -and
                    $pidMatches -and
                    $identityOk -and
                    $stateSafetyOk -and
                    $heartbeatSafetyOk -and
                    $heartbeatFresh
                )

                if ($ready) {
                    $lastPhase = "ready"
                }
                elseif (-not [bool]$state.integrity_ok) {
                    $lastPhase = "integrity verification not yet successful"
                }
                else {
                    $lastPhase = "waiting for fresh safe running heartbeat"
                }
            }
            catch {
                $ready = $false
            }
        }
    }
    catch {
        $ready = $false
    }

    if ($ready) {
        break
    }

    Start-Sleep -Milliseconds 250
}

if (-not $ready) {
    $ownedAlive = $false

    if ($ownedPid -gt 0) {
        $ownedProcess = Get-CimInstance `
            Win32_Process `
            -Filter ("ProcessId={0}" -f $ownedPid) `
            -ErrorAction SilentlyContinue

        $ownedAlive = (
            $null -ne $ownedProcess -and
            [string]$ownedProcess.CommandLine -match (
                '(?i)-m\s+"?lite_frequency_v4\.bot"?\s*$'
            )
        )
    }

    if ($ownershipObserved -and $ownedAlive) {
        Write-Error (
            "V4 readiness timed out during {0}. The nonce-correlated " +
            "runtime PID {1} is still alive and was not killed. " +
            "Do not launch a duplicate. Inspect status and logs; use the " +
            "dedicated stop script only after deliberate operator review. " +
            "stdout={2} stderr={3}"
        ) -f $lastPhase, $ownedPid, $stdout, $stderr `
            -ErrorAction Continue

        & $statusScript
        exit 3
    }

    $exactProcesses = @(
        Get-CimInstance Win32_Process |
            Where-Object {
                $_.Name -match '^(python|pythonw|py)\.exe$' -and
                $_.CommandLine -match (
                    '(?i)-m\s+"?lite_frequency_v4\.bot"?\s*$'
                )
            }
    )

    if ($exactProcesses.Count -gt 0) {
        Write-Error (
            "V4 readiness timed out with an exact V4 process present, " +
            "but nonce-correlated ownership could not be verified. No " +
            "process was killed. Refusing a duplicate; inspect status, " +
            "process.lock, stdout, and stderr."
        ) -ErrorAction Continue

        & $statusScript
        exit 4
    }

    $launcher.Refresh()

    $launcherResult = if ($launcher.HasExited) {
        "launcher exited with code $($launcher.ExitCode)"
    }
    else {
        "launcher process remains alive without verified ownership"
    }

    Write-Error (
        "V4 failed to reach verified post-integrity shadow readiness: " +
        "$launcherResult. No process was killed automatically. " +
        "stdout=$stdout stderr=$stderr"
    ) -ErrorAction Continue

    exit 1
}

Write-Output (
    "Verified Frequency V4 runtime PID {0} after successful integrity " +
    "verification."
) -f $ownedPid

& $statusScript
exit $LASTEXITCODE
