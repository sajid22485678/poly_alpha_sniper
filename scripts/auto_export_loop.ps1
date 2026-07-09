# Persistent read-only auto-export loop for poly_alpha_sniper.
#
# Runs tools\export_agent_readonly.py every -IntervalSeconds (default 10),
# then copies the two report outputs into the Obsidian vault. This script
# IS the persistent loop -- it sleeps internally between exports. A
# scheduled task, if installed via install_auto_export_startup_task.ps1,
# only starts THIS script once at logon; it never spawns the exporter
# directly itself.
#
# Never touches .env or secrets. Never places or cancels an order -- it
# only invokes the existing read-only exporter (tools/export_agent_readonly.py)
# and copies plain markdown files.
param(
    [int]$IntervalSeconds = 10
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$ExporterScript = Join-Path $ProjectRoot "tools\export_agent_readonly.py"
$ExportDir = "D:\claude\agent_readonly\poly_alpha_sniper"
$LockFile = Join-Path $ExportDir "auto_export.lock"
$LogFile = Join-Path $ExportDir "auto_export.log"
$StatusFile = Join-Path $ExportDir "auto_export_status.json"

$VaultReportDest = "D:\TradingVault\06_Poly_Hermes_Reports\Latest Exported Report.md"
$VaultDailyDest = "D:\TradingVault\04_Poly_Daily_Reports\Today Shadow Report.md"

New-Item -ItemType Directory -Force -Path $ExportDir | Out-Null

if (Test-Path $LockFile) {
    $existingPid = Get-Content $LockFile -ErrorAction SilentlyContinue
    if ($existingPid -and (Get-Process -Id $existingPid -ErrorAction SilentlyContinue)) {
        Write-Output "Auto-export loop already running (PID $existingPid). Exiting."
        exit 1
    }
}
Set-Content -Path $LockFile -Value $PID -Encoding utf8

function Write-Log([string]$Message) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -Path $LogFile -Value $line
}

# PowerShell's `Set-Content -Encoding utf8` writes a UTF-8 BOM, which makes
# Node's JSON.parse() throw ("Unexpected token ﻿") when Dashboard V3's
# API route reads this file -- write plain UTF-8 without a BOM instead.
function Write-Utf8NoBom([string]$Path, [string]$Content) {
    [System.IO.File]::WriteAllText($Path, $Content, (New-Object System.Text.UTF8Encoding $false))
}

function Write-Status([bool]$Running) {
    $obj = [ordered]@{
        running               = $Running
        pid                   = $PID
        last_started_at       = $script:lastStartedAt
        last_finished_at      = $script:lastFinishedAt
        last_success_at       = $script:lastSuccessAt
        last_error_at         = $script:lastErrorAt
        last_error_message    = $script:lastErrorMessage
        interval_seconds      = $IntervalSeconds
        exports_completed     = $script:exportsCompleted
        consecutive_failures  = $script:consecutiveFailures
        lock_active           = $Running
        log_path              = $LogFile
        status_path           = $StatusFile
    }
    Write-Utf8NoBom -Path $StatusFile -Content ($obj | ConvertTo-Json -Depth 5)
}

$script:exportsCompleted = 0
$script:consecutiveFailures = 0
$script:lastStartedAt = $null
$script:lastFinishedAt = $null
$script:lastSuccessAt = $null
$script:lastErrorAt = $null
$script:lastErrorMessage = ""

Write-Log "auto_export_loop started (PID $PID, interval=${IntervalSeconds}s)"

try {
    while ($true) {
        $script:lastStartedAt = (Get-Date).ToString("o")
        Write-Status -Running $true

        try {
            $output = & $Python $ExporterScript 2>&1
            $output | ForEach-Object { Write-Log $_ }
            if ($LASTEXITCODE -ne 0) {
                throw "exporter exited with code $LASTEXITCODE"
            }

            # Never overwrite the vault copies with missing/partial content --
            # only copy if the source file exists and is non-empty.
            $reportSrc = Join-Path $ExportDir "daily_report.md"
            $dailySrc = Join-Path $ExportDir "obsidian_daily_note.md"
            if ((Test-Path $reportSrc) -and ((Get-Item $reportSrc).Length -gt 0)) {
                New-Item -ItemType Directory -Force -Path (Split-Path $VaultReportDest) | Out-Null
                Copy-Item -Path $reportSrc -Destination $VaultReportDest -Force
            } else {
                Write-Log "skip vault copy: $reportSrc missing or empty"
            }
            if ((Test-Path $dailySrc) -and ((Get-Item $dailySrc).Length -gt 0)) {
                New-Item -ItemType Directory -Force -Path (Split-Path $VaultDailyDest) | Out-Null
                Copy-Item -Path $dailySrc -Destination $VaultDailyDest -Force
            } else {
                Write-Log "skip vault copy: $dailySrc missing or empty"
            }

            $script:exportsCompleted++
            $script:consecutiveFailures = 0
            $script:lastSuccessAt = (Get-Date).ToString("o")
            Write-Log "export #$($script:exportsCompleted) OK"
        } catch {
            $script:consecutiveFailures++
            $script:lastErrorAt = (Get-Date).ToString("o")
            $script:lastErrorMessage = $_.Exception.Message
            Write-Log "export FAILED (consecutive_failures=$($script:consecutiveFailures)): $($script:lastErrorMessage)"
            # deliberately does not re-throw -- one failed export must not
            # kill the loop; it retries after the normal interval.
        }

        $script:lastFinishedAt = (Get-Date).ToString("o")
        Write-Status -Running $true
        Start-Sleep -Seconds $IntervalSeconds
    }
} finally {
    Write-Log "auto_export_loop stopping"
    Write-Status -Running $false
    Remove-Item -Path $LockFile -ErrorAction SilentlyContinue
}
