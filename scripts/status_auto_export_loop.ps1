# Read-only status check for the auto-export loop. Prints whether the lock
# PID is alive and the full contents of auto_export_status.json.
$ExportDir = "D:\claude\agent_readonly\poly_alpha_sniper"
$StatusFile = Join-Path $ExportDir "auto_export_status.json"
$LockFile = Join-Path $ExportDir "auto_export.lock"

$lockAlive = $false
if (Test-Path $LockFile) {
    $lockPid = Get-Content $LockFile -ErrorAction SilentlyContinue
    if ($lockPid -and (Get-Process -Id $lockPid -ErrorAction SilentlyContinue)) {
        $lockAlive = $true
    }
}
Write-Output "RUNNING: $lockAlive"

if (-not (Test-Path $StatusFile)) {
    Write-Output "UNKNOWN -- no status file yet at $StatusFile. Has the loop ever been started?"
    exit 0
}
Get-Content $StatusFile -Raw
