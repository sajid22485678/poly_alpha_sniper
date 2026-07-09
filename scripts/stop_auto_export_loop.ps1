# Stops ONLY the auto_export_loop.ps1 process, identified via the PID
# recorded in auto_export.lock. Never touches the bot process, dashboards,
# or any other python/node process -- refuses to act on a PID that isn't
# actually a powershell process (stale/reused PID protection).
$ExportDir = "D:\claude\agent_readonly\poly_alpha_sniper"
$LockFile = Join-Path $ExportDir "auto_export.lock"

if (-not (Test-Path $LockFile)) {
    Write-Output "Not running (no lock file at $LockFile)."
    exit 0
}

$targetPid = Get-Content $LockFile -ErrorAction SilentlyContinue
if (-not $targetPid) {
    Write-Output "Lock file present but empty/unreadable. Removing stale lock."
    Remove-Item -Path $LockFile -ErrorAction SilentlyContinue
    exit 0
}

$proc = Get-Process -Id $targetPid -ErrorAction SilentlyContinue
if (-not $proc) {
    Write-Output "No process with PID $targetPid (stale lock). Removing lock file."
    Remove-Item -Path $LockFile -ErrorAction SilentlyContinue
    exit 0
}

if ($proc.ProcessName -notin @("powershell", "pwsh")) {
    Write-Output "PID $targetPid is '$($proc.ProcessName)', not powershell -- refusing to stop it (stale/reused PID). Removing lock file only."
    Remove-Item -Path $LockFile -ErrorAction SilentlyContinue
    exit 1
}

Stop-Process -Id $targetPid -Force
Remove-Item -Path $LockFile -ErrorAction SilentlyContinue

# Stop-Process -Force kills the loop before its own `finally` block can run,
# so auto_export_status.json would otherwise be left stuck at running=true.
# Patch it directly here so the dashboard/status script never reports a
# stopped loop as still running.
$StatusFile = Join-Path $ExportDir "auto_export_status.json"
if (Test-Path $StatusFile) {
    try {
        $status = Get-Content $StatusFile -Raw | ConvertFrom-Json
        $status.running = $false
        $status.lock_active = $false
        # Plain UTF-8 without a BOM -- Set-Content -Encoding utf8 writes a
        # BOM, which breaks Node's JSON.parse() in Dashboard V3's API route.
        $json = $status | ConvertTo-Json -Depth 5
        [System.IO.File]::WriteAllText($StatusFile, $json, (New-Object System.Text.UTF8Encoding $false))
    } catch {
        Write-Output "Warning: could not patch $StatusFile after stop: $($_.Exception.Message)"
    }
}

Write-Output "Stopped auto_export_loop.ps1 (PID $targetPid)."
