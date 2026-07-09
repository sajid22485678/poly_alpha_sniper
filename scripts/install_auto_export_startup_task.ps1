# Registers a Windows Scheduled Task that starts auto_export_loop.ps1 ONCE
# at user logon. The task does NOT run the exporter directly and does NOT
# itself repeat every 10s -- that interval is entirely internal to
# auto_export_loop.ps1's own Start-Sleep loop. This task is just "make sure
# the persistent loop is running after I log in."
#
# System-modifying (creates a Task Scheduler entry) -- run this yourself
# when you're ready; it is not executed automatically by anything else in
# this project.
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ScriptPath = Join-Path $ProjectRoot "scripts\auto_export_loop.ps1"
$TaskName = "PolyAlphaSniper_AutoExportLoop"

$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$ScriptPath`""
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Settings $settings -Description ("Starts the poly_alpha_sniper read-only auto-export loop at logon. " +
    "The loop itself sleeps 10s between exports; this task does not run the exporter directly.") -Force | Out-Null

Write-Output "Installed scheduled task '$TaskName' -- starts auto_export_loop.ps1 at logon."
Write-Output "Remove it with scripts\uninstall_auto_export_startup_task.ps1."
