# Removes the "PolyAlphaSniper_AutoExportLoop" scheduled task installed by
# install_auto_export_startup_task.ps1. Does not stop an already-running
# loop process -- use stop_auto_export_loop.ps1 for that.
$TaskName = "PolyAlphaSniper_AutoExportLoop"

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Output "Removed scheduled task '$TaskName'."
} else {
    Write-Output "Scheduled task '$TaskName' not found -- nothing to remove."
}
