$listener=Get-NetTCPConnection -State Listen -LocalPort 8504 -ErrorAction SilentlyContinue | Select-Object -First 1
if($null -eq $listener){Write-Output "Frequency V4 dashboard (127.0.0.1:8504): STOPPED";exit 1}
$process=Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $listener.OwningProcess) -ErrorAction SilentlyContinue
$valid=$null -ne $process -and [string]$process.Name -eq "node.exe" -and [string]$process.CommandLine -match 'dashboard_frequency_v4.*next'
Write-Output ("Frequency V4 dashboard: {0} PID {1}" -f $(if($valid){"LISTENING"}else{"UNSAFE_OWNER"}),$listener.OwningProcess)
if($valid){exit 0};exit 2
