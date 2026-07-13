$listener=Get-NetTCPConnection -State Listen -LocalPort 8504 -ErrorAction SilentlyContinue | Select-Object -First 1
if($null -eq $listener){Write-Output "Frequency V4 dashboard already stopped.";exit 0}
$process=Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $listener.OwningProcess) -ErrorAction SilentlyContinue
if($null -eq $process -or [string]$process.Name -ne "node.exe" -or [string]$process.CommandLine -notmatch 'dashboard_frequency_v4.*next'){Write-Error "Port 8504 is not owned by the V4 dashboard; refusing to stop it.";exit 2}
Stop-Process -Id $listener.OwningProcess -Force;Write-Output "Stopped only Frequency V4 dashboard PID $($listener.OwningProcess).";exit 0
