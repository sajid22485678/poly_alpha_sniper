$ErrorActionPreference="Stop"
$root=[System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."));$app=Join-Path $root "dashboard_frequency_v4";$status=Join-Path $PSScriptRoot "status_frequency_v4_dashboard.ps1"
& $status *> $null;if($LASTEXITCODE -eq 0){Write-Output "Frequency V4 dashboard already running.";exit 0};if($LASTEXITCODE -eq 2){Write-Error "Port 8504 has an unsafe owner.";exit 2}
if(-not (Test-Path (Join-Path $app ".next\BUILD_ID"))){Write-Error "Build dashboard_frequency_v4 before starting it.";exit 1}
$logs=Join-Path $root "logs\lite_frequency_v4_shadow";New-Item -ItemType Directory -Force -Path $logs|Out-Null
$p=Start-Process -FilePath "npm.cmd" -ArgumentList @("run","start") -WorkingDirectory $app -WindowStyle Hidden -RedirectStandardOutput (Join-Path $logs "dashboard.stdout.log") -RedirectStandardError (Join-Path $logs "dashboard.stderr.log") -PassThru
$deadline=[DateTime]::UtcNow.AddSeconds(20);do{Start-Sleep -Milliseconds 250;& $status *> $null;if($LASTEXITCODE -eq 0){Write-Output "Frequency V4 dashboard verified on http://127.0.0.1:8504 (launcher PID $($p.Id)).";exit 0}}while([DateTime]::UtcNow -lt $deadline)
if(-not $p.HasExited){Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue}
$listener=Get-NetTCPConnection -State Listen -LocalPort 8504 -ErrorAction SilentlyContinue | Select-Object -First 1
if($null -ne $listener){
    $child=Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $listener.OwningProcess) -ErrorAction SilentlyContinue
    if($null -ne $child -and [string]$child.Name -eq "node.exe" -and [string]$child.CommandLine -match 'dashboard_frequency_v4.*next'){
        Stop-Process -Id $listener.OwningProcess -Force -ErrorAction SilentlyContinue
    }else{Write-Error "Dashboard start timed out with an unsafe owner on port 8504; refusing cleanup.";exit 2}
}
Write-Error "Frequency V4 dashboard failed to listen on 8504.";exit 1
