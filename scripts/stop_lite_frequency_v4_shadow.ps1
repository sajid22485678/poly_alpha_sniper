# Stops only the PID whose V4 lock, executable, module, mode, and launch nonce corroborate.
[CmdletBinding()]
# The grace period must outlast a real graceful shutdown, not just a fast one.
# V4's stop sequence drains the ingest queues, stops the telemetry lane, writes
# the terminal end_runtime_session command, and publishes a final export. On a
# multi-GB database that routinely exceeds ten seconds, and force-killing
# before end_runtime_session commits leaves the session row open forever --
# which then makes every later startup unable to reconcile that session's
# unfinished maker observations, permanently fail-closing the engine.
param([ValidateRange(1,600)][int]$GracePeriodSeconds = 180)
$ErrorActionPreference="Stop"
$root=[System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$runtime=Join-Path $root "runtime\lite_frequency_v4_shadow"
$lockFile=Join-Path $runtime "process.lock"; $stateFile=Join-Path $runtime "state.json"; $heartbeatFile=Join-Path $runtime "heartbeat.json"; $stopFile=Join-Path $runtime "stop.request"
function Read-Json([string]$path){try{return Get-Content -LiteralPath $path -Raw | ConvertFrom-Json -ErrorAction Stop}catch{return $null}}
if(-not (Test-Path -LiteralPath $lockFile)){Write-Output "Frequency V4 is not running (no process lock).";exit 0}
$lock=Read-Json $lockFile; $state=Read-Json $stateFile; $heartbeat=Read-Json $heartbeatFile
if($null -eq $lock -or $null -eq $state -or $null -eq $heartbeat){Write-Error "Malformed V4 ownership evidence; refusing to stop any PID.";exit 2}
$pidValue=0
if(-not [int]::TryParse([string]$lock.pid,[ref]$pidValue)-or $pidValue -le 0){Write-Error "Invalid V4 lock PID.";exit 2}
$nonce=[string]$lock.launch_nonce
if([string]$lock.mode -ne "lite_frequency_v4_shadow" -or [string]$state.mode -ne "lite_frequency_v4_shadow" -or [string]$heartbeat.mode -ne "lite_frequency_v4_shadow" -or [string]::IsNullOrWhiteSpace($nonce) -or [string]$state.launch_nonce -ne $nonce -or [string]$heartbeat.launch_nonce -ne $nonce){Write-Error "V4 mode/nonce evidence mismatch; refusing to stop.";exit 2}
$process=Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $pidValue) -ErrorAction SilentlyContinue
if($null -eq $process){Remove-Item -LiteralPath $lockFile,$stopFile -Force -ErrorAction SilentlyContinue;Write-Output "Removed stale V4 control files for absent PID $pidValue.";exit 0}
if([string]$process.CommandLine -notmatch '(?i)^\s*(?:"[^"]+"|\S+)\s+-m\s+"?lite_frequency_v4\.bot"?\s*$'){Write-Error "PID $pidValue is not the exact V4 module; refusing to stop.";exit 2}
$recordedCreate=0.0
if(-not [double]::TryParse([string]$lock.process_create_time,[Globalization.NumberStyles]::Float,[Globalization.CultureInfo]::InvariantCulture,[ref]$recordedCreate)-or $recordedCreate -le 0){Write-Error "V4 process creation evidence is invalid; refusing to stop.";exit 2}
try{$native=Get-Process -Id $pidValue -ErrorAction Stop;$actualCreate=([DateTimeOffset]($native.StartTime.ToUniversalTime())).ToUnixTimeMilliseconds()/1000.0}catch{Write-Error "Unable to corroborate V4 process creation time; refusing to stop.";exit 2}
if([math]::Abs($actualCreate-$recordedCreate)-ge 1.0){Write-Error "V4 PID creation time does not match its lock; refusing to stop.";exit 2}
$creation=[string]$process.CreationDate
$request=[ordered]@{requested_ts_ms=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds();target_pid=$pidValue;mode="lite_frequency_v4_shadow";module="lite_frequency_v4.bot";launch_nonce=$nonce}
$tmp="$stopFile.tmp.$PID"; [System.IO.File]::WriteAllText($tmp,($request|ConvertTo-Json -Compress),(New-Object System.Text.UTF8Encoding($false))); Move-Item -LiteralPath $tmp -Destination $stopFile -Force
Write-Output "Graceful V4 stop requested for PID $pidValue."
$deadline=[DateTime]::UtcNow.AddSeconds($GracePeriodSeconds);$exited=$false
while([DateTime]::UtcNow -lt $deadline){$current=Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $pidValue) -ErrorAction SilentlyContinue;if($null -eq $current){$exited=$true;break};if([string]$current.CreationDate -ne $creation){Write-Error "PID reuse detected; refusing force-stop.";exit 2};Start-Sleep -Milliseconds 250}
if(-not $exited){$current=Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $pidValue);try{$currentNative=Get-Process -Id $pidValue -ErrorAction Stop;$currentCreate=([DateTimeOffset]($currentNative.StartTime.ToUniversalTime())).ToUnixTimeMilliseconds()/1000.0}catch{Write-Error "V4 process identity became unverifiable; refusing force-stop.";exit 2};if([string]$current.CreationDate -ne $creation -or [math]::Abs($currentCreate-$recordedCreate)-ge 1.0 -or [string]$current.CommandLine -notmatch 'lite_frequency_v4\.bot'){Write-Error "V4 identity changed; refusing force-stop.";exit 2};Stop-Process -Id $pidValue -Force;Start-Sleep -Milliseconds 500;$exited=$null -eq (Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $pidValue) -ErrorAction SilentlyContinue)}
if(-not $exited){Write-Error "Validated V4 PID did not exit.";exit 1}
Remove-Item -LiteralPath $lockFile,$stopFile -Force -ErrorAction SilentlyContinue
Write-Output "Stopped only Frequency V4 PID $pidValue. V3 and Advanced were not touched."
exit 0
