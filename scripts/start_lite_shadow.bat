@echo off
setlocal

REM Starts ONLY Poly Alpha Lite in hidden/background shadow mode.
REM It never invokes main.py, the advanced watchdog, or any live-order path.
for %%I in ("%~dp0..") do set "PROJECT_ROOT=%%~fI"
set "PYTHON_EXE=%PROJECT_ROOT%\.venv\Scripts\python.exe"
set "STATUS_SCRIPT=%~dp0status_lite_shadow.ps1"

if not exist "%PYTHON_EXE%" (
    echo ERROR: Lite Python was not found at "%PYTHON_EXE%".
    exit /b 1
)

if not exist "%PROJECT_ROOT%\lite\lite_bot.py" (
    echo ERROR: Lite entrypoint was not found at "%PROJECT_ROOT%\lite\lite_bot.py".
    exit /b 1
)

REM Exit code 0 means healthy Lite is already running. Exit code 2 means a
REM validated Lite process exists but is stale/unsafe/unknown; never start a
REM duplicate in either case. Exit code 1 means no validated Lite process.
powershell -NoProfile -ExecutionPolicy Bypass -File "%STATUS_SCRIPT%" >nul 2>&1
set "PRE_STATUS=%ERRORLEVEL%"
if "%PRE_STATUS%"=="0" goto already_running
if "%PRE_STATUS%"=="2" goto existing_unhealthy

set "POLY_ALPHA_LITE_ROOT=%PROJECT_ROOT%"
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference = 'Stop';" ^
  "$root = $env:POLY_ALPHA_LITE_ROOT;" ^
  "$python = Join-Path $root '.venv\Scripts\python.exe';" ^
  "$runtimeDir = Join-Path $root 'runtime\lite_shadow';" ^
  "$lockFile = Join-Path $runtimeDir 'process.lock';" ^
  "$heartbeatFile = Join-Path $runtimeDir 'heartbeat.json';" ^
  "$stateFile = Join-Path $runtimeDir 'state.json';" ^
  "$logDir = Join-Path $root 'logs';" ^
  "$stdoutLog = Join-Path $logDir 'poly_alpha_lite_shadow.stdout.log';" ^
  "$stderrLog = Join-Path $logDir 'poly_alpha_lite_shadow.stderr.log';" ^
  "$basePython = [string](& $python -I -c 'import sys; print(sys._base_executable)');" ^
  "$expectedExecutables = @([System.IO.Path]::GetFullPath($python), [System.IO.Path]::GetFullPath($basePython.Trim()));" ^
  "$modulePattern = '(?i)(?:^|\s)-m\s+\S*lite[.]lite_bot\S*(?:\s|$)';" ^
  "New-Item -ItemType Directory -Force -Path $logDir | Out-Null;" ^
  "$launchNonce = [Guid]::NewGuid().ToString('N');" ^
  "$launchStartedMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds();" ^
  "$env:POLY_ALPHA_LITE_LAUNCH_NONCE = $launchNonce;" ^
  "$process = Start-Process -FilePath $python -ArgumentList @('-m','lite.lite_bot') -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog -PassThru;" ^
  "$env:POLY_ALPHA_LITE_LAUNCH_NONCE = $null;" ^
  "Write-Output ('Started Lite shadow launcher PID {0}; waiting for its safety heartbeat...' -f $process.Id);" ^
  "$deadline = [DateTime]::UtcNow.AddSeconds(15);" ^
  "$ready = $false;" ^
  "$litePid = 0;" ^
  "$liteIdentityValid = $false;" ^
  "$ownedLaunch = $false;" ^
  "while ([DateTime]::UtcNow -lt $deadline) {" ^
  "  if ((Test-Path -LiteralPath $lockFile) -and (Test-Path -LiteralPath $heartbeatFile) -and (Test-Path -LiteralPath $stateFile)) {" ^
  "    try {" ^
  "      $lock = Get-Content -LiteralPath $lockFile -Raw | ConvertFrom-Json;" ^
  "      $heartbeat = Get-Content -LiteralPath $heartbeatFile -Raw | ConvertFrom-Json;" ^
  "      $state = Get-Content -LiteralPath $stateFile -Raw | ConvertFrom-Json;" ^
  "      $litePid = [int]$lock.pid;" ^
  "      $liteProcess = Get-CimInstance Win32_Process -Filter ('ProcessId={0}' -f $litePid) -ErrorAction SilentlyContinue;" ^
  "      $actualExecutable = if ($liteProcess) { [System.IO.Path]::GetFullPath([string]$liteProcess.ExecutablePath) } else { '' };" ^
  "      $executableMatch = @($expectedExecutables | Where-Object { [string]::Equals($actualExecutable, $_, [System.StringComparison]::OrdinalIgnoreCase) }).Count -gt 0;" ^
  "      $commandMatch = $liteProcess -and ([string]$liteProcess.CommandLine -match $modulePattern);" ^
  "      $liteIdentityValid = $executableMatch -and $commandMatch;" ^
  "      $ownedLaunch = $liteIdentityValid -and ([string]$lock.launch_nonce -eq $launchNonce) -and ([int64]$lock.started_ts_ms -ge $launchStartedMs);" ^
  "      $nonceMatch = $ownedLaunch -and ([string]$heartbeat.launch_nonce -eq $launchNonce) -and ([string]$state.launch_nonce -eq $launchNonce);" ^
  "      $ready = $nonceMatch -and ([string]$lock.mode -eq 'lite_shadow') -and ([int]$heartbeat.pid -eq $litePid) -and ([string]$heartbeat.mode -eq 'lite_shadow') -and ([int]$state.pid -eq $litePid) -and ([string]$state.mode -eq 'lite_shadow') -and ($state.running -is [bool]) -and ([bool]$state.running) -and ($state.dry_run -is [bool]) -and ([bool]$state.dry_run) -and ($state.live_enabled -is [bool]) -and (-not [bool]$state.live_enabled);" ^
  "    } catch { $ready = $false; $liteIdentityValid = $false };" ^
  "  };" ^
  "  if ($ready) { break };" ^
  "  Start-Sleep -Milliseconds 250;" ^
  "};" ^
  "if (-not $ready) {" ^
  "  if ($ownedLaunch -and $litePid -gt 0) { Stop-Process -Id $litePid -Force -ErrorAction SilentlyContinue };" ^
  "  if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue };" ^
  "  Write-Error ('Lite failed to publish a verified lite_shadow/dry_run=true/live_enabled=false heartbeat within 15 seconds. See {0} and {1}.' -f $stdoutLog, $stderrLog);" ^
  "  exit 1;" ^
  "};" ^
  "Write-Output ('Verified Lite shadow runtime PID {0}.' -f $litePid);" ^
  "exit 0"
set "START_CODE=%ERRORLEVEL%"
set "POLY_ALPHA_LITE_ROOT="

if not "%START_CODE%"=="0" exit /b %START_CODE%

echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%STATUS_SCRIPT%"
exit /b %ERRORLEVEL%

:already_running
echo Poly Alpha Lite shadow is already running.
powershell -NoProfile -ExecutionPolicy Bypass -File "%STATUS_SCRIPT%"
exit /b %ERRORLEVEL%

:existing_unhealthy
echo ERROR: A validated Lite process already exists but its health or safety state is not OK.
echo Refusing to start a duplicate. Inspect the status below.
powershell -NoProfile -ExecutionPolicy Bypass -File "%STATUS_SCRIPT%"
exit /b 2
