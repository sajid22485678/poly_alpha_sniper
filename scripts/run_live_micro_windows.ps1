# Run live_micro ($1 trades). BLOCKED unless every live gate passes:
# .env flags, preflight, reconciliation and live readiness.
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
Write-Host "LIVE MICRO MODE — real money. All gates will be enforced." -ForegroundColor Red
& ".venv\Scripts\python.exe" main.py --mode live_micro
