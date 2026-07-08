# One-click Windows install for poly_alpha_sniper.
# Run: powershell -ExecutionPolicy Bypass -File scripts/install_windows.ps1
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
Write-Host "== poly_alpha_sniper installer ==" -ForegroundColor Cyan

# find python 3.11+
$python = ""
foreach ($candidate in @("python", "py")) {
    try {
        $v = & $candidate --version 2>$null
        if ($v -match "Python 3\.(1[1-9]|[2-9]\d)") { $python = $candidate; break }
    } catch {}
}
if (-not $python) {
    $appdata = "$env:LOCALAPPDATA\Programs\Python"
    $found = Get-ChildItem $appdata -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match "Python31[1-9]|Python3[2-9]" } |
        Sort-Object Name -Descending | Select-Object -First 1
    if ($found) { $python = Join-Path $found.FullName "python.exe" }
}
if (-not $python) { Write-Error "Python 3.11+ not found. Install from python.org first." }
Write-Host "Using python: $python"

if (-not (Test-Path ".venv")) {
    & $python -m venv .venv
    Write-Host "virtualenv created"
}
& ".venv\Scripts\python.exe" -m pip install --upgrade pip -q
& ".venv\Scripts\python.exe" -m pip install -r requirements.txt -q
Write-Host "dependencies installed"

foreach ($d in @("logs", "backups", "runtime", "storage_data", "data")) {
    New-Item -ItemType Directory -Force $d | Out-Null
}
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host ".env created from .env.example — fill in your keys" -ForegroundColor Yellow
}
& ".venv\Scripts\python.exe" -m pytest tests -q
Write-Host "`nInstall complete. Next steps:" -ForegroundColor Green
Write-Host "  1. Edit .env (Telegram token/chat id; Polymarket keys only for live)"
Write-Host "  2. Start shadow mode:  scripts\start_shadow.bat"
Write-Host "  3. Start dashboard:    scripts\start_dashboard.bat"
