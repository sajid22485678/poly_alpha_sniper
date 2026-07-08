# Create .env from the template (never overwrites an existing .env).
# Run: powershell -ExecutionPolicy Bypass -File scripts/setup_env.ps1
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if (Test-Path ".env") {
    Write-Host ".env already exists — not touching it." -ForegroundColor Yellow
} else {
    Copy-Item ".env.example" ".env"
    Write-Host ".env created. Open it and fill in:" -ForegroundColor Green
    Write-Host "  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID   (alerts + controls)"
    Write-Host "  DASHBOARD_USERNAME / DASHBOARD_PASSWORD (dashboard login)"
    Write-Host "  POLYMARKET_* keys                        (ONLY needed for live)"
    Write-Host ""
    Write-Host "Live trading stays OFF until you also set:" -ForegroundColor Yellow
    Write-Host "  LIVE_TRADING_ENABLED=true"
    Write-Host "  I_UNDERSTAND_REAL_MONEY_RISK=true"
}
