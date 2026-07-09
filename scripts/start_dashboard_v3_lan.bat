@echo off
REM ==========================================================================
REM  Start the READ-ONLY Dashboard V3 bound to the LAN so a phone on the same
REM  Wi-Fi/router can view it. LAN-only:
REM    - binds 0.0.0.0:8503 (localhost still works for the PC)
REM    - requires a viewing password (Basic Auth) -- refuses to start without one,
REM      so the dashboard is never exposed on the LAN unauthenticated
REM    - reads the SAME read-only exported JSON; no order/write endpoints
REM    - does NOT start or stop the trading bot; never touches .env/secrets
REM  Actual LAN reachability also requires the one-time firewall rule
REM  (scripts\allow_dashboard_v3_lan_firewall.ps1, run once as admin).
REM ==========================================================================
setlocal
cd /d "%~dp0.."
set "PATH=C:\Program Files\nodejs;%PATH%"

echo ============================================================
echo   Poly Alpha Sniper - Dashboard V3 (LAN / phone access)
echo ============================================================
echo Read-only diagnostics only. Does not trade, does not place orders.
echo.

REM Require a viewing password. NOTE: what you type is visible on screen --
REM this is a LAN viewing password you choose, not a stored secret.
set "DASHBOARD_LAN_PASSWORD="
set /p DASHBOARD_LAN_PASSWORD=Set a dashboard viewing password (required):
if "%DASHBOARD_LAN_PASSWORD%"=="" (
  echo.
  echo ERROR: no password entered. LAN mode aborted -- refusing to expose the
  echo        dashboard on the LAN without authentication.
  pause
  exit /b 1
)
set "DASHBOARD_LAN_MODE=1"

echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0print_mobile_dashboard_url.ps1"
echo.
echo If the phone cannot connect, add the firewall rule once (as admin):
echo   powershell -ExecutionPolicy Bypass -File scripts\allow_dashboard_v3_lan_firewall.ps1
echo.
echo Starting on 0.0.0.0:8503 ... (close this window or run
echo scripts\stop_dashboard_v3_lan.ps1 to stop)
echo.
cd dashboard_v3
call npm run dev:lan
pause
