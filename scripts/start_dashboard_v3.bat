@echo off
cd /d "%~dp0.."
set "PATH=C:\Program Files\nodejs;%PATH%"
cd dashboard_v3
call npm run dev
pause
