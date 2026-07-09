@echo off
REM Starts the persistent auto-export loop (scripts\auto_export_loop.ps1)
REM detached and hidden in the background, then returns immediately -- no
REM pause, this is meant to be double-clicked and left alone. Stop it with
REM scripts\stop_auto_export_loop.ps1, check status with
REM scripts\status_auto_export_loop.ps1.
cd /d "%~dp0.."
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process powershell -ArgumentList '-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File \"%~dp0auto_export_loop.ps1\"' -WindowStyle Hidden"
echo Auto-export loop started in the background (exports every 10s).
echo Check status:  powershell -File scripts\status_auto_export_loop.ps1
echo Stop it:       powershell -File scripts\stop_auto_export_loop.ps1
