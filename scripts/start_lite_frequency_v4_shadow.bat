@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_lite_frequency_v4_shadow.ps1"
exit /b %ERRORLEVEL%
