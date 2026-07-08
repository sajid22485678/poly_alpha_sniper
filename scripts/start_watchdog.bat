@echo off
cd /d "%~dp0..\.."
"poly_alpha_sniper\.venv\Scripts\python.exe" -m poly_alpha_sniper.core.watchdog --mode shadow_live
pause
