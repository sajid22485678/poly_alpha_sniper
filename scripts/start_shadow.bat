@echo off
cd /d "%~dp0.."
".venv\Scripts\python.exe" main.py --mode shadow_live
pause
