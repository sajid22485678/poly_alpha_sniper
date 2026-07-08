@echo off
cd /d "%~dp0.."
".venv\Scripts\python.exe" tools\export_agent_readonly.py %*
pause
