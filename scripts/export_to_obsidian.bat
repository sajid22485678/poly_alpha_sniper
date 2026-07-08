@echo off
cd /d "%~dp0.."
".venv\Scripts\python.exe" tools\export_to_obsidian.py
pause
