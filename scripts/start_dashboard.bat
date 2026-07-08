@echo off
cd /d "%~dp0.."
".venv\Scripts\python.exe" -m streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port 8501
pause
