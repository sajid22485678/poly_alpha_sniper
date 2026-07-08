# Run the read-only dashboard on all interfaces (LAN access).
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
& ".venv\Scripts\python.exe" -m streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port 8501
