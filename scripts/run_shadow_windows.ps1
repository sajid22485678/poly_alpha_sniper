# Run shadow mode (real data, NO real orders).
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
& ".venv\Scripts\python.exe" main.py --mode shadow_live
