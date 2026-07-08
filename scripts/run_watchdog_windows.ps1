# Run the watchdog (spawns + supervises the bot, auto-restarts on crash).
param([string]$Mode = "shadow_live")
$root = Split-Path -Parent $PSScriptRoot
Set-Location (Split-Path -Parent $root)  # parent of package for -m imports
& "$root\.venv\Scripts\python.exe" -m poly_alpha_sniper.core.watchdog --mode $Mode
