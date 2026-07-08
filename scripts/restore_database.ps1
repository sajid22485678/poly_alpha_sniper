# Restore the database from a backup file. STOPS if the bot is running.
# Usage: powershell -ExecutionPolicy Bypass -File scripts/restore_database.ps1 -Backup backups\poly_alpha_sniper_XXXX.db
param([Parameter(Mandatory=$true)][string]$Backup)
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if (Test-Path "runtime\live.lock") {
    Write-Host "runtime\live.lock exists — stop the bot before restoring." -ForegroundColor Red
    exit 1
}
$env:PAS_RESTORE_BACKUP = $Backup
& ".venv\Scripts\python.exe" -c @'
import os, sys; sys.path.insert(0, r"..")
from poly_alpha_sniper.core.clock import WallClock
from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.storage.backup_manager import BackupManager
from poly_alpha_sniper.storage.db import default_sqlite_path
bm = BackupManager(load_config(), WallClock(), default_sqlite_path())
restored = bm.restore(os.environ["PAS_RESTORE_BACKUP"], confirm=True)
print("restored to:", restored)
'@
