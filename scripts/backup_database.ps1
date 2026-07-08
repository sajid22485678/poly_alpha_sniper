# Manual database backup (online-safe via sqlite backup API).
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
& ".venv\Scripts\python.exe" -c @'
import sys; sys.path.insert(0, r"..")
from poly_alpha_sniper.core.clock import WallClock
from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.storage.backup_manager import BackupManager
from poly_alpha_sniper.storage.db import default_sqlite_path
bm = BackupManager(load_config(), WallClock(), default_sqlite_path())
print("backup written:", bm.backup_now())
'@
