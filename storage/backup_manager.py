"""Database backup / restore.

Backups use the sqlite3 online backup API (consistent even mid-write), pruned
to cfg.runtime.keep_backups. restore() requires confirm=True and makes a
safety copy of the current DB first.
"""
from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

from poly_alpha_sniper.core.config_loader import PROJECT_ROOT
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("backup")

# Refuse to copy a runaway DB. Post-mortem (2026-07-10): a feature-store
# flooding bug grew the DB to 2.17 GB and the 30-min backup loop faithfully
# copied it until backups/ held 9.6 GB. A healthy shadow DB here is <100 MB;
# past this cap a backup would only replicate a bug, so skip + warn instead.
MAX_BACKUP_DB_BYTES = 500 * 1024 * 1024


class BackupSkipped(RuntimeError):
    """Raised instead of copying an oversized DB -- callers treat as warning."""


class BackupManager:
    def __init__(self, cfg, clock, db_path: str, backup_dir: str = ""):
        self.cfg = cfg
        self.clock = clock
        self.db_path = Path(db_path) if db_path else None
        self.backup_dir = Path(backup_dir) if backup_dir else (PROJECT_ROOT / "backups")

    def backup_now(self) -> Path:
        if self.db_path is None or not self.db_path.exists():
            raise FileNotFoundError(f"database not found: {self.db_path}")
        size = self.db_path.stat().st_size
        if size > MAX_BACKUP_DB_BYTES:
            log.warning("backup_skipped_db_too_large", extra={"extra": {
                "size_mb": round(size / 1e6), "cap_mb": round(MAX_BACKUP_DB_BYTES / 1e6),
                "hint": "run tools/db_maintenance.py after stopping the bot"}})
            raise BackupSkipped(f"db {size / 1e6:.0f}MB exceeds backup cap")
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        dest = self.backup_dir / f"{self.db_path.stem}_{self.clock.now_ms()}.db"
        src = sqlite3.connect(str(self.db_path))
        try:
            dst = sqlite3.connect(str(dest))
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
        self.prune()
        log.info("backup_done", extra={"extra": {"dest": str(dest)}})
        return dest

    def list_backups(self) -> list[Path]:
        if not self.backup_dir.exists():
            return []
        return sorted(self.backup_dir.glob(f"{self.db_path.stem}_*.db")) if self.db_path \
            else sorted(self.backup_dir.glob("*.db"))

    def prune(self) -> int:
        keep = max(1, self.cfg.runtime.keep_backups)
        backups = self.list_backups()
        removed = 0
        for old in backups[:-keep] if len(backups) > keep else []:
            try:
                old.unlink()
                removed += 1
            except OSError:
                pass
        return removed

    def restore(self, backup_path: str, confirm: bool = False) -> Path:
        if not confirm:
            raise PermissionError("restore requires confirm=True")
        src = Path(backup_path)
        if not src.exists():
            raise FileNotFoundError(f"backup not found: {src}")
        if self.db_path is None:
            raise ValueError("no db_path configured")
        if self.db_path.exists():
            safety = self.db_path.with_suffix(f".pre_restore_{self.clock.now_ms()}.db")
            shutil.copy2(self.db_path, safety)
            log.info("restore_safety_copy", extra={"extra": {"path": str(safety)}})
        shutil.copy2(src, self.db_path)
        log.info("restore_done", extra={"extra": {"from": str(src)}})
        return self.db_path
