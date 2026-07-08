"""Optional copy of the daily Obsidian note into a vault folder.

Disabled by default (cfg.obsidian.enabled=False in config.yaml). Does NOT
require Obsidian to be installed -- this is just a plain file copy into a
folder Obsidian happens to watch. Never overwrites an existing note unless
cfg.obsidian.overwrite_existing is explicitly set True. Uses a date-stamped
filename so each day gets its own note.
"""
from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Optional


def dated_note_filename(now_ms: int, prefix: str = "poly_alpha_sniper_daily") -> str:
    date_str = time.strftime("%Y-%m-%d", time.gmtime(now_ms / 1000))
    return f"{prefix}_{date_str}.md"


def backup_before_overwrite(path: str, backup_dir: Optional[str] = None,
                            now_ms: Optional[int] = None) -> Optional[str]:
    """If `path` exists, copies it to a timestamped backup before any caller
    proceeds to overwrite it. Returns the backup path, or None if there was
    nothing to back up (path didn't exist -- a normal, expected case, not an
    error). Used both by copy_note_to_vault() below and manually whenever an
    existing Obsidian note is edited in place."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    src = Path(path)
    if not src.exists():
        return None
    backup_root = Path(backup_dir) if backup_dir else src.parent
    backup_root.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y-%m-%d_%H%M%S", time.gmtime(now_ms / 1000))
    backup_path = backup_root / f"{src.stem} (backup {stamp}){src.suffix}"
    shutil.copy2(src, backup_path)
    return str(backup_path)


def copy_note_to_vault(source_path: str, cfg, now_ms: Optional[int] = None) -> dict:
    """Returns {"copied": bool, "dest": str|None, "reason": str,
    "backup_path": str|None}. Never raises on a missing/disabled config --
    callers get a clear reason instead of an exception, since this is an
    optional best-effort step. When overwrite_existing allows replacing an
    existing note, the prior version is always backed up first -- "allowed
    to overwrite" never means "allowed to destroy without a copy"."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    if not cfg.obsidian.enabled:
        return {"copied": False, "dest": None, "backup_path": None,
                "reason": "obsidian.enabled is False in config.yaml (default) -- Obsidian sync is opt-in"}
    src = Path(source_path)
    if not src.exists():
        return {"copied": False, "dest": None, "backup_path": None,
                "reason": f"source note not found: {src}"}
    vault_dir = Path(cfg.obsidian.vault_notes_dir)
    try:
        vault_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return {"copied": False, "dest": None, "backup_path": None,
                "reason": f"cannot create vault dir: {exc}"}
    dest = vault_dir / dated_note_filename(now_ms)
    backup_path = None
    if dest.exists():
        if not cfg.obsidian.overwrite_existing:
            return {"copied": False, "dest": str(dest), "backup_path": None,
                    "reason": "a note for this date already exists and obsidian.overwrite_existing is False"}
        backup_path = backup_before_overwrite(str(dest), now_ms=now_ms)
    shutil.copy2(src, dest)
    return {"copied": True, "dest": str(dest), "backup_path": backup_path, "reason": ""}
