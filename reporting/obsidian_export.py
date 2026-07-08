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


def copy_note_to_vault(source_path: str, cfg, now_ms: Optional[int] = None) -> dict:
    """Returns {"copied": bool, "dest": str|None, "reason": str}. Never
    raises on a missing/disabled config -- callers get a clear reason
    instead of an exception, since this is an optional best-effort step."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    if not cfg.obsidian.enabled:
        return {"copied": False, "dest": None,
                "reason": "obsidian.enabled is False in config.yaml (default) -- Obsidian sync is opt-in"}
    src = Path(source_path)
    if not src.exists():
        return {"copied": False, "dest": None, "reason": f"source note not found: {src}"}
    vault_dir = Path(cfg.obsidian.vault_notes_dir)
    try:
        vault_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return {"copied": False, "dest": None, "reason": f"cannot create vault dir: {exc}"}
    dest = vault_dir / dated_note_filename(now_ms)
    if dest.exists() and not cfg.obsidian.overwrite_existing:
        return {"copied": False, "dest": str(dest),
                "reason": "a note for this date already exists and obsidian.overwrite_existing is False"}
    shutil.copy2(src, dest)
    return {"copied": True, "dest": str(dest), "reason": ""}
