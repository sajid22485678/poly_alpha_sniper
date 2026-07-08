"""Export DB tables to CSV for external analysis."""
from __future__ import annotations

import csv
from pathlib import Path

from poly_alpha_sniper.storage.migrations import TABLES


def export_table(store, table: str, out_path: str) -> int:
    rows = store.query(f"SELECT * FROM {table}")  # table names come from TABLES only
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return 0
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def export_all(store, out_dir: str) -> dict[str, int]:
    out = {}
    for table in TABLES:
        out[table] = export_table(store, table, str(Path(out_dir) / f"{table}.csv"))
    return out
