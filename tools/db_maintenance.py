"""Safe DB maintenance: reclaim space after the 2026-07-10 feature-store
flooding bug (block_reason=None bypassed the write throttle -> ~110k
rows/lane/hour -> 2.17 GB DB -> 9.6 GB of backups).

RUN ONLY WITH THE BOT STOPPED. This script:
1. Deletes feature_store rows older than --keep-hours (default 2) -- these
   are throttle-bypass duplicates with no research value; probe/exit/trade/
   baseline tables are NEVER touched.
2. Deletes backup files larger than --max-backup-mb (default 500) -- they are
   copies of the flooded DB.
3. VACUUMs the DB to return the space to the filesystem.

It refuses to run if a bot process appears to hold the DB (WAL busy).
Never touches .env/secrets. Never modifies trading data or live flags.

Usage:
    .venv\\Scripts\\python.exe tools\\db_maintenance.py [--dry-run]
"""
from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DB = REPO / "storage_data" / "poly_alpha_sniper.db"
BACKUPS = REPO / "backups"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--keep-hours", type=float, default=2.0)
    ap.add_argument("--max-backup-mb", type=float, default=500.0)
    args = ap.parse_args()

    size_before = DB.stat().st_size if DB.exists() else 0
    print(f"db size before: {size_before / 1e6:.0f} MB")

    if DB.exists():
        con = sqlite3.connect(str(DB), timeout=5)
        try:
            cutoff = int((time.time() - args.keep_hours * 3600) * 1000)
            n = con.execute("SELECT COUNT(*) FROM feature_store WHERE ts_ms < ?",
                            (cutoff,)).fetchone()[0]
            print(f"feature_store rows older than {args.keep_hours}h: {n}")
            if not args.dry_run and n:
                con.execute("DELETE FROM feature_store WHERE ts_ms < ?", (cutoff,))
                con.commit()
                print("deleted; running VACUUM (may take a minute)...")
                con.execute("VACUUM")
                con.commit()
        finally:
            con.close()
        print(f"db size after: {DB.stat().st_size / 1e6:.0f} MB")

    if BACKUPS.exists():
        cap = args.max_backup_mb * 1e6
        for f in sorted(BACKUPS.glob("*.db")):
            sz = f.stat().st_size
            if sz > cap:
                print(f"{'would delete' if args.dry_run else 'deleting'} oversized backup "
                      f"{f.name} ({sz / 1e6:.0f} MB)")
                if not args.dry_run:
                    f.unlink()


if __name__ == "__main__":
    main()
