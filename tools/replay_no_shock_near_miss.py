"""Read-only replay of recent rejected_by_no_shock diagnostics: how close
did the market actually come to firing a shock?

Parses the shock_score=X.XX token this fix adds to every rejected_by_no_shock
detail string (see strategy/shock_near_miss.py, wired in core/app.py's
_scan_entries). Rows written before this fix won't have the token and are
reported separately, not silently dropped or fabricated.

Also compares no_shock vs no_fresh_cex_price counts over the same recent
window, since the user complaint was about which gate actually dominates.

Read-only: opens the DB read-only, writes nothing back.

Usage:
    .venv\\Scripts\\python.exe tools\\replay_no_shock_near_miss.py
    .venv\\Scripts\\python.exe tools\\replay_no_shock_near_miss.py --out report.json
"""
from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse
import json

from poly_alpha_sniper.dashboard.db_reader import DashboardData, resolve_db_path
from poly_alpha_sniper.strategy.shock_near_miss import NEAR_MISS_SCORE_THRESHOLD

_SCORE_RE = re.compile(r"shock_score=([0-9.]+)")


def replay(db_path: str | None = None, limit: int = 3000) -> dict:
    data = DashboardData(db_path or resolve_db_path())
    rows = data.diagnostics(limit)

    no_shock = [r for r in rows if r.get("reason") == "rejected_by_no_shock"]
    no_fresh = [r for r in rows if r.get("reason") == "rejected_by_no_fresh_cex_price"]
    watchlist = [r for r in rows if r.get("reason") == "watchlist_no_shock_near_miss"]

    scored, unscored = [], []
    for r in no_shock:
        m = _SCORE_RE.search(r.get("detail") or "")
        if m:
            scored.append((r, float(m.group(1))))
        else:
            unscored.append(r)

    by_asset = Counter(r.get("asset") for r, _ in scored)
    scores = [s for _, s in scored]
    near_miss_count = sum(1 for s in scores if s >= NEAR_MISS_SCORE_THRESHOLD)

    summary = {
        "window_rows_analyzed": len(rows),
        "no_shock_count": len(no_shock),
        "no_fresh_cex_price_count": len(no_fresh),
        "dominant_gate": ("no_fresh_cex_price" if len(no_fresh) > len(no_shock)
                          else "no_shock" if len(no_shock) > len(no_fresh) else "tied"),
        "no_shock_rows_with_score": len(scored),
        "no_shock_rows_without_score_pre_fix": len(unscored),
        "no_shock_count_by_asset": dict(by_asset),
        "average_shock_score": round(sum(scores) / len(scores), 4) if scores else None,
        "near_miss_threshold": NEAR_MISS_SCORE_THRESHOLD,
        "near_miss_count": near_miss_count,
        "near_miss_pct_of_scored": (round(100 * near_miss_count / len(scored), 1)
                                    if scored else None),
        "watchlist_rows_persisted": len(watchlist),
        "note": ("no_shock_rows_without_score_pre_fix are historical rows written "
                 "before this fix -- honestly excluded from the score/near-miss "
                 "stats above, not treated as zero."),
    }
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="", help="optional path to write JSON summary")
    ap.add_argument("--limit", type=int, default=3000)
    args = ap.parse_args()
    summary = replay(limit=args.limit)
    print(json.dumps(summary, indent=2, default=str))
    if args.out:
        Path(args.out).write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        print(f"\nwritten to {args.out}")


if __name__ == "__main__":
    main()
