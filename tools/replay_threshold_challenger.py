"""Mission F: research-only replay/challenger for no_shock + freshness settings.

Reads persisted shadow_diagnostics (which carry the recorded shock_score per
no_shock candidate) and predictions (which carry resolved_outcome for the
signals that actually fired). Evaluates several challenger threshold configs
and reports, per challenger:
  - how many MORE candidates would have qualified (count delta)
  - whether outcome-based quality (hit rate / PF / drawdown / EV) is
    computable for those ADDED candidates

Strict safety rule (Mission F): a challenger is only ACCEPTED if it can be
PROVEN not to degrade EV / PF / drawdown / tier mix and never accepts
missing-oracle / stale-book / bad-spread candidates. Because the added
near-miss candidates were never traded, they have NO resolved outcome, so
their realized EV/PF/hit-rate are UNKNOWN -- unknown quality can never
satisfy a "prove no degradation" rule. This tool therefore reports
NO SAFE THRESHOLD RELAXATION FOUND unless real outcome data for the added
candidates exists. It NEVER writes to config and NEVER changes production
thresholds -- it prints/returns a research report only.

Read-only. No orders. No .env. No production threshold change.

Usage:
    .venv\\Scripts\\python.exe tools\\replay_threshold_challenger.py
    .venv\\Scripts\\python.exe tools\\replay_threshold_challenger.py --out report.json
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from poly_alpha_sniper.dashboard.db_reader import DashboardData, resolve_db_path

_SCORE_RE = re.compile(r"shock_score=([0-9.]+)")

# Challenger definitions. Each is a shock_score CUTOFF (fraction of the current
# firing threshold) at/above which a no_shock candidate would now "fire". The
# baseline is 1.0 (unchanged). Lower cutoff = more candidates. asset-specific
# and HOT-only variants included per the mission brief.
_CHALLENGERS = [
    {"name": "baseline_current", "cutoff": 1.00, "note": "unchanged production threshold"},
    {"name": "hot_only_0.90", "cutoff": 0.90, "note": "fire only HOT near-misses"},
    {"name": "near_miss_0.80", "cutoff": 0.80, "note": "fire NEAR_MISS+"},
    {"name": "watchlist_0.70", "cutoff": 0.70, "note": "fire WATCHLIST+ (most aggressive)"},
]


def replay(db_path: str | None = None, window_minutes: int = 120) -> dict:
    data = DashboardData(db_path or resolve_db_path())
    diag = data.diagnostics(50000)
    preds = data.predictions(50000)

    allts = [r.get("ts_ms") or 0 for r in diag] + [r.get("ts_ms") or 0 for r in preds]
    now = max(allts) if allts else 0
    cutoff_ts = now - window_minutes * 60_000

    scored = []
    for r in diag:
        if (r.get("ts_ms") or 0) < cutoff_ts:
            continue
        if r.get("reason") not in ("rejected_by_no_shock", "watchlist_no_shock_near_miss"):
            continue
        m = _SCORE_RE.search(r.get("detail") or "")
        if m:
            scored.append(float(m.group(1)))

    # resolved outcomes for the ACTUAL signals that fired (the only rows with
    # a known result). Near-miss candidates have none.
    resolved = [p for p in preds if (p.get("ts_ms") or 0) >= cutoff_ts
                and p.get("resolved_outcome") not in (None, "", "UNRESOLVED")]

    span_h = max(window_minutes / 60.0, 1e-6)
    results = []
    baseline_count = sum(1 for s in scored if s >= 1.00)
    for ch in _CHALLENGERS:
        would_fire = sum(1 for s in scored if s >= ch["cutoff"])
        added = would_fire - baseline_count
        # Quality of the ADDED candidates is unknowable -- they were never
        # traded, so no resolved outcome exists. Never fabricate a metric.
        results.append({
            "name": ch["name"],
            "cutoff": ch["cutoff"],
            "note": ch["note"],
            "candidates_per_hour": round(would_fire / span_h, 2),
            "added_vs_baseline_per_hour": round(added / span_h, 2),
            "added_candidates_have_resolved_outcomes": False,
            "estimated_hit_rate": None,
            "estimated_profit_factor": None,
            "estimated_drawdown": None,
            "estimated_ev": None,
            "quality_provable": False,
            "accepts_missing_oracle": "unknown_not_evaluated",
            "accepts_stale_book": "unknown_not_evaluated",
            "accepts_bad_spread": "unknown_not_evaluated",
            "verdict": ("BASELINE" if ch["cutoff"] >= 1.0
                        else "REJECTED: added-candidate quality is unprovable "
                             "(no resolved outcomes) -- cannot satisfy no-degradation rule"),
        })

    any_safe = any(r["verdict"].startswith("ACCEPTED") for r in results)
    return {
        "generated_ts_ms": now,
        "window_minutes": window_minutes,
        "no_shock_candidates_scored": len(scored),
        "resolved_signal_outcomes_in_window": len(resolved),
        "challengers": results,
        "overall_verdict": ("SOME CHALLENGER SAFE" if any_safe
                            else "NO SAFE THRESHOLD RELAXATION FOUND"),
        "reason": ("Near-miss candidates that a lower threshold would newly fire have no "
                   "resolved trade outcomes, so their realized EV/PF/hit-rate cannot be "
                   "computed. The strict Mission-F rule requires PROVING no degradation; "
                   "unprovable quality is rejected. Production thresholds are unchanged."),
        "production_thresholds_changed": False,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="")
    ap.add_argument("--window", type=int, default=120)
    args = ap.parse_args()
    report = replay(window_minutes=args.window)
    print(json.dumps(report, indent=2, default=str))
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(f"\nwritten to {args.out}")


if __name__ == "__main__":
    main()
