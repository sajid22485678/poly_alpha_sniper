"""Walk-forward validation: train/validation/test day folds + stability rules.

Reject a configuration when (master spec):
- train good but test bad
- pnl depends on one lucky day (>60% of profit from a single day)
- compounded good but fixed bad
- max drawdown too high (>30%)
- too few trades (<30)
- profit factor unstable across folds (stdev > 0.5)

Run: python -m poly_alpha_sniper.backtest.walk_forward --cex data/cex.csv
"""
from __future__ import annotations

import argparse
import math
from collections import defaultdict


def _day(ts_ms: int) -> str:
    return str(int(ts_ms // 86_400_000))


def _stats(trades: list[dict]) -> dict:
    pnls = [t["pnl"] for t in trades]
    wins = sum(p for p in pnls if p > 0)
    losses = abs(sum(p for p in pnls if p <= 0))
    return {"n": len(trades), "pnl": round(sum(pnls), 4),
            "pf": round(wins / losses, 3) if losses > 1e-9 else (2.0 if wins else 0.0)}


def evaluate_folds(trades: list[dict], k: int = 3,
                   fixed_roi: float | None = None,
                   compounded_roi: float | None = None,
                   max_dd_pct: float | None = None) -> dict:
    reasons: list[str] = []
    if len(trades) < 30:
        reasons.append(f"too few trades ({len(trades)} < 30)")

    by_day: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_day[_day(t["ts_ms"])].append(t)
    days = sorted(by_day)

    # single lucky day dominance
    total_pnl = sum(t["pnl"] for t in trades)
    if total_pnl > 0:
        best_day_pnl = max((sum(t["pnl"] for t in by_day[d]) for d in days), default=0)
        if best_day_pnl > 0.6 * total_pnl and len(days) > 1:
            reasons.append(f"single day contributes {best_day_pnl / total_pnl:.0%} of pnl")

    # fold stability
    folds: list[dict] = []
    if len(days) >= k:
        per_fold = max(1, len(days) // k)
        for i in range(k):
            fold_days = days[i * per_fold:(i + 1) * per_fold] if i < k - 1 \
                else days[(k - 1) * per_fold:]
            fold_trades = [t for d in fold_days for t in by_day[d]]
            folds.append(_stats(fold_trades))
        train, test = folds[0], folds[-1]
        if train["pf"] >= 1.2 and test["pf"] < 0.9:
            reasons.append(f"train pf {train['pf']} good but test pf {test['pf']} bad")
        pfs = [f["pf"] for f in folds if f["n"] > 0]
        if len(pfs) >= 2:
            mean = sum(pfs) / len(pfs)
            stdev = math.sqrt(sum((p - mean) ** 2 for p in pfs) / len(pfs))
            if stdev > 0.5:
                reasons.append(f"profit factor unstable across folds (stdev {stdev:.2f})")
    else:
        folds = [_stats(trades)]
        reasons.append(f"only {len(days)} distinct days — walk-forward weak")

    if fixed_roi is not None and compounded_roi is not None:
        if compounded_roi > 0 >= fixed_roi:
            reasons.append("compounded positive but fixed-size not — sizing dependent")
    if max_dd_pct is not None and max_dd_pct > 30:
        reasons.append(f"max drawdown {max_dd_pct:.1f}% > 30%")

    return {"accept": not reasons, "reasons": reasons, "folds": folds,
            "days": len(days)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cex", default="", help="CEX ticks CSV; demo folds when omitted")
    parser.add_argument("--k", type=int, default=3)
    args = parser.parse_args()
    if args.cex:
        from poly_alpha_sniper.backtest.replay_engine import ReplayEngine
        from poly_alpha_sniper.core.config_loader import load_config
        cfg = load_config(profile_override="backtest_5min")
        result = ReplayEngine(cfg, args.cex).run_both()
        m = result["metrics"]
        verdict = evaluate_folds(result["compounded"]["trades"], args.k,
                                 fixed_roi=m["fixed_vs_compounded"]["fixed_roi_pct"],
                                 compounded_roi=m["fixed_vs_compounded"]["compounded_roi_pct"],
                                 max_dd_pct=m["max_drawdown_pct"])
    else:
        demo = [{"ts_ms": i * 3_600_000, "pnl": (0.1 if i % 3 else -0.08)}
                for i in range(80)]
        verdict = evaluate_folds(demo, args.k)
        print("(demo trades — supply --cex for a real run)")
    print(f"ACCEPT: {verdict['accept']}")
    for r in verdict["reasons"]:
        print(f" - {r}")
    print(f"folds: {verdict['folds']}")


if __name__ == "__main__":
    main()
