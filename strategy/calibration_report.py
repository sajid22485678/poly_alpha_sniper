"""Calibration report: are predicted probabilities reliable?

Run: python -m poly_alpha_sniper.strategy.calibration_report [--db path]
Reads resolved predictions (resolved_outcome WIN/LOSS) and reports buckets,
Brier score, calibration error, over/under-confidence, drift and per-asset /
per-tier / per-market-type slices.
"""
from __future__ import annotations

import argparse
from collections import defaultdict

from poly_alpha_sniper.strategy.calibration import (
    brier_score, bucketize, calibration_error, confidence_flags)


def _pairs(rows: list[dict]) -> list[tuple[float, bool]]:
    out = []
    for r in rows:
        p = r.get("fair_probability")
        res = str(r.get("resolved_outcome") or "")
        if p is None or res not in ("WIN", "LOSS"):
            continue
        out.append((float(p), res == "WIN"))
    return out


def build_report(rows: list[dict], n_buckets: int = 10) -> dict:
    preds = _pairs(rows)
    buckets = bucketize(preds, n_buckets)
    report = {
        "n": len(preds),
        "buckets": buckets,
        "brier": round(brier_score(preds), 4),
        "calibration_error": round(calibration_error(buckets), 4),
        **confidence_flags(buckets),
        "by_asset": {}, "by_tier": {}, "by_market_type": {}, "drift": {},
    }
    for key, field in (("by_asset", "asset"), ("by_tier", "tier")):
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            groups[str(r.get(field) or "?")].append(r)
        for name, grp in groups.items():
            gp = _pairs(grp)
            if gp:
                gb = bucketize(gp, 5)
                report[key][name] = {"n": len(gp), "brier": round(brier_score(gp), 4),
                                     "calibration_error": round(calibration_error(gb), 4)}
    # market type via title heuristic when column missing
    groups = defaultdict(list)
    for r in rows:
        title = str(r.get("market_title") or "").lower()
        kind = "UP_DOWN" if "up or down" in title or "higher or lower" in title else "THRESHOLD"
        groups[kind].append(r)
    for name, grp in groups.items():
        gp = _pairs(grp)
        if gp:
            gb = bucketize(gp, 5)
            report["by_market_type"][name] = {"n": len(gp),
                                              "calibration_error": round(calibration_error(gb), 4)}
    # drift: first half vs second half by time
    rows_sorted = sorted(rows, key=lambda r: r.get("ts_ms") or 0)
    half = len(rows_sorted) // 2
    for name, part in (("first_half", rows_sorted[:half]), ("second_half", rows_sorted[half:])):
        gp = _pairs(part)
        if gp:
            gb = bucketize(gp, 5)
            report["drift"][name] = round(calibration_error(gb), 4)
    return report


def render_text(report: dict) -> str:
    lines = [f"CALIBRATION REPORT — n={report['n']}"]
    if report["n"] == 0:
        lines.append("No resolved predictions yet. Run shadow mode to collect data.")
        return "\n".join(lines)
    lines.append(f"Brier score: {report['brier']}  |  Calibration error: {report['calibration_error']}")
    if report.get("overconfident"):
        lines.append("WARNING: model is OVERCONFIDENT (predicted > realized).")
    if report.get("underconfident"):
        lines.append("NOTE: model is underconfident (predicted < realized).")
    lines.append(f"{'bucket':>12} {'n':>6} {'pred':>8} {'winrate':>8} {'gap':>8}")
    for b in report["buckets"]:
        if b["n"]:
            lines.append(f"{b['bucket_lo']:.1f}-{b['bucket_hi']:.1f}".rjust(12)
                         + f"{b['n']:>6} {b['mean_pred']:>8.3f} {b['winrate']:>8.3f} {b['gap']:>8.3f}")
    for scope in ("by_asset", "by_tier", "by_market_type"):
        if report[scope]:
            lines.append(f"-- {scope}: " + ", ".join(
                f"{k}: ece={v['calibration_error']}" for k, v in report[scope].items()))
    if report["drift"]:
        lines.append(f"-- drift: {report['drift']}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="")
    args = parser.parse_args()
    from poly_alpha_sniper.storage.db import default_sqlite_path
    from poly_alpha_sniper.storage.sqlite_store import SqliteStore
    path = args.db or default_sqlite_path()
    try:
        store = SqliteStore(path)
        rows = store.query("SELECT * FROM predictions WHERE resolved_outcome IN ('WIN','LOSS')")
    except Exception as exc:  # noqa: BLE001
        print(f"cannot read predictions from {path}: {exc}")
        return
    print(render_text(build_report(rows)))


if __name__ == "__main__":
    main()
