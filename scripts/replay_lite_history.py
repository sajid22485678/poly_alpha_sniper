"""Honest chronological Lite replay over fields the historical DB contains.

The old schema cannot replay the new direction score or pullback optimizer.
This script therefore evaluates only evidence-supported corrections: official
backfill, taker fees, and first-entry-wins asset/window locking.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from pathlib import Path


TERMINAL = {"CLOSED_BOOK_EXIT", "CLOSED_WIN", "CLOSED_LOSS"}


def rows(path: Path) -> list[dict]:
    con = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in con.execute("SELECT * FROM lite_trades ORDER BY id")]
    finally:
        con.close()


def metrics(data: list[dict]) -> dict:
    terminal = [row for row in data if row.get("status") in TERMINAL and row.get("pnl") is not None]
    values = [float(row["pnl"]) for row in terminal]
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    by_exit = defaultdict(float)
    for row in terminal:
        by_exit[int(row["exit_ts"])] += float(row["pnl"])
    cumulative = peak = drawdown = 0.0
    for timestamp in sorted(by_exit):
        cumulative += by_exit[timestamp]
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak-cumulative)
    first = min((int(row["entry_ts"]) for row in data), default=0)
    last = max((int(row.get("exit_ts") or row["entry_ts"]) for row in data), default=first)
    hours = max((last-first)/3_600_000, 1/3600)
    total = sum(values)
    gross_profit, gross_loss = sum(wins), -sum(losses)
    return {
        "entries": len(data), "completed": len(values),
        "net_realized_pnl": round(total, 10),
        "profit_factor": round(gross_profit/gross_loss, 6) if gross_loss else None,
        "expectancy": round(total/len(values), 10) if values else None,
        "win_rate": round(len(wins)/len(values), 6) if values else None,
        "average_win": round(gross_profit/len(wins), 10) if wins else None,
        "average_loss": round(sum(losses)/len(losses), 10) if losses else None,
        "max_drawdown_timestamp_batched": round(drawdown, 10),
        "entries_per_hour": round(len(data)/hours, 6),
        "completed_per_hour": round(len(values)/hours, 6),
        "unresolved": sum(row.get("status") == "UNRESOLVED_FINAL" for row in data),
    }


def breakdown(data: list[dict], field: str) -> dict:
    grouped = defaultdict(list)
    for row in data:
        grouped[str(row.get(field) or "UNKNOWN")].append(row)
    return {key: metrics(value) for key, value in sorted(grouped.items())}


def first_entry_per_window(data: list[dict]) -> list[dict]:
    chosen = {}
    for row in sorted(data, key=lambda item: (int(item["entry_ts"]), int(item["id"]))):
        chosen.setdefault((row["asset"], int(row["window_close_ts"])), row)
    return sorted(chosen.values(), key=lambda item: (int(item["entry_ts"]), int(item["id"])))


def conflict_metrics(data: list[dict]) -> dict:
    grouped = defaultdict(set)
    for row in data:
        grouped[(str(row["asset"]), int(row["window_close_ts"]))].add(str(row["side"]))
    conflicts = sum(len(sides) > 1 for sides in grouped.values())
    windows = len(grouped)
    return {
        "asset_windows": windows,
        "opposite_side_conflicts": conflicts,
        "opposite_side_conflict_rate": round(conflicts/windows, 10) if windows else 0.0,
    }


def segments(data: list[dict]) -> dict[str, set[tuple]]:
    keys = sorted({(int(row["window_close_ts"]), str(row["asset"])) for row in data})
    n = len(keys)
    train_end, validation_end = int(n*0.60), int(n*0.80)
    return {
        "train": set(keys[:train_end]),
        "validation": set(keys[train_end:validation_end]),
        "holdout": set(keys[validation_end:]),
    }


def in_segment(data: list[dict], keys: set[tuple]) -> list[dict]:
    return [row for row in data
            if (int(row["window_close_ts"]), str(row["asset"])) in keys]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-db", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    baseline = rows(Path(args.baseline_db).resolve())
    corrected = rows(Path(args.db).resolve())
    candidate = first_entry_per_window(corrected)
    split = segments(corrected)
    report = {
        "method": {
            "chronological_split": "60/20/20 by exact asset/window",
            "candidate": "first entered side locks exact asset/window; fees and exact official outcomes included",
            "optimizer_replay_supported": False,
            "optimizer_replay_blocker": (
                "legacy DB lacks point-in-time horizon returns, full book levels/hash/timestamps, "
                "decision snapshots, and pullback observations"),
            "lookahead": False,
            "fees_and_costs": (
                "current crypto taker fee curve applied at each recorded historical entry/exit price; "
                "per-level fee reconstruction is impossible for legacy rows and no slippage is invented"),
            "execution_realism": (
                "historical top-of-book fills remain unverified because full levels, source timestamps, "
                "hashes, and five-share depth were not stored; forward schema now requires them"),
            "entry_price_improvement": (
                "not historically measurable; forward WAIT_FOR_PULLBACK rows persist expected and actual improvement"),
        },
        "legacy_headline": metrics(baseline),
        "corrected_all_entries": metrics(corrected),
        "candidate_conflict_free_first_entry": metrics(candidate),
        "corrected_by_asset": breakdown(corrected, "asset"),
        "corrected_by_side": breakdown(corrected, "side"),
        "candidate_by_asset": breakdown(candidate, "asset"),
        "candidate_by_side": breakdown(candidate, "side"),
        "conflict_comparison": {
            "corrected_all_entries": conflict_metrics(corrected),
            "candidate_conflict_free_first_entry": conflict_metrics(candidate),
        },
        "frequency_change": {
            "entries_before": len(corrected),
            "entries_after": len(candidate),
            "retained_fraction": round(len(candidate)/len(corrected), 10) if corrected else None,
        },
        "entry_mode": {
            "historical": "UNKNOWN_LEGACY_NOT_RECORDED",
            "new_forward_schema_records": ["ENTER_NOW", "WAIT_FOR_PULLBACK"],
        },
        "segments": {},
    }
    for name, keys in split.items():
        report["segments"][name] = {
            "corrected_all_entries": metrics(in_segment(corrected, keys)),
            "candidate_conflict_free_first_entry": metrics(in_segment(candidate, keys)),
        }
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({
        "output": str(output),
        "legacy": report["legacy_headline"],
        "corrected": report["corrected_all_entries"],
        "candidate": report["candidate_conflict_free_first_entry"],
        "holdout": report["segments"]["holdout"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
