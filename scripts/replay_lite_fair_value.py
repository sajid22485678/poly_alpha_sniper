"""Focused chronological evidence report for the Lite fair-value rebuild.

This script reads a SQLite snapshot in read-only mode.  It never mutates a DB,
fetches network data, loads environment files, or invents paired-book features
that were not historically stored.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path


BOUNDARY_MS = 1_783_798_375_000
BASELINE_STRATEGY = "lite_direction_sniper_v2"
NEW_STRATEGY = "lite_fair_value_edge_v3"
TERMINAL = {"CLOSED_BOOK_EXIT", "CLOSED_WIN", "CLOSED_LOSS"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def _performance(rows: list[dict]) -> dict:
    ordered = sorted(rows, key=lambda row: (int(row["exit_ts"]), int(row["id"])))
    values = [float(row["pnl"]) for row in ordered]
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    by_exit: dict[int, float] = defaultdict(float)
    for row in ordered:
        by_exit[int(row["exit_ts"])] += float(row["pnl"])
    cumulative = peak = drawdown = 0.0
    for timestamp in sorted(by_exit):
        cumulative += by_exit[timestamp]
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak-cumulative)
    longest = current = 0
    for value in values:
        current = current + 1 if value < 0 else 0
        longest = max(longest, current)
    gross_profit = sum(wins)
    gross_loss = -sum(losses)
    total = sum(values)
    return {
        "completed": len(values),
        "net_pnl": round(total, 10),
        "gross_profit": round(gross_profit, 10),
        "gross_loss": round(gross_loss, 10),
        "win_rate": round(len(wins)/len(values), 10) if values else None,
        "profit_factor": round(gross_profit/gross_loss, 10) if gross_loss else None,
        "expectancy": round(total/len(values), 10) if values else None,
        "average_win": round(gross_profit/len(wins), 10) if wins else None,
        "average_loss": round(sum(losses)/len(losses), 10) if losses else None,
        "payoff_ratio": round(
            (gross_profit/len(wins))/(-sum(losses)/len(losses)), 10
        ) if wins and losses else None,
        "max_drawdown_timestamp_batched": round(drawdown, 10),
        "longest_loss_streak": longest,
    }


def _group(rows: list[dict], field: str) -> dict[str, dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(field) or "UNKNOWN")].append(row)
    return {key: _performance(group) for key, group in sorted(grouped.items())}


def _split(rows: list[dict]) -> dict[str, list[dict]]:
    ordered = sorted(rows, key=lambda row: (int(row["entry_ts"]), int(row["id"])))
    train_end = math.floor(len(ordered)*0.60)
    validation_end = train_end + math.floor(len(ordered)*0.20)
    return {
        "train": ordered[:train_end],
        "validation": ordered[train_end:validation_end],
        "holdout": ordered[validation_end:],
    }


def build_report(db_path: Path, observation_end_ms: int | None = None) -> dict:
    uri = f"file:{db_path.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        total_rows = int(connection.execute(
            "SELECT COUNT(*) FROM lite_trades").fetchone()[0])
        query = """
            SELECT * FROM lite_trades
            WHERE entry_ts>=? AND strategy_name=?
              AND execution_verified=1 AND resolution_verified=1
              AND accounting_version=2
              AND status IN ('CLOSED_BOOK_EXIT','CLOSED_WIN','CLOSED_LOSS')
              AND pnl IS NOT NULL
            ORDER BY entry_ts,id
        """
        rows = [dict(row) for row in connection.execute(
            query, (BOUNDARY_MS, BASELINE_STRATEGY)).fetchall()]
        entries = [dict(row) for row in connection.execute(
            """SELECT * FROM lite_trades WHERE entry_ts>=? AND strategy_name=?
               AND execution_verified=1 ORDER BY entry_ts,id""",
            (BOUNDARY_MS, BASELINE_STRATEGY)).fetchall()]
        new_rows = int(connection.execute(
            "SELECT COUNT(*) FROM lite_trades WHERE strategy_name=?",
            (NEW_STRATEGY,)).fetchone()[0])
    finally:
        connection.close()

    if not rows:
        raise RuntimeError("verified forward baseline cohort is empty")
    observation_end = (int(observation_end_ms) if observation_end_ms is not None
                       else max(int(row["entry_ts"]) for row in entries))
    elapsed_hours = (
        observation_end-min(int(row["entry_ts"]) for row in entries))/3_600_000
    headline = _performance(rows)
    headline.update({
        "verified_entries": len(entries),
        "open_entries": sum(row["status"] not in TERMINAL for row in entries),
        "observation_hours": round(elapsed_hours, 10),
        "entries_per_hour": round(len(entries)/elapsed_hours, 10),
        "completed_per_hour": round(len(rows)/elapsed_hours, 10),
        "fees": round(sum(
            float(row.get("entry_fee") or 0)+float(row.get("exit_fee") or 0)
            for row in rows), 10),
        "gross_pre_fee_pnl": round(sum(
            float(row.get("gross_pnl") or 0) for row in rows), 10),
    })
    waits = [row for row in rows if row.get("entry_mode") == "WAIT_FOR_PULLBACK"]
    true_pullbacks = [row for row in waits
                      if float(row.get("expected_improvement") or 0) > 0]
    false_delays = [row for row in waits
                    if float(row.get("expected_improvement") or 0) <= 0]
    book_exits = [row for row in rows if row.get("resolution_source") == "book_exit"]
    official = [row for row in rows
                if row.get("resolution_source") == "official_outcome"]
    segments = {name: _performance(segment)
                for name, segment in _split(rows).items()}
    return {
        "artifact_version": 1,
        "method": {
            "read_only": True,
            "lookahead": False,
            "cohort_boundary_ms": BOUNDARY_MS,
            "cohort_strategy": BASELINE_STRATEGY,
            "cohort_requirements": [
                "execution_verified=1", "resolution_verified=1",
                "accounting_version=2", "terminal status", "pnl is not null",
            ],
            "chronological_split": "60/20/20 by entry timestamp and id",
            "observation_end_ms": observation_end,
            "new_candidate_replayable": False,
            "new_candidate_replay_blockers": [
                "simultaneous executable YES and NO books were not stored",
                "continuous CEX and book response paths were not stored",
                "provider move and lead-lag timestamps were not stored",
                "maker queue, trade-through, and executed volume were not stored",
                "official outcomes after historical book exits were not retained",
            ],
        },
        "snapshot": {
            "path": str(db_path.resolve()), "sha256": _sha256(db_path),
            "integrity": integrity, "rows": total_rows,
        },
        "forward_baseline": headline,
        "chronological_segments": segments,
        "by_asset": _group(rows, "asset"),
        "by_side": _group(rows, "side"),
        "by_entry_mode_label": _group(rows, "entry_mode"),
        "loss_attribution": {
            "book_exit": _performance(book_exits),
            "official_outcome": _performance(official),
            "true_pullback_target": _performance(true_pullbacks),
            "false_delayed_lock_labeled_pullback": _performance(false_delays),
            "fees_erased_gross_edge": headline["gross_pre_fee_pnl"] > 0
                                      and headline["net_pnl"] < 0,
        },
        "new_candidate": {
            "strategy_name": NEW_STRATEGY,
            "rows_in_snapshot": new_rows,
            "configuration_count": 1,
            "historical_pnl_claim": None,
            "profitability_status": "UNPROVEN_REQUIRES_FORWARD_SHADOW",
            "code_evidence_only": [
                "paired-book probability coherence",
                "bounded point-in-time CEX adjustment",
                "per-level five-share fee-net edge",
                "maker observation without fill assumption",
                "deterministic exit-now versus hold value",
            ],
        },
        "forward_acceptance_requirement": {
            "verified_terminal_trades": 300,
            "minimum_per_asset": 75,
            "minimum_per_side": 100,
            "minimum_calendar_days": 7,
            "chronological_split": "60/20/20 with at least 60 holdout trades",
            "validation_expectancy_gt": 0,
            "holdout_expectancy_gt": 0,
            "validation_profit_factor_gt": 1,
            "holdout_profit_factor_gt": 1,
            "max_drawdown_lte": round(
                headline["max_drawdown_timestamp_batched"]*0.75, 10),
            "minimum_entry_frequency_per_hour_unless_positive_evidence_justifies_less":
                round(headline["entries_per_hour"]*0.75, 10),
            "required_invariants": [
                "zero both-side conflicts", "zero duplicate entries",
                "zero assumed maker fills", "unresolved_final=0",
                "execution_verified=1", "resolution_verified=1",
            ],
        },
        "verdict": "READY_FOR_MORE_SHADOW",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--observation-end-ms", type=int)
    args = parser.parse_args()
    report = build_report(
        Path(args.db).resolve(), observation_end_ms=args.observation_end_ms)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8")
    print(json.dumps({
        "output": str(output),
        "baseline": report["forward_baseline"],
        "verdict": report["verdict"],
    }, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
