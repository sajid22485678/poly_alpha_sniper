"""Offline, read-only counterfactual PnL replay over the authoritative V4 cohort.

This script reproduces ``lite_frequency_v4.metrics._performance`` arithmetic
over the authoritative verified-terminal trade set and evaluates deterministic
what-if exclusion scenarios.  It writes nothing to the database and changes no
runtime state; its only output is a JSON report plus stdout.

Anti-overfit discipline: each scenario is evaluated both on the full set and
via leave-one-asset-out (LOAO) cross-validation so a rule that only looks good
because of one asset's contribution is exposed.  A scenario is "robust" only
if its fee-net expectancy remains positive and PF > 1 across the LOAO folds,
not merely on the full set.

Usage::

    python -m scripts.counterfactual_replay_v4
    python -m scripts.counterfactual_replay_v4 --cohort dynamic_universe_phase2_130usd_successor

Intentionally does NOT import the engine or store writer; it opens the database
read-only and runs the same attribution join the metrics module uses.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Iterable


AUTHORITATIVE_COHORT = "dynamic_universe_phase2_130usd_successor"
DEFAULT_DB = Path(__file__).resolve().parents[1] / "data" / "poly_alpha_frequency_v4.db"


def _load_terminal_trades(db_path: Path, cohort: str) -> list[dict[str, Any]]:
    """Return the authoritative verified-terminal trade rows.

    Mirrors the attribution join in ``metrics.performance_metrics``:
    pnl_records -> entries -> candidates (dominant_model) -> asset_windows ->
    runtime_sessions (cohort).  Only ``verified=1`` rows are authoritative.
    """
    connection = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT p.entry_id, p.terminal_ts_ms, p.net_pnl, p.total_fees,
               p.gross_pnl, p.verified, p.outcome, p.exit_source,
               e.entry_mode, e.selected_net_edge, e.executable_vwap,
               e.outcome_side, e.status, e.session_id,
               c.dominant_model, c.candidate_id,
               w.asset
        FROM pnl_records p
        JOIN entries e          ON e.entry_id   = p.entry_id
        JOIN candidates c       ON c.candidate_id = e.candidate_id
        JOIN asset_windows w    ON w.window_id    = e.window_id
        JOIN runtime_sessions rs ON rs.session_id  = e.session_id
        WHERE rs.cohort = ?
          AND p.verified = 1
        ORDER BY p.terminal_ts_ms, p.entry_id
        """,
        (cohort,),
    ).fetchall()
    connection.close()
    return [dict(row) for row in rows]


def _performance(trades: list[dict[str, Any]]) -> dict[str, Any]:
    """Reproduce ``metrics._performance`` arithmetic on a trade subset."""
    if not trades:
        return {
            "count": 0, "net_pnl": 0.0, "fees": 0.0,
            "gross_profit": 0.0, "gross_loss": 0.0, "profit_factor": 0.0,
            "expectancy": 0.0, "winrate": 0.0,
            "average_win": 0.0, "average_loss": 0.0, "max_drawdown": 0.0,
            "wins": 0, "losses": 0,
        }
    pnls = [float(t["net_pnl"]) for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_profit = sum(wins)
    gross_loss = sum(losses)  # negative
    fees = sum(float(t["total_fees"]) for t in trades)
    profit_factor = (
        round(gross_profit / abs(gross_loss), 6)
        if gross_loss != 0 else float("inf") if gross_profit > 0 else 0.0)
    # Max drawdown over the cumulative realized-PnL walk.
    peak = 0.0
    cumulative = 0.0
    max_dd = 0.0
    for pnl in pnls:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_dd = min(max_dd, cumulative - peak)
    return {
        "count": len(trades),
        "net_pnl": round(sum(pnls), 6),
        "fees": round(fees, 6),
        "gross_profit": round(gross_profit, 6),
        "gross_loss": round(gross_loss, 6),
        "profit_factor": profit_factor,
        "expectancy": round(sum(pnls) / len(pnls), 6),
        "winrate": round(len(wins) / len(pnls), 6),
        "average_win": round(sum(wins) / len(wins), 6) if wins else 0.0,
        "average_loss": round(sum(losses) / len(losses), 6) if losses else 0.0,
        "max_drawdown": round(abs(max_dd), 6),
        "wins": len(wins),
        "losses": len(losses),
    }


def _equity_curve(trades: list[dict[str, Any]], starting_equity: float) -> list[float]:
    equity = starting_equity
    curve = [round(equity, 6)]
    for trade in sorted(trades, key=lambda t: t["terminal_ts_ms"]):
        equity += float(trade["net_pnl"])
        curve.append(round(equity, 6))
    return curve


def _by_field(trades: list[dict[str, Any]], field: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        key = str(trade.get(field) or "UNKNOWN")
        grouped.setdefault(key, []).append(trade)
    return {key: _performance(group) for key, group in sorted(grouped.items())}


# --- Scenario predicates --------------------------------------------------


def _baseline(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return list(trades)


def _exclude_model(model: str):
    def predicate(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [t for t in trades if t.get("dominant_model") != model]
    predicate.__name__ = f"exclude_{model}"
    return predicate


def _exclude_entry_mode_below_edge(mode: str, edge_floor: float):
    def predicate(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            t for t in trades
            if not (t.get("entry_mode") == mode
                    and float(t.get("selected_net_edge") or 0) < edge_floor)
        ]
    predicate.__name__ = f"exclude_{mode}_below_edge_{edge_floor}"
    return predicate


def _higher_edge_threshold(edge_floor: float):
    def predicate(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [t for t in trades
                if float(t.get("selected_net_edge") or 0) >= edge_floor]
    predicate.__name__ = f"edge_ge_{edge_floor}"
    return predicate


def _exclude_assets(assets: Iterable[str]):
    asset_set = set(assets)
    def predicate(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [t for t in trades if t.get("asset") not in asset_set]
    predicate.__name__ = "exclude_assets_" + "_".join(sorted(asset_set))
    return predicate


def _maker_first(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Exclude CROSS_SPREAD entries, keep only MAKER_TO_CROSS.

    Models the maker-first preference: only entries that observed the maker
    book before crossing are admitted.
    """
    return [t for t in trades if t.get("entry_mode") == "MAKER_TO_CROSS"]


SCENARIOS = [
    ("baseline_current", _baseline),
    ("exclude_window_open_displacement", _exclude_model("window_open_displacement")),
    ("exclude_cex_lead_lag_impulse", _exclude_model("cex_lead_lag_impulse")),
    ("maker_to_cross_only", _maker_first),
    ("exclude_cross_spread_below_edge_0.030",
     _exclude_entry_mode_below_edge("CROSS_SPREAD", 0.030)),
    ("edge_ge_0.030", _higher_edge_threshold(0.030)),
    ("exclude_hype_doge", _exclude_assets(["HYPE", "DOGE"])),
]


def _evaluate_scenario(
    name: str, predicate, trades: list[dict[str, Any]], starting_equity: float,
) -> dict[str, Any]:
    kept = predicate(trades)
    excluded = len(trades) - len(kept)
    perf = _performance(kept)
    curve = _equity_curve(kept, starting_equity)
    return {
        "scenario": name,
        "predicate": predicate.__name__,
        "kept": len(kept),
        "excluded": excluded,
        "performance": perf,
        "ending_equity": curve[-1] if curve else starting_equity,
        "min_equity": min(curve) if curve else starting_equity,
        "by_model": _by_field(kept, "dominant_model"),
        "by_entry_mode": _by_field(kept, "entry_mode"),
    }


def _leave_one_asset_out(
    predicate, trades: list[dict[str, Any]], starting_equity: float,
) -> dict[str, Any]:
    """LOAO robustness: drop each asset in turn, apply the predicate, report."""
    assets = sorted({str(t.get("asset")) for t in trades if t.get("asset")})
    folds = []
    for asset in assets:
        subset = [t for t in trades if t.get("asset") != asset]
        kept = predicate(subset)
        perf = _performance(kept)
        folds.append({
            "held_out_asset": asset,
            "kept": len(kept),
            "net_pnl": perf["net_pnl"],
            "profit_factor": perf["profit_factor"],
            "expectancy": perf["expectancy"],
            "max_drawdown": perf["max_drawdown"],
        })
    # A scenario is LOAO-robust if expectancy > 0 and PF > 1 in every fold.
    robust = all(
        f["expectancy"] > 0 and f["profit_factor"] > 1 for f in folds
    ) if folds else False
    return {"robust": robust, "folds": folds}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--cohort", default=AUTHORITATIVE_COHORT)
    parser.add_argument(
        "--starting-equity", type=float, default=None,
        help="Override starting equity; default reads the cohorts table.")
    parser.add_argument("--json", type=Path, default=None,
                        help="Optional path to write the full report as JSON.")
    args = parser.parse_args(argv)

    if not args.db.exists():
        print(f"ERROR: database not found: {args.db}", file=sys.stderr)
        return 2

    trades = _load_terminal_trades(args.db, args.cohort)
    if not trades:
        print(f"ERROR: no verified terminal trades for cohort {args.cohort}",
              file=sys.stderr)
        return 2

    # Starting equity: cohorts table unless overridden.
    if args.starting_equity is not None:
        starting_equity = float(args.starting_equity)
    else:
        conn = sqlite3.connect(f"file:{args.db.as_posix()}?mode=ro", uri=True)
        row = conn.execute(
            "SELECT starting_equity_usd FROM cohorts WHERE cohort=?",
            (args.cohort,)).fetchone()
        conn.close()
        starting_equity = float(row[0]) if row and row[0] else 130.0

    print(f"=== Counterfactual replay: cohort={args.cohort} ===")
    print(f"verified terminal trades: {len(trades)}")
    print(f"starting equity: {starting_equity}")
    print()
    print("--- baseline breakdowns ---")
    print("by_model:")
    for model, perf in _by_field(trades, "dominant_model").items():
        print(f"  {model}: n={perf['count']} net={perf['net_pnl']} "
              f"PF={perf['profit_factor']} exp={perf['expectancy']} "
              f"wr={perf['winrate']} avgW={perf['average_win']} "
              f"avgL={perf['average_loss']}")
    print("by_entry_mode:")
    for mode, perf in _by_field(trades, "entry_mode").items():
        print(f"  {mode}: n={perf['count']} net={perf['net_pnl']} "
              f"PF={perf['profit_factor']} exp={perf['expectancy']}")
    print("by_asset:")
    for asset, perf in _by_field(trades, "asset").items():
        print(f"  {asset}: n={perf['count']} net={perf['net_pnl']} "
              f"PF={perf['profit_factor']} exp={perf['expectancy']}")
    print()

    results = []
    print("--- scenarios ---")
    print(f"{'scenario':<42} {'kept':>5} {'excl':>5} {'net_pnl':>9} "
          f"{'PF':>7} {'exp':>7} {'wr':>5} {'maxdd':>8} {'end_eq':>9}")
    for name, predicate in SCENARIOS:
        result = _evaluate_scenario(name, predicate, trades, starting_equity)
        perf = result["performance"]
        print(f"{name:<42} {result['kept']:>5} {result['excluded']:>5} "
              f"{perf['net_pnl']:>9.3f} {perf['profit_factor']:>7.3f} "
              f"{perf['expectancy']:>7.3f} {perf['winrate']:>5.2f} "
              f"{perf['max_drawdown']:>8.2f} {result['ending_equity']:>9.3f}")
        results.append(result)
    print()

    print("--- leave-one-asset-out robustness ---")
    print(f"{'scenario':<42} {'robust':>7}  folds (asset: net / PF / exp)")
    loao_results = {}
    for name, predicate in SCENARIOS:
        loao = _leave_one_asset_out(predicate, trades, starting_equity)
        loao_results[name] = loao
        fold_summary = "  ".join(
            f"{f['held_out_asset']}:{f['net_pnl']:.1f}/{f['profit_factor']:.2f}"
            f"/{f['expectancy']:+.2f}" for f in loao["folds"])
        print(f"{name:<42} {str(loao['robust']):>7}  {fold_summary}")
    print()

    report = {
        "cohort": args.cohort,
        "starting_equity": starting_equity,
        "verified_terminal_trades": len(trades),
        "baseline_by_model": _by_field(trades, "dominant_model"),
        "baseline_by_entry_mode": _by_field(trades, "entry_mode"),
        "baseline_by_asset": _by_field(trades, "asset"),
        "scenarios": results,
        "leave_one_asset_out": loao_results,
    }
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(report, indent=2, default=str),
                             encoding="utf-8")
        print(f"full report written to: {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
