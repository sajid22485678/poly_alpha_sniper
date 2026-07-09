"""WS4: read-only replay of historical REJECTED_MIN_ORDER_SIZE_TOO_HIGH
predictions under the new fixed_min_shares sizing mode.

Answers: of the candidates the bot rejected because max_trade_usd couldn't
afford Polymarket's 5-share minimum, how many would fixed_min_shares have
allowed?

Approximation, clearly labeled: cash/exposure headroom is evaluated against
the bot's CURRENT equity/cash (from the live DB), not the bankroll state at
the exact historical moment of each reject (which isn't persisted
per-event). This is a reasonable estimate, not a byte-for-byte replay.

Read-only: opens the DB read-only (same DashboardData path the dashboard
uses), places/cancels nothing, writes nothing back to the bot's DB. Prints a
report and optionally writes a JSON summary to --out.

Usage:
    .venv\\Scripts\\python.exe tools\\replay_min_order_rejects.py
    .venv\\Scripts\\python.exe tools\\replay_min_order_rejects.py --out report.json
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse
import json

from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.core.contracts import PortfolioSnapshot, RejectReason, TradingMode
from poly_alpha_sniper.dashboard.db_reader import DashboardData, resolve_db_path
from poly_alpha_sniper.dashboard.metrics import equity_curve, min_order_sizing_rows
from poly_alpha_sniper.risk.position_sizer import compute_position_size


def _fake_snapshot(equity: float, cash: float, market_exposure_usd: float,
                   total_exposure_usd: float) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        equity_usd=equity, available_cash_usd=cash, realized_pnl_usd=0.0,
        unrealized_pnl_usd=0.0, realized_pnl_today_usd=0.0, open_positions=0,
        total_exposure_usd=total_exposure_usd,
        exposure_by_market={"__replay__": market_exposure_usd},
        consecutive_losses=0, positions_by_market={}, equity_ath_usd=equity,
        trades_today=0)


class _ReplayMarket:
    """Minimal stand-in for MarketInfo -- position_sizer only reads
    market_id and min_order_size_usd."""
    def __init__(self, market_id: str):
        self.market_id = market_id
        self.min_order_size_usd = 0.0


def replay(db_path: str | None = None, cfg=None) -> dict:
    cfg = cfg or load_config()
    data = DashboardData(db_path or resolve_db_path())
    preds = data.predictions()
    historical = min_order_sizing_rows(preds, cfg.risk.max_trade_usd, limit=10_000)

    fixed_cfg = cfg.model_copy(deep=True)
    fixed_cfg.risk.sizing_mode = "fixed_min_shares"
    fixed_cfg.risk.use_max_trade_usd = False

    curve = equity_curve(data.pnl_series(), cfg.risk.starting_bankroll_usd)
    equity = curve[-1]["equity"] if curve else cfg.risk.starting_bankroll_usd
    cash = equity  # approximation: no open exposure assumed at replay time

    allowed, blocked_cash, blocked_exposure, blocked_other = [], [], [], []
    for row in historical:
        ask = row["ask_price"]
        market = _ReplayMarket(row.get("market_title", "replay"))
        snap = _fake_snapshot(equity, cash, market_exposure_usd=0.0, total_exposure_usd=0.0)
        decision = compute_position_size(fixed_cfg, snap, market, TradingMode.SHADOW_LIVE,
                                         edge_after_slippage=float(row.get("edge") or 0.01),
                                         executable_price=ask)
        row_out = {**row, "proposed_usd": round(fixed_cfg.risk.fixed_order_shares * ask, 4)}
        if decision.approved:
            allowed.append(row_out)
        elif decision.reject_reason == RejectReason.INSUFFICIENT_CASH_FOR_5_SHARES:
            blocked_cash.append(row_out)
        elif decision.reject_reason == RejectReason.MAX_EXPOSURE:
            blocked_exposure.append(row_out)
        else:
            blocked_other.append({**row_out, "reason": decision.reject_reason})

    total = len(historical)
    summary = {
        "approximation_note": (
            "cash/exposure headroom evaluated against CURRENT equity/cash "
            f"(${equity:.2f}), not the exact historical bankroll at each reject's "
            "moment -- a reasonable estimate, not an exact replay."),
        "total_historical_min_order_rejects_analyzed": total,
        "would_be_allowed_count": len(allowed),
        "would_be_allowed_pct": round(100 * len(allowed) / total, 1) if total else 0.0,
        "still_blocked_insufficient_cash_count": len(blocked_cash),
        "still_blocked_max_exposure_count": len(blocked_exposure),
        "still_blocked_other_count": len(blocked_other),
        "fixed_order_shares": fixed_cfg.risk.fixed_order_shares,
        "current_equity_usd": equity,
        "expected_shadow_trade_frequency_impact": (
            f"+{len(allowed)} additional shadow-tradable signals out of {total} "
            "historically min-order-blocked candidates, if the same signal mix "
            "recurs -- not a guarantee, past reject volume is not future volume."),
        "allowed_sample": allowed[:10],
        "still_blocked_cash_sample": blocked_cash[:5],
        "still_blocked_exposure_sample": blocked_exposure[:5],
    }
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="", help="optional path to write JSON summary")
    args = ap.parse_args()
    summary = replay()
    print(json.dumps(summary, indent=2, default=str))
    if args.out:
        Path(args.out).write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        print(f"\nwritten to {args.out}")


if __name__ == "__main__":
    main()
