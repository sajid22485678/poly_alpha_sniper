"""Dynamic tier-based exposure cap: read-only replay of recent
REJECTED_MAX_EXPOSURE / REJECTED_MIN_ORDER_SIZE_TOO_HIGH predictions under
fixed_min_shares sizing WITH the new per-tier market exposure cap.

Answers: of the candidates recently blocked by the (flat 10%) market
exposure cap or the old max_trade_usd min-order check, how many would the
new tier-aware cap (A_PLUS/A=50%, B=10%, unknown=10%) now allow -- broken
down by tier?

Approximation, clearly labeled: cash/exposure headroom is evaluated against
the bot's CURRENT equity/cash (from the live DB) with zero other open
exposure assumed, not the bankroll state at the exact historical moment of
each reject. This is a reasonable estimate, not a byte-for-byte replay.

Read-only: opens the DB read-only, places/cancels nothing, writes nothing
back to the bot's DB.

Usage:
    .venv\\Scripts\\python.exe tools\\replay_tier_exposure_caps.py
    .venv\\Scripts\\python.exe tools\\replay_tier_exposure_caps.py --out report.json
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse
import json
from collections import Counter

from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.core.contracts import PortfolioSnapshot, RejectReason, Tier, TradingMode
from poly_alpha_sniper.dashboard.db_reader import DashboardData, resolve_db_path
from poly_alpha_sniper.dashboard.metrics import equity_curve
from poly_alpha_sniper.risk.position_sizer import compute_position_size

_TIER_MAP = {t.value: t for t in Tier}


def _fake_snapshot(equity: float, cash: float) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        equity_usd=equity, available_cash_usd=cash, realized_pnl_usd=0.0,
        unrealized_pnl_usd=0.0, realized_pnl_today_usd=0.0, open_positions=0,
        total_exposure_usd=0.0, exposure_by_market={},
        consecutive_losses=0, positions_by_market={}, equity_ath_usd=equity,
        trades_today=0)


class _ReplayMarket:
    def __init__(self, market_id: str):
        self.market_id = market_id
        self.min_order_size_usd = 0.0


def replay(db_path: str | None = None, cfg=None) -> dict:
    cfg = cfg or load_config()
    data = DashboardData(db_path or resolve_db_path())
    preds = data.predictions(2000)

    candidates = [r for r in preds if (r.get("reject_reason") or "").find("MAX_EXPOSURE") >= 0
                 or (r.get("reject_reason") or "").find("MIN_ORDER") >= 0]

    fixed_cfg = cfg.model_copy(deep=True)
    fixed_cfg.risk.sizing_mode = "fixed_min_shares"
    fixed_cfg.risk.use_max_trade_usd = False

    curve = equity_curve(data.pnl_series(), cfg.risk.starting_bankroll_usd)
    equity = curve[-1]["equity"] if curve else cfg.risk.starting_bankroll_usd
    cash = equity

    by_tier = {"A_PLUS": Counter(), "A": Counter(), "B": Counter(), "other": Counter()}
    samples = {"A_PLUS": [], "A": [], "B": [], "other": []}

    for row in candidates:
        ask = row.get("polymarket_price") or 0.0
        if ask <= 0:
            continue
        tier_str = row.get("tier") or ""
        tier = _TIER_MAP.get(tier_str)
        bucket = tier_str if tier_str in ("A_PLUS", "A", "B") else "other"
        market = _ReplayMarket(row.get("market_title", "replay"))
        snap = _fake_snapshot(equity, cash)
        decision = compute_position_size(
            fixed_cfg, snap, market, TradingMode.SHADOW_LIVE,
            edge_after_slippage=float(row.get("edge") or 0.01),
            executable_price=ask, tier=tier)
        row_out = {"ts_ms": row.get("ts_ms"), "asset": row.get("asset"),
                  "market_title": row.get("market_title"), "tier": tier_str,
                  "ask_price": round(ask, 4),
                  "proposed_usd": round(fixed_cfg.risk.fixed_order_shares * ask, 4),
                  "original_reject_reason": row.get("reject_reason")}
        if decision.approved:
            by_tier[bucket]["allowed"] += 1
        elif decision.reject_reason == RejectReason.INSUFFICIENT_CASH_FOR_5_SHARES:
            by_tier[bucket]["blocked_insufficient_cash"] += 1
        elif decision.reject_reason == RejectReason.MAX_EXPOSURE:
            by_tier[bucket]["blocked_exposure"] += 1
        else:
            by_tier[bucket]["blocked_other"] += 1
            row_out["blocked_reason"] = decision.reject_reason
        if len(samples[bucket]) < 5:
            samples[bucket].append(row_out)

    total = len(candidates)
    total_allowed = sum(c["allowed"] for c in by_tier.values())
    summary = {
        "approximation_note": (
            "cash/exposure headroom evaluated against CURRENT equity/cash "
            f"(${equity:.2f}) with zero other open exposure assumed, not the "
            "exact historical bankroll at each reject's moment."),
        "total_candidates_analyzed": total,
        "would_be_allowed_total": total_allowed,
        "by_tier": {tier: dict(counts) for tier, counts in by_tier.items()},
        "current_equity_usd": equity,
        "expected_shadow_trade_frequency_impact": (
            f"+{total_allowed} additional shadow-tradable signals out of {total} "
            "historically exposure/min-order-blocked candidates, if the same "
            "signal mix recurs -- not a guarantee, past reject volume is not "
            "future volume."),
        "samples_by_tier": samples,
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
