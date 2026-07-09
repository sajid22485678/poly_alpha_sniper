"""Mission H: shadow-only compounding simulator.

Simulates a compounded equity curve from COMPLETED SHADOW trades only. It is a
read-only analytic over the `exits` table -- it never touches a real balance,
never places or cancels an order, never mutates portfolio state, and contains
no Martingale / averaging-down / loss-scaling logic (each trade compounds a
fixed fraction of the then-current simulated equity; size never increases
after a loss).

Two curves are kept strictly separate:
  - "standard": completed accepted shadow trades (the main curve).
  - "research": reserved for watchlist/relaxed-path candidates IF they ever
    gain a resolved outcome; today that is always empty and clearly labeled.

Below MIN_SAMPLE completed trades the verdict is INSUFFICIENT SHADOW TRADE
SAMPLE -- no risk-of-ruin or safety claim is made on a tiny sample.
"""
from __future__ import annotations

import statistics

MIN_SAMPLE = 30
MIN_SAMPLE_FOR_ROR = 50
FIXED_FRACTION = 0.10  # simulate risking a fixed 10% of current equity per trade


def _curve(exit_rows: list[dict], starting: float, fraction: float) -> list[dict]:
    """Compound a fixed fraction of current equity per trade. pnl_usd from the
    exit row is treated as the realized return on a unit stake and scaled to
    the fixed-fraction stake -- never scaled UP after a loss."""
    equity = starting
    points = [{"trade": 0, "equity": round(equity, 4)}]
    for i, r in enumerate(sorted(exit_rows, key=lambda x: x.get("ts_ms") or 0), start=1):
        pnl = float(r.get("pnl_usd") or 0.0)
        cost = abs(float(r.get("price") or 0.0) * float(r.get("shares") or 0.0)) or 1.0
        roi = pnl / cost  # realized return per $1 staked
        stake = equity * fraction  # fixed fraction -- constant rule, no revenge sizing
        equity = max(0.0, equity + stake * roi)
        points.append({"trade": i, "equity": round(equity, 4)})
    return points


def _drawdown(curve: list[dict]) -> dict:
    peak = curve[0]["equity"] if curve else 0.0
    dd_usd = dd_pct = 0.0
    for p in curve:
        peak = max(peak, p["equity"])
        draw = peak - p["equity"]
        dd_usd = max(dd_usd, draw)
        if peak > 0:
            dd_pct = max(dd_pct, draw / peak * 100)
    return {"usd": round(dd_usd, 4), "pct": round(dd_pct, 2)}


def _max_loss_streak(exit_rows: list[dict]) -> int:
    streak = worst = 0
    for r in sorted(exit_rows, key=lambda x: x.get("ts_ms") or 0):
        if float(r.get("pnl_usd") or 0) < 0:
            streak += 1
            worst = max(worst, streak)
        else:
            streak = 0
    return worst


def _calibration(prediction_rows: list[dict]) -> list[dict]:
    """Probability-bucket calibration for resolved predictions, if any."""
    buckets = {(lo, lo + 0.1): {"n": 0, "wins": 0} for lo in
               [round(x * 0.1, 1) for x in range(10)]}
    for p in prediction_rows:
        outcome = p.get("resolved_outcome")
        if outcome in (None, "", "UNRESOLVED"):
            continue
        prob = p.get("fair_probability")
        if prob is None:
            continue
        prob = float(prob)
        for (lo, hi), agg in buckets.items():
            if lo <= prob < hi or (hi >= 1.0 and prob == 1.0):
                agg["n"] += 1
                if str(outcome).upper() in ("UP", "WIN", "YES", "1", "TRUE"):
                    agg["wins"] += 1
                break
    out = []
    for (lo, hi), agg in buckets.items():
        if agg["n"]:
            out.append({"bucket": f"{lo:.1f}-{hi:.1f}", "n": agg["n"],
                        "empirical_win_rate": round(agg["wins"] / agg["n"], 3)})
    return out


def simulate(exit_rows: list[dict], prediction_rows: list[dict], starting: float,
             research_exit_rows: list[dict] | None = None) -> dict:
    """Main entry. Returns the standard shadow compounding result plus a
    clearly-separated research curve (empty unless research outcomes exist)."""
    n = len(exit_rows)
    pnls = [float(r.get("pnl_usd") or 0.0) for r in exit_rows]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    pf = round(gross_win / gross_loss, 3) if gross_loss > 1e-9 else (2.0 if gross_win > 0 else 0.0)
    curve = _curve(exit_rows, starting, FIXED_FRACTION)
    dd = _drawdown(curve)

    research_rows = research_exit_rows or []
    research = {
        "curve": _curve(research_rows, starting, FIXED_FRACTION) if research_rows else [],
        "trades": len(research_rows),
        "note": ("research/watchlist curve is separate from the standard curve; "
                 "empty until relaxed-path candidates gain resolved outcomes"),
    }

    base = {
        "starting_equity": starting,
        "standard_shadow_trades": n,
        "min_sample": MIN_SAMPLE,
        "no_martingale": True,
        "no_averaging_down": True,
        "no_loss_scaling": True,
        "touches_real_balance": False,
        "touches_order_path": False,
        "fixed_fraction": FIXED_FRACTION,
        "research_curve": research,
    }
    if n < MIN_SAMPLE:
        base.update({
            "verdict": "INSUFFICIENT SHADOW TRADE SAMPLE",
            "detail": f"{n}/{MIN_SAMPLE} completed standard shadow trades",
            "simulated_equity": curve[-1]["equity"] if curve else starting,
            "partial_curve": curve,
        })
        return base

    base.update({
        "verdict": "COMPUTED",
        "simulated_equity": curve[-1]["equity"],
        "compounded_curve": curve,
        "winrate": round(len(wins) / n, 4) if n else 0.0,
        "profit_factor": pf,
        "expectancy_usd": round(sum(pnls) / n, 4) if n else 0.0,
        "max_drawdown_usd": dd["usd"],
        "max_drawdown_pct": dd["pct"],
        "max_loss_streak": _max_loss_streak(exit_rows),
        "pnl_stdev": round(statistics.pstdev(pnls), 4) if len(pnls) > 1 else 0.0,
        "calibration_by_bucket": _calibration(prediction_rows),
        "risk_of_ruin_estimate": (None if n < MIN_SAMPLE_FOR_ROR else _risk_of_ruin(wins, losses, n)),
        "compounding_safe": (pf >= 1.3 and dd["pct"] <= 25.0 and n >= MIN_SAMPLE_FOR_ROR),
    })
    return base


def _risk_of_ruin(wins: list[float], losses: list[float], n: int) -> float:
    """Crude Gambler's-ruin style estimate; only meaningful with enough
    trades. Never presented as precise."""
    p_win = len(wins) / n if n else 0.0
    avg_win = statistics.mean(wins) if wins else 0.0
    avg_loss = abs(statistics.mean(losses)) if losses else 1.0
    if avg_loss <= 1e-9 or p_win <= 0:
        return 1.0 if p_win <= 0 else 0.0
    edge = p_win * avg_win - (1 - p_win) * avg_loss
    if edge <= 0:
        return 1.0
    # very rough: lower edge / higher loss size -> higher ruin risk
    return round(max(0.0, min(1.0, (1 - p_win) ** (avg_win / avg_loss + 1))), 4)
