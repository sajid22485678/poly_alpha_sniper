"""Pure metric computations for the dashboard (NO streamlit imports here)."""
from __future__ import annotations

from collections import Counter, defaultdict


def _pnls(exit_rows: list[dict]) -> list[float]:
    return [float(r.get("pnl_usd") or 0) for r in exit_rows]


def equity_curve(pnl_rows: list[dict], starting: float = 10.0) -> list[dict]:
    points = []
    equity = starting
    for r in sorted(pnl_rows, key=lambda x: x.get("ts_ms") or 0):
        if r.get("equity_usd") is not None:
            equity = float(r["equity_usd"])
        else:
            equity += float(r.get("realized_pnl_usd") or 0)
        points.append({"ts_ms": r.get("ts_ms") or 0, "equity": round(equity, 4)})
    return points


def compounded_roi(pnl_rows: list[dict], starting: float = 10.0) -> float:
    curve = equity_curve(pnl_rows, starting)
    if not curve or starting <= 0:
        return 0.0
    return round((curve[-1]["equity"] - starting) / starting * 100.0, 2)


def winrate(exit_rows: list[dict]) -> float:
    pnls = _pnls(exit_rows)
    if not pnls:
        return 0.0
    return round(sum(1 for p in pnls if p > 0) / len(pnls), 4)


def profit_factor(exit_rows: list[dict]) -> float:
    pnls = _pnls(exit_rows)
    gross_win = sum(p for p in pnls if p > 0)
    gross_loss = abs(sum(p for p in pnls if p <= 0))
    if gross_loss <= 1e-9:
        return 2.0 if gross_win > 0 else 0.0
    return round(gross_win / gross_loss, 3)


def expectancy(exit_rows: list[dict]) -> float:
    pnls = _pnls(exit_rows)
    return round(sum(pnls) / len(pnls), 4) if pnls else 0.0


def max_drawdown(pnl_rows: list[dict], starting: float = 10.0) -> dict:
    curve = equity_curve(pnl_rows, starting)
    peak = starting
    dd_usd = 0.0
    dd_pct = 0.0
    for p in curve:
        peak = max(peak, p["equity"])
        draw = peak - p["equity"]
        dd_usd = max(dd_usd, draw)
        if peak > 0:
            dd_pct = max(dd_pct, draw / peak * 100)
    return {"usd": round(dd_usd, 4), "pct": round(dd_pct, 2)}


def per_hour(rows: list[dict], span_hours: float = 24.0) -> float:
    if not rows or span_hours <= 0:
        return 0.0
    ts = [r.get("ts_ms") or 0 for r in rows if r.get("ts_ms")]
    if len(ts) < 2:
        return round(len(rows) / span_hours, 2)
    span = max((max(ts) - min(ts)) / 3_600_000, 1e-6)
    return round(len(rows) / span, 2)


def avg_edge(prediction_rows: list[dict]) -> float:
    edges = [float(r.get("edge_after_slippage") or r.get("edge") or 0)
             for r in prediction_rows]
    return round(sum(edges) / len(edges), 4) if edges else 0.0


def realized_edge(exit_rows: list[dict]) -> float:
    """Realized pnl per $1 of stake (approx via price*shares cost)."""
    total_pnl, total_cost = 0.0, 0.0
    for r in exit_rows:
        total_pnl += float(r.get("pnl_usd") or 0)
        total_cost += abs(float(r.get("price") or 0) * float(r.get("shares") or 0))
    return round(total_pnl / total_cost, 4) if total_cost > 0 else 0.0


def fill_quality_avg(fq_rows: list[dict]) -> float:
    scores = [float(r.get("score") or 0) for r in fq_rows]
    return round(sum(scores) / len(scores), 1) if scores else 0.0


def latency_percentiles(rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for r in rows:
        stage = str(r.get("stage") or "?")
        out[stage] = {"p50": r.get("p50", 0), "p95": r.get("p95", 0),
                      "p99": r.get("p99", 0), "n": r.get("n", 0)}
    return out


def tier_distribution(prediction_rows: list[dict]) -> dict[str, int]:
    return dict(Counter(str(r.get("tier") or "?") for r in prediction_rows))


def pnl_by_tier(exit_rows: list[dict], orders_rows: list[dict] | None = None) -> dict[str, float]:
    """Uses exits joined with tier when present, else '?' bucket."""
    out: dict[str, float] = defaultdict(float)
    for r in exit_rows:
        out[str(r.get("tier") or "?")] += float(r.get("pnl_usd") or 0)
    return {k: round(v, 4) for k, v in out.items()}


def b_allowed_vs_skipped(gate_rows: list[dict]) -> dict:
    b_rows = [r for r in gate_rows if str(r.get("tier")) == "B"]
    approved = sum(1 for r in b_rows if str(r.get("decision")) == "APPROVE")
    shadow = sum(1 for r in b_rows if str(r.get("decision")) == "SHADOW_ONLY")
    rejected = sum(1 for r in b_rows if str(r.get("decision")) == "REJECT")
    return {"total_b": len(b_rows), "approved": approved,
            "shadow_only": shadow, "rejected": rejected}


def reject_breakdown(prediction_rows: list[dict], top: int = 8) -> dict[str, int]:
    counts = Counter(str(r.get("reject_reason") or "") for r in prediction_rows
                     if r.get("decision") == "REJECT" and r.get("reject_reason"))
    return dict(counts.most_common(top))


def min_order_sizing_rows(prediction_rows: list[dict], max_trade_usd: float,
                          default_min_shares: float = 5.0, limit: int = 20) -> list[dict]:
    """Per-signal breakdown for REJECTED_MIN_ORDER_SIZE_TOO_HIGH rows: why the
    order was actually infeasible (Polymarket's share minimum vs. this
    bankroll's max_trade_usd), not "edge/confidence must improve" -- these
    signals already had edge/confidence, that's why they reached this gate.
    Uses only columns already written to `predictions` (no schema change);
    ask is approximated by polymarket_price, which is the market price read
    at signal time."""
    rows = []
    for r in prediction_rows:
        if "MIN_ORDER" not in str(r.get("reject_reason") or ""):
            continue
        ask = float(r.get("polymarket_price") or 0)
        if ask <= 0:
            continue
        min_required_usd = round(default_min_shares * ask, 4)
        shortfall = round(max(0.0, min_required_usd - max_trade_usd), 4)
        rows.append({
            "ts_ms": r.get("ts_ms"), "asset": r.get("asset"),
            "market_title": r.get("market_title"), "tier": r.get("tier"),
            "edge": r.get("edge_after_slippage") or r.get("edge"),
            "confidence": r.get("confidence"),
            "ask_price": round(ask, 4), "min_shares": default_min_shares,
            "min_required_usd": min_required_usd,
            "configured_max_trade_usd": max_trade_usd,
            "shortfall_usd": shortfall,
        })
    rows.sort(key=lambda r: r.get("ts_ms") or 0, reverse=True)
    return rows[:limit]


def frequency_vs_target(exit_rows: list[dict], target_per_hour: float,
                        max_per_hour: float) -> dict:
    actual = per_hour(exit_rows)
    warning = ""
    if actual > max_per_hour:
        warning = "OVERTRADING"
    elif actual < target_per_hour * 0.3 and exit_rows:
        warning = "UNDERTRADING"
    return {"actual_per_hour": actual, "target_per_hour": target_per_hour,
            "max_per_hour": max_per_hour, "warning": warning}


def open_positions_split(position_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    yes = [r for r in position_rows if str(r.get("outcome")) == "YES"]
    no = [r for r in position_rows if str(r.get("outcome")) == "NO"]
    return yes, no


def aggression_timeline(prediction_rows: list[dict]) -> list[dict]:
    out = []
    last = None
    for r in sorted(prediction_rows, key=lambda x: x.get("ts_ms") or 0):
        mode = str(r.get("aggression_mode") or "")
        if mode and mode != last:
            out.append({"ts_ms": r.get("ts_ms"), "mode": mode})
            last = mode
    return out
