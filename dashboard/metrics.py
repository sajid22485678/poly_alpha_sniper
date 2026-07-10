"""Pure metric computations for the dashboard (NO streamlit imports here)."""
from __future__ import annotations

import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Optional


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


# ---------------------------------------------------------------------------
# Premium dashboard / agent-export additions
# ---------------------------------------------------------------------------

def today_pnl(pnl_rows: list[dict], now_ms: Optional[int] = None) -> float:
    """Realized PnL delta for the current UTC calendar day only. now_ms is
    injectable for tests; real callers omit it and get wall-clock time."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    day_start = int(datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
                    .replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    todays = [float(r.get("realized_pnl_usd") or 0) for r in pnl_rows
             if (r.get("ts_ms") or 0) >= day_start]
    return round(sum(todays), 4)


def all_time_pnl(pnl_rows: list[dict]) -> float:
    return round(sum(float(r.get("realized_pnl_usd") or 0) for r in pnl_rows), 4)


# Canonical 7-bucket taxonomy the premium dashboard reports against. Maps both
# pipeline stages: shadow_diagnostics.reason (pre-signal) and
# predictions.reject_reason (post-signal, from the risk/order-validator gates).
_REJECT_BUCKET_RULES: list[tuple[str, str]] = [
    ("no_shock", "no_shock"),
    ("no_fresh_cex_price", "no_fresh_cex_price"),
    ("stale_cex", "no_fresh_cex_price"),
    ("stale_book", "stale_book"),
    ("book_fresh", "stale_book"),          # hard-reject check name for a stale/missing book
    ("orderbook", "stale_book"),
    ("spread", "spread"),
    ("max_exposure", "max_exposure"),
    ("max_open_positions", "max_exposure"),
    ("min_order", "min_order"),
    ("edge", "edge"),                      # keep last: "edge" also substrings some other reasons
]
CANONICAL_REJECT_BUCKETS = ("no_shock", "no_fresh_cex_price", "stale_book",
                            "spread", "edge", "max_exposure", "min_order")


def _canonical_bucket(raw_reason: str) -> str:
    low = raw_reason.lower()
    for needle, bucket in _REJECT_BUCKET_RULES:
        if needle in low:
            return bucket
    return "other"


def unified_reject_breakdown(diag_rows: list[dict], prediction_rows: list[dict]) -> dict:
    """Merges the two rejection stages (shadow_diagnostics pre-signal blocks +
    predictions post-signal rejects) into the 7 canonical buckets the premium
    dashboard displays, plus an honest 'other' bucket for anything that
    doesn't map cleanly -- counts are never silently dropped. Also returns the
    raw reason strings per bucket for drill-down."""
    counts: dict[str, int] = {b: 0 for b in CANONICAL_REJECT_BUCKETS}
    counts["other"] = 0
    raw_by_bucket: dict[str, Counter] = defaultdict(Counter)

    for r in diag_rows:
        reason = str(r.get("reason") or "")
        if not reason:
            continue
        bucket = _canonical_bucket(reason)
        counts[bucket] = counts.get(bucket, 0) + 1
        raw_by_bucket[bucket][reason] += 1

    for r in prediction_rows:
        if r.get("decision") != "REJECT":
            continue
        reason = str(r.get("reject_reason") or "")
        if not reason:
            continue
        bucket = _canonical_bucket(reason)
        counts[bucket] = counts.get(bucket, 0) + 1
        raw_by_bucket[bucket][reason] += 1

    return {
        "buckets": counts,
        "raw_by_bucket": {k: dict(v) for k, v in raw_by_bucket.items()},
        "total": sum(counts.values()),
    }


# Ordered pipeline stages for the entry-rarity gate waterfall (Task C).
# First matching needle wins, in pipeline order, so a candidate is attributed
# to the EARLIEST gate it failed -- matching how _evaluate_market actually
# short-circuits. "accepted" is derived separately from prediction decisions.
_WATERFALL_STAGES: list[tuple[str, tuple[str, ...]]] = [
    ("no_fresh_cex_price", ("no_fresh_cex_price", "stale_cex")),
    ("price_window_not_ready", ("price_window_not_ready",)),
    ("no_shock", ("no_shock",)),
    ("no_candidate_market", ("time_to_expiry", "no_candidate")),
    ("missing_oracle_anchor", ("missing_oracle_anchor", "oracle_anchor_stale",
                               "price_to_beat_mismatch", "oracle_source_unknown",
                               "oracle_cex_basis_unstable")),
    ("book_fetch_failed", ("book_fetch_failed",)),
    ("stale_book", ("stale_book", "book_fresh", "orderbook", "no_best_bid_ask")),
    ("edge_too_small", ("edge", "incomplete_trade_packet")),
    ("ev_too_low", ("ev_too_low", "time_to_close_risk", "book_too_thin", "model_uncalibrated")),
    ("spread", ("spread", "slippage")),
    ("max_exposure", ("max_exposure", "max_open_positions")),
    ("insufficient_cash", ("insufficient_cash", "min_order")),
    ("frequency", ("frequency", "cooldown")),
]


def _waterfall_stage(reason: str) -> str:
    low = reason.lower()
    for stage, needles in _WATERFALL_STAGES:
        if any(n in low for n in needles):
            return stage
    return "other"


def gate_waterfall(diag_rows: list[dict], prediction_rows: list[dict],
                   now_ms: int, window_minutes: int = 60) -> dict:
    """Ordered entry-rarity gate waterfall over the recent window: how many
    candidates fell out at each pipeline stage, plus how many were accepted.
    Purely descriptive of what already happened -- computed from persisted
    shadow_diagnostics + predictions, never re-runs or loosens any gate."""
    cutoff = now_ms - window_minutes * 60_000
    stage_order = [s for s, _ in _WATERFALL_STAGES] + ["other", "accepted"]
    counts = {s: 0 for s in stage_order}

    for r in diag_rows:
        if (r.get("ts_ms") or 0) < cutoff:
            continue
        reason = str(r.get("reason") or "")
        if not reason or reason.startswith("watchlist_"):  # watchlist rows aren't rejects
            continue
        counts[_waterfall_stage(reason)] += 1

    for r in prediction_rows:
        if (r.get("ts_ms") or 0) < cutoff:
            continue
        decision = str(r.get("decision") or "")
        if decision in ("APPROVE", "SHADOW_ONLY"):
            counts["accepted"] += 1
        elif decision == "REJECT":
            counts[_waterfall_stage(str(r.get("reject_reason") or ""))] += 1

    total = sum(counts.values())
    return {
        "window_minutes": window_minutes,
        "generated_ts_ms": now_ms,
        "stages": counts,
        "stage_order": stage_order,
        "total_candidates": total,
        "accepted": counts["accepted"],
        "acceptance_pct": round(100 * counts["accepted"] / total, 2) if total else 0.0,
    }


_DECISION_LABELS = {"APPROVE": "ENTER", "SHADOW_ONLY": "ENTER (shadow)",
                    "WAIT": "WAIT", "REJECT": "SKIP"}


SIGNAL_SNAPSHOT_STALE_MS = 10 * 60 * 1000  # 10 min: older than any live 5-min window
STALE_SIGNAL_LABEL = "STALE HISTORICAL SIGNAL — not current blocker"


def latest_market_state(prediction_rows: list[dict], diag: Optional[dict] = None,
                        now_ms: Optional[int] = None) -> dict:
    """HISTORICAL last-signal snapshot: built from the most recent predictions
    row plus live runtime diagnostics. A predictions row only exists when a
    shock once fired and reached signal-building, so this can be HOURS old
    while the pipeline is perfectly healthy -- it must never be read as "the
    current blocker" (that is last_scan_snapshot / gate_waterfall). When
    now_ms is provided, age/staleness labels are attached so the dashboard
    can say so explicitly. Returns explicit None/'' for anything not actually
    recorded -- callers must render those as 'not available', never fabricate."""
    diag = diag or {}
    if not prediction_rows:
        return {"available": False, "snapshot_kind": "historical_last_signal"}
    latest = max(prediction_rows, key=lambda r: r.get("ts_ms") or 0)
    asset = latest.get("asset")
    decision_raw = latest.get("decision")
    ts_ms = latest.get("ts_ms")
    age_ms = (now_ms - ts_ms) if (now_ms is not None and ts_ms) else None
    is_stale = bool(age_ms is not None and age_ms > SIGNAL_SNAPSHOT_STALE_MS)
    return {
        "available": True,
        "snapshot_kind": "historical_last_signal",
        "age_ms": age_ms,
        "is_stale": is_stale,
        "staleness_label": STALE_SIGNAL_LABEL if is_stale else None,
        "ts_ms": ts_ms,
        "asset": asset,
        "market_title": latest.get("market_title"),
        "direction": latest.get("direction"),
        "signal_side_price": latest.get("polymarket_price"),
        "fair_probability": latest.get("fair_probability"),
        "edge": latest.get("edge_after_slippage") or latest.get("edge"),
        "confidence": latest.get("confidence"),
        "tier": latest.get("tier"),
        "decision_raw": decision_raw,
        "decision_label": _DECISION_LABELS.get(str(decision_raw), "SKIP"),
        "reject_reason": latest.get("reject_reason") or None,
        "cex_selected_source": (diag.get("cex_selected_source") or {}).get(asset),
        "cex_freshest_age_ms": (diag.get("cex_freshest_age_ms") or {}).get(asset),
        "cex_price": (diag.get("latest_prices") or {}).get(asset),
        "fresh_books": diag.get("fresh_books"),
        "total_books": diag.get("total_books"),
        "last_block_reason": diag.get("last_block_reason"),
    }


def min_order_summary(prediction_rows: list[dict], max_trade_usd: float) -> dict:
    """Aggregate view for the min-order sizing panel: how many rejects, and
    the most recent one's full breakdown. Empty/zeroed when none exist --
    never fabricated."""
    rows = min_order_sizing_rows(prediction_rows, max_trade_usd, limit=1)
    if not rows:
        return {"blocked_count": 0, "latest": None}
    all_rows = min_order_sizing_rows(prediction_rows, max_trade_usd, limit=10_000)
    return {"blocked_count": len(all_rows), "latest": rows[0]}
