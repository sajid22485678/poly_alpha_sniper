"""Read-only opportunity-frequency + diagnostics metrics (Missions A/B/C/G/I).

Pure functions over persisted shadow_diagnostics + predictions rows. Never
re-runs a gate, never loosens a threshold, never touches orders or .env.
Every count is derived from what already happened.
"""
from __future__ import annotations

import re
import statistics
from collections import Counter
from typing import Optional

from poly_alpha_sniper.dashboard import metrics as _m

_SCORE_RE = re.compile(r"shock_score=([0-9.]+)")
_TIER_RE = re.compile(r"tier=([A-Z_]+)")

# near-miss tier thresholds mirror strategy/shock_near_miss.py
_HOT, _NEAR, _WATCH = 0.90, 0.80, 0.70


def _within(rows: list[dict], now_ms: int, window_minutes: int) -> list[dict]:
    cutoff = now_ms - window_minutes * 60_000
    return [r for r in rows if (r.get("ts_ms") or 0) >= cutoff]


def _shock_score(detail: str) -> Optional[float]:
    m = _SCORE_RE.search(detail or "")
    return float(m.group(1)) if m else None


def _tier_of_score(score: float) -> str:
    if score >= 1.0:
        return "FIRED"
    if score >= _HOT:
        return "HOT_NEAR_MISS"
    if score >= _NEAR:
        return "NEAR_MISS"
    if score >= _WATCH:
        return "WATCHLIST"
    return "ROUTINE_NO_SHOCK"


def cex_freshness_bucket(age_ms: Optional[int], live_ms: int, shadow_eval_ms: int,
                         fail_closed_ms: int) -> str:
    """Mission C: classify a selected-source age against the tiered budgets.
    FRESH <= live; DEGRADED <= shadow_eval (shadow evaluation allowed, EV
    penalised); FAIL_CLOSED beyond; NO_SOURCE when no age."""
    if age_ms is None:
        return "NO_SOURCE"
    if age_ms <= live_ms:
        return "FRESH"
    if age_ms <= shadow_eval_ms:
        return "DEGRADED"
    return "FAIL_CLOSED"


def cex_freshness_report(diag: dict, cfg) -> dict:
    """Mission C: per-asset CEX freshness diagnostics from live runtime diag,
    with the ladder thresholds and whether the degraded path was allowed.
    Live behavior is unchanged -- degraded only ever affects shadow eval."""
    cf = cfg.cex_freshness
    sources = diag.get("cex_selected_source") or {}
    ages = diag.get("cex_freshest_age_ms") or {}
    degraded = diag.get("cex_freshness_degraded") or {}
    out = {}
    for asset in cfg.assets:
        age = ages.get(asset)
        bucket = cex_freshness_bucket(age, cf.live_signal_max_age_ms,
                                      cf.shadow_eval_max_age_ms, cf.fail_closed_max_age_ms)
        out[asset] = {
            "cex_freshness_bucket": bucket,
            "selected_source": sources.get(asset),
            "selected_age_ms": age,
            "live_threshold_ms": cf.live_signal_max_age_ms,
            "shadow_eval_threshold_ms": cf.shadow_eval_max_age_ms,
            "fail_closed_threshold_ms": cf.fail_closed_max_age_ms,
            "degraded_path_allowed": bucket == "DEGRADED",
            "ev_penalty_if_degraded": cf.degraded_adverse_selection_buffer_add,
        }
    return {"by_asset": out, "live_behavior_unchanged": True}


def opportunity_diagnostics_summary(diag_rows: list[dict], prediction_rows: list[dict],
                                    now_ms: int, windows=(30, 60, 120)) -> dict:
    """Mission A: precise per-window blocker attribution + shape of the funnel.
    Uses the same earliest-stage attribution as metrics.gate_waterfall so the
    numbers agree with the Entry Gate Waterfall panel."""
    out = {"generated_ts_ms": now_ms, "windows": {}}
    for win in windows:
        wf = _m.gate_waterfall(diag_rows, prediction_rows, now_ms, win)
        total = wf["total_candidates"] or 1
        stages_pct = {k: round(100 * v / total, 1) for k, v in wf["stages"].items() if v}
        out["windows"][str(win)] = {
            "total_candidates": wf["total_candidates"],
            "accepted": wf["accepted"],
            "acceptance_pct": wf["acceptance_pct"],
            "stages": {k: v for k, v in wf["stages"].items() if v},
            "stages_pct": stages_pct,
            "dominant_blocker": max(
                ((k, v) for k, v in wf["stages"].items() if k != "accepted"),
                key=lambda kv: kv[1], default=("none", 0))[0],
        }
    # honest bug-vs-market read: no_shock/no_fresh_cex dominance in a calm
    # market is a legitimate market-condition outcome, not a pipeline bug,
    # UNLESS book_fetch_failed or missing_oracle_anchor dominate (those point
    # at infra/config). State it plainly rather than implying a defect.
    w60 = out["windows"].get("60", {})
    dom = w60.get("dominant_blocker", "none")
    infra = {"book_fetch_failed", "missing_oracle_anchor", "price_window_not_ready"}
    out["diagnosis"] = (
        "infra_or_config_suspect" if dom in infra else
        "market_condition_expected" if dom in ("no_shock", "no_fresh_cex_price") else
        "review")
    return out


def no_shock_board(diag_rows: list[dict], now_ms: int, window_minutes: int = 60,
                   limit: int = 25) -> dict:
    """Mission B: recent no_shock candidates with shock_score, tier, distance
    to threshold, and percentile rank over the window's scores. Rows written
    before shock_score was added are counted separately, never as zero."""
    rows = _within(diag_rows, now_ms, window_minutes)
    scored: list[tuple[dict, float]] = []
    unscored = 0
    for r in rows:
        if r.get("reason") not in ("rejected_by_no_shock", "watchlist_no_shock_near_miss"):
            continue
        sc = _shock_score(r.get("detail") or "")
        if sc is None:
            unscored += 1
        else:
            scored.append((r, sc))
    scores = sorted(s for _, s in scored)

    def _percentile(v: float) -> float:
        if not scores:
            return 0.0
        below = sum(1 for s in scores if s <= v)
        return round(100 * below / len(scores), 1)

    tier_counts = Counter(_tier_of_score(s) for _, s in scored)
    board = []
    for r, sc in sorted(scored, key=lambda rs: rs[1], reverse=True)[:limit]:
        board.append({
            "ts_ms": r.get("ts_ms"), "asset": r.get("asset"),
            "shock_score": round(sc, 3), "tier": _tier_of_score(sc),
            "distance_to_threshold": round(max(0.0, 1.0 - sc), 3),
            "percentile": _percentile(sc),
        })
    return {
        "window_minutes": window_minutes,
        "no_shock_scored": len(scored),
        "no_shock_unscored_pre_fix": unscored,
        "avg_shock_score": round(statistics.mean(scores), 3) if scores else None,
        "max_shock_score": round(max(scores), 3) if scores else None,
        "tier_counts": dict(tier_counts),
        "hot_near_miss": tier_counts.get("HOT_NEAR_MISS", 0),
        "near_miss": tier_counts.get("NEAR_MISS", 0),
        "watchlist": tier_counts.get("WATCHLIST", 0),
        "board": board,
    }


def opportunity_frequency(diag_rows: list[dict], prediction_rows: list[dict],
                          now_ms: int, window_minutes: int = 60) -> dict:
    """Mission G: per-hour opportunity/blocker rates over the window."""
    scale = 60.0 / max(window_minutes, 1)
    diag = _within(diag_rows, now_ms, window_minutes)
    preds = _within(prediction_rows, now_ms, window_minutes)

    def _rate(n: int) -> float:
        return round(n * scale, 2)

    no_shock = sum(1 for r in diag if r.get("reason") == "rejected_by_no_shock")
    no_fresh = sum(1 for r in diag if r.get("reason") == "rejected_by_no_fresh_cex_price")
    near = 0
    hot = 0
    for r in diag:
        if r.get("reason") not in ("rejected_by_no_shock", "watchlist_no_shock_near_miss"):
            continue
        sc = _shock_score(r.get("detail") or "")
        if sc is None:
            continue
        t = _tier_of_score(sc)
        if t in ("HOT_NEAR_MISS", "NEAR_MISS", "WATCHLIST"):
            near += 1
        if t == "HOT_NEAR_MISS":
            hot += 1
    accepted = sum(1 for r in preds if r.get("decision") in ("APPROVE", "SHADOW_ONLY"))
    # "qualified" = standard pipeline accepted (APPROVE/SHADOW_ONLY) -- the only
    # honest definition; there is no separate 'qualified but not accepted' path.
    return {
        "window_minutes": window_minutes,
        "qualified_opportunities_per_hour": _rate(accepted),
        "accepted_shadow_trades_per_hour": _rate(accepted),
        "near_miss_per_hour": _rate(near),
        "hot_near_miss_per_hour": _rate(hot),
        "watchlist_per_hour": _rate(near),
        "no_shock_rejects_per_hour": _rate(no_shock),
        "no_fresh_cex_rejects_per_hour": _rate(no_fresh),
    }


def tier_breakdown(prediction_rows: list[dict], now_ms: int, window_minutes: int = 120) -> dict:
    """Mission G: A_PLUS/A/B/C funnel -- candidates / accepted / rejected /
    avg edge / top blocker per tier. C stays diagnostic-only by design."""
    preds = _within(prediction_rows, now_ms, window_minutes)
    out = {}
    for tier in ("A_PLUS", "A", "B", "C"):
        rows = [r for r in preds if str(r.get("tier")) == tier]
        accepted = [r for r in rows if r.get("decision") in ("APPROVE", "SHADOW_ONLY")]
        rejected = [r for r in rows if r.get("decision") == "REJECT"]
        edges = [float(r.get("edge_after_slippage") or r.get("edge") or 0) for r in rows]
        blockers = Counter(str(r.get("reject_reason") or "") for r in rejected if r.get("reject_reason"))
        out[tier] = {
            "candidates": len(rows),
            "accepted": len(accepted),
            "rejected": len(rejected),
            "avg_edge": round(statistics.mean(edges), 4) if edges else None,
            "top_blocker": blockers.most_common(1)[0][0] if blockers else None,
        }
    return {"window_minutes": window_minutes, "by_tier": out}


def live_readiness(data_summary: dict, cfg) -> dict:
    """Mission I: strict live-readiness report. NEVER enables live -- only
    reports PASS/FAIL with the exact unmet requirements. Verdict is
    LIVE_NOT_READY until every requirement holds."""
    trades = int(data_summary.get("standard_shadow_trades", 0))
    pf = float(data_summary.get("profit_factor", 0.0))
    expectancy = float(data_summary.get("expectancy_usd", 0.0))
    max_dd_pct = float(data_summary.get("max_drawdown_pct", 100.0))
    loss_streak = int(data_summary.get("max_loss_streak", 999))
    panic = bool(data_summary.get("panic_active", False))
    kill = bool(data_summary.get("kill_active", False))
    stuck = int(data_summary.get("stuck_positions", 0))
    errors = int(data_summary.get("errors_last_hour", 0))
    min_pf = float(getattr(cfg.capital_scaling, "min_rolling_pf", 1.3)) if hasattr(cfg, "capital_scaling") else 1.3

    reqs = [
        ("50+ standard shadow trades after oracle-aware changes", trades >= 50, f"trades={trades}"),
        (f"profit factor >= {min_pf}", pf >= min_pf, f"pf={pf}"),
        ("positive expectancy", expectancy > 0, f"expectancy={expectancy}"),
        ("max drawdown <= 25%", max_dd_pct <= 25.0, f"max_dd_pct={max_dd_pct}"),
        ("max loss streak <= 4", loss_streak <= 4, f"loss_streak={loss_streak}"),
        ("no panic active", not panic, f"panic={panic}"),
        ("no kill switch active", not kill, f"kill={kill}"),
        ("no stuck positions", stuck == 0, f"stuck={stuck}"),
        ("no errors last hour", errors == 0, f"errors={errors}"),
    ]
    unmet = [f"{name} ({detail})" for name, ok, detail in reqs if not ok]
    passed = not unmet
    return {
        "verdict": "LIVE_READY_REVIEW" if passed else "LIVE_NOT_READY",
        "passed": passed,
        "requirements": [{"name": n, "ok": ok, "detail": d} for n, ok, d in reqs],
        "unmet": unmet,
        "note": ("All automated checks pass -- STILL requires manual review; this "
                 "tool never enables live." if passed else
                 "Live is NOT ready. Continue shadow_live."),
    }
