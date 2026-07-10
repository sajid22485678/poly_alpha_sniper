"""RESEARCH / SHADOW-ONLY: baseline-vs-experimental challenger report built
from real feature-store rows. Never places orders, never affects the baseline
trading decision or live readiness.

Lane separation contract:
- BASELINE_SHADOW ("baseline" lane): the strict production-like shadow
  pipeline. Its stats are the ONLY input to live readiness.
- EXPERIMENTAL_SHADOW ("experimental" lane): research challengers. May loosen
  shock/cooldown/EV-buffer variants, but must NEVER loosen: oracle anchor,
  CEX fail-closed (>8s), executable book, token correctness, extreme
  spread/depth, severe negative EV, duplicate-position guard, cash/exposure
  accounting. Experimental stats are reported separately and never mixed.

Promotion rule (enforced by reporting, applied by a human): a challenger may
only be recommended when replay proves PF, expectancy, drawdown and bad-trade
rate are all not-worse AND no missing-anchor/stale-book/bad-spread candidate
was accepted. Anything less -> NOT_PROMOTED (INSUFFICIENT_SAMPLE when data is
too thin, which at the current shadow trade count it always is).
"""
from __future__ import annotations

from collections import Counter
from typing import Optional

from poly_alpha_sniper.research.markov_chain import (
    MarkovTransitionTracker, classify_state)
from poly_alpha_sniper.research.regime_tagger import tag_regime

# Below this many completed baseline trades no challenger comparison is
# statistically meaningful -- mirrors the dashboard's 30-trade minimum.
MIN_TRADES_FOR_PROMOTION_REVIEW = 30


def _rows_for_asset(rows: list[dict], asset: str) -> list[dict]:
    return sorted((r for r in rows if r.get("asset") == asset),
                  key=lambda r: r.get("ts_ms") or 0)


def _markov_for_asset(asset_rows: list[dict]) -> Optional[dict]:
    if not asset_rows:
        return None
    tracker = MarkovTransitionTracker()
    prev_state = None
    state = None
    for r in asset_rows:
        ttc = r.get("time_to_close_s")
        state = classify_state(
            ret_2s=r.get("ret_2s") or 0.0,
            zscore=r.get("zscore") or 0.0,
            shock_score=r.get("shock_score") or 0.0,
            direction_flips_30s=0,  # not recorded per-row; conservative default
            time_to_close_s=float(ttc) if ttc is not None else 1e9,  # None = no candidate market -> far from close
            vol=r.get("volatility") or 0.0,
            prev_state=prev_state)
        if prev_state is not None:
            tracker.observe(prev_state, state)
        prev_state = state
    return {
        "state_now": state,
        "n_observations": tracker.n_observations,
        "continuation_probability": round(tracker.continuation_probability(state), 4),
        "reversal_probability": round(tracker.reversal_probability(state), 4),
    }


def _regime_for_asset(asset_rows: list[dict]) -> Optional[dict]:
    if not asset_rows:
        return None
    r = asset_rows[-1]
    returns = {k: r.get(f"ret_{k}s") for k in (1, 2, 5) if r.get(f"ret_{k}s") is not None}
    tag = tag_regime(
        returns=returns,
        volatility_per_s=r.get("volatility") or 0.0,
        shock_score=r.get("shock_score") or 0.0,
        spread=r.get("spread"),
        depth_usd=r.get("depth_usd"),
        anchor_distance_pct=None,  # not derivable from a single row without anchor+price
        time_to_close_s=r.get("time_to_close_s"))
    return {"regime": tag.regime, "confidence": round(tag.confidence, 3),
            "reasons": tag.reasons[:4]}


def _drift(rows: list[dict]) -> dict:
    """Blocker/anchor/freshness drift: newest half of the window vs oldest
    half, from the same append-only rows -- no fabrication, no lookahead."""
    if len(rows) < 4:
        return {"available": False, "note": f"only {len(rows)} feature rows"}
    ordered = sorted(rows, key=lambda r: r.get("ts_ms") or 0)
    half = len(ordered) // 2
    old, new = ordered[:half], ordered[half:]

    def summarize(chunk: list[dict]) -> dict:
        blockers = Counter((r.get("blocker") or "none") for r in chunk)
        anchored = sum(1 for r in chunk if r.get("anchor_status") == "available")
        fresh = sum(1 for r in chunk if r.get("cex_freshness") == "fresh")
        return {
            "top_blockers": dict(blockers.most_common(4)),
            "anchor_available_rate": round(anchored / len(chunk), 3),
            "cex_fresh_rate": round(fresh / len(chunk), 3),
        }

    return {"available": True, "older_half": summarize(old),
            "newer_half": summarize(new), "n_rows": len(ordered)}


def _parse_extra(row: dict) -> dict:
    import json
    try:
        extra = json.loads(row.get("extra") or "{}")
        return extra if isinstance(extra, dict) else {}
    except (TypeError, ValueError):
        return {}


def _blocker_distribution(rows: list[dict]) -> dict:
    return dict(Counter((r.get("blocker") or "none") for r in rows).most_common(6))


def _per_challenger_stats(experimental_rows: list[dict], baseline_trades: int) -> dict:
    """Per-challenger counts/would-enter/reject-reasons/avg-EV parsed from the
    experimental rows' decision payloads. Status is honest and never a
    promotion: INSUFFICIENT_SAMPLE below the trade floor, CANDIDATE_FOR_REPLAY
    when a challenger has >=10 clean would-enters (a human then runs replay),
    else NOT_PROMOTED."""
    stats: dict[str, dict] = {}
    for row in experimental_rows:
        for name, d in (_parse_extra(row).get("challengers") or {}).items():
            if not isinstance(d, dict):
                continue
            s = stats.setdefault(name, {"rows": 0, "would_enter": 0,
                                        "reject_reasons": Counter(), "evs": []})
            s["rows"] += 1
            if d.get("would_enter"):
                s["would_enter"] += 1
            elif d.get("blocker"):
                s["reject_reasons"][d["blocker"]] += 1
            if isinstance(d.get("ev"), (int, float)):
                s["evs"].append(float(d["ev"]))
    out = {}
    for name, s in stats.items():
        if baseline_trades < MIN_TRADES_FOR_PROMOTION_REVIEW:
            status = "INSUFFICIENT_SAMPLE"
        elif s["would_enter"] >= 10:
            status = "CANDIDATE_FOR_REPLAY"
        else:
            status = "NOT_PROMOTED"
        out[name] = {
            "rows": s["rows"],
            "would_enter": s["would_enter"],
            "top_reject_reasons": dict(s["reject_reasons"].most_common(3)),
            "avg_ev": round(sum(s["evs"]) / len(s["evs"]), 5) if s["evs"] else None,
            "status": status,
        }
    return out


def _experimental_zero_reason(enabled: bool, baseline_rows: int) -> str:
    if not enabled:
        return "research_challengers.enabled=false"
    if baseline_rows == 0:
        return ("no feature rows at all yet — bot restart required to activate "
                "the feature store + challenger writer")
    return ("baseline rows exist but no experimental rows — the running bot "
            "predates the challenger writer; restart to activate it")


def build_research_challenger(feature_rows: list[dict], baseline_trades: int,
                              assets: list[str], now_ms: int,
                              challengers_enabled: bool = True) -> dict:
    """Assemble the research/challenger export section from real feature-store
    rows. Purely diagnostic; the returned promotion_status can only ever be
    NOT_PROMOTED at current sample sizes -- promotion is a human decision on
    replay evidence, never automatic."""
    rows = [r for r in feature_rows if isinstance(r, dict)]
    baseline_rows = [r for r in rows if (r.get("lane") or "baseline") == "baseline"]
    experimental_rows = [r for r in rows if r.get("lane") == "experimental"]

    per_asset = {}
    for asset in assets:
        asset_rows = _rows_for_asset(baseline_rows, asset)
        per_asset[asset] = {
            "markov": _markov_for_asset(asset_rows),
            "regime": _regime_for_asset(asset_rows),
            "n_feature_rows": len(asset_rows),
        }

    insufficient = baseline_trades < MIN_TRADES_FOR_PROMOTION_REVIEW
    return {
        "generated_ts_ms": now_ms,
        "research_only": True,
        "note": ("EXPERIMENTAL / RESEARCH lane -- never used for live readiness, "
                 "never mixed into baseline shadow stats, never places orders."),
        "scoring_version": "shock_and_gate_v2",
        "lane_separation": {
            "baseline_rows": len(baseline_rows),
            "experimental_rows": len(experimental_rows),
            "mixed": False,
        },
        "experimental_zero_reason": (_experimental_zero_reason(challengers_enabled,
                                                               len(baseline_rows))
                                     if not experimental_rows else None),
        "challengers": _per_challenger_stats(experimental_rows, baseline_trades),
        "baseline_blocker_distribution": _blocker_distribution(baseline_rows),
        "experimental_blocker_distribution": _blocker_distribution(experimental_rows),
        "per_asset": per_asset,
        "drift": _drift(baseline_rows),
        "promotion_status": ("NOT_PROMOTED — INSUFFICIENT_SAMPLE "
                             f"({baseline_trades}/{MIN_TRADES_FOR_PROMOTION_REVIEW} baseline trades)"
                             if insufficient else
                             "NOT_PROMOTED — awaiting replay proof (human review required)"),
    }
