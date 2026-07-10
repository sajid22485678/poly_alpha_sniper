"""RESEARCH / SHADOW-ONLY: experimental challenger lane engine. Never places
orders, never affects the baseline trading decision or live readiness.

Evaluates every (throttled) scan snapshot against a fixed set of named
challenger variants and records their would-enter decisions as
lane="experimental" feature-store rows. The baseline lane is untouched: this
module only READS the same point-in-time features the baseline scan recorded.

What a challenger MAY loosen (vs baseline): the shock trigger proximity
(10-15%), HOT_NEAR_MISS entry, EV buffer. What NO challenger can ever bypass
(HARD GATES, enforced centrally in hard_safety_gates): oracle anchor
(price_to_beat), CEX fail-closed (>8s), valid current market with usable time
to close, token mapping, an executable book, catastrophic spread/depth,
severe negative EV, available cash, and the duplicate-position guard.

would_enter is an ELIGIBILITY RECORD for replay comparison -- not a position,
not an order, and promotion is never automatic (NOT_PROMOTED always; replay
may at most flag CANDIDATE_FOR_REPLAY for human review).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

from poly_alpha_sniper.research.bayesian_ev import calibrate_ev
from poly_alpha_sniper.research.burst_detector import BurstDetector
from poly_alpha_sniper.research.monte_carlo_ev import simulate as mc_simulate
from poly_alpha_sniper.research.regime_tagger import tag_regime

CHALLENGER_VERSION = "challengers_v1"
CHALLENGER_NAMES = (
    "loose_shock_10", "loose_shock_15", "hot_near_miss_entry",
    "bayesian_ev_adjusted", "regime_penalty", "burst_momentum",
    "combined_research",
)

# Hard-gate constants (experimental lane; deliberately WIDER than baseline's
# strict gates -- e.g. baseline max_spread=0.035 -- but still catastrophic-
# blocking. Loosening past these is not research, it's garbage-in.)
MAX_CEX_AGE_MS = 8000            # mirrors cex_freshness.fail_closed_max_age_ms
MAX_SPREAD = 0.10                # catastrophic spread cap
MIN_DEPTH_USD = 10.0             # catastrophic depth floor
MIN_TIME_TO_CLOSE_S = 45.0       # mirrors ultra_short_expiry.reject_if_less_than_seconds
SEVERE_NEGATIVE_EV = -0.02       # per-share EV below this is never recordable as entry
FIXED_SHARES = 5.0               # experimental sizing mirrors baseline fixed_min_shares

# anchor gate failures name the precise upstream reason (never generic)
_ANCHOR_BLOCKER = {
    "UPSTREAM_NOT_PUBLISHED": "anchor_upstream_not_published",
    "HYDRATION_FAILED": "anchor_hydration_failed",
    "SCHEMA_UNKNOWN": "anchor_schema_unknown",
    "EVENT_NOT_FOUND": "missing_anchor",
}


@dataclass
class ChallengerDecision:
    challenger_name: str
    would_enter: bool
    reason: str
    blocker: str                 # "" when would_enter
    ev: Optional[float]
    safety_gates_passed: bool
    promotion_status: str = "NOT_PROMOTED"

    def as_dict(self) -> dict:
        return {"would_enter": self.would_enter, "reason": self.reason,
                "blocker": self.blocker,
                "ev": round(self.ev, 5) if self.ev is not None else None,
                "safety_gates_passed": self.safety_gates_passed,
                "promotion_status": self.promotion_status}


def hard_safety_gates(f: dict, available_cash_usd: Optional[float]) -> tuple[bool, str, dict]:
    """The gates NO challenger may loosen. Returns (ok, first_failure, gates).
    Evaluated from the same point-in-time feature row the baseline recorded --
    no fresher data, no lookahead."""
    gates: dict[str, bool] = {}

    gates["anchor_available"] = f.get("price_to_beat") is not None
    gates["cex_not_fail_closed"] = (
        f.get("cex_age_ms") is not None and f["cex_age_ms"] <= MAX_CEX_AGE_MS
        and f.get("cex_freshness") in ("fresh", "degraded"))
    ttc = f.get("time_to_close_s")
    gates["market_valid"] = bool(f.get("market_id")) and ttc is not None and ttc > MIN_TIME_TO_CLOSE_S
    gates["token_mapping"] = bool(f.get("yes_token_id")) and bool(f.get("no_token_id"))
    bid, ask = f.get("book_bid"), f.get("book_ask")
    gates["executable_book"] = bid is not None and ask is not None and 0 < ask < 1
    spread = f.get("spread")
    gates["spread_ok"] = spread is not None and spread <= MAX_SPREAD
    depth = f.get("depth_usd")
    gates["depth_ok"] = depth is not None and depth >= MIN_DEPTH_USD
    # cash: None means the caller could not snapshot the portfolio this scan --
    # recorded as not-evaluated (True with a flag), never silently fabricated.
    if available_cash_usd is None:
        gates["cash_ok"] = True
        gates["cash_evaluated"] = False
    else:
        gates["cash_ok"] = (ask is not None and available_cash_usd >= FIXED_SHARES * ask)
        gates["cash_evaluated"] = True

    order = ("anchor_available", "cex_not_fail_closed", "market_valid",
             "token_mapping", "executable_book", "spread_ok", "depth_ok", "cash_ok")
    for name in order:
        if not gates[name]:
            return False, name, gates
    return True, "", gates


class ChallengerEngine:
    """Stateful evaluator: per-asset burst detectors and a per-challenger
    duplicate-position guard (one simulated entry per market per challenger).
    Pure computation over recorded features; deterministic given the same
    scan sequence."""

    def __init__(self) -> None:
        self._bursts: dict[str, BurstDetector] = {}
        self._entered: dict[str, set[str]] = {n: set() for n in CHALLENGER_NAMES}

    # ------------------------------------------------------------------
    def _burst(self, asset: str) -> BurstDetector:
        if asset not in self._bursts:
            self._bursts[asset] = BurstDetector()
        return self._bursts[asset]

    def _bayesian_ev(self, f: dict, regime: str) -> Optional[float]:
        ask = f.get("book_ask")
        if ask is None or not (0 < ask < 1):
            return None
        result = calibrate_ev(
            prior_probability=0.5,  # neutral prior; evidence does the work
            executable_price=ask,
            fee_rate=0.0,
            evidence={
                "shock_strength": f.get("shock_score") or 0.0,
                "cex_staleness_ms": f.get("cex_age_ms") or 0,
                "spread": f.get("spread"),
                "regime": regime,
                "time_to_close_s": f.get("time_to_close_s"),
            })
        return result.ev_posterior if result is not None else None

    # ------------------------------------------------------------------
    def evaluate(self, f: dict, now_ms: int,
                 available_cash_usd: Optional[float] = None) -> dict[str, ChallengerDecision]:
        """Evaluate all challengers against one scan feature row. Returns
        name -> decision. Never raises on missing fields."""
        asset = f.get("asset") or ""
        ret_score = f.get("ret_score") or 0.0
        z_score = f.get("zscore_score") or 0.0
        tier = f.get("near_miss_tier") or ""
        market_id = str(f.get("market_id") or "")

        # feed the burst detector: a meaningful 2s move is an impulse
        ret2 = f.get("ret_2s") or 0.0
        if abs(ret2) >= 0.0005:
            self._burst(asset).record_impulse(now_ms, magnitude=min(2.0, abs(ret2) / 0.0005))
        burst = self._burst(asset).snapshot(now_ms)

        returns = {k: f.get(f"ret_{k}s") for k in (1, 2, 5) if f.get(f"ret_{k}s") is not None}
        regime = tag_regime(
            returns=returns, volatility_per_s=f.get("volatility") or 0.0,
            shock_score=f.get("shock_score") or 0.0, spread=f.get("spread"),
            depth_usd=f.get("depth_usd"), anchor_distance_pct=None,
            time_to_close_s=f.get("time_to_close_s")).regime

        gates_ok, gate_failure, gates = hard_safety_gates(f, available_cash_usd)
        ev = self._bayesian_ev(f, regime)

        # Precise blocker taxonomy: gate failures name the actual problem, and
        # a fundamental data problem (anchor/CEX/book) outranks "no trigger" --
        # a scan with no anchor must never read as merely no_challenger_trigger.
        _GATE_BLOCKER = {
            "anchor_available": _ANCHOR_BLOCKER.get(
                str(f.get("anchor_missing_reason") or ""), "missing_anchor"),
            "cex_not_fail_closed": "cex_fail_closed",
            "market_valid": "invalid_market",
            "token_mapping": "token_invalid",
            "executable_book": "no_executable_book",
            "spread_ok": "spread_depth_bad",
            "depth_ok": "spread_depth_bad",
            "cash_ok": "cash_or_exposure",
        }
        gate_blocker = _GATE_BLOCKER.get(gate_failure, gate_failure)
        _DATA_GATES = ("anchor_available", "cex_not_fail_closed", "market_valid",
                       "token_mapping", "executable_book")

        def decide(name: str, trigger: bool, trigger_reason: str,
                   extra_ok: bool = True, extra_blocker: str = "",
                   no_trigger_blocker: str = "shock_too_low") -> ChallengerDecision:
            """Shared decision ladder: data gates -> trigger -> extra -> EV ->
            dup guard. ROUTINE_NO_SHOCK never enters by default -- a challenger
            must have its own named trigger to get past the trigger rung."""
            if not gates_ok and gate_failure in _DATA_GATES:
                return ChallengerDecision(name, False,
                                          f"hard gate failed: {gate_failure}",
                                          gate_blocker, ev, False)
            if not trigger:
                return ChallengerDecision(name, False, trigger_reason,
                                          no_trigger_blocker, ev, gates_ok)
            if not gates_ok:
                return ChallengerDecision(name, False,
                                          f"hard gate failed: {gate_failure}",
                                          gate_blocker, ev, False)
            if not extra_ok:
                return ChallengerDecision(name, False, extra_blocker,
                                          extra_blocker, ev, True)
            if ev is None or ev <= 0 or ev < SEVERE_NEGATIVE_EV:
                return ChallengerDecision(name, False,
                                          f"ev not positive ({ev})",
                                          "ev_not_positive", ev, True)
            if market_id and market_id in self._entered[name]:
                return ChallengerDecision(name, False,
                                          "already simulated an entry on this market",
                                          "duplicate_experimental_position", ev, True)
            if market_id:
                self._entered[name].add(market_id)
            return ChallengerDecision(name, True, trigger_reason, "", ev, True)

        both_legs = min(ret_score, z_score)  # honest AND-gate proximity, no max()
        decisions = {
            "loose_shock_10": decide(
                "loose_shock_10", both_legs >= 0.90,
                f"both detector legs >= 90% of threshold (min={both_legs:.2f})"),
            "loose_shock_15": decide(
                "loose_shock_15", both_legs >= 0.85,
                f"both detector legs >= 85% of threshold (min={both_legs:.2f})"),
            "hot_near_miss_entry": decide(
                "hot_near_miss_entry", tier in ("HOT_NEAR_MISS", "FIRED"),
                f"near-miss tier {tier or 'none'}",
                no_trigger_blocker="no_hot_near_miss"),
            "bayesian_ev_adjusted": decide(
                "bayesian_ev_adjusted",
                both_legs >= 0.85 and ev is not None and ev > 0.01,
                f"posterior EV {ev} with both legs >= 85%"),
            "regime_penalty": decide(
                "regime_penalty", both_legs >= 0.90,
                f"legs >= 90% and regime={regime}",
                extra_ok=regime not in ("FAKEOUT_PRONE", "CHOP", "FINAL_WINDOW_CHAOS"),
                extra_blocker=f"regime_penalty:{regime}"),
            "burst_momentum": decide(
                "burst_momentum",
                both_legs >= 0.85 and burst["cluster_score"] >= 0.3,
                f"legs >= 85% with live burst (cluster={burst['cluster_score']:.2f})"),
        }

        # combined_research: loose_shock_10 trigger + regime ok + burst not
        # dead + Monte Carlo robustness when computable (seeded, cheap).
        mc_ok, mc_note = True, "mc skipped (inputs unavailable)"
        if (both_legs >= 0.90 and gates_ok and f.get("cex_price") and f.get("price_to_beat")
                and (f.get("volatility") or 0) > 0 and (f.get("time_to_close_s") or 0) > 0):
            try:
                direction = "UP" if ret2 >= 0 else "DOWN"
                mc = mc_simulate(
                    current_price=f["cex_price"], price_to_beat=f["price_to_beat"],
                    vol_per_s=f["volatility"], drift_per_s=0.0,
                    time_remaining_s=f["time_to_close_s"], side=direction,
                    executable_price=f.get("book_ask") or 0.5, n_paths=200, seed=42)
                mc_ok = mc.ev_mean > 0
                mc_note = f"mc ev_mean={mc.ev_mean:.4f}"
            except Exception:  # noqa: BLE001 -- research must never raise upward
                mc_ok, mc_note = True, "mc failed; not counted against"
        decisions["combined_research"] = decide(
            "combined_research",
            both_legs >= 0.90,
            f"legs >= 90%, regime={regime}, burst={burst['cluster_score']:.2f}, {mc_note}",
            extra_ok=(regime not in ("FAKEOUT_PRONE", "CHOP", "FINAL_WINDOW_CHAOS")
                      and burst["cluster_score"] >= 0.2 and mc_ok),
            extra_blocker="combined_conditions_not_met")
        return decisions


def build_experimental_row(baseline_row: dict,
                           decisions: dict[str, ChallengerDecision]) -> dict:
    """One lane="experimental" feature-store row per throttled scan, carrying
    ALL challenger decisions as a JSON payload (no schema change needed).
    Headline columns come from combined_research. Baseline row is copied, not
    mutated."""
    headline = decisions.get("combined_research")
    row = dict(baseline_row)
    row["lane"] = "experimental"
    row["decision"] = ("WOULD_ENTER" if headline is not None and headline.would_enter
                       else "REJECT")
    row["blocker"] = headline.blocker if headline is not None else "no_evaluation"
    row["ev"] = headline.ev if headline is not None else None
    row["extra"] = json.dumps({
        "challenger_version": CHALLENGER_VERSION,
        "headline_challenger": "combined_research",
        "simulated_entry_price": baseline_row.get("book_ask"),
        "challengers": {name: d.as_dict() for name, d in decisions.items()},
    }, default=str)[:4000]
    return row
