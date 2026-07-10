"""Near-miss diagnostics for the no_shock gate -- read-only, never affects
the actual shock/entry decision (see strategy/shock_detector.py).

The user complaint this answers: entries are extremely rare and the
oracle-aware EV engine almost never gets exercised, but the dashboard gave
no visibility into HOW CLOSE the market came to firing a shock. This module
computes a normalized shock_score (0-1, >=1 roughly means a real shock
would have fired) and flags WATCHLIST_NO_SHOCK_NEAR_MISS candidates so an
operator can tell "consistently far from threshold" from "keeps almost
firing" -- the latter is a calibration signal, the former is not.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from poly_alpha_sniper.core.contracts import MultiCexView

NEAR_MISS_SCORE_THRESHOLD = 0.7

# Diagnostic scoring version, embedded in every near-miss detail row so
# pre-fix history (max()-scored, could fake FIRED from one leg) is
# distinguishable from post-fix rows without destructively mutating old data.
# v1 (implicit, unversioned): shock_score = max(ret_score, zscore_score).
# shock_and_gate_v2: shock_score = min(...) -- mirrors the detector's AND-gate.
SCORING_VERSION = "shock_and_gate_v2"

# Ordered near-miss tiers (Mission B). All are for shock_score < 1.0 (below the
# firing threshold); at >= 1.0 a real shock would have fired. Purely
# diagnostic labels -- a near-miss is NEVER converted into an accepted trade.
HOT_NEAR_MISS = "HOT_NEAR_MISS"        # >= 0.90 of threshold
NEAR_MISS = "NEAR_MISS"                # >= 0.80
WATCHLIST = "WATCHLIST"                # >= 0.70
ROUTINE_NO_SHOCK = "ROUTINE_NO_SHOCK"  # < 0.70
FIRED = "FIRED"                        # >= 1.0 (would have fired; not a no_shock case)


def near_miss_tier(shock_score: float) -> str:
    if shock_score >= 1.0:
        return FIRED
    if shock_score >= 0.90:
        return HOT_NEAR_MISS
    if shock_score >= 0.80:
        return NEAR_MISS
    if shock_score >= 0.70:
        return WATCHLIST
    return ROUTINE_NO_SHOCK


@dataclass
class ShockNearMiss:
    asset: str
    trigger_ret: float
    zscore: float
    ret_score: float          # |trigger_ret| / shock_min_abs_return, clamped [0, 2]
    zscore_score: float       # |zscore| / shock_min_zscore, clamped [0, 2]
    shock_score: float        # min(ret_score, zscore_score) -- how close to firing.
                              # min, NOT max: ShockDetector requires BOTH the
                              # return AND the z-score thresholds (an AND-gate),
                              # so proximity is bounded by the weaker leg.
                              # max() used to label z-only spikes on
                              # economically-nothing moves (z=10 on a 0.03%
                              # move in a quiet market) as tier=FIRED while the
                              # detector correctly refused -- a false
                              # "downstream blocker" impression on the board.
    is_near_miss: bool        # shock_score >= NEAR_MISS_SCORE_THRESHOLD and < 1.0
    direction: str
    fresh: bool
    tier: str = ROUTINE_NO_SHOCK  # HOT_NEAR_MISS | NEAR_MISS | WATCHLIST | ROUTINE_NO_SHOCK | FIRED
    distance_to_threshold: float = 0.0  # 1.0 - shock_score (>0 means below firing)

    def detail_suffix(self) -> str:
        return (f"shock_score={self.shock_score:.2f} "
                f"ret_score={self.ret_score:.2f} zscore_score={self.zscore_score:.2f} "
                f"tier={self.tier} near_miss={self.is_near_miss} "
                f"scoring={SCORING_VERSION}")


def compute_shock_near_miss(view: Optional[MultiCexView], cfg) -> Optional[ShockNearMiss]:
    """Mirrors ShockDetector.detect()'s trigger/zscore computation (not the
    freshness/direction-agreement/cooldown/fakeout gates -- those are binary
    pass/fail already visible elsewhere in the diagnostic row)."""
    if view is None or view.primary is None:
        return None
    stats = view.primary
    s = cfg.strategy
    trigger_ret = 0.0
    for sec in (1, 2, 3):
        r = stats.returns.get(sec, 0.0)
        if abs(r) > abs(trigger_ret):
            trigger_ret = r
    ret_floor = max(s.shock_min_abs_return, 1e-9)
    z_floor = max(s.shock_min_zscore, 1e-9)
    ret_score = min(2.0, abs(trigger_ret) / ret_floor)
    zscore_score = min(2.0, abs(stats.zscore) / z_floor)
    # min(): both legs must clear for the detector to fire (see dataclass note).
    shock_score = min(ret_score, zscore_score)
    direction = "UP" if trigger_ret > 0 else ("DOWN" if trigger_ret < 0 else "FLAT")
    return ShockNearMiss(
        asset=stats.asset, trigger_ret=trigger_ret, zscore=stats.zscore,
        ret_score=ret_score, zscore_score=zscore_score, shock_score=shock_score,
        is_near_miss=NEAR_MISS_SCORE_THRESHOLD <= shock_score < 1.0,
        direction=direction, fresh=stats.fresh,
        tier=near_miss_tier(shock_score),
        distance_to_threshold=round(max(0.0, 1.0 - shock_score), 4))
