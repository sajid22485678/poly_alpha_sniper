"""RESEARCH / SHADOW-ONLY: diagnostics and challenger evaluation only. Never places
orders, never affects the baseline trading decision or live readiness.

Deterministic rule-based market regime tagger.

WHY rules instead of an HMM: a true hidden-Markov-model regime classifier is
deferred because this repo is intentionally stdlib-only (no numpy / hmmlearn
dependency). A transparent, ordered rule ladder is deterministic, auditable in
shadow logs, and gives the same tag for the same inputs every time -- which is
exactly what challenger evaluation needs. If/when an HMM is introduced it can
be validated *against* these tags rather than replacing them blindly.

Rule precedence (first match wins):
  1. FINAL_WINDOW_CHAOS -- close to market resolution with shock or elevated vol.
  2. IMPULSE            -- extreme shock and the 2s return dominates the 30s return.
  3. FAKEOUT_PRONE      -- strong 2s move reversed at 10s/30s, or thin depth,
                           or wide spread (conditions where snaps mean-revert).
  4. CHOP               -- signs alternate across the returns ladder.
  5. QUIET              -- nothing above fired.

Confidence is a margin-based deterministic score: 0.5 + 0.5 * m, where m in
[0, 1] measures how far past the rule's thresholds the inputs are (for QUIET,
how far *below* every trigger the inputs sit). Clamped to [0.01, 0.99] so no
downstream consumer ever sees a degenerate 0/1 certainty.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

REGIMES: tuple[str, ...] = (
    "QUIET",
    "IMPULSE",
    "FAKEOUT_PRONE",
    "CHOP",
    "FINAL_WINDOW_CHAOS",
)

# Ordered horizons (seconds) of the returns ladder.
RETURN_KEYS: tuple[int, ...] = (1, 2, 5, 10, 30)

# --- thresholds (documented so shadow logs are interpretable) ---------------
FINAL_WINDOW_S = 45.0          # within 45s of close, resolution dynamics dominate
FWC_SHOCK = 0.7                # shock level that makes the final window "chaos"
VOL_ELEVATED_PER_S = 0.004     # 0.4%/s stdev is elevated for a [0,1] prob market
IMPULSE_SHOCK = 0.9            # near-max shock required to call an impulse
DOMINANCE_RATIO = 0.5          # |ret_2| >= 0.5*|ret_30| => move is fresh, not stale
STRONG_RET_2S = 0.002          # 0.2% in 2s counts as a strong short burst
DEPTH_THIN_USD = 100.0         # below this, one small order moves the book
SPREAD_WIDE = 0.04             # 4c spread: quotes too loose to trust the mid
CHOP_MIN_FLIPS = 2             # >=2 sign flips across the ladder means chop
_EPS = 1e-12

CONF_MIN = 0.01
CONF_MAX = 0.99


@dataclass
class RegimeTag:
    """One deterministic regime classification with its audit trail."""

    regime: str
    confidence: float
    features_used: dict = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _finite(x: Optional[float]) -> Optional[float]:
    """Treat NaN/inf as missing so a bad upstream feature never fabricates a tag."""
    if x is None:
        return None
    if not math.isfinite(x):
        return None
    return float(x)


def _sign(x: float) -> int:
    return (x > 0) - (x < 0)


def _confidence(margin: float) -> float:
    """Map a [0,1] rule margin to a clamped confidence; deterministic by design."""
    return _clamp(0.5 + 0.5 * _clamp(margin, 0.0, 1.0), CONF_MIN, CONF_MAX)


def _sign_flips(rets: dict[int, float]) -> tuple[int, int]:
    """Count sign alternations between consecutive available (nonzero) horizons.

    Returns (flips, possible_flips). Zero returns carry no direction, so they
    are skipped rather than counted as flips -- flat is not chop.
    """
    signs = [_sign(rets[k]) for k in RETURN_KEYS if k in rets]
    nonzero = [s for s in signs if s != 0]
    flips = sum(1 for a, b in zip(nonzero, nonzero[1:]) if a != b)
    return flips, max(len(nonzero) - 1, 0)


def tag_regime(
    returns: dict[int, float],
    volatility_per_s: float,
    shock_score: float,
    spread: Optional[float] = None,
    depth_usd: Optional[float] = None,
    anchor_distance_pct: Optional[float] = None,
    time_to_close_s: Optional[float] = None,
) -> RegimeTag:
    """Classify the current microstructure state into one of REGIMES.

    Pure and deterministic: same inputs -> identical RegimeTag. Missing or
    non-finite features are treated as absent (never guessed), and with no
    informative features at all the tagger returns an explicitly neutral
    QUIET with confidence 0.5 rather than fabricating certainty.

    ``anchor_distance_pct`` is recorded in ``features_used`` but does not yet
    drive any rule: no validated threshold exists for it, and an unvalidated
    heuristic would silently bias challenger evaluation.
    """
    rets: dict[int, float] = {}
    for k in RETURN_KEYS:
        v = _finite(returns.get(k)) if returns else None
        if v is not None:
            rets[k] = v

    vol = _finite(volatility_per_s) or 0.0
    shock = _finite(shock_score) or 0.0
    spread_v = _finite(spread)
    depth_v = _finite(depth_usd)
    anchor_v = _finite(anchor_distance_pct)
    ttc = _finite(time_to_close_s)

    ret_2 = rets.get(2)
    ret_10 = rets.get(10)
    ret_30 = rets.get(30)
    flips, possible_flips = _sign_flips(rets)

    features: dict = {"volatility_per_s": vol, "shock_score": shock,
                      "sign_flips": flips}
    features.update({f"ret_{k}s": v for k, v in rets.items()})
    if spread_v is not None:
        features["spread"] = spread_v
    if depth_v is not None:
        features["depth_usd"] = depth_v
    if anchor_v is not None:
        features["anchor_distance_pct"] = anchor_v
    if ttc is not None:
        features["time_to_close_s"] = ttc

    # Explicit neutral: nothing informative was supplied, so refuse to guess.
    if not rets and vol == 0.0 and shock == 0.0 and spread_v is None \
            and depth_v is None and ttc is None:
        return RegimeTag(
            regime="QUIET", confidence=0.5, features_used=features,
            reasons=["no informative features supplied; neutral QUIET default"])

    # 1) FINAL_WINDOW_CHAOS -- time pressure plus a live disturbance.
    if ttc is not None and ttc <= FINAL_WINDOW_S:
        shock_hit = shock >= FWC_SHOCK
        vol_hit = vol >= VOL_ELEVATED_PER_S
        if shock_hit or vol_hit:
            time_m = _clamp((FINAL_WINDOW_S - ttc) / FINAL_WINDOW_S, 0.0, 1.0)
            trig_m = _clamp(max((shock - FWC_SHOCK) / (1.0 - FWC_SHOCK),
                                (vol - VOL_ELEVATED_PER_S) / VOL_ELEVATED_PER_S),
                            0.0, 1.0)
            reasons = [f"time_to_close_s={ttc:.1f}<= {FINAL_WINDOW_S:.0f}"]
            if shock_hit:
                reasons.append(f"shock_score={shock:.3f}>={FWC_SHOCK}")
            if vol_hit:
                reasons.append(
                    f"volatility_per_s={vol:.5f}>={VOL_ELEVATED_PER_S}")
            return RegimeTag("FINAL_WINDOW_CHAOS",
                             _confidence(0.5 * (time_m + trig_m)),
                             features, reasons)

    # 2) IMPULSE -- extreme shock and a fresh move (2s return dominates 30s).
    if shock >= IMPULSE_SHOCK and ret_2 is not None:
        abs2 = abs(ret_2)
        if ret_30 is None or abs(ret_30) < _EPS:
            dominant = abs2 >= _EPS
            dom_m = 1.0 if dominant else 0.0  # nothing at 30s: any 2s move is fresh
        else:
            dom = abs2 / max(abs(ret_30), _EPS)  # divide-by-zero guarded
            dominant = dom >= DOMINANCE_RATIO
            dom_m = _clamp((dom - DOMINANCE_RATIO) / DOMINANCE_RATIO, 0.0, 1.0)
        if dominant:
            shock_m = _clamp((shock - IMPULSE_SHOCK) / (1.0 - IMPULSE_SHOCK),
                             0.0, 1.0)
            return RegimeTag(
                "IMPULSE", _confidence(0.5 * (shock_m + dom_m)), features,
                [f"shock_score={shock:.3f}>={IMPULSE_SHOCK}",
                 "ret_2s dominates ret_30s"])

    # 3) FAKEOUT_PRONE -- reversal signature, or a book too fragile to trust.
    fk_reasons: list[str] = []
    fk_margins: list[float] = []
    if ret_2 is not None and abs(ret_2) >= STRONG_RET_2S:
        s2 = _sign(ret_2)
        for k, r in ((10, ret_10), (30, ret_30)):
            if r is not None and _sign(r) == -s2:
                fk_reasons.append(f"strong ret_2s reversed by ret_{k}s")
                fk_margins.append(
                    _clamp(abs(ret_2) / STRONG_RET_2S - 1.0, 0.0, 1.0))
                break
    if depth_v is not None and depth_v < DEPTH_THIN_USD:
        fk_reasons.append(f"thin depth {depth_v:.0f}<{DEPTH_THIN_USD:.0f} usd")
        fk_margins.append(
            _clamp((DEPTH_THIN_USD - depth_v) / DEPTH_THIN_USD, 0.0, 1.0))
    if spread_v is not None and spread_v > SPREAD_WIDE:
        fk_reasons.append(f"wide spread {spread_v:.3f}>{SPREAD_WIDE}")
        fk_margins.append(
            _clamp((spread_v - SPREAD_WIDE) / SPREAD_WIDE, 0.0, 1.0))
    if fk_reasons:
        return RegimeTag("FAKEOUT_PRONE", _confidence(max(fk_margins)),
                         features, fk_reasons)

    # 4) CHOP -- direction keeps flipping across the ladder.
    if flips >= CHOP_MIN_FLIPS:
        chop_m = flips / possible_flips if possible_flips > 0 else 0.0
        return RegimeTag("CHOP", _confidence(chop_m), features,
                         [f"{flips} sign flips across returns ladder"])

    # 5) QUIET -- confidence grows with slack below every trigger.
    proximities = [shock / IMPULSE_SHOCK, vol / VOL_ELEVATED_PER_S,
                   flips / CHOP_MIN_FLIPS]
    if ret_2 is not None:
        proximities.append(abs(ret_2) / STRONG_RET_2S)
    if spread_v is not None:
        proximities.append(spread_v / SPREAD_WIDE)
    if depth_v is not None:
        proximities.append(DEPTH_THIN_USD / max(depth_v, _EPS))
    quiet_m = 1.0 - _clamp(max(proximities), 0.0, 1.0)
    return RegimeTag("QUIET", _confidence(quiet_m), features,
                     ["no regime rule fired"])
