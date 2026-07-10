"""RESEARCH / SHADOW-ONLY: diagnostics and challenger evaluation only. Never places orders, never affects the baseline trading decision or live readiness.

Bayesian EV calibrator (challenger).

WHY: the baseline edge model produces a raw prior probability. This module
re-expresses that prior in log-odds space, applies small, individually
bounded evidence adjustments (shock strength, anchor distance, CEX
staleness, book quality, spread, regime, time-to-close), and converts back
to a posterior probability. Working in log-odds keeps updates additive and
symmetric around 0.5, and per-signal caps plus a global cap on the total
adjustment (+/-1.5 log-odds) guarantee no single noisy input can swing the
posterior violently. Output is purely diagnostic: it lets us compare
challenger EV against baseline EV offline without touching live decisions.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

# Probability outputs are always clamped to this band: it guards log(0) /
# division-by-zero in the odds transform and reflects that we never claim
# certainty on a binary market.
PROB_FLOOR = 0.01
PROB_CEIL = 0.99

# Global cap on the summed evidence adjustment, in log-odds. WHY: even if
# every signal agrees, +/-1.5 log-odds (~4.5x odds shift) is the most we let
# soft evidence move the prior.
TOTAL_ADJUSTMENT_CAP = 1.5

# Fixed regime offsets, in log-odds. WHY: regimes are categorical context —
# impulse regimes historically favor momentum entries (+0.2), fakeout-prone
# regimes are the single worst context (-0.4), chop dilutes signal (-0.2),
# final-window chaos adds terminal noise (-0.3), quiet is neutral (0).
REGIME_LOG_ODDS: dict[str, float] = {
    "IMPULSE": 0.2,
    "FAKEOUT_PRONE": -0.4,
    "CHOP": -0.2,
    "FINAL_WINDOW_CHAOS": -0.3,
    "QUIET": 0.0,
}

# Per-signal saturation points (see evidence_log_odds for the mappings).
_SHOCK_MAX_BONUS = 0.5          # at shock_strength == 2.0
_ANCHOR_CAP = 0.4               # saturates at |anchor_distance_pct| == 2.0
_ANCHOR_SLOPE = 0.2             # log-odds per favorable percent of distance
_STALENESS_MAX_PENALTY = 0.5    # at cex_staleness_ms >= 8000
_STALENESS_SATURATION_MS = 8000.0
_BOOK_MAX_PENALTY = 0.3         # at book_quality == 0.0
_SPREAD_MAX_PENALTY = 0.3       # at spread >= 0.10 (10 cents)
_SPREAD_SATURATION = 0.10
_CLOSE_MAX_PENALTY = 0.2        # at time_to_close_s == 0
_CLOSE_WINDOW_S = 60.0


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _as_finite_float(value: object) -> Optional[float]:
    """Coerce to float only if it is a real, finite number; else None.

    WHY: evidence dicts come from heterogeneous telemetry — malformed values
    must be ignored (neutral), never silently fabricated into adjustments.
    bool is excluded because True/False are ints in Python and almost always
    indicate a caller bug rather than a magnitude.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    return None


def _prob_to_log_odds(p: float) -> float:
    """Clamp first so log(0) and division by zero are impossible."""
    p = _clamp(p, PROB_FLOOR, PROB_CEIL)
    return math.log(p / (1.0 - p))


def _log_odds_to_prob(log_odds: float) -> float:
    """Numerically stable sigmoid, then clamp to [PROB_FLOOR, PROB_CEIL]."""
    if log_odds >= 0.0:
        p = 1.0 / (1.0 + math.exp(-log_odds))
    else:
        e = math.exp(log_odds)
        p = e / (1.0 + e)
    return _clamp(p, PROB_FLOOR, PROB_CEIL)


def evidence_log_odds(evidence: Optional[dict]) -> dict[str, float]:
    """Map each recognized evidence key to a bounded log-odds contribution.

    Mappings (each capped so no single noisy signal dominates):
    - shock_strength (0..2): only above-baseline shocks (>1) add conviction,
      linear from 0 at 1.0 up to +0.5 at 2.0. Values are clamped into [0, 2];
      sub-baseline shocks are neutral, never punitive.
    - anchor_distance_pct (signed; positive = favorable direction): 0.2
      log-odds per percent, capped at +/-0.4 (saturates at |2%|). Distance in
      the unfavorable direction subtracts symmetrically.
    - cex_staleness_ms: a stale reference price weakens the prior linearly,
      from 0 at fresh (0 ms) down to -0.5 at >= 8000 ms. Negative values are
      treated as fresh.
    - book_quality (0..1): a thin/degraded book penalizes linearly, -0.3 at
      quality 0.0, 0 at quality 1.0. Values are clamped into [0, 1].
    - spread: wide spreads mean the executable price is uncertain; linear
      penalty up to -0.3 at spread >= 0.10 (10c). Negative spreads treated
      as 0.
    - regime (str): fixed offsets — IMPULSE +0.2, FAKEOUT_PRONE -0.4,
      CHOP -0.2, FINAL_WINDOW_CHAOS -0.3, QUIET 0. Unknown regimes are
      neutral (0), matched case-insensitively.
    - time_to_close_s: under 60s to close, terminal noise penalizes linearly
      up to -0.2 at 0s (or less). At >= 60s the contribution is 0.

    Unrecognized keys and malformed values are ignored — the returned dict
    contains only recognized keys that carried a usable value, so it doubles
    as a per-signal diagnostic log.
    """
    contributions: dict[str, float] = {}
    if not evidence:
        return contributions

    v = _as_finite_float(evidence.get("shock_strength"))
    if v is not None:
        s = _clamp(v, 0.0, 2.0)
        contributions["shock_strength"] = _SHOCK_MAX_BONUS * max(0.0, s - 1.0)

    v = _as_finite_float(evidence.get("anchor_distance_pct"))
    if v is not None:
        contributions["anchor_distance_pct"] = _clamp(
            _ANCHOR_SLOPE * v, -_ANCHOR_CAP, _ANCHOR_CAP
        )

    v = _as_finite_float(evidence.get("cex_staleness_ms"))
    if v is not None:
        frac = _clamp(v, 0.0, _STALENESS_SATURATION_MS) / _STALENESS_SATURATION_MS
        contributions["cex_staleness_ms"] = -_STALENESS_MAX_PENALTY * frac

    v = _as_finite_float(evidence.get("book_quality"))
    if v is not None:
        q = _clamp(v, 0.0, 1.0)
        contributions["book_quality"] = -_BOOK_MAX_PENALTY * (1.0 - q)

    v = _as_finite_float(evidence.get("spread"))
    if v is not None:
        frac = _clamp(v, 0.0, _SPREAD_SATURATION) / _SPREAD_SATURATION
        contributions["spread"] = -_SPREAD_MAX_PENALTY * frac

    regime = evidence.get("regime")
    if isinstance(regime, str):
        contributions["regime"] = REGIME_LOG_ODDS.get(regime.strip().upper(), 0.0)

    v = _as_finite_float(evidence.get("time_to_close_s"))
    if v is not None:
        t = _clamp(v, 0.0, _CLOSE_WINDOW_S)
        contributions["time_to_close_s"] = (
            -_CLOSE_MAX_PENALTY * (_CLOSE_WINDOW_S - t) / _CLOSE_WINDOW_S
        )

    return contributions


def posterior_probability(prior: float, evidence: Optional[dict]) -> float:
    """Bayesian-style update: prior -> log-odds, add capped evidence, back.

    WHY log-odds: adjustments compose additively and behave symmetrically
    around 0.5, so a +0.2 nudge means the same odds multiplier regardless of
    where the prior sits. The summed adjustment is capped to
    [-TOTAL_ADJUSTMENT_CAP, +TOTAL_ADJUSTMENT_CAP] and the result clamped to
    [0.01, 0.99]. An invalid prior yields explicit neutral 0.5 (then updated
    by evidence) rather than a fabricated number; empty/missing evidence
    returns the clamped prior unchanged.
    """
    p = _as_finite_float(prior)
    if p is None:
        p = 0.5  # explicit neutral: no usable prior information
    total = _clamp(
        sum(evidence_log_odds(evidence).values()),
        -TOTAL_ADJUSTMENT_CAP,
        TOTAL_ADJUSTMENT_CAP,
    )
    return _log_odds_to_prob(_prob_to_log_odds(p) + total)


def expected_value(
    probability: float, executable_price: float, fee_rate: float = 0.0
) -> Optional[float]:
    """EV per share of buying a binary YES at ``executable_price``:

        ev = p * (1 - price) - (1 - p) * price - fee_rate * price

    WHY: a winning share pays out (1 - price), a losing share forfeits the
    price paid, and fees are modeled proportional to notional spent
    (fee_rate * price). Returns None when probability or price is not a
    finite number — an EV of unknown inputs must not be fabricated. Negative
    fee rates are treated as 0.
    """
    p = _as_finite_float(probability)
    price = _as_finite_float(executable_price)
    if p is None or price is None:
        return None
    fee = _as_finite_float(fee_rate)
    fee = 0.0 if fee is None else max(0.0, fee)
    return p * (1.0 - price) - (1.0 - p) * price - fee * price


@dataclass
class BayesianEv:
    """Diagnostic record comparing prior vs evidence-adjusted EV.

    ``evidence_log_odds`` holds the per-signal contributions actually
    applied, and ``total_adjustment`` is their sum after the +/-1.5 cap —
    together they make every posterior fully auditable.
    """

    prior: float
    posterior: float
    ev_prior: Optional[float]
    ev_posterior: Optional[float]
    evidence_log_odds: dict[str, float] = field(default_factory=dict)
    total_adjustment: float = 0.0


def calibrate_ev(
    prior_probability: float,
    executable_price: float,
    fee_rate: float,
    evidence: Optional[dict] = None,
) -> BayesianEv:
    """Compute prior EV and posterior EV under the same executable price.

    The posterior is bounded to [0.01, 0.99] and the total log-odds
    adjustment is capped to [-1.5, +1.5]. With empty evidence the posterior
    equals the clamped prior and ev_posterior equals ev_prior — a neutral,
    non-fabricated result. EV fields are None when the price is unusable.
    """
    p = _as_finite_float(prior_probability)
    if p is None:
        p = 0.5  # explicit neutral prior
    prior = _clamp(p, PROB_FLOOR, PROB_CEIL)

    contributions = evidence_log_odds(evidence)
    total = _clamp(
        sum(contributions.values()), -TOTAL_ADJUSTMENT_CAP, TOTAL_ADJUSTMENT_CAP
    )
    posterior = _log_odds_to_prob(_prob_to_log_odds(prior) + total)

    return BayesianEv(
        prior=prior,
        posterior=posterior,
        ev_prior=expected_value(prior, executable_price, fee_rate),
        ev_posterior=expected_value(posterior, executable_price, fee_rate),
        evidence_log_odds=contributions,
        total_adjustment=total,
    )
