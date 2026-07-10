"""RESEARCH / SHADOW-ONLY: diagnostics and challenger evaluation only. Never
places orders, never affects the baseline trading decision or live readiness.

Markov transition tracker over discrete market states.

WHY: the baseline strategy reacts to instantaneous features. A first-order
Markov view over coarse market states lets us ask, offline, questions like
"after a SHOCK_UP, how often does momentum continue vs. reverse?" — a cheap
regime diagnostic to challenge (not replace) the live decision logic.

classify_state rule table (evaluated top-down, first match wins):

  P1  any input non-finite (NaN/inf)          -> QUIET            (neutral,
      never fabricate a directional read from corrupt data)
  P2  time_to_close_s <= 45:
        shock_score >= 1.0 or vol >= VOL_HIGH -> FINAL_WINDOW_CHAOS
        otherwise                              -> FINAL_WINDOW
  P3  shock_score >= 1.0                       -> SHOCK_UP  if dir >= 0
                                                  SHOCK_DOWN otherwise
  P4  prev_state in {SHOCK_UP, SHOCK_DOWN} and dir opposes the prior shock
      direction (prev_state is the 30s-momentum proxy) -> REVERSAL
  P5  0.5 <= shock_score < 1.0                 -> BUILDUP_UP if dir >= 0
                                                  BUILDUP_DOWN otherwise
  P6  direction_flips_30s >= 3                 -> CHOPPY
  P7  otherwise                                -> QUIET

where dir = sign(ret_2s), falling back to sign(zscore) when ret_2s == 0
(the z-score carries the same directional information at a longer horizon,
so it is the natural tie-breaker). Ties (both zero) count as "up" — an
arbitrary but fixed convention so classification is fully deterministic.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

STATES: tuple[str, ...] = (
    "QUIET",
    "BUILDUP_UP",
    "BUILDUP_DOWN",
    "SHOCK_UP",
    "SHOCK_DOWN",
    "CHOPPY",
    "REVERSAL",
    "FINAL_WINDOW",
    "FINAL_WINDOW_CHAOS",
)

# Above this per-window volatility the final window is treated as chaotic:
# fills are unreliable and state persistence breaks down.
VOL_HIGH_THRESHOLD: float = 0.02

# Scalar probability outputs are clamped here so downstream consumers never
# see a degenerate 0.0/1.0 that would blow up log-odds or Kelly-style math.
PROB_FLOOR: float = 0.01
PROB_CEIL: float = 0.99

# Same-direction "families": continuation of an UP state means landing in any
# UP state next (a shock decaying into a buildup still continues the move).
_UP_FAMILY = frozenset({"BUILDUP_UP", "SHOCK_UP"})
_DOWN_FAMILY = frozenset({"BUILDUP_DOWN", "SHOCK_DOWN"})


def _clamp(p: float) -> float:
    """Clamp a scalar probability into [PROB_FLOOR, PROB_CEIL]."""
    return max(PROB_FLOOR, min(PROB_CEIL, p))


def classify_state(
    ret_2s: float,
    zscore: float,
    shock_score: float,
    direction_flips_30s: int,
    time_to_close_s: float,
    vol: float,
    prev_state: Optional[str] = None,
) -> str:
    """Map instantaneous features to one of STATES via the docstring's rule
    table. Deterministic: same inputs always yield the same state, which is
    what makes downstream transition counts reproducible.
    """
    for x in (ret_2s, zscore, shock_score, time_to_close_s, vol):
        if not math.isfinite(x):
            return "QUIET"

    # Direction: 2s return, tie-broken by z-score; ties resolve "up".
    raw = ret_2s if ret_2s != 0.0 else zscore
    dir_up = raw >= 0.0

    if time_to_close_s <= 45.0:
        if shock_score >= 1.0 or vol >= VOL_HIGH_THRESHOLD:
            return "FINAL_WINDOW_CHAOS"
        return "FINAL_WINDOW"

    if shock_score >= 1.0:
        return "SHOCK_UP" if dir_up else "SHOCK_DOWN"

    # Reversal: short-horizon direction now opposes the direction of a shock
    # still in force at the 30s horizon (prev_state acts as the 30s proxy).
    if prev_state == "SHOCK_UP" and ret_2s < 0.0:
        return "REVERSAL"
    if prev_state == "SHOCK_DOWN" and ret_2s > 0.0:
        return "REVERSAL"

    if 0.5 <= shock_score < 1.0:
        return "BUILDUP_UP" if dir_up else "BUILDUP_DOWN"

    if direction_flips_30s >= 3:
        return "CHOPPY"

    return "QUIET"


@dataclass
class MarkovTransitionTracker:
    """Counts observed state->state transitions and exposes smoothed
    probabilities.

    WHY Laplace add-1 smoothing: with 9 states and short sessions most cells
    are never observed; add-1 keeps every transition strictly positive so
    log-likelihood comparisons between challenger models never hit -inf,
    and it converges to the empirical frequencies as counts grow.
    """

    _counts: dict[str, dict[str, int]] = field(default_factory=dict)
    _n: int = 0

    @property
    def n_observations(self) -> int:
        return self._n

    def observe(self, prev_state: str, next_state: str) -> None:
        """Record one prev->next transition. Unknown states raise ValueError
        rather than silently polluting the matrix."""
        if prev_state not in STATES:
            raise ValueError(f"unknown prev_state: {prev_state!r}")
        if next_state not in STATES:
            raise ValueError(f"unknown next_state: {next_state!r}")
        row = self._counts.setdefault(prev_state, {})
        row[next_state] = row.get(next_state, 0) + 1
        self._n += 1

    def transition_matrix(self) -> dict[str, dict[str, float]]:
        """Row-stochastic matrix with Laplace add-1 smoothing.

        Rows sum to 1.0 exactly (they are distributions, so they are NOT
        individually clamped — clamping is applied only to the scalar
        convenience outputs below). With zero observations every row is
        uniform (1/9): explicit neutrality instead of fabricated structure.
        """
        n_states = len(STATES)
        matrix: dict[str, dict[str, float]] = {}
        for s in STATES:
            row_counts = self._counts.get(s, {})
            total = sum(row_counts.values())
            denom = total + n_states  # add-1 smoothing over n_states cells
            matrix[s] = {
                t: (row_counts.get(t, 0) + 1) / denom for t in STATES
            }
        return matrix

    def _family(self, state: str) -> frozenset[str]:
        """Same-direction family of a state; non-directional states form a
        singleton family (continuation == staying put)."""
        if state in _UP_FAMILY:
            return _UP_FAMILY
        if state in _DOWN_FAMILY:
            return _DOWN_FAMILY
        return frozenset({state})

    def continuation_probability(self, state: str) -> float:
        """P(next state stays in the same direction family | current state),
        clamped to [0.01, 0.99]. For non-directional states this is simply
        the self-transition (persistence) probability."""
        if state not in STATES:
            raise ValueError(f"unknown state: {state!r}")
        row = self.transition_matrix()[state]
        return _clamp(sum(row[t] for t in self._family(state)))

    def reversal_probability(self, state: str) -> float:
        """P(next state flips to the opposite direction family or is
        REVERSAL | current state), clamped to [0.01, 0.99]. Non-directional
        states have no opposite family, so only REVERSAL counts — the honest
        answer rather than an invented complement."""
        if state not in STATES:
            raise ValueError(f"unknown state: {state!r}")
        row = self.transition_matrix()[state]
        if state in _UP_FAMILY:
            targets = _DOWN_FAMILY | {"REVERSAL"}
        elif state in _DOWN_FAMILY:
            targets = _UP_FAMILY | {"REVERSAL"}
        else:
            targets = frozenset({"REVERSAL"})
        return _clamp(sum(row[t] for t in targets))
