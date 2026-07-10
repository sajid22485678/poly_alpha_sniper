"""RESEARCH / SHADOW-ONLY: diagnostics and challenger evaluation only. Never
places orders, never affects the baseline trading decision or live readiness.

Hawkes-style burst / impulse-cluster detector.

WHY an exponential-decay approximation instead of a true Hawkes fit:
a Hawkes process models self-exciting event arrivals with conditional
intensity  lambda(t) = mu + sum_i alpha * exp(-beta * (t - t_i)).  Fitting
(mu, alpha, beta) by maximum likelihood requires an iterative numerical
optimizer, is unstable on the short, bursty samples we see per market, and
its solver path is not bit-for-bit reproducible across platforms.  For
shadow diagnostics we only need the *shape* of the excitation term, so we
freeze the kernel to a configurable half-life and compute the deterministic
sum  intensity(t) = sum_i m_i * 0.5 ** ((t - t_i) / half_life).  This is
exactly the Hawkes excitation with beta = ln(2) / half_life and per-event
weight m_i, evaluated in O(n) with no fitting.  True Hawkes MLE is deferred
until enough labeled burst episodes exist to validate a fitted kernel
against this fixed-kernel baseline.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

# Events whose age exceeds this many half-lives contribute < 0.1% of their
# original magnitude (2**-10 ~= 0.00098), so pruning them bounds memory
# while changing intensity by a negligible amount.
_PRUNE_HALF_LIVES = 10.0

# Saturation constant for cluster_score: score = intensity / (intensity + K).
# K = 2 means an intensity of 2 (e.g. two full-magnitude impulses "just now")
# maps to 0.5 -- the default burst threshold -- so "burst" intuitively means
# "at least a couple of strong impulses still fully alive".
_SATURATION_K = 2.0


@dataclass
class _Impulse:
    """One recorded impulse: timestamp in ms and non-negative magnitude."""

    ts_ms: int
    magnitude: float


@dataclass
class BurstDetector:
    """Deterministic exponential-decay burst detector over an impulse stream.

    Callers record impulses (trades, quote shocks, oracle jumps...) as they
    observe them; all queries are pure functions of (recorded events, now_ms)
    so the same event sequence always yields the same diagnostics.
    """

    decay_half_life_s: float = 10.0
    _events: list[_Impulse] = field(default_factory=list)
    _last_ts_ms: Optional[int] = field(default=None)

    def __post_init__(self) -> None:
        if not math.isfinite(self.decay_half_life_s) or self.decay_half_life_s <= 0.0:
            raise ValueError(
                f"decay_half_life_s must be a finite positive number, "
                f"got {self.decay_half_life_s!r}"
            )

    # ------------------------------------------------------------------ #
    # recording
    # ------------------------------------------------------------------ #
    def record_impulse(self, ts_ms: int, magnitude: float = 1.0) -> None:
        """Record one impulse.

        Non-finite or non-positive magnitudes are ignored (never fabricate a
        contribution, and negative magnitudes would make 'intensity' lose its
        meaning as a non-negative excitation level).  Out-of-order timestamps
        are accepted: the event list is kept sorted so decay math stays exact.
        """
        if not math.isfinite(magnitude) or magnitude <= 0.0:
            return
        event = _Impulse(ts_ms=int(ts_ms), magnitude=float(magnitude))
        if self._events and event.ts_ms < self._events[-1].ts_ms:
            # Rare out-of-order arrival: insert in timestamp order.
            lo, hi = 0, len(self._events)
            while lo < hi:
                mid = (lo + hi) // 2
                if self._events[mid].ts_ms <= event.ts_ms:
                    lo = mid + 1
                else:
                    hi = mid
            self._events.insert(lo, event)
        else:
            self._events.append(event)
        if self._last_ts_ms is None or event.ts_ms > self._last_ts_ms:
            self._last_ts_ms = event.ts_ms

    # ------------------------------------------------------------------ #
    # queries (pure functions of recorded events + now_ms)
    # ------------------------------------------------------------------ #
    def intensity(self, now_ms: int) -> float:
        """Decayed excitation: sum of magnitude * 0.5 ** (age_s / half_life).

        Events older than 10 half-lives are pruned (negligible contribution,
        bounded memory).  Ages are clamped at 0 so a clock-skewed 'future'
        event contributes at most its raw magnitude instead of exploding the
        sum through a negative exponent.
        """
        self._prune(now_ms)
        half_life = self.decay_half_life_s
        total = 0.0
        for ev in self._events:
            age_s = max(0.0, (now_ms - ev.ts_ms) / 1000.0)
            total += ev.magnitude * 0.5 ** (age_s / half_life)
        return total

    def impulse_count(self, now_ms: int, window_s: float) -> int:
        """Number of retained impulses within the trailing window.

        A raw count complements 'intensity': many tiny impulses and one huge
        one can share an intensity value, but they are different regimes.
        """
        if window_s < 0:
            return 0
        self._prune(now_ms)
        cutoff_ms = now_ms - window_s * 1000.0
        return sum(1 for ev in self._events if ev.ts_ms >= cutoff_ms)

    def time_since_last_impulse_s(self, now_ms: int) -> Optional[float]:
        """Seconds since the newest impulse ever recorded, or None if none.

        Tracked separately from the pruned event buffer so a long-quiet
        detector reports a large truthful age rather than pretending no
        impulse ever happened.  Clamped at 0 for clock-skewed future events.
        """
        if self._last_ts_ms is None:
            return None
        return max(0.0, (now_ms - self._last_ts_ms) / 1000.0)

    def cluster_score(self, now_ms: int) -> float:
        """Saturating map of intensity into [0, 1]: i / (i + 2).

        WHY this map: intensity is unbounded above, but downstream consumers
        want a stable, comparable-across-markets score.  x / (x + K) is
        monotone, hits 0 at zero intensity, and approaches (never reaches) 1,
        so no single outlier magnitude can pin the score.  This is a shape
        score, NOT a calibrated probability of anything.
        """
        i = self.intensity(now_ms)
        score = i / (i + _SATURATION_K)
        # Defensive clamp; mathematically already in [0, 1).
        return min(1.0, max(0.0, score))

    def burst_active(self, now_ms: int, threshold: float = 0.5) -> bool:
        """True when cluster_score >= threshold (default 0.5, i.e. the
        decayed excitation of ~two fresh unit impulses)."""
        return self.cluster_score(now_ms) >= threshold

    def snapshot(self, now_ms: int) -> dict:
        """One-call diagnostic bundle for logging / research dashboards."""
        return {
            "intensity": self.intensity(now_ms),
            "cluster_score": self.cluster_score(now_ms),
            "count_10s": self.impulse_count(now_ms, 10.0),
            "count_30s": self.impulse_count(now_ms, 30.0),
            "time_since_last_impulse_s": self.time_since_last_impulse_s(now_ms),
        }

    # ------------------------------------------------------------------ #
    # internals
    # ------------------------------------------------------------------ #
    def _prune(self, now_ms: int) -> None:
        """Drop events older than 10 half-lives (contribution < 0.1%)."""
        cutoff_ms = now_ms - _PRUNE_HALF_LIVES * self.decay_half_life_s * 1000.0
        if not self._events or self._events[0].ts_ms >= cutoff_ms:
            return
        self._events = [ev for ev in self._events if ev.ts_ms >= cutoff_ms]
