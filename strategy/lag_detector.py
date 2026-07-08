"""Polymarket repricing-lag detection.

Tracks implied probability per token over time; after a shock, measures how
much of the fair-vs-implied gap remains and how fast it is closing.
"""
from __future__ import annotations

from collections import deque

from poly_alpha_sniper.core.contracts import Shock


class LagDetector:
    def __init__(self, clock, history_ms: int = 60_000):
        self.clock = clock
        self.history_ms = history_ms
        self._series: dict[str, deque[tuple[int, float]]] = {}

    def observe(self, token_id: str, implied_prob: float, ts_ms: int | None = None) -> None:
        ts = ts_ms if ts_ms is not None else self.clock.now_ms()
        buf = self._series.setdefault(token_id, deque())
        buf.append((ts, implied_prob))
        cutoff = ts - self.history_ms
        while buf and buf[0][0] < cutoff:
            buf.popleft()

    def repricing_speed_per_s(self, token_id: str, window_ms: int = 3000) -> float:
        buf = self._series.get(token_id)
        if not buf or len(buf) < 2:
            return 0.0
        now = buf[-1][0]
        past = None
        for t, p in reversed(buf):
            if now - t >= window_ms:
                past = (t, p)
                break
        if past is None:
            past = buf[0]
        dt_s = (now - past[0]) / 1000.0
        if dt_s <= 0:
            return 0.0
        return (buf[-1][1] - past[1]) / dt_s

    def assess(self, shock: Shock, fair_p: float, current_implied: float,
               token_id: str, min_gap: float = 0.03) -> dict:
        gap = fair_p - current_implied
        speed = self.repricing_speed_per_s(token_id)
        lag_duration_ms = self.clock.now_ms() - shock.ts_ms
        # lag is real when the gap persists and repricing toward fair is slow
        closing_fast = abs(speed) > abs(gap) / 3.0 and (speed * gap) > 0
        lag_detected = abs(gap) >= min_gap and not closing_fast
        return {
            "lag_detected": lag_detected,
            "repricing_gap": gap,
            "repricing_speed_per_s": speed,
            "lag_duration_ms": lag_duration_ms,
            "edge_before_spread": gap,
        }
