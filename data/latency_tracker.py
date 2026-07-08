"""Ring-buffer latency percentiles per pipeline stage."""
from __future__ import annotations

from collections import deque


class LatencyTracker:
    def __init__(self, maxlen: int = 500):
        self._stages: dict[str, deque[float]] = {}
        self.maxlen = maxlen

    def record(self, stage: str, ms: float) -> None:
        if stage not in self._stages:
            self._stages[stage] = deque(maxlen=self.maxlen)
        self._stages[stage].append(float(ms))

    def _pct(self, stage: str, q: float) -> float:
        buf = self._stages.get(stage)
        if not buf:
            return 0.0
        data = sorted(buf)
        idx = min(len(data) - 1, int(q * (len(data) - 1)))
        return data[idx]

    def p50(self, stage: str) -> float:
        return self._pct(stage, 0.50)

    def p95(self, stage: str) -> float:
        return self._pct(stage, 0.95)

    def p99(self, stage: str) -> float:
        return self._pct(stage, 0.99)

    def snapshot(self) -> dict:
        return {stage: {"p50": round(self.p50(stage), 1),
                        "p95": round(self.p95(stage), 1),
                        "p99": round(self.p99(stage), 1),
                        "n": len(buf)}
                for stage, buf in self._stages.items()}
