"""Pipeline stage latency benchmark (tick->signal->order->fill)."""
from __future__ import annotations

from poly_alpha_sniper.data.latency_tracker import LatencyTracker

STAGES = ("tick_to_signal", "signal_to_order", "order_to_fill", "book_update")


class LatencyBenchmark:
    def __init__(self, tracker: LatencyTracker | None = None):
        self.tracker = tracker or LatencyTracker()

    def record(self, stage: str, ms: float) -> None:
        self.tracker.record(stage, ms)

    def summary(self) -> dict:
        return self.tracker.snapshot()

    def to_records(self, ts_ms: int) -> list[dict]:
        out = []
        for stage, vals in self.tracker.snapshot().items():
            out.append({"ts_ms": ts_ms, "stage": stage, "p50": vals["p50"],
                        "p95": vals["p95"], "p99": vals["p99"], "n": vals["n"]})
        return out

    def healthy(self, stage: str = "signal_to_order", p95_limit_ms: float = 500.0) -> bool:
        return self.tracker.p95(stage) <= p95_limit_ms
