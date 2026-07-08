"""Auto-tuner: threshold-only, tighten-only.

Every tune interval with enough samples it may PROPOSE (never apply itself):
- raise min edge when realized edge chronically underperforms expectation
- raise the market-quality floor when losses cluster in low-quality markets
It can NEVER loosen thresholds or touch risk caps
(auto_tuning.never_increase_risk_automatically is enforced structurally:
there is no code path that lowers a threshold or raises a size).
"""
from __future__ import annotations


class AutoTuner:
    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self._observations: list[dict] = []
        self._last_tune_ms = 0
        self._proposed_min_edge_bump = 0.0
        self._proposed_quality_bump = 0.0

    def observe(self, trade: dict) -> None:
        """trade: {pnl, edge_expected, edge_realized, market_quality}"""
        self._observations.append(trade)
        if len(self._observations) > 2000:
            self._observations = self._observations[-1000:]

    def maybe_tune(self) -> list[dict]:
        a = self.cfg.auto_tuning
        if not a.enabled:
            return []
        now = self.clock.now_ms()
        if now - self._last_tune_ms < a.tune_every_minutes * 60_000:
            return []
        if len(self._observations) < a.min_sample_size:
            return []
        self._last_tune_ms = now
        changes: list[dict] = []
        obs = self._observations[-a.min_sample_size * 2:]

        exp = sum(o.get("edge_expected", 0.0) for o in obs)
        real = sum(o.get("edge_realized", 0.0) for o in obs)
        if exp > 0 and real / exp < 0.5 and self._proposed_min_edge_bump < 0.02:
            old = self._proposed_min_edge_bump
            self._proposed_min_edge_bump = min(0.02, old + 0.005)
            changes.append({"param": "min_edge_bump", "old": old,
                            "new": self._proposed_min_edge_bump, "ts_ms": now,
                            "reason": f"edge realization {real / exp:.2f} < 0.5 — tighten only"})

        low_q = [o for o in obs if o.get("market_quality", 100) < 60]
        if low_q:
            lq_pnl = sum(o.get("pnl", 0.0) for o in low_q)
            if lq_pnl < 0 and self._proposed_quality_bump < 10:
                old = self._proposed_quality_bump
                self._proposed_quality_bump = min(10.0, old + 5.0)
                changes.append({"param": "market_quality_floor_bump", "old": old,
                                "new": self._proposed_quality_bump, "ts_ms": now,
                                "reason": f"low-quality markets lost ${lq_pnl:.2f} — tighten only"})
        return changes

    @property
    def min_edge_bump(self) -> float:
        return self._proposed_min_edge_bump

    @property
    def quality_floor_bump(self) -> float:
        return self._proposed_quality_bump
