"""Automatic, fail-closed model-health quarantine for the V4 edge ensemble.

A member model whose rolling fee-net performance crosses a minimum-observation
threshold and breaches both the profit-factor and expectancy floors (and
optionally a loss-asymmetry cap) is quarantined for a cooldown.  While
quarantined its ensemble contribution is zeroed and entries it would have
driven are rejected.

The gate is **data-driven from realized cohort performance**, not a hard-coded
model kill: it reads the verified-terminal trade set attributed by
``candidates.dominant_model`` and recomputes the same fee-net arithmetic the
metrics module uses.  This makes the quarantine generalize to any model that
underperforms in production rather than overfitting to the current cohort.

Health is recomputed on a bounded cadence (``resample_s``) and cached; the
hot evaluation path only reads the cached quarantined set.  All decisions are
auditable: the last computed per-model statistics and the active quarantine
set with reasons are exposed via :meth:`snapshot`.
"""
from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol


class _StoreLike(Protocol):
    def query(self, sql: str, params: tuple[Any, ...] = ...) -> list[dict[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class ModelHealthConfig:
    enabled: bool = True
    min_observations: int = 30
    min_profit_factor: float = 0.90
    min_expectancy: float = -0.10
    max_loss_asymmetry: float = 3.0
    cooldown_s: float = 1_800.0
    resample_s: float = 300.0

    def __post_init__(self) -> None:
        if isinstance(self.enabled, bool):
            pass
        else:
            raise TypeError("model_health enabled must be bool")
        if (isinstance(self.min_observations, bool)
                or not isinstance(self.min_observations, int)
                or self.min_observations < 1):
            raise ValueError("model_health min_observations must be a positive int")
        for name, value in (
            ("min_profit_factor", self.min_profit_factor),
            ("min_expectancy", self.min_expectancy),
            ("max_loss_asymmetry", self.max_loss_asymmetry),
            ("cooldown_s", self.cooldown_s),
            ("resample_s", self.resample_s),
        ):
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(float(value))):
                raise ValueError(f"model_health {name} must be a finite number")
        if self.cooldown_s <= 0 or self.resample_s <= 0:
            raise ValueError("model_health cooldown_s/resample_s must be positive")
        if self.min_profit_factor < 0:
            raise ValueError("model_health min_profit_factor must be >= 0")


@dataclass(slots=True)
class ModelStatistic:
    model_name: str
    observations: int
    net_pnl: float
    profit_factor: float
    expectancy: float
    average_win: float
    average_loss: float
    loss_asymmetry: float
    quarantined: bool
    reason: str = ""


@dataclass(slots=True)
class _QuarantineEntry:
    model_name: str
    quarantined_until_monotonic: float
    reason: str
    statistic: ModelStatistic


def _performance(pnls: list[float]) -> dict[str, float]:
    if not pnls:
        return {"count": 0, "net_pnl": 0.0, "pf": 0.0, "exp": 0.0,
                "avg_win": 0.0, "avg_loss": 0.0, "asymmetry": 0.0}
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_profit = sum(wins)
    gross_loss = sum(losses)  # negative
    pf = gross_profit / abs(gross_loss) if gross_loss != 0 else (
        float("inf") if gross_profit > 0 else 0.0)
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0
    asymmetry = abs(avg_loss) / avg_win if avg_win > 0 else float("inf")
    return {
        "count": len(pnls),
        "net_pnl": sum(pnls),
        "pf": pf,
        "exp": sum(pnls) / len(pnls),
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "asymmetry": asymmetry,
    }


class ModelHealthQuarantine:
    """Thread-safe, cached, data-driven model-health quarantine gate."""

    def __init__(self, config: ModelHealthConfig, *, monotonic=time.monotonic) -> None:
        self._config = config
        self._monotonic = monotonic
        self._lock = threading.RLock()
        self._last_resample_monotonic: float = 0.0
        self._statistics: dict[str, ModelStatistic] = {}
        self._quarantine: dict[str, _QuarantineEntry] = {}

    @property
    def config(self) -> ModelHealthConfig:
        return self._config

    def _load_model_pnls(
        self, store: _StoreLike, cohort: str,
    ) -> dict[str, list[float]]:
        """Return per-dominant-model realized net-PnL lists for the cohort.

        Mirrors the attribution join in ``metrics.performance_metrics``:
        verified terminal pnl_records attributed via candidates.dominant_model.
        """
        rows = store.query(
            """
            SELECT c.dominant_model AS model, p.net_pnl AS pnl
            FROM pnl_records p
            JOIN entries e          ON e.entry_id   = p.entry_id
            JOIN candidates c       ON c.candidate_id = e.candidate_id
            JOIN runtime_sessions rs ON rs.session_id  = e.session_id
            WHERE rs.cohort = ?
              AND p.verified = 1
            """,
            (cohort,),
        )
        grouped: dict[str, list[float]] = {}
        for row in rows:
            model = str(row.get("model") or "UNKNOWN")
            pnl = row.get("pnl")
            if pnl is None:
                continue
            grouped.setdefault(model, []).append(float(pnl))
        return grouped

    def _compute_statistics(
        self, grouped: Mapping[str, list[float]],
    ) -> dict[str, ModelStatistic]:
        cfg = self._config
        stats: dict[str, ModelStatistic] = {}
        for model, pnls in grouped.items():
            perf = _performance(pnls)
            observations = int(perf["count"])
            reasons: list[str] = []
            quarantined = False
            if observations >= cfg.min_observations:
                if perf["pf"] < cfg.min_profit_factor:
                    reasons.append("profit_factor_below_floor")
                    quarantined = True
                if perf["exp"] < cfg.min_expectancy:
                    reasons.append("expectancy_below_floor")
                    quarantined = True
                if (math.isfinite(perf["asymmetry"])
                        and perf["asymmetry"] > cfg.max_loss_asymmetry):
                    reasons.append("loss_asymmetry_above_cap")
                    quarantined = True
            stats[model] = ModelStatistic(
                model_name=model,
                observations=observations,
                net_pnl=round(perf["net_pnl"], 6),
                profit_factor=round(perf["pf"], 6)
                if math.isfinite(perf["pf"]) else float("inf"),
                expectancy=round(perf["exp"], 6),
                average_win=round(perf["avg_win"], 6),
                average_loss=round(perf["avg_loss"], 6),
                loss_asymmetry=round(perf["asymmetry"], 6)
                if math.isfinite(perf["asymmetry"]) else float("inf"),
                quarantined=quarantined,
                reason=";".join(reasons),
            )
        return stats

    def maybe_resample(self, store: _StoreLike, cohort: str) -> None:
        """Recompute model health if the resample cadence has elapsed.

        Cheap on the hot path: a timestamp check under the lock.  The expensive
        SQL + arithmetic only runs at most once per ``resample_s``.
        """
        if not self._config.enabled:
            return
        now = self._monotonic()
        with self._lock:
            if (now - self._last_resample_monotonic) < self._config.resample_s:
                return
            self._last_resample_monotonic = now
        # Load + compute outside the lock so a slow read does not block readers.
        grouped = self._load_model_pnls(store, cohort)
        self.apply_realized_pnls(grouped)

    def apply_realized_pnls(self, grouped: Mapping[str, list[float]]) -> None:
        """Apply freshly loaded per-model realized PnL to the quarantine state.

        Decoupled from :meth:`maybe_resample` so a caller can fetch the rows on
        the read-only worker thread (the store enforces thread ownership) and
        apply them here on the engine thread.  Idempotent within a resample
        window; safe to call directly with pre-fetched data.
        """
        if not self._config.enabled:
            return
        stats = self._compute_statistics(grouped)
        with self._lock:
            self._statistics = stats
            now = self._monotonic()
            # Apply fresh quarantines; prune expired ones.
            for model, stat in stats.items():
                if stat.quarantined:
                    existing = self._quarantine.get(model)
                    if (existing is None
                            or existing.statistic.observations != stat.observations
                            or existing.reason != stat.reason):
                        self._quarantine[model] = _QuarantineEntry(
                            model_name=model,
                            quarantined_until_monotonic=(
                                now + self._config.cooldown_s),
                            reason=stat.reason or "model_health_quarantine",
                            statistic=stat,
                        )
            for model in list(self._quarantine.keys()):
                if model not in stats or not stats[model].quarantined:
                    entry = self._quarantine[model]
                    if not stats.get(model) or not stats[model].quarantined:
                        # Model recovered or dropped below observation floor:
                        # let the cooldown elapse, then release.
                        if now >= entry.quarantined_until_monotonic:
                            self._quarantine.pop(model, None)

    def needs_resample(self) -> bool:
        """True if the resample cadence has elapsed (cheap hot-path check)."""
        if not self._config.enabled:
            return False
        return (self._monotonic() - self._last_resample_monotonic) >= self._config.resample_s

    def is_quarantined(self, model_name: str) -> bool:
        """True if ``model_name`` is currently within its quarantine window."""
        if not self._config.enabled:
            return False
        now = self._monotonic()
        with self._lock:
            entry = self._quarantine.get(model_name)
            if entry is None:
                return False
            if now >= entry.quarantined_until_monotonic:
                # Cooldown elapsed: release unless the next resample re-locks it.
                self._quarantine.pop(model_name, None)
                return False
            return True

    def quarantined_models(self) -> frozenset[str]:
        """Return the current quarantined model set (snapshot, fail-closed)."""
        if not self._config.enabled:
            return frozenset()
        now = self._monotonic()
        with self._lock:
            live = frozenset(
                name for name, entry in self._quarantine.items()
                if now < entry.quarantined_until_monotonic
            )
            return live

    def snapshot(self) -> dict[str, Any]:
        """Auditable view of computed health and the active quarantine set."""
        now = self._monotonic()
        with self._lock:
            stats = {name: {
                "model_name": s.model_name,
                "observations": s.observations,
                "net_pnl": s.net_pnl,
                "profit_factor": s.profit_factor,
                "expectancy": s.expectancy,
                "average_win": s.average_win,
                "average_loss": s.average_loss,
                "loss_asymmetry": s.loss_asymmetry,
                "quarantined": s.quarantined,
                "reason": s.reason,
            } for name, s in self._statistics.items()}
            active = {
                name: {
                    "model_name": e.model_name,
                    "reason": e.reason,
                    "remaining_cooldown_s": round(
                        max(0.0, e.quarantined_until_monotonic - now), 1),
                    "observations": e.statistic.observations,
                    "profit_factor": e.statistic.profit_factor,
                    "expectancy": e.statistic.expectancy,
                }
                for name, e in self._quarantine.items()
                if now < e.quarantined_until_monotonic
            }
            return {
                "enabled": self._config.enabled,
                "min_observations": self._config.min_observations,
                "min_profit_factor": self._config.min_profit_factor,
                "min_expectancy": self._config.min_expectancy,
                "max_loss_asymmetry": self._config.max_loss_asymmetry,
                "cooldown_s": self._config.cooldown_s,
                "quarantined_models": sorted(active.keys()),
                "active_quarantines": active,
                "model_statistics": stats,
            }


__all__ = ["ModelHealthConfig", "ModelHealthQuarantine", "ModelStatistic"]
