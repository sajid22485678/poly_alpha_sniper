"""Tests for the data-driven model-health quarantine gate."""
from __future__ import annotations

from pathlib import Path

import pytest

from lite_frequency_v4.model_health import (
    ModelHealthConfig, ModelHealthQuarantine,
)


class _FakeStore:
    """Minimal store-like exposing the ``query`` contract the quarantine reads."""

    def __init__(self, rows):
        self._rows = rows
        self.calls = 0

    def query(self, sql, params=()):
        self.calls += 1
        return list(self._rows)


def _cfg(**overrides):
    base = dict(
        enabled=True, min_observations=5,
        min_profit_factor=0.90, min_expectancy=-0.10,
        max_loss_asymmetry=3.0, cooldown_s=100.0, resample_s=50.0,
    )
    base.update(overrides)
    return ModelHealthConfig(**base)


def test_disabled_quarantine_never_quarantines():
    q = ModelHealthQuarantine(_cfg(enabled=False))
    store = _FakeStore([
        {"model": "bad_model", "pnl": -1.0}] * 20)
    q.maybe_resample(store, "cohort")
    assert q.quarantined_models() == frozenset()
    assert q.is_quarantined("bad_model") is False
    # Disabled gate does not even hit the store on the hot path.
    assert store.calls == 0


def test_underperforming_model_is_quarantined():
    q = ModelHealthQuarantine(_cfg())
    # 10 trades, all small wins and one huge loss -> PF < 0.9, exp < -0.10.
    rows = [{"model": "window_open_displacement", "pnl": p} for p in
            [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, -20.0]]
    store = _FakeStore(rows)
    q.maybe_resample(store, "cohort")
    assert "window_open_displacement" in q.quarantined_models()
    assert q.is_quarantined("window_open_displacement") is True
    snap = q.snapshot()
    stat = snap["model_statistics"]["window_open_displacement"]
    assert stat["quarantined"] is True
    assert "profit_factor_below_floor" in stat["reason"]
    assert "expectancy_below_floor" in stat["reason"]


def test_profitable_model_is_protected():
    q = ModelHealthQuarantine(_cfg())
    rows = [{"model": "short_horizon_trend", "pnl": p} for p in
            [2.0, 2.0, 2.0, 2.0, -1.0, 2.0, 2.0, 2.0]]
    store = _FakeStore(rows)
    q.maybe_resample(store, "cohort")
    assert "short_horizon_trend" not in q.quarantined_models()
    assert q.is_quarantined("short_horizon_trend") is False


def test_below_observation_floor_does_not_quarantine():
    q = ModelHealthQuarantine(_cfg(min_observations=30))
    # Only 2 observations: even catastrophic loss must NOT quarantine (fail to
    # the safe non-action state rather than acting on noise).
    rows = [{"model": "rare_model", "pnl": -10.0}, {"model": "rare_model", "pnl": -10.0}]
    store = _FakeStore(rows)
    q.maybe_resample(store, "cohort")
    assert "rare_model" not in q.quarantined_models()


def test_resample_cadence_is_respected():
    clock = {"t": 100.0}
    q = ModelHealthQuarantine(_cfg(resample_s=50.0), monotonic=lambda: clock["t"])
    store = _FakeStore([{"model": "m", "pnl": 1.0}])
    q.maybe_resample(store, "cohort")
    assert store.calls == 1
    first = store.calls
    # Within the cadence: no re-query.
    clock["t"] = 110.0
    q.maybe_resample(store, "cohort")
    assert store.calls == first
    # After the cadence: re-query.
    clock["t"] = 160.0
    q.maybe_resample(store, "cohort")
    assert store.calls == first + 1


def test_cooldown_release_after_window():
    clock = {"t": 10.0}
    q = ModelHealthQuarantine(_cfg(cooldown_s=100.0, resample_s=1.0),
                              monotonic=lambda: clock["t"])
    rows = [{"model": "bad", "pnl": p} for p in [1.0] * 9 + [-20.0]]
    store = _FakeStore(rows)
    q.maybe_resample(store, "cohort")
    assert q.is_quarantined("bad") is True
    # Inside cooldown.
    clock["t"] = 60.0
    assert q.is_quarantined("bad") is True
    # After cooldown elapses and no fresh quarantine re-locks it: released.
    clock["t"] = 200.0
    assert q.is_quarantined("bad") is False


def test_apply_realized_pnls_directly():
    """The engine applies rows fetched via the read worker on its own thread."""
    q = ModelHealthQuarantine(_cfg())
    grouped = {"bad": [1.0] * 9 + [-20.0], "good": [2.0, 2.0, -1.0, 2.0]}
    q.apply_realized_pnls(grouped)
    assert "bad" in q.quarantined_models()
    assert "good" not in q.quarantined_models()


def test_config_validation_rejects_bad_inputs():
    with pytest.raises(ValueError):
        ModelHealthConfig(min_observations=0)
    with pytest.raises(ValueError):
        ModelHealthConfig(cooldown_s=0)
    with pytest.raises(TypeError):
        ModelHealthConfig(enabled="yes")
