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
        max_loss_asymmetry=3.0, severe_asymmetry_min_observations=5,
        cooldown_s=100.0, resample_s=50.0,
    )
    base.update(overrides)
    return ModelHealthConfig(**base)


# ---------------------------------------------------------------------------
# Exhaustive quarantine truth table.
#
#     normal_metric_breach    = pf_below AND expectancy_below
#     severe_asymmetry_breach = asymmetry > cap AND n >= asymmetry sample floor
#     quarantine              = n >= min_observations
#                               AND (normal_metric_breach OR severe_asymmetry)
#
# Each fixture below is checked against the three underlying predicates as
# well as the verdict, so a change to any threshold cannot silently reshape
# which row of the table a fixture actually exercises.
# ---------------------------------------------------------------------------

# name -> (pnls, pf_below, expectancy_below, asymmetry_above, quarantined)
_TRUTH_TABLE = {
    # PF 1.00, expectancy 0.00, asymmetry 1.0
    "neither_breached": ([1.0] * 5 + [-1.0] * 5, False, False, False, False),
    # PF 0.889, expectancy -0.056, asymmetry 0.9
    "profit_factor_only": ([1.0] * 4 + [-0.9] * 5, True, False, False, False),
    # PF 0.95, expectancy -0.25, asymmetry 1.05
    "expectancy_only": ([9.5] * 5 + [-10.0] * 5, False, True, False, False),
    # PF 0.50, expectancy -2.50, asymmetry 2.0
    "both_metrics": ([5.0] * 5 + [-10.0] * 5, True, True, False, True),
    # PF 4.00, expectancy 0.714, asymmetry 5.0
    "asymmetry_only": ([1.0] * 20 + [-5.0], False, False, True, True),
    # PF 0.45, expectancy -1.10, asymmetry 20.0
    "both_plus_asymmetry": ([1.0] * 9 + [-20.0], True, True, True, True),
}


@pytest.mark.parametrize(
    "case", sorted(_TRUTH_TABLE), ids=sorted(_TRUTH_TABLE))
def test_quarantine_truth_table(case):
    pnls, pf_below, expectancy_below, asymmetry_above, quarantined = (
        _TRUTH_TABLE[case])
    cfg = _cfg()
    q = ModelHealthQuarantine(cfg)
    q.apply_realized_pnls({case: pnls})
    stat = q.snapshot()["model_statistics"][case]

    assert stat["observations"] >= cfg.min_observations
    assert (stat["profit_factor"] < cfg.min_profit_factor) is pf_below
    assert (stat["expectancy"] < cfg.min_expectancy) is expectancy_below
    assert (stat["loss_asymmetry"] > cfg.max_loss_asymmetry) is asymmetry_above

    assert stat["quarantined"] is quarantined
    assert (case in q.quarantined_models()) is quarantined
    assert q.is_quarantined(case) is quarantined

    reasons = stat["reason"]
    if pf_below and expectancy_below:
        assert "profit_factor_below_floor" in reasons
        assert "expectancy_below_floor" in reasons
    else:
        # A single noisy metric must not even be reported as a breach reason.
        assert "profit_factor_below_floor" not in reasons
        assert "expectancy_below_floor" not in reasons
    assert ("loss_asymmetry_above_cap" in reasons) is asymmetry_above


def test_single_noisy_metric_never_quarantines_however_many_observations():
    """Scaling the sample does not turn a one-metric breach into a verdict."""
    q = ModelHealthQuarantine(_cfg(min_observations=5))
    q.apply_realized_pnls({
        "pf_only": ([1.0] * 4 + [-0.9] * 5) * 40,
        "expectancy_only": ([9.5] * 5 + [-10.0] * 5) * 40,
    })
    stats = q.snapshot()["model_statistics"]
    assert stats["pf_only"]["observations"] == 360
    assert stats["expectancy_only"]["observations"] == 400
    assert q.quarantined_models() == frozenset()


def test_asymmetry_alone_requires_its_own_sample_floor():
    """The independently-disqualifying branch states its own evidence bar."""
    pnls = [1.0] * 20 + [-5.0]
    lenient = ModelHealthQuarantine(_cfg(severe_asymmetry_min_observations=5))
    lenient.apply_realized_pnls({"m": pnls})
    assert "m" in lenient.quarantined_models()

    strict = ModelHealthQuarantine(_cfg(severe_asymmetry_min_observations=500))
    strict.apply_realized_pnls({"m": pnls})
    assert strict.quarantined_models() == frozenset()
    assert strict.snapshot()["model_statistics"]["m"]["quarantined"] is False


def test_all_losses_model_is_still_caught_by_the_metric_branch():
    """Infinite asymmetry is not a breach, but PF 0 and expectancy < 0 are."""
    q = ModelHealthQuarantine(_cfg())
    q.apply_realized_pnls({"only_losses": [-1.0] * 10})
    stat = q.snapshot()["model_statistics"]["only_losses"]
    assert stat["loss_asymmetry"] == float("inf")
    assert "loss_asymmetry_above_cap" not in stat["reason"]
    assert stat["quarantined"] is True
    assert "profit_factor_below_floor" in stat["reason"]


@pytest.mark.parametrize("case", sorted(_TRUTH_TABLE), ids=sorted(_TRUTH_TABLE))
def test_insufficient_observations_never_quarantines(case):
    pnls = _TRUTH_TABLE[case][0]
    q = ModelHealthQuarantine(_cfg(min_observations=len(pnls) + 1,
                                   severe_asymmetry_min_observations=1))
    q.apply_realized_pnls({case: pnls})
    assert q.quarantined_models() == frozenset()
    assert q.snapshot()["model_statistics"][case]["quarantined"] is False


def test_cooldown_holds_a_quarantine_even_once_the_breach_clears():
    clock = {"t": 10.0}
    q = ModelHealthQuarantine(_cfg(cooldown_s=100.0, resample_s=1.0),
                              monotonic=lambda: clock["t"])
    q.apply_realized_pnls({"m": [5.0] * 5 + [-10.0] * 5})
    assert q.is_quarantined("m") is True

    # Recovered data arrives while the cooldown is still running.
    clock["t"] = 60.0
    q.apply_realized_pnls({"m": [1.0] * 5 + [-1.0] * 5})
    assert q.snapshot()["model_statistics"]["m"]["quarantined"] is False
    assert q.is_quarantined("m") is True, "cooldown must be deterministic"
    assert "m" in q.quarantined_models()


def test_recovery_releases_only_after_the_cooldown_elapses():
    clock = {"t": 10.0}
    q = ModelHealthQuarantine(_cfg(cooldown_s=100.0, resample_s=1.0),
                              monotonic=lambda: clock["t"])
    q.apply_realized_pnls({"m": [5.0] * 5 + [-10.0] * 5})
    assert q.is_quarantined("m") is True

    clock["t"] = 200.0
    q.apply_realized_pnls({"m": [1.0] * 5 + [-1.0] * 5})
    assert q.is_quarantined("m") is False
    assert q.quarantined_models() == frozenset()
    assert q.snapshot()["active_quarantines"] == {}


def test_persisting_breach_after_cooldown_stays_quarantined():
    """Recovery must be evidenced, not merely waited out."""
    clock = {"t": 10.0}
    q = ModelHealthQuarantine(_cfg(cooldown_s=100.0, resample_s=1.0),
                              monotonic=lambda: clock["t"])
    breach = {"m": [5.0] * 5 + [-10.0] * 5}
    q.apply_realized_pnls(breach)
    clock["t"] = 200.0
    q.apply_realized_pnls({"m": [5.0] * 6 + [-10.0] * 6})
    assert q.is_quarantined("m") is True


def test_snapshot_publishes_the_quarantine_rule_and_thresholds():
    snap = ModelHealthQuarantine(_cfg()).snapshot()
    assert snap["severe_asymmetry_min_observations"] == 5
    rule = snap["quarantine_rule"]
    assert "AND expectancy<min_expectancy" in rule
    assert "OR" in rule


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
    with pytest.raises(ValueError):
        ModelHealthConfig(severe_asymmetry_min_observations=0)
