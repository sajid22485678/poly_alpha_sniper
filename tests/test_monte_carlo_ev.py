"""Tests for the shadow-only seeded Monte Carlo EV validator (research challenger)."""
from __future__ import annotations

import pytest

from poly_alpha_sniper.research.monte_carlo_ev import (
    McResult,
    PROB_CEIL,
    PROB_FLOOR,
    simulate,
)

# A moderate baseline scenario: p_up lands well inside (0, 1) so sampling
# noise is visible and clamps do not engage.
BASE = dict(
    current_price=100.0,
    price_to_beat=100.0,
    vol_per_s=0.001,
    drift_per_s=0.0,
    time_remaining_s=300.0,
    side="UP",
    executable_price=0.55,
    fee_rate=0.01,
    n_paths=500,
    seed=42,
)


def test_determinism_same_inputs_same_seed():
    a = simulate(**BASE)
    b = simulate(**BASE)
    assert a == b  # bit-identical dataclass, including cache_key


def test_different_seed_changes_sampling():
    a = simulate(**BASE)
    b = simulate(**{**BASE, "seed": 43})
    assert a.cache_key != b.cache_key
    # With 500 paths near p=0.5 the sampled win fraction differs across seeds.
    assert a.p_up != b.p_up or a.ev_mean != b.ev_mean


def test_degenerate_zero_time_resolves_by_price_comparison():
    up = simulate(**{**BASE, "time_remaining_s": 0.0, "current_price": 101.0})
    down = simulate(**{**BASE, "time_remaining_s": 0.0, "current_price": 99.0})
    tie = simulate(**{**BASE, "time_remaining_s": 0.0, "current_price": 100.0})
    assert up.p_up == pytest.approx(PROB_CEIL)  # raw 1.0, band-clamped
    assert down.p_up == pytest.approx(PROB_FLOOR)  # raw 0.0, band-clamped
    assert tie.p_up == pytest.approx(0.5)  # exact tie -> neutral
    for res in (up, down, tie):
        assert res.n_paths == 0
        assert res.ev_p05 == pytest.approx(res.ev_mean)  # no sampling tail


def test_degenerate_zero_vol_uses_pure_drift():
    # 100 * exp(0.001 * 100) ~ 110.5 > 101 -> deterministic win for UP.
    res = simulate(
        **{
            **BASE,
            "vol_per_s": 0.0,
            "drift_per_s": 0.001,
            "time_remaining_s": 100.0,
            "price_to_beat": 101.0,
        }
    )
    assert res.p_up == pytest.approx(PROB_CEIL)
    assert res.n_paths == 0


def test_degenerate_ev_matches_clamped_probability_payout_math():
    # Certain win for UP: p_win clamped to 0.99; every path pays the fee.
    res = simulate(**{**BASE, "time_remaining_s": 0.0, "current_price": 105.0})
    x = BASE["executable_price"]
    fee = BASE["fee_rate"] * x
    expected = 0.99 * ((1.0 - x) - fee) + 0.01 * (-x - fee)
    assert res.ev_mean == pytest.approx(expected)


def test_nonpositive_n_paths_falls_back_to_deterministic_result():
    res = simulate(**{**BASE, "n_paths": 0, "current_price": 102.0})
    assert res.n_paths == 0
    assert res.ev_p05 == pytest.approx(res.ev_mean)
    assert PROB_FLOOR <= res.p_up <= PROB_CEIL


def test_probabilities_clamped_and_complementary():
    # Extreme upward setup: raw p_up = 1.0 -> clamped band edges.
    sure = simulate(
        **{**BASE, "current_price": 200.0, "price_to_beat": 100.0, "vol_per_s": 1e-6}
    )
    assert sure.p_up == pytest.approx(PROB_CEIL)
    assert sure.p_down == pytest.approx(PROB_FLOOR)
    # Interior case: complements sum to 1 exactly (no clamp engaged).
    mid = simulate(**BASE)
    assert PROB_FLOOR < mid.p_up < PROB_CEIL
    assert mid.p_up + mid.p_down == pytest.approx(1.0)


def test_ev_p05_never_exceeds_ev_mean():
    for seed in (1, 7, 42, 1234):
        res = simulate(**{**BASE, "seed": seed})
        assert res.ev_p05 <= res.ev_mean


def test_side_down_mirrors_up_win_probability():
    up = simulate(**BASE)
    down = simulate(**{**BASE, "side": "DOWN"})
    # Same seed -> same paths -> identical p_up; only the payout side flips.
    assert down.p_up == pytest.approx(up.p_up)
    x = BASE["executable_price"]
    fee = BASE["fee_rate"] * x
    p_win_down = 1.0 - up.p_up  # interior case: raw fraction == clamped
    expected = p_win_down * ((1.0 - x) - fee) + (1.0 - p_win_down) * (-x - fee)
    assert down.ev_mean == pytest.approx(expected)


def test_higher_drift_raises_p_up_with_same_seed():
    lo = simulate(**{**BASE, "drift_per_s": -0.0005})
    hi = simulate(**{**BASE, "drift_per_s": 0.0005})
    # Identical gaussian draws (same seed), so the ordering is deterministic.
    assert hi.p_up > lo.p_up


def test_cache_key_stable_and_input_sensitive():
    a = simulate(**BASE)
    b = simulate(**BASE)
    c = simulate(**{**BASE, "current_price": 100.000001})
    assert a.cache_key == b.cache_key
    assert len(a.cache_key) == 12
    assert int(a.cache_key, 16) >= 0  # valid hex
    assert a.cache_key != c.cache_key  # differs at the rounding resolution


def test_invalid_side_raises_value_error():
    with pytest.raises(ValueError):
        simulate(**{**BASE, "side": "SIDEWAYS"})


def test_result_is_frozen_dataclass():
    res = simulate(**BASE)
    assert isinstance(res, McResult)
    with pytest.raises(Exception):
        res.p_up = 0.5  # type: ignore[misc]
