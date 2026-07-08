import pytest

from poly_alpha_sniper.strategy.distance_to_strike_model import prob_above_threshold


def test_far_above_strike_near_certain():
    p_above, p_below, conf = prob_above_threshold(
        price=100_000, threshold=95_000, tte_s=120, vol_per_s=0.0002)
    assert p_above >= 0.95
    assert p_above + p_below == pytest.approx(1.0)
    assert conf > 50


def test_far_below_strike_near_zero():
    p_above, _, _ = prob_above_threshold(
        price=90_000, threshold=95_000, tte_s=120, vol_per_s=0.0002)
    assert p_above <= 0.05


def test_at_strike_near_half():
    p_above, _, _ = prob_above_threshold(
        price=95_000, threshold=95_000, tte_s=120, vol_per_s=0.0002)
    assert 0.35 <= p_above <= 0.65


def test_more_time_widens_distribution():
    near, _, _ = prob_above_threshold(100_000, 99_500, tte_s=30, vol_per_s=0.0002)
    far, _, _ = prob_above_threshold(100_000, 99_500, tte_s=3000, vol_per_s=0.0002)
    # with more time, the currently-above outcome is less certain
    assert far < near


def test_momentum_shifts_probability():
    up, _, _ = prob_above_threshold(95_000, 95_000, 120, 0.0002, momentum=0.9)
    down, _, _ = prob_above_threshold(95_000, 95_000, 120, 0.0002, momentum=-0.9)
    assert up > down


def test_mean_reversion_shrinks_extreme_drift():
    normal, _, _ = prob_above_threshold(95_000, 95_000, 120, 0.0002,
                                        momentum=0.9, zscore=2.0)
    extreme, _, _ = prob_above_threshold(95_000, 95_000, 120, 0.0002,
                                         momentum=0.9, zscore=6.0,
                                         mean_reversion_penalty=0.2)
    assert extreme < normal


def test_clamps():
    p_above, p_below, _ = prob_above_threshold(200_000, 1_000, 60, 0.0001)
    assert p_above <= 0.98
    assert p_below >= 0.02
