"""Tests for the RESEARCH / SHADOW-ONLY deterministic regime tagger."""
from __future__ import annotations

from poly_alpha_sniper.research.regime_tagger import (
    REGIMES,
    RegimeTag,
    tag_regime,
)


def test_empty_inputs_return_explicit_neutral():
    tag = tag_regime({}, volatility_per_s=0.0, shock_score=0.0)
    assert tag.regime == "QUIET"
    assert tag.confidence == 0.5
    assert any("no informative features" in r for r in tag.reasons)


def test_final_window_chaos_on_shock_near_close():
    tag = tag_regime({2: 0.001}, volatility_per_s=0.001, shock_score=0.8,
                     time_to_close_s=30.0)
    assert tag.regime == "FINAL_WINDOW_CHAOS"
    assert 0.5 <= tag.confidence <= 0.99
    assert tag.features_used["time_to_close_s"] == 30.0


def test_final_window_without_disturbance_is_not_chaos():
    tag = tag_regime({2: 0.0001}, volatility_per_s=0.0001, shock_score=0.1,
                     time_to_close_s=30.0, depth_usd=500.0, spread=0.01)
    assert tag.regime == "QUIET"


def test_impulse_when_ret2_dominates_ret30():
    tag = tag_regime({2: 0.01, 30: 0.005}, volatility_per_s=0.001,
                     shock_score=0.95, depth_usd=500.0, spread=0.01)
    assert tag.regime == "IMPULSE"
    assert 0.5 <= tag.confidence <= 0.99


def test_impulse_zero_ret30_guarded_not_div_by_zero():
    tag = tag_regime({2: 0.01, 30: 0.0}, volatility_per_s=0.001,
                     shock_score=0.95, depth_usd=500.0, spread=0.01)
    assert tag.regime == "IMPULSE"


def test_high_shock_but_stale_move_is_not_impulse():
    tag = tag_regime({2: 0.0001, 30: 0.01}, volatility_per_s=0.001,
                     shock_score=0.95, depth_usd=500.0, spread=0.01)
    assert tag.regime != "IMPULSE"


def test_fakeout_prone_on_reversal():
    tag = tag_regime({2: 0.005, 10: -0.003, 30: -0.004},
                     volatility_per_s=0.001, shock_score=0.2,
                     depth_usd=500.0, spread=0.01)
    assert tag.regime == "FAKEOUT_PRONE"
    assert any("reversed" in r for r in tag.reasons)


def test_fakeout_prone_on_thin_depth_and_wide_spread():
    thin = tag_regime({}, volatility_per_s=0.0, shock_score=0.0,
                      depth_usd=50.0)
    wide = tag_regime({}, volatility_per_s=0.0, shock_score=0.0,
                      spread=0.08)
    assert thin.regime == "FAKEOUT_PRONE"
    assert wide.regime == "FAKEOUT_PRONE"


def test_chop_on_alternating_signs():
    tag = tag_regime({1: 0.001, 2: -0.001, 5: 0.001, 10: -0.001},
                     volatility_per_s=0.0005, shock_score=0.1,
                     depth_usd=500.0, spread=0.01)
    assert tag.regime == "CHOP"
    assert tag.features_used["sign_flips"] == 3


def test_precedence_final_window_beats_impulse():
    tag = tag_regime({2: 0.01, 30: 0.001}, volatility_per_s=0.01,
                     shock_score=0.95, time_to_close_s=10.0)
    assert tag.regime == "FINAL_WINDOW_CHAOS"


def test_extreme_values_confidence_clamped():
    tag = tag_regime({2: 1e6, 30: 1e-9}, volatility_per_s=1e9,
                     shock_score=1e9, time_to_close_s=0.0)
    assert tag.regime in REGIMES
    assert 0.01 <= tag.confidence <= 0.99


def test_non_finite_features_treated_as_missing():
    tag = tag_regime({2: float("nan")}, volatility_per_s=float("inf"),
                     shock_score=float("nan"))
    assert tag.regime == "QUIET"
    assert tag.confidence == 0.5  # sanitized to the explicit neutral path
    assert "ret_2s" not in tag.features_used


def test_deterministic_same_inputs_same_tag():
    kwargs = dict(returns={1: 0.001, 2: 0.004, 10: -0.002},
                  volatility_per_s=0.002, shock_score=0.6, spread=0.02,
                  depth_usd=250.0, anchor_distance_pct=1.5,
                  time_to_close_s=300.0)
    a = tag_regime(**kwargs)
    b = tag_regime(**kwargs)
    assert isinstance(a, RegimeTag)
    assert a == b
    assert a.features_used["anchor_distance_pct"] == 1.5
