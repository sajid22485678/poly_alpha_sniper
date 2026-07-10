"""Tests for the shadow-only Bayesian EV calibrator (research challenger)."""
from __future__ import annotations

import math

import pytest

from poly_alpha_sniper.research.bayesian_ev import (
    BayesianEv,
    PROB_CEIL,
    PROB_FLOOR,
    TOTAL_ADJUSTMENT_CAP,
    calibrate_ev,
    evidence_log_odds,
    expected_value,
    posterior_probability,
)


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def test_empty_evidence_returns_clamped_prior():
    assert posterior_probability(0.6, {}) == pytest.approx(0.6)
    assert posterior_probability(0.6, None) == pytest.approx(0.6)
    # out-of-band priors are clamped, guarding log(0)/div-by-zero
    assert posterior_probability(0.0, {}) == pytest.approx(PROB_FLOOR)
    assert posterior_probability(1.0, {}) == pytest.approx(PROB_CEIL)


def test_posterior_always_within_probability_band():
    strong_negative = {
        "cex_staleness_ms": 50_000,
        "book_quality": 0.0,
        "spread": 0.5,
        "regime": "FAKEOUT_PRONE",
        "time_to_close_s": 0,
        "anchor_distance_pct": -100,
    }
    strong_positive = {"shock_strength": 2.0, "anchor_distance_pct": 100,
                       "regime": "IMPULSE"}
    assert posterior_probability(0.02, strong_negative) == PROB_FLOOR
    assert posterior_probability(0.98, strong_positive) == PROB_CEIL


def test_shock_strength_only_above_baseline_adds():
    # prior 0.5 has log-odds 0, so posterior = sigmoid(adjustment)
    assert posterior_probability(0.5, {"shock_strength": 2.0}) == pytest.approx(
        _sigmoid(0.5)
    )
    assert posterior_probability(0.5, {"shock_strength": 1.5}) == pytest.approx(
        _sigmoid(0.25)
    )
    # at or below baseline: neutral, never punitive
    assert posterior_probability(0.5, {"shock_strength": 1.0}) == pytest.approx(0.5)
    assert posterior_probability(0.5, {"shock_strength": 0.2}) == pytest.approx(0.5)
    # clamped above 2.0 -> still +0.5 max
    assert posterior_probability(0.5, {"shock_strength": 7.0}) == pytest.approx(
        _sigmoid(0.5)
    )


def test_anchor_distance_signed_and_capped():
    contrib = evidence_log_odds({"anchor_distance_pct": 1.0})
    assert contrib["anchor_distance_pct"] == pytest.approx(0.2)
    contrib = evidence_log_odds({"anchor_distance_pct": -1.0})
    assert contrib["anchor_distance_pct"] == pytest.approx(-0.2)
    # saturation at +/-0.4
    assert evidence_log_odds({"anchor_distance_pct": 50})[
        "anchor_distance_pct"
    ] == pytest.approx(0.4)
    assert evidence_log_odds({"anchor_distance_pct": -50})[
        "anchor_distance_pct"
    ] == pytest.approx(-0.4)


def test_staleness_book_spread_and_close_penalties():
    assert evidence_log_odds({"cex_staleness_ms": 0})[
        "cex_staleness_ms"
    ] == pytest.approx(0.0)
    assert evidence_log_odds({"cex_staleness_ms": 4000})[
        "cex_staleness_ms"
    ] == pytest.approx(-0.25)
    # capped at -0.5 for >= 8000 ms
    assert evidence_log_odds({"cex_staleness_ms": 8000})[
        "cex_staleness_ms"
    ] == pytest.approx(-0.5)
    assert evidence_log_odds({"cex_staleness_ms": 999_999})[
        "cex_staleness_ms"
    ] == pytest.approx(-0.5)
    assert evidence_log_odds({"book_quality": 0.0})["book_quality"] == pytest.approx(
        -0.3
    )
    assert evidence_log_odds({"book_quality": 1.0})["book_quality"] == pytest.approx(
        0.0
    )
    assert evidence_log_odds({"spread": 0.05})["spread"] == pytest.approx(-0.15)
    assert evidence_log_odds({"spread": 0.5})["spread"] == pytest.approx(-0.3)
    assert evidence_log_odds({"time_to_close_s": 0})[
        "time_to_close_s"
    ] == pytest.approx(-0.2)
    assert evidence_log_odds({"time_to_close_s": 30})[
        "time_to_close_s"
    ] == pytest.approx(-0.1)
    assert evidence_log_odds({"time_to_close_s": 600})[
        "time_to_close_s"
    ] == pytest.approx(0.0)


def test_regime_offsets_and_unknown_regime_neutral():
    expected = {
        "IMPULSE": 0.2,
        "FAKEOUT_PRONE": -0.4,
        "CHOP": -0.2,
        "FINAL_WINDOW_CHAOS": -0.3,
        "QUIET": 0.0,
    }
    for regime, offset in expected.items():
        assert evidence_log_odds({"regime": regime})["regime"] == pytest.approx(offset)
    # case-insensitive, unknown neutral
    assert evidence_log_odds({"regime": "impulse"})["regime"] == pytest.approx(0.2)
    assert evidence_log_odds({"regime": "MARTIAN_WEATHER"})["regime"] == pytest.approx(
        0.0
    )


def test_malformed_and_unknown_evidence_ignored():
    junk = {
        "shock_strength": "huge",
        "anchor_distance_pct": None,
        "cex_staleness_ms": float("nan"),
        "book_quality": float("inf"),
        "spread": True,  # bools are not magnitudes
        "regime": 42,
        "totally_unknown_key": 3.0,
    }
    assert evidence_log_odds(junk) == {}
    assert posterior_probability(0.7, junk) == pytest.approx(0.7)


def test_total_adjustment_capped_at_1_5_log_odds():
    pile_on = {
        "anchor_distance_pct": -100,
        "cex_staleness_ms": 100_000,
        "book_quality": 0.0,
        "spread": 1.0,
        "regime": "FAKEOUT_PRONE",
        "time_to_close_s": 0,
    }
    result = calibrate_ev(0.5, 0.5, 0.0, pile_on)
    # raw sum is -2.1 but the applied total is capped
    assert sum(result.evidence_log_odds.values()) == pytest.approx(-2.1)
    assert result.total_adjustment == pytest.approx(-TOTAL_ADJUSTMENT_CAP)
    assert result.posterior == pytest.approx(_sigmoid(-1.5))


def test_calibrate_ev_formula_and_fields():
    result = calibrate_ev(0.6, 0.5, 0.02, {})
    assert isinstance(result, BayesianEv)
    # ev = p*(1-price) - (1-p)*price - fee*price
    assert result.ev_prior == pytest.approx(0.6 * 0.5 - 0.4 * 0.5 - 0.02 * 0.5)
    # empty evidence: neutral — posterior equals prior, EVs match
    assert result.posterior == pytest.approx(result.prior)
    assert result.ev_posterior == pytest.approx(result.ev_prior)
    assert result.evidence_log_odds == {}
    assert result.total_adjustment == 0.0


def test_calibrate_ev_positive_evidence_raises_posterior_ev():
    result = calibrate_ev(
        0.55, 0.5, 0.0, {"shock_strength": 2.0, "regime": "IMPULSE"}
    )
    assert result.total_adjustment == pytest.approx(0.7)
    assert result.posterior > result.prior
    assert result.ev_posterior > result.ev_prior
    # EV must be consistent with the posterior at the same price
    assert result.ev_posterior == pytest.approx(
        expected_value(result.posterior, 0.5, 0.0)
    )


def test_invalid_inputs_yield_explicit_neutral_or_none():
    # unusable prior -> explicit neutral 0.5, not fabricated
    assert posterior_probability(float("nan"), {}) == pytest.approx(0.5)
    # unusable price -> EV is None, posterior still computed
    result = calibrate_ev(0.6, float("nan"), 0.01, {})
    assert result.ev_prior is None
    assert result.ev_posterior is None
    assert result.posterior == pytest.approx(0.6)
    assert expected_value(float("nan"), 0.5) is None


def test_deterministic_pure_function():
    evidence = {
        "shock_strength": 1.4,
        "anchor_distance_pct": 0.7,
        "cex_staleness_ms": 2500,
        "book_quality": 0.8,
        "spread": 0.03,
        "regime": "CHOP",
        "time_to_close_s": 45,
    }
    first = calibrate_ev(0.62, 0.48, 0.015, evidence)
    for _ in range(3):
        again = calibrate_ev(0.62, 0.48, 0.015, dict(evidence))
        assert again == first
