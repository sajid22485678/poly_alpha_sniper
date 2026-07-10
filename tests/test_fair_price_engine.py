"""Tests for the shadow-only deterministic weighted fair CEX price engine."""
from __future__ import annotations

import math

import pytest

from poly_alpha_sniper.research.fair_price_engine import (
    FairPrice,
    compute_fair_price,
)

NOW = 1_000_000_000


def _q(price: float, age_ms: int) -> dict:
    return {"price": price, "ts_ms": NOW - age_ms}


def test_empty_quotes_returns_no_source():
    fp = compute_fair_price({}, NOW)
    assert fp == FairPrice(None, 0.0, {}, [], 0, None, "NO_SOURCE", 1.0)


def test_all_non_positive_prices_returns_no_source():
    fp = compute_fair_price(
        {"a": _q(0.0, 100), "b": _q(-5.0, 100)},
        NOW,
    )
    assert fp.fair_price is None
    assert fp.stale_bucket == "NO_SOURCE"
    assert fp.confidence == 0.0
    assert fp.ev_stale_penalty == 1.0


def test_all_too_stale_dropped_returns_no_source():
    # Hard drop threshold is degraded_max_age_ms * 2 = 16000ms.
    fp = compute_fair_price({"a": _q(100.0, 16_001), "b": _q(101.0, 20_000)}, NOW)
    assert fp.stale_bucket == "NO_SOURCE"
    assert fp.n_sources == 0
    assert fp.fair_price is None


def test_single_fresh_source():
    fp = compute_fair_price({"binance": _q(50_000.0, 500)}, NOW)
    assert fp.fair_price == pytest.approx(50_000.0)
    assert fp.n_sources == 1
    assert fp.freshest_source == "binance"
    assert fp.stale_bucket == "FRESH"
    assert fp.ev_stale_penalty == 0.0
    assert fp.source_weights == {"binance": pytest.approx(1.0)}
    assert fp.outlier_sources == []
    assert 0.0 <= fp.confidence <= 1.0


def test_three_sources_outlier_excluded():
    fp = compute_fair_price(
        {
            "a": _q(100.0, 0),
            "b": _q(100.1, 0),
            "c": _q(110.0, 0),  # ~9.9% off median -> outlier
        },
        NOW,
    )
    assert fp.outlier_sources == ["c"]
    assert fp.source_weights["c"] == 0.0
    # Fair price built only from a and b (equal ages -> equal weights).
    assert fp.fair_price == pytest.approx((100.0 + 100.1) / 2)
    assert fp.n_sources == 3


def test_two_sources_no_outliers_fresher_weighted_more():
    fp = compute_fair_price(
        {"fresh": _q(100.0, 0), "stale": _q(110.0, 3000)},
        NOW,
    )
    # With 2 sources no outliers are marked even at 10% deviation.
    assert fp.outlier_sources == []
    # One half-life apart: weights 2/3 vs 1/3.
    assert fp.source_weights["fresh"] == pytest.approx(2.0 / 3.0)
    assert fp.source_weights["stale"] == pytest.approx(1.0 / 3.0)
    assert fp.fair_price == pytest.approx(100.0 * 2 / 3 + 110.0 / 3)
    assert fp.freshest_source == "fresh"


def test_weights_normalized_and_equal_ages_give_arithmetic_mean():
    fp = compute_fair_price(
        {"a": _q(100.0, 1000), "b": _q(100.2, 1000), "c": _q(100.4, 1000)},
        NOW,
    )
    assert sum(fp.source_weights.values()) == pytest.approx(1.0)
    assert fp.fair_price == pytest.approx((100.0 + 100.2 + 100.4) / 3)


def test_degraded_bucket_and_penalty_scaling():
    fp = compute_fair_price({"a": _q(100.0, 6000)}, NOW)
    assert fp.stale_bucket == "DEGRADED"
    # 6000ms = 2 half-lives -> 0.015 * 2.
    assert fp.ev_stale_penalty == pytest.approx(0.03)

    fp2 = compute_fair_price({"a": _q(100.0, 2000)}, NOW)
    assert fp2.stale_bucket == "DEGRADED"
    # Below one half-life the multiplier floors at 1x the config buffer.
    assert fp2.ev_stale_penalty == pytest.approx(0.015)


def test_fail_closed_bucket_full_penalty_but_price_reported():
    fp = compute_fair_price({"a": _q(100.0, 10_000)}, NOW)
    assert fp.stale_bucket == "FAIL_CLOSED"
    assert fp.ev_stale_penalty == 1.0
    assert fp.fair_price == pytest.approx(100.0)


def test_all_outlier_degenerate_dispersion_falls_back_to_freshest():
    # Even count, two clusters straddling the median: every source deviates
    # >0.5% from the median. Engine must fall back to the freshest source.
    fp = compute_fair_price(
        {
            "a": _q(100.0, 100),  # freshest
            "b": _q(100.0, 200),
            "c": _q(200.0, 300),
            "d": _q(200.0, 400),
        },
        NOW,
    )
    assert fp.fair_price == pytest.approx(100.0)
    assert fp.freshest_source == "a"
    assert sorted(fp.outlier_sources) == ["b", "c", "d"]
    assert fp.source_weights["a"] == pytest.approx(1.0)


def test_confidence_bounds_and_monotonic_in_source_count():
    single = compute_fair_price({"a": _q(100.0, 0)}, NOW)
    triple = compute_fair_price(
        {"a": _q(100.0, 0), "b": _q(100.0, 0), "c": _q(100.0, 0)},
        NOW,
    )
    assert 0.0 <= single.confidence <= 1.0
    assert 0.0 <= triple.confidence <= 1.0
    # Three fresh, perfectly agreeing sources beat a lone source.
    assert triple.confidence > single.confidence
    assert triple.confidence == pytest.approx(1.0)


def test_deterministic_same_inputs_same_output():
    quotes = {
        "kraken": _q(99.5, 1200),
        "binance": _q(100.0, 300),
        "coinbase": _q(100.05, 700),
    }
    a = compute_fair_price(dict(quotes), NOW)
    b = compute_fair_price(dict(quotes), NOW)
    assert a == b
    assert a.fair_price is not None and math.isfinite(a.fair_price)


def test_future_timestamp_clamped_to_live():
    # Clock skew: exchange timestamp slightly ahead of now must not blow up
    # or over-weight; it reads as age 0 (FRESH).
    fp = compute_fair_price({"a": _q(100.0, -500)}, NOW)
    assert fp.stale_bucket == "FRESH"
    assert fp.fair_price == pytest.approx(100.0)


def test_malformed_quote_entries_skipped():
    fp = compute_fair_price(
        {
            "good": _q(100.0, 100),
            "missing_price": {"ts_ms": NOW},
            "bad_type": {"price": "oops", "ts_ms": NOW},
            "nan": {"price": float("nan"), "ts_ms": NOW},
        },
        NOW,
    )
    assert fp.n_sources == 1
    assert fp.fair_price == pytest.approx(100.0)
