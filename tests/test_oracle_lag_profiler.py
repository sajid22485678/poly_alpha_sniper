"""WS5A: oracle lag profiler."""
from __future__ import annotations

from poly_alpha_sniper.strategy.oracle_lag_profiler import profile_asset_lag, profile_lag


def _row(asset="BTC", basis=0.001, quality="good"):
    return {"asset": asset, "basis_pct": basis, "anchor_quality": quality}


def test_insufficient_data_reported_honestly_not_a_fabricated_number():
    rows = [_row() for _ in range(3)]  # below MIN_SAMPLES
    result = profile_asset_lag(rows, "BTC", min_samples=10)
    assert result["insufficient_data"] is True
    assert result["basis_stability"] is None
    assert result["n_samples"] == 3


def test_lag_seconds_never_fabricated_even_with_enough_samples():
    rows = [_row(basis=0.001 * i) for i in range(20)]
    result = profile_asset_lag(rows, "BTC", min_samples=10)
    assert result["insufficient_data"] is False
    assert result["cex_lead_seconds"] is None
    assert result["oracle_lag_seconds_estimate"] is None
    assert result["cex_to_oracle_correlation"] is None
    assert "not_measurable_reason" in result


def test_basis_stability_is_a_real_computed_number():
    stable_rows = [_row(basis=0.001) for _ in range(20)]
    unstable_rows = [_row(basis=(0.001 if i % 2 == 0 else 0.05)) for i in range(20)]
    stable = profile_asset_lag(stable_rows, "BTC", min_samples=10)
    unstable = profile_asset_lag(unstable_rows, "BTC", min_samples=10)
    assert stable["basis_stability"] < unstable["basis_stability"]


def test_oracle_print_risk_reflects_quality_mix():
    all_good = [_row(quality="good") for _ in range(20)]
    half_stale = [_row(quality=("good" if i % 2 == 0 else "stale")) for i in range(20)]
    assert profile_asset_lag(all_good, "BTC", min_samples=10)["oracle_print_risk"] == 0.0
    assert profile_asset_lag(half_stale, "BTC", min_samples=10)["oracle_print_risk"] == 0.5


def test_profile_lag_covers_all_three_assets_and_never_fabricates_exchange_score():
    rows = [_row(asset=a) for a in ("BTC", "ETH", "SOL") for _ in range(3)]
    result = profile_lag(rows, min_samples=10)
    assert set(result["by_asset"].keys()) == {"BTC", "ETH", "SOL"}
    assert result["exchange_lead_score"] is None
    assert "exchange_lead_score_reason" in result
    for asset_result in result["by_asset"].values():
        assert asset_result["insufficient_data"] is True  # only 3 rows each
