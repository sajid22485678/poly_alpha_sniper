import pytest

from poly_alpha_sniper.lite_frequency_v4.books import five_share_buy_sweep, normalize_book
from poly_alpha_sniper.lite_frequency_v4.contracts import AnchorStatus, EntrySide, MarketIdentity
from poly_alpha_sniper.lite_frequency_v4.risk import (
    assess_shadow_exposure,
    entry_commitment,
    entry_idempotency_key,
    fee_per_share,
    fill_levels_taker_fee,
    taker_fee,
)


NOW_MS = 1_800_000_010_000
OPEN_MS = 1_800_000_000_000


def _market():
    return MarketIdentity(
        asset="ETH", slug=f"eth-updown-5m-{OPEN_MS // 1000}",
        market_id="m", event_id="e", condition_id="c",
        yes_token_id="yes", no_token_id="no",
        window_open_ms=OPEN_MS, window_close_ms=OPEN_MS + 300_000,
        anchor_status=AnchorStatus.FIELD_MISSING,
    )


def _sweep():
    raw = {
        "asset_id": "yes", "market": "c", "timestamp": str(NOW_MS),
        "hash": "h", "min_order_size": "1",
        "bids": [{"price": "0.40", "size": "5"}],
        "asks": [
            {"price": "0.40", "size": "2"},
            {"price": "0.60", "size": "3"},
        ],
    }
    book = normalize_book(
        raw, expected_token_id="yes", expected_condition_id="c",
        receipt_ts_ms=NOW_MS).book
    return five_share_buy_sweep(book)


def test_official_crypto_taker_fee_curve_and_rounding():
    assert taker_fee(5, 0.5, 0.07) == 0.0875
    assert taker_fee(5, 0.0, 0.07) == 0.0
    assert taker_fee(5, 1.0, 0.07) == 0.0
    with pytest.raises(ValueError):
        taker_fee(5, float("nan"), 0.07)


def test_multi_level_fee_is_exact_and_probability_edge_uses_per_share_fee():
    sweep = _sweep()
    expected = fill_levels_taker_fee(sweep.levels, 0.07)
    manual = taker_fee(2, 0.4, 0.07) + taker_fee(3, 0.6, 0.07)
    assert expected == pytest.approx(manual)
    assert fee_per_share(sweep, 0.07) == pytest.approx(expected / 5)


def test_entry_commitment_is_exactly_five_shares_plus_fee_and_buffer():
    sweep = _sweep()
    assert entry_commitment(
        sweep=sweep, fee_buffer_usd=0.02) == pytest.approx(
            sweep.notional + fill_levels_taker_fee(sweep.levels) + 0.02)
    with pytest.raises(ValueError, match="five shares"):
        entry_commitment(0.5, shares=4)


def test_idempotency_binds_strategy_market_window_side_and_fixed_size():
    market = _market()
    first = entry_idempotency_key(market, EntrySide.BUY_YES)
    assert first == entry_idempotency_key(market, "BUY_YES")
    assert first != entry_idempotency_key(market, EntrySide.BUY_NO)
    assert len(first) == 64


def test_shadow_exposure_enforces_global_cap_without_claiming_live_authority():
    allowed = assess_shadow_exposure(
        entry_price=0.50, committed_exposure_usd=0,
        open_positions=0, open_for_asset=0, fee_buffer_usd=0.02)
    assert allowed.allowed and allowed.live_order_allowed is False
    blocked = assess_shadow_exposure(
        entry_price=0.50, committed_exposure_usd=8,
        open_positions=2, open_for_asset=0, fee_buffer_usd=0.02)
    assert blocked.allowed is False
    assert blocked.reason == "global_exposure_cap"
    assert blocked.exposure_cap_usd == 9.75


def test_one_open_position_per_asset_and_global_concurrency_are_hard_guards():
    per_asset = assess_shadow_exposure(
        entry_price=0.10, committed_exposure_usd=0,
        open_positions=1, open_for_asset=1)
    assert not per_asset.allowed and per_asset.reason == "max_open_per_asset"
    concurrent = assess_shadow_exposure(
        entry_price=0.10, committed_exposure_usd=0,
        open_positions=6, open_for_asset=0)
    assert not concurrent.allowed and concurrent.reason == "max_open_positions"


@pytest.mark.parametrize("field", [float("nan"), float("inf"), -1.0])
def test_nonfinite_or_negative_capital_fails_closed(field):
    decision = assess_shadow_exposure(
        entry_price=0.5, committed_exposure_usd=field,
        open_positions=0, open_for_asset=0)
    assert not decision.allowed and decision.reason == "invalid_exposure_state"
