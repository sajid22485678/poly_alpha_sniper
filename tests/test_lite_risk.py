import math
import pytest

from poly_alpha_sniper.lite.lite_risk import (
    assess_live_small_exposure,
    entry_commitment,
    fill_levels_taker_fee,
    idempotent_intent_key,
    taker_fee,
)


def test_crypto_taker_fee_uses_official_curve_and_rounding():
    assert taker_fee(5, 0.5, 0.07) == 0.0875
    assert taker_fee(5, 0.01, 0.07) == 0.00347
    assert fill_levels_taker_fee(((0.4, 2), (0.8, 3)), 0.07) == 0.0672


def test_entry_commitment_is_exactly_five_shares_plus_fee_and_buffer():
    assert entry_commitment(0.5, fee_buffer_usd=0.02) == pytest.approx(2.6075)
    with pytest.raises(ValueError, match="five shares"):
        entry_commitment(0.5, shares=6)


def test_seventy_five_percent_cap_allows_no_overcommitment_on_13_dollars():
    decision = assess_live_small_exposure(
        entry_price=0.5, committed_exposure_usd=8.0, equity_usd=13,
        exposure_cap_pct=0.75, fee_buffer_usd=0.02)
    assert decision.allowed is False
    assert decision.exposure_cap_usd == 9.75
    assert decision.projected_exposure_usd > 9.75
    assert decision.reason == "equity_exposure_cap"


def test_available_balance_is_independently_enforced():
    decision = assess_live_small_exposure(
        entry_price=0.5, committed_exposure_usd=0, equity_usd=13,
        available_balance_usd=2.0, exposure_cap_pct=0.75)
    assert decision.reason == "insufficient_available_balance"


@pytest.mark.parametrize("field", ["equity_usd", "committed_exposure_usd", "entry_price"])
def test_nonfinite_capital_inputs_fail_closed(field):
    values = dict(entry_price=0.5, committed_exposure_usd=0.0, equity_usd=13.0)
    values[field] = math.nan
    decision = assess_live_small_exposure(**values)
    assert decision.allowed is False
    assert decision.reason in ("invalid_equity", "invalid_commitment")


def test_daily_loss_streak_and_kill_switch_guards_are_fail_closed():
    common = dict(entry_price=0.4, committed_exposure_usd=0, equity_usd=13)
    assert assess_live_small_exposure(**common, kill_switch=True).reason == "live_kill_switch"
    assert assess_live_small_exposure(
        **common, today_realized_pnl=-2, max_daily_realized_loss_usd=2
    ).reason == "max_daily_realized_loss"
    assert assess_live_small_exposure(
        **common, consecutive_losses=3, max_consecutive_losses=3
    ).reason == "max_consecutive_losses"


def test_idempotent_order_intent_key_is_exact_identity_and_side_stable():
    values = dict(
        asset="BTC", slug="btc-updown-5m-300", market_id="m1", event_id="e1",
        condition_id="c1", window_open_ts=300_000, window_close_ts=600_000,
        side="BUY_YES")
    assert idempotent_intent_key(**values) == idempotent_intent_key(**values)
    assert idempotent_intent_key(**values) != idempotent_intent_key(
        **{**values, "side": "BUY_NO"})
