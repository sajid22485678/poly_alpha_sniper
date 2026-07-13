from __future__ import annotations

from copy import deepcopy

import pytest

from lite_frequency_v4.contracts import AnchorStatus, MarketIdentity
from lite_frequency_v4.resolver import (
    corroborated_resolution,
    event_market_row,
    official_outcome,
    official_pnl,
    validate_market_identity,
)


OPEN_MS = 1_800_000_000_000


def identity() -> MarketIdentity:
    return MarketIdentity(
        asset="BTC",
        slug=f"btc-updown-5m-{OPEN_MS // 1000}",
        market_id="market-1",
        event_id="event-1",
        condition_id="condition-1",
        yes_token_id="yes-token",
        no_token_id="no-token",
        window_open_ms=OPEN_MS,
        window_close_ms=OPEN_MS + 300_000,
        anchor_status=AnchorStatus.FIELD_MISSING,
    )


def market_row(*, outcome: str = "YES", closed: bool = True) -> dict:
    prices = '["1", "0"]' if outcome == "YES" else '["0", "1"]'
    return {
        "id": "market-1",
        "slug": f"btc-updown-5m-{OPEN_MS // 1000}",
        "conditionId": "condition-1",
        "outcomes": '["Up", "Down"]',
        "clobTokenIds": '["yes-token", "no-token"]',
        "closed": closed,
        "outcomePrices": prices,
        # This metadata is deliberately present to prove it is never treated
        # as outcome or anchor evidence.
        "resolutionSource": "https://example.invalid/metadata-only",
    }


def event(row: dict | None = None) -> dict:
    return {
        "id": "event-1",
        "slug": f"btc-updown-5m-{OPEN_MS // 1000}",
        "markets": [deepcopy(row or market_row())],
    }


def test_exact_market_event_condition_window_and_token_identity_matches() -> None:
    valid, reason = validate_market_identity(market_row(), identity())
    assert valid is True
    assert reason == "identity_match"
    related, event_reason = event_market_row(event(), identity())
    assert related == market_row()
    assert event_reason == "identity_match"


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("id", "other-market", "market_id_mismatch"),
        ("slug", f"btc-updown-5m-{OPEN_MS // 1000 + 300}", "slug_mismatch"),
        ("conditionId", "other-condition", "condition_id_mismatch"),
        ("outcomes", '["Yes", "Maybe"]', "token_mapping_unparseable"),
        ("clobTokenIds", '["yes-token", "yes-token"]', "token_mapping_unparseable"),
        ("clobTokenIds", '["wrong-yes", "no-token"]', "yes_token_mismatch"),
    ],
)
def test_identity_mismatch_is_rejected_with_exact_reason(field, value, reason) -> None:
    row = market_row()
    row[field] = value
    valid, actual = validate_market_identity(row, identity())
    assert valid is False
    assert actual == reason


def test_slug_must_encode_the_exact_five_minute_window() -> None:
    row = market_row()
    row["slug"] = "btc-updown-5m-not-a-timestamp"
    # Use a corresponding identity slug only to isolate the embedded window
    # validation from the exact slug-identity comparison.
    market = identity()
    object.__setattr__(market, "slug", row["slug"])
    valid, reason = validate_market_identity(row, market)
    assert valid is False
    assert reason == "window_open_mismatch"


def test_official_outcome_requires_closed_degenerate_prices_and_ignores_url_metadata() -> None:
    outcome, reason = official_outcome(market_row(outcome="YES"), identity())
    assert (outcome, reason) == ("YES", "resolved")

    open_outcome = official_outcome(market_row(closed=False), identity())
    assert open_outcome == (None, "market_not_closed_yet")

    nondegenerate = market_row()
    nondegenerate["outcomePrices"] = '["0.6", "0.4"]'
    assert official_outcome(nondegenerate, identity()) == (
        None, "outcome_prices_not_degenerate"
    )

    no_prices = market_row()
    no_prices.pop("outcomePrices")
    # A resolution-source URL alone is never sufficient.
    assert official_outcome(no_prices, identity()) == (None, "outcome_unparseable")


def test_corroborated_resolution_requires_direct_and_event_agreement() -> None:
    verified = corroborated_resolution(
        identity=identity(),
        direct_market=market_row(outcome="NO"),
        event=event(market_row(outcome="NO")),
    )
    assert verified.verified is True
    assert verified.outcome == "NO"
    assert verified.reason == "official_corroborated"

    conflict = corroborated_resolution(
        identity=identity(),
        direct_market=market_row(outcome="YES"),
        event=event(market_row(outcome="NO")),
    )
    assert conflict.verified is False
    assert conflict.outcome is None
    assert conflict.reason == "resolution_conflict"


def test_corroborated_resolution_fails_closed_on_event_identity_mismatch() -> None:
    wrong_event = event(market_row(outcome="YES"))
    wrong_event["id"] = "other-event"
    result = corroborated_resolution(
        identity=identity(), direct_market=market_row(), event=wrong_event
    )
    assert result.verified is False
    assert result.outcome is None
    assert result.reason == "event_id_mismatch"


def test_official_pnl_reconciles_wins_losses_fees_and_exact_five_shares() -> None:
    win_pnl, win_price, won = official_pnl(
        side="BUY_YES",
        entry_price=0.40,
        entry_fee=0.05,
        winning_outcome="YES",
    )
    assert won is True
    assert win_price == 1.0
    assert win_pnl == pytest.approx(5.0 - (5.0 * 0.40 + 0.05))

    loss_pnl, loss_price, won = official_pnl(
        side="BUY_YES",
        entry_price=0.40,
        entry_fee=0.05,
        winning_outcome="NO",
    )
    assert won is False
    assert loss_price == 0.0
    assert loss_pnl == pytest.approx(-(5.0 * 0.40 + 0.05))

    with pytest.raises(ValueError, match="invalid official settlement"):
        official_pnl(
            side="BUY_YES", entry_price=0.40, entry_fee=0.05,
            winning_outcome="YES", shares=4.0,
        )


def test_resolution_replay_is_deterministic() -> None:
    kwargs = dict(
        identity=identity(),
        direct_market=market_row(outcome="YES"),
        event=event(market_row(outcome="YES")),
    )
    assert corroborated_resolution(**kwargs) == corroborated_resolution(**kwargs)
