from __future__ import annotations

import pytest

from lite_frequency_v4.config import FrequencyV4Config
from lite_frequency_v4.contracts import (
    AnchorStatus,
    BookLevel,
    BookState,
    EntrySide,
    FairValueResult,
    FairValueSide,
    MarketIdentity,
)
from lite_frequency_v4.positions import (
    evaluate_exit_vs_hold,
    exit_record,
    management_record,
)


OPEN_MS = 1_800_000_000_000
CLOSE_MS = OPEN_MS + 300_000
NOW_MS = OPEN_MS + 120_000


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
        window_close_ms=CLOSE_MS,
        anchor_status=AnchorStatus.FIELD_MISSING,
    )


def fair(probability_yes: float, *, uncertainty: float = 0.01,
         latency: float = 0.001) -> FairValueResult:
    yes = FairValueSide(
        side=EntrySide.BUY_YES,
        fair_probability=probability_yes,
        executable_vwap=0.50,
        worst_consumed_price=0.50,
        spread=0.02,
        depth_shares=10.0,
        estimated_fee=0.01,
        execution_buffer=0.002,
        latency_buffer=latency,
        uncertainty_buffer=uncertainty,
        net_edge=probability_yes - 0.523,
        evidence_age_ms=100,
        valid=True,
    )
    no_probability = 1.0 - probability_yes
    no = FairValueSide(
        side=EntrySide.BUY_NO,
        fair_probability=no_probability,
        executable_vwap=0.50,
        worst_consumed_price=0.50,
        spread=0.02,
        depth_shares=10.0,
        estimated_fee=0.01,
        execution_buffer=0.002,
        latency_buffer=latency,
        uncertainty_buffer=uncertainty,
        net_edge=no_probability - 0.523,
        evidence_age_ms=100,
        valid=True,
    )
    return FairValueResult(
        calculated_ts_ms=NOW_MS,
        phase="MANAGEMENT",
        fair_probability_yes=probability_yes,
        fair_probability_no=no_probability,
        yes=yes,
        no=no,
        selected_side=None,
        selected_net_edge=None,
        coherent=True,
    )


def owned_book(
    *,
    token: str = "yes-token",
    condition: str = "condition-1",
    bids=((0.70, 5.0),),
    asks=((0.72, 5.0),),
    provider_ts_ms: int = NOW_MS - 100,
    receipt_ts_ms: int = NOW_MS - 50,
    hydrated: bool = True,
) -> BookState:
    return BookState(
        token_id=token,
        condition_id=condition,
        bids=tuple(BookLevel(price, shares) for price, shares in bids),
        asks=tuple(BookLevel(price, shares) for price, shares in asks),
        provider_ts_ms=provider_ts_ms,
        receipt_ts_ms=receipt_ts_ms,
        receipt_monotonic_ns=receipt_ts_ms * 1_000_000,
        source="polymarket_ws",
        event_id=f"book-{provider_ts_ms}",
        payload_hash=f"hash-{provider_ts_ms}",
        connection_epoch=1,
        min_order_size=5.0,
        tick_size=0.01,
        hydrated=hydrated,
    )


def entry(**overrides) -> dict:
    row = {
        "id": 7,
        "side": "BUY_YES",
        "entry_price": 0.55,
        "fair_probability": 0.65,
        "entry_cost": 2.80,
    }
    row.update(overrides)
    return row


def test_exit_selected_only_when_executable_exit_value_dominates_hold() -> None:
    decision = evaluate_exit_vs_hold(
        entry=entry(),
        identity=identity(),
        fair_value=fair(0.40),
        owned_book=owned_book(),
        now_ms=NOW_MS,
        cfg=FrequencyV4Config(),
    )
    assert decision.action == "EXIT_BOOK"
    assert decision.reason == "exit_value_dominates_hold"
    assert decision.exit_selected is True
    assert decision.executable_exit_vwap == 0.70
    assert decision.executable_exit_value is not None
    assert decision.executable_exit_value > decision.hold_expected_value + 0.02
    assert decision.thesis_state == "INVALIDATED"


def test_high_hold_value_is_not_arbitrarily_liquidated() -> None:
    decision = evaluate_exit_vs_hold(
        entry=entry(fair_probability=0.75),
        identity=identity(),
        fair_value=fair(0.80),
        owned_book=owned_book(),
        now_ms=NOW_MS,
        cfg=FrequencyV4Config(),
    )
    assert decision.action == "HOLD"
    assert decision.reason == "hold_value_not_worse"
    assert decision.thesis_state == "INTACT"
    assert decision.exit_selected is False


def test_thesis_invalidation_alone_does_not_force_an_uneconomic_exit() -> None:
    decision = evaluate_exit_vs_hold(
        entry=entry(entry_price=0.80, fair_probability=0.75),
        identity=identity(),
        fair_value=fair(0.70),
        owned_book=owned_book(bids=((0.50, 5.0),), asks=((0.52, 5.0),)),
        now_ms=NOW_MS,
        cfg=FrequencyV4Config(),
    )
    assert decision.thesis_state == "INVALIDATED"
    assert decision.action == "HOLD"
    assert decision.reason == "hold_value_not_worse"


def test_after_close_official_resolution_is_authoritative_even_with_a_book() -> None:
    after_close = CLOSE_MS + 1
    book = owned_book(
        provider_ts_ms=CLOSE_MS - 100,
        receipt_ts_ms=CLOSE_MS - 50,
    )
    decision = evaluate_exit_vs_hold(
        entry=entry(),
        identity=identity(),
        fair_value=fair(0.20),
        owned_book=book,
        now_ms=after_close,
        cfg=FrequencyV4Config(),
    )
    assert decision.action == "HOLD_OFFICIAL_RESOLUTION"
    assert decision.reason == "post_close_book_exit_forbidden"
    assert decision.sweep is None


def test_preclose_management_refuses_book_evidence_at_or_after_market_close() -> None:
    decision = evaluate_exit_vs_hold(
        entry=entry(),
        identity=identity(),
        fair_value=fair(0.20),
        owned_book=owned_book(
            provider_ts_ms=CLOSE_MS,
            receipt_ts_ms=CLOSE_MS,
        ),
        now_ms=CLOSE_MS - 1,
        cfg=FrequencyV4Config(),
    )
    assert decision.action == "HOLD_OFFICIAL_RESOLUTION"
    assert decision.reason == "post_close_book_evidence_forbidden"


@pytest.mark.parametrize(
    ("book", "reason"),
    [
        (None, "owned_token_book_missing"),
        (owned_book(token="wrong-token"), "wrong_owned_token_book"),
        (owned_book(condition="wrong-condition"), "wrong_market_book"),
        (owned_book(hydrated=False), "owned_book_not_hydrated"),
        (owned_book(
            provider_ts_ms=NOW_MS - 3_000,
            receipt_ts_ms=NOW_MS - 2_900,
        ), "stale_owned_book"),
        (owned_book(bids=((0.70, 4.0),)), "insufficient_exit_depth"),
    ],
)
def test_unsafe_or_incomplete_exit_evidence_fails_to_hold(book, reason) -> None:
    decision = evaluate_exit_vs_hold(
        entry=entry(),
        identity=identity(),
        fair_value=fair(0.20),
        owned_book=book,
        now_ms=NOW_MS,
        cfg=FrequencyV4Config(),
    )
    assert decision.action == "HOLD"
    assert decision.reason == reason
    assert decision.exit_selected is False


def test_management_and_exit_records_reconcile_fees_and_pnl() -> None:
    row = entry(entry_cost=2.80)
    decision = evaluate_exit_vs_hold(
        entry=row,
        identity=identity(),
        fair_value=fair(0.20),
        owned_book=owned_book(),
        now_ms=NOW_MS,
        cfg=FrequencyV4Config(),
    )
    management = management_record(decision, runtime_session_id="session-1")
    exited = exit_record(decision, row, runtime_session_id="session-1")
    assert management["entry_id"] == 7
    assert management["action"] == "EXIT_BOOK"
    assert exited["exit_source"] == "BOOK_PRE_CLOSE"
    assert exited["payout"] == pytest.approx(
        exited["exit_notional"] - exited["exit_fee"]
    )
    assert exited["net_pnl"] == pytest.approx(exited["payout"] - row["entry_cost"])
    assert exited["resolution_verified"] == 0


def test_exit_record_rejects_a_non_exit_decision() -> None:
    hold = evaluate_exit_vs_hold(
        entry=entry(),
        identity=identity(),
        fair_value=fair(0.90),
        owned_book=owned_book(),
        now_ms=NOW_MS,
        cfg=FrequencyV4Config(),
    )
    with pytest.raises(ValueError, match="did not select"):
        exit_record(hold, entry(), runtime_session_id="session-1")
