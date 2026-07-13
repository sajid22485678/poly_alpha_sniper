from __future__ import annotations

import pytest

from lite_frequency_v4.contracts import BookLevel, BookState, CexObservation
from lite_frequency_v4.features import BookHistoryBuffer, CexFeatureBuffer


BASE_MS = 1_800_000_100_000


def observation(
    offset_ms: int,
    price: float,
    *,
    event_id: str | None = None,
    unchanged: bool = False,
    classification: str = "NEW_TICK",
    epoch: int = 1,
    receipt_offset_ms: int = 0,
) -> CexObservation:
    provider_ts = BASE_MS + offset_ms
    return CexObservation(
        provider="okx",
        asset="BTC",
        instrument="BTC-USDT",
        price=price,
        provider_ts_ms=provider_ts,
        receipt_ts_ms=provider_ts + receipt_offset_ms,
        receipt_monotonic_ns=(provider_ts + receipt_offset_ms) * 1_000_000,
        event_id=event_id or f"tick-{offset_ms}",
        connection_epoch=epoch,
        unchanged=unchanged,
        classification=classification,
    )


def book(token: str, provider_ts_ms: int, *, event_id: str) -> BookState:
    return BookState(
        token_id=token,
        condition_id="condition-1",
        bids=(BookLevel(0.49, 10.0),),
        asks=(BookLevel(0.51, 10.0),),
        provider_ts_ms=provider_ts_ms,
        receipt_ts_ms=provider_ts_ms,
        receipt_monotonic_ns=provider_ts_ms * 1_000_000,
        source="polymarket_ws",
        event_id=event_id,
        payload_hash=event_id,
        connection_epoch=1,
        min_order_size=5.0,
        tick_size=0.01,
    )


def test_feature_buffer_builds_strict_point_in_time_horizons_and_window_open() -> None:
    buffer = CexFeatureBuffer(reference_tolerance_ms=100)
    prices = {offset: 100.0 + offset / 10_000.0 for offset in range(0, 31_000, 1_000)}
    for offset, price in prices.items():
        assert buffer.append(observation(offset, price))

    evidence = buffer.build(
        "btc",
        now_ms=BASE_MS + 30_100,
        window_open_ms=BASE_MS + 10_000,
        max_age_ms=500,
    )

    assert evidence.features.valid is True
    assert evidence.features.provider_ts_ms == BASE_MS + 30_000
    assert evidence.features.returns[1] == pytest.approx(
        prices[30_000] / prices[29_000] - 1.0
    )
    assert evidence.features.returns[30] == pytest.approx(
        prices[30_000] / prices[0] - 1.0
    )
    assert evidence.features.window_open_price == prices[10_000]
    assert evidence.features.window_return == pytest.approx(
        prices[30_000] / prices[10_000] - 1.0
    )
    assert all(row.provider_ts_ms <= evidence.features.provider_ts_ms for row in evidence.observations)
    assert len({row.event_id for row in evidence.observations}) == len(evidence.observations)


def test_feature_buffer_never_uses_a_future_reference() -> None:
    buffer = CexFeatureBuffer(reference_tolerance_ms=500)
    assert buffer.append(observation(10_100, 101.0))
    assert buffer.append(observation(11_000, 102.0))

    evidence = buffer.build(
        "BTC",
        now_ms=BASE_MS + 11_000,
        window_open_ms=BASE_MS + 10_000,
        max_age_ms=100,
    )

    # The only observation near the 10-second open is after it, so no open
    # reference may be synthesized by looking forward 100 ms.
    assert evidence.features.window_open_price is None
    assert evidence.features.window_return is None
    assert evidence.features.returns[1] is None


def test_unchanged_but_fresh_tick_is_valid_no_new_tick_evidence() -> None:
    buffer = CexFeatureBuffer()
    assert buffer.append(observation(0, 100.0))
    assert buffer.append(observation(
        1_000,
        100.0,
        unchanged=True,
        classification="NO_NEW_TICK",
    ))

    evidence = buffer.build(
        "BTC",
        now_ms=BASE_MS + 1_100,
        window_open_ms=BASE_MS,
        max_age_ms=250,
    )

    assert evidence.features.valid is True
    assert evidence.features.classification == "NO_NEW_TICK"
    assert evidence.features.latest_move_ts_ms == BASE_MS
    assert evidence.features.tick_return == 0.0
    assert evidence.features.invalidation_reason == ""


def test_buffer_rejects_future_regressed_duplicate_and_old_epoch_observations() -> None:
    buffer = CexFeatureBuffer()
    assert buffer.append(observation(1_000, 100.0, epoch=2))
    assert not buffer.append(observation(
        2_000, 101.0, epoch=2, receipt_offset_ms=-1
    ))
    assert not buffer.append(observation(999, 99.0, epoch=2))
    assert not buffer.append(observation(2_000, 101.0, event_id="tick-1000", epoch=2))
    assert not buffer.append(observation(2_000, 101.0, epoch=1))
    assert buffer.latest("BTC") == observation(1_000, 100.0, epoch=2)


def test_stale_and_future_builds_fail_closed() -> None:
    buffer = CexFeatureBuffer()
    assert buffer.append(observation(0, 100.0))
    stale = buffer.build(
        "BTC",
        now_ms=BASE_MS + 5_000,
        window_open_ms=BASE_MS,
        max_age_ms=1_000,
    )
    assert stale.features.valid is False
    assert stale.features.invalidation_reason == "stale_cex_evidence"

    future = buffer.build(
        "BTC",
        now_ms=BASE_MS - 1,
        window_open_ms=BASE_MS - 10_000,
        max_age_ms=1_000,
    )
    assert future.features.valid is False
    assert future.features.invalidation_reason == "future_cex_evidence"


def test_feature_replay_is_deterministic() -> None:
    buffer = CexFeatureBuffer()
    for offset in range(0, 31_000, 1_000):
        assert buffer.append(observation(offset, 50_000.0 + offset / 1_000.0))

    kwargs = dict(
        asset="BTC",
        now_ms=BASE_MS + 30_250,
        window_open_ms=BASE_MS,
        max_age_ms=500,
    )
    first = buffer.build(**kwargs)
    second = buffer.build(**kwargs)
    assert first == second
    assert first.features.to_dict() == second.features.to_dict()


def test_book_history_rejects_regression_and_filters_future_points() -> None:
    history = BookHistoryBuffer(max_points_per_window=3)
    yes1, no1 = book("yes", BASE_MS, event_id="yes-1"), book("no", BASE_MS, event_id="no-1")
    yes2, no2 = book("yes", BASE_MS + 1_000, event_id="yes-2"), book(
        "no", BASE_MS + 1_000, event_id="no-2"
    )
    future_yes, future_no = book("yes", BASE_MS + 2_000, event_id="yes-3"), book(
        "no", BASE_MS + 2_000, event_id="no-3"
    )

    assert history.append_pair("BTC:window", yes1, no1)
    assert history.append_pair("BTC:window", yes2, no2)
    assert not history.append_pair("BTC:window", yes1, no1)
    assert history.append_pair("BTC:window", future_yes, future_no)

    visible = history.points("BTC:window", now_ms=BASE_MS + 1_500)
    assert [point.provider_ts_ms for point in visible] == [BASE_MS, BASE_MS + 1_000]
    history.remove("BTC:window")
    assert history.points("BTC:window", now_ms=BASE_MS + 10_000) == ()
