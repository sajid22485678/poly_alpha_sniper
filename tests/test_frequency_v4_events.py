from __future__ import annotations

import pytest

from lite_frequency_v4.contracts import CexObservation, SourceEvent
from lite_frequency_v4.events import (
    BACKOFF_SECONDS,
    EventDecision,
    EventDisposition,
    EventGate,
    SequencePolicy,
    SourceHealth,
    backoff_seconds,
    canonical_json,
    canonical_payload_hash,
    parse_positive_millis,
)


def source_event(
        *, ts: int = 1_000, receipt: int = 1_010, mono: int = 10,
        payload_hash: str = "hash-1", sequence: int | None = None,
        epoch: int = 1,
) -> SourceEvent:
    return SourceEvent(
        source="test",
        channel="market",
        event_type="book",
        event_key=f"event-{payload_hash}",
        payload_hash=payload_hash,
        provider_ts_ms=ts,
        receipt_ts_ms=receipt,
        receipt_monotonic_ns=mono,
        sequence=sequence,
        connection_epoch=epoch,
    )


def cex_event(
        *, ts: int, receipt: int, mono: int, event_id: str, price: float,
        sequence: int | None = None, epoch: int = 1,
) -> CexObservation:
    return CexObservation(
        provider="okx",
        asset="BTC",
        instrument="BTC-USDT",
        price=price,
        provider_ts_ms=ts,
        receipt_ts_ms=receipt,
        receipt_monotonic_ns=mono,
        event_id=event_id,
        event_type="trade",
        side="buy",
        sequence=sequence,
        connection_epoch=epoch,
        size=1.0,
    )


def test_canonical_payload_is_stable_and_strict() -> None:
    left = {"z": [2, 1], "a": {"b": "value"}}
    right = {"a": {"b": "value"}, "z": [2, 1]}
    assert canonical_json(left) == canonical_json(right)
    assert canonical_payload_hash(left) == canonical_payload_hash(right)
    with pytest.raises(ValueError):
        canonical_json({"unsafe": float("nan")})


@pytest.mark.parametrize(
    ("value", "expected"),
    [("123", 123), (123, 123), ("0", None), (0, None),
     (-1, None), (True, None), ("1.5", None), (" 12 ", 12)],
)
def test_positive_millisecond_parser_is_integer_only(value: object,
                                                       expected: int | None) -> None:
    assert parse_positive_millis(value) == expected


def test_gate_rejects_future_and_regressed_provider_timestamps() -> None:
    gate = EventGate()
    future = source_event(ts=1_011, receipt=1_010)
    assert gate.evaluate(future, stream_key="btc").disposition is \
        EventDisposition.REJECT_FUTURE

    accepted = source_event(ts=1_000, receipt=1_010, mono=20)
    assert gate.evaluate(accepted, stream_key="btc").accepted
    regressed = source_event(
        ts=999, receipt=1_020, mono=30, payload_hash="hash-2")
    assert gate.evaluate(regressed, stream_key="btc").disposition is \
        EventDisposition.REJECT_TIMESTAMP_REGRESSION


def test_gate_rejects_first_seen_stale_evidence() -> None:
    gate = EventGate()
    stale = source_event(ts=1_000, receipt=3_001)
    decision = gate.evaluate(stale, stream_key="btc", max_age_ms=2_000)
    assert decision.disposition is EventDisposition.REJECT_STALE
    assert gate.stream_state("test", "market", "btc") == {}


def test_exact_replay_is_deduplicated_before_regression_checks() -> None:
    gate = EventGate()
    original = source_event()
    assert gate.evaluate(original, stream_key="btc").accepted
    newer = source_event(
        ts=1_020, receipt=1_030, mono=30, payload_hash="hash-new")
    assert gate.evaluate(newer, stream_key="btc").accepted
    assert gate.evaluate(original, stream_key="btc").disposition is \
        EventDisposition.DROP_DUPLICATE


def test_same_timestamp_conflict_requires_explicit_stream_permission() -> None:
    strict = EventGate()
    first = source_event()
    second = source_event(payload_hash="different", mono=11)
    assert strict.evaluate(first, stream_key="btc").accepted
    assert strict.evaluate(second, stream_key="btc").disposition is \
        EventDisposition.REJECT_TIMESTAMP_CONFLICT

    permissive = EventGate()
    assert permissive.evaluate(first, stream_key="btc").accepted
    assert permissive.evaluate(
        second, stream_key="btc",
        allow_equal_timestamp_distinct=True).accepted


def test_monotonic_receipt_cannot_regress_within_connection() -> None:
    gate = EventGate()
    assert gate.evaluate(source_event(mono=20), stream_key="btc").accepted
    decision = gate.evaluate(
        source_event(ts=1_001, receipt=1_011, mono=19, payload_hash="next"),
        stream_key="btc")
    assert decision.disposition is EventDisposition.REJECT_RECEIPT_REGRESSION


def test_unchanged_but_fresh_tick_is_valid_without_moving_signal_watermark() -> None:
    gate = EventGate()
    first = cex_event(
        ts=1_000, receipt=1_010, mono=10, event_id="trade-1", price=100.0)
    unchanged = cex_event(
        ts=1_020, receipt=1_030, mono=20, event_id="trade-2", price=100.0)
    changed = cex_event(
        ts=1_040, receipt=1_050, mono=30, event_id="trade-3", price=101.0)

    assert gate.evaluate(
        first, stream_key="trades:BTC-USDT",
        observation_value=first.price).disposition is EventDisposition.ACCEPT_NEW
    assert gate.latest_move_ts_ms(
        "okx", "cex", "trades:BTC-USDT") == 1_000
    assert gate.evaluate(
        unchanged, stream_key="trades:BTC-USDT",
        observation_value=unchanged.price).disposition is \
        EventDisposition.ACCEPT_NO_NEW_TICK
    assert gate.latest_move_ts_ms(
        "okx", "cex", "trades:BTC-USDT") == 1_000
    assert gate.evaluate(
        changed, stream_key="trades:BTC-USDT",
        observation_value=changed.price).disposition is EventDisposition.ACCEPT_NEW
    assert gate.latest_move_ts_ms(
        "okx", "cex", "trades:BTC-USDT") == 1_040


def test_noncontiguous_okx_sequences_are_not_fictional_gap_detection() -> None:
    gate = EventGate()
    values = [
        cex_event(ts=1_000, receipt=1_010, mono=10, event_id="a",
                  price=100, sequence=50),
        cex_event(ts=1_001, receipt=1_011, mono=11, event_id="b",
                  price=101, sequence=50),
        cex_event(ts=1_002, receipt=1_012, mono=12, event_id="c",
                  price=102, sequence=900),
    ]
    for event in values:
        assert gate.evaluate(
            event, stream_key="trades:BTC-USDT",
            sequence_policy=SequencePolicy.NONDECREASING,
            observation_value=event.price).accepted
    lower = cex_event(
        ts=1_003, receipt=1_013, mono=13, event_id="d",
        price=103, sequence=899)
    assert gate.evaluate(
        lower, stream_key="trades:BTC-USDT",
        sequence_policy=SequencePolicy.NONDECREASING,
        observation_value=lower.price).disposition is \
        EventDisposition.REJECT_SEQUENCE

    reset = cex_event(
        ts=1_004, receipt=1_014, mono=14, event_id="e",
        price=104, sequence=1, epoch=2)
    assert gate.evaluate(
        reset, stream_key="trades:BTC-USDT",
        sequence_policy=SequencePolicy.NONDECREASING,
        observation_value=reset.price).accepted


def test_health_counts_actual_hydration_request_once() -> None:
    health = SourceHealth("test")
    health.record(EventDecision(
        EventDisposition.BUFFER_UNHYDRATED,
        source_event(),
        "not_hydrated",
        request_hydration=True,
    ))
    assert health.buffered_events == 1
    # The adapter increments this only when it actually invokes recovery.
    assert health.hydration_requests == 0


def test_bounded_backoff_is_exact_and_capped() -> None:
    assert BACKOFF_SECONDS == (1.0, 2.0, 4.0, 8.0, 16.0, 30.0)
    assert [backoff_seconds(i) for i in range(8)] == [
        1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]
    assert backoff_seconds(-5) == 1.0


def test_stream_and_dedupe_state_are_bounded() -> None:
    gate = EventGate(dedupe_capacity=2, stream_capacity=2)
    for index in range(3):
        event = source_event(
            ts=1_000 + index,
            receipt=1_010 + index,
            mono=10 + index,
            payload_hash=f"hash-{index}",
        )
        assert gate.evaluate(event, stream_key=f"stream-{index}").accepted
    assert gate.stream_state("test", "market", "stream-0") == {}
    assert gate.stream_state("test", "market", "stream-1")
    assert len(gate._seen) == 2
