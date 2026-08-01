"""Telemetry accounting, conservation and readiness semantics.

The lane deliberately sheds noncritical rows when offered load exceeds the
deadline-safe sink capacity.  Before this suite existed, those policy decisions
were accounted the same way as unexpected persistence failure, so a healthy
overloaded runtime could never regain ``operational_ready``.

These tests pin the separation: an approved policy outcome is reported, never
hidden, and never counted as loss; anything the lane was expected to carry and
did not remains blocking.
"""
from __future__ import annotations

import threading
import time

import pytest

from poly_alpha_sniper.lite_frequency_v4.config import FrequencyV4Config
from poly_alpha_sniper.lite_frequency_v4.export import (
    build_frequency_v4_dashboard,
)
from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    POLICY_LOSS_CATEGORIES,
    UNEXPECTED_LOSS_CATEGORIES,
    TelemetryCapacityState,
    TelemetryCommand,
    TelemetryDisposition,
    TelemetryLossCategory,
    TelemetryOverloadPolicy,
    V4TelemetryWriter,
    _AdaptiveTelemetryController,
    _classify_batch_failure,
)
from poly_alpha_sniper.lite_frequency_v4.store import V4Store
from tests.test_frequency_v4_export import (
    CACHED_OK_INTEGRITY,
    _healthy_runtime_state,
    _store_with_health,
)
from tests.test_frequency_v4_store import NOW


class _Sink:
    """Deterministic stand-in for the physical writer."""

    def __init__(self, *, fail: BaseException | None = None) -> None:
        self.fail = fail
        self.batches: list[list[dict]] = []
        self._lock = threading.Lock()

    def submit_telemetry_batch(self, commands, *, timeout_s):
        _ = timeout_s
        if self.fail is not None:
            raise self.fail
        rows = [dict(command) for command in commands]
        with self._lock:
            self.batches.append(rows)
        return len(rows)


def _writer(sink, **overrides) -> V4TelemetryWriter:
    values = {
        "capacity": 64,
        "flush_interval_s": 0.01,
        "coalescing_interval_s": 60.0,
        "submit_timeout_s": 1.0,
        "heartbeat_interval_s": 0.01,
    }
    values.update(overrides)
    values.setdefault("batch_size", max(1, min(8, int(values["capacity"]))))
    return V4TelemetryWriter(sink, **values)


def _controller(*, maximum: int = 32) -> _AdaptiveTelemetryController:
    return _AdaptiveTelemetryController(
        physical_max_chunk=maximum,
        queue_capacity=1_000,
        flush_interval_s=0.25,
        budgeted_sink=True,
        started_monotonic=0.0,
    )


def _drain(writer: V4TelemetryWriter, timeout_s: float = 5.0) -> None:
    assert writer.flush(timeout_s=timeout_s)


# ---------------------------------------------------------------------------
# Taxonomy: categories are exhaustive, disjoint, and correctly assigned
# ---------------------------------------------------------------------------


def test_loss_categories_are_disjoint_and_exhaustive():
    every = {category.value for category in TelemetryLossCategory}
    assert POLICY_LOSS_CATEGORIES | UNEXPECTED_LOSS_CATEGORIES == every
    assert not (POLICY_LOSS_CATEGORIES & UNEXPECTED_LOSS_CATEGORIES)
    # Critical evidence is never a telemetry-lane category: the critical lane is
    # a separate durable writer and its loss is fatal, not sampled.
    assert not any("CRITICAL" in name for name in every)


@pytest.mark.parametrize(
    "error,deadline,expected",
    [
        (None, True, TelemetryLossCategory.DEADLINE_EXPIRED),
        ("anything", True, TelemetryLossCategory.DEADLINE_EXPIRED),
        ("RuntimeError:sink exploded", False, TelemetryLossCategory.SINK_FAILURE),
        (None, False, TelemetryLossCategory.SINK_FAILURE),
        # No message may produce a deduplication verdict any more -- not even
        # the one that used to.  A UNIQUE violation says the key is taken; it
        # says nothing about the evidence outside the key, and most of
        # ``book_snapshots`` is outside it.
        ("IntegrityError:UNIQUE constraint failed: book_snapshots.state_hash",
         False, TelemetryLossCategory.SINK_FAILURE),
        ("IntegrityError:UNIQUE constraint failed: "
         "book_snapshots.market_identity_id, book_snapshots.token_id, "
         "book_snapshots.state_hash, book_snapshots.receipt_ts_ms",
         False, TelemetryLossCategory.SINK_FAILURE),
        ("IntegrityError:UNIQUE constraint failed: entries.entry_id",
         False, TelemetryLossCategory.SINK_FAILURE),
    ],
)
def test_batch_failure_classification(error, deadline, expected):
    assert _classify_batch_failure(
        error, deadline_exceeded=deadline) is expected


def test_only_a_verified_duplicate_can_be_classified_as_deduplication():
    """The verdict comes from the sink's comparison, never from the text.

    The sink reads the stored row and compares every evidence-bearing field
    before it will claim anything is already stored.  This is that contract:
    the typed flag decides, and no error string can substitute for it.
    """

    assert _classify_batch_failure(
        None, deadline_exceeded=False, verified_duplicate=True,
    ) is TelemetryLossCategory.POLICY_DEDUPLICATED
    assert _classify_batch_failure(
        "IntegrityError:UNIQUE constraint failed: book_snapshots.state_hash",
        deadline_exceeded=False, verified_duplicate=True,
    ) is TelemetryLossCategory.POLICY_DEDUPLICATED
    # A cooperative deadline miss is still its own cause and outranks it: the
    # transaction was abandoned, so no comparison happened.
    assert _classify_batch_failure(
        None, deadline_exceeded=True, verified_duplicate=True,
    ) is TelemetryLossCategory.DEADLINE_EXPIRED


# ---------------------------------------------------------------------------
# 1-3. Policy outcomes are not unexpected loss
# ---------------------------------------------------------------------------


def test_policy_sampled_rows_are_not_unexpected_loss():
    writer = _writer(_Sink())
    for index in range(200):
        writer.submit(
            "record_event", index,
            overload_policy=TelemetryOverloadPolicy.SAMPLE,
            overload_key="stream",
        )
    snapshot = writer.snapshot()
    sampled = snapshot["noncritical_rows_policy_sampled"]
    assert sampled > 0                       # the queue is small; sampling fires
    assert snapshot["noncritical_rows_unexpectedly_lost"] == 0
    assert snapshot["loss_by_category"][
        TelemetryLossCategory.QUEUE_OVERFLOW.value] == 0
    assert snapshot["telemetry_data_safety"] == "HEALTHY"


def test_policy_coalesced_rows_are_not_unexpected_loss():
    # A small queue puts the lane under pressure so LATEST actually supersedes.
    writer = _writer(_Sink(), capacity=8)
    for index in range(200):
        writer.submit(
            "update_state", index, state_key="one", state_value={"v": index},
            overload_policy=TelemetryOverloadPolicy.LATEST,
            overload_key="one",
        )
    snapshot = writer.snapshot()
    assert snapshot["noncritical_rows_policy_coalesced"] > 0
    assert snapshot["noncritical_rows_unexpectedly_lost"] == 0
    assert snapshot["telemetry_data_safety"] == "HEALTHY"


def test_deferred_and_requeued_rows_are_not_double_counted():
    writer = _writer(_Sink(), capacity=4)
    dispositions = [
        writer.submit(
            "record_event", index,
            overload_policy=TelemetryOverloadPolicy.DEFER,
            overload_key="stream",
        )
        for index in range(40)
    ]
    assert TelemetryDisposition.DEFERRED in dispositions
    reconciliation = writer.reconcile()
    assert reconciliation["mismatch"] == 0
    # A deferred row was refused admission, so it is never also committed.
    assert (reconciliation["policy_deferred"]
            <= reconciliation["submitted"] - reconciliation["logical_committed"])


# ---------------------------------------------------------------------------
# 4-8. Unexpected loss stays unexpected; critical loss stays fatal
# ---------------------------------------------------------------------------


def test_queue_overflow_outside_policy_remains_unexpected_loss():
    writer = _writer(_Sink(), capacity=4)
    for index in range(200):
        writer.submit(
            "record_event", index,
            overload_policy=TelemetryOverloadPolicy.ADMIT,
        )
    snapshot = writer.snapshot()
    assert snapshot["queue_overflow_rows"] > 0
    assert snapshot["noncritical_rows_unexpectedly_lost"] > 0
    assert snapshot["telemetry_data_safety"] == "UNSAFE"
    assert "unexpected_noncritical_loss" in (
        snapshot["telemetry_data_safety_reasons"])


def test_sink_failure_remains_unexpected_loss():
    writer = _writer(_Sink(fail=RuntimeError("sink unavailable")))
    writer.start()
    try:
        for index in range(8):
            writer.submit("record_event", index)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if writer.snapshot()["sink_failure_rows"] > 0:
                break
            time.sleep(0.02)
    finally:
        writer.stop(drain=False, timeout_s=5.0)
    snapshot = writer.snapshot()
    assert snapshot["sink_failure_rows"] > 0
    assert snapshot["noncritical_rows_unexpectedly_lost"] > 0
    assert snapshot["telemetry_data_safety"] == "UNSAFE"


def test_shutdown_abandonment_remains_unexpected_loss():
    writer = _writer(_Sink())
    for index in range(16):
        writer.submit("record_event", index)
    writer.stop(drain=False, timeout_s=2.0)
    snapshot = writer.snapshot()
    assert snapshot["shutdown_abandoned_rows"] > 0
    assert snapshot["shutdown_abandoned_rows"] in (
        snapshot["loss_by_category"][
            TelemetryLossCategory.SHUTDOWN_ABANDONED.value],)
    assert snapshot["noncritical_rows_unexpectedly_lost"] > 0


def test_submit_after_stop_is_shutdown_abandoned_not_policy():
    writer = _writer(_Sink())
    writer.stop(drain=False, timeout_s=2.0)
    assert writer.submit("record_event", 1) is TelemetryDisposition.DROPPED
    snapshot = writer.snapshot()
    assert snapshot["shutdown_abandoned_rows"] >= 1
    assert snapshot["noncritical_rows_policy_sampled"] == 0


def test_critical_loss_is_never_a_telemetry_policy_category(tmp_path):
    """Critical evidence loss must fail closed, never appear as sampling."""

    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["persistence"]["telemetry"]["true_lost_critical_rows"] = 1
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY,
        )
        persistence = payload["persistence"]
        assert "critical_evidence_lost" in persistence["critical_blocked_reasons"]
        assert persistence["critical_execution_ready"] is False
        assert persistence["operational_ready"] is False
    finally:
        store.close()


# ---------------------------------------------------------------------------
# 9-10. Conservation
# ---------------------------------------------------------------------------


def test_conservation_holds_exactly_under_mixed_categories():
    writer = _writer(_Sink(), capacity=8)
    writer.start()
    try:
        for index in range(120):
            writer.submit(
                "record_event", index,
                overload_policy=TelemetryOverloadPolicy.SAMPLE,
                overload_key=f"stream-{index % 3}",
            )
            writer.submit(
                "update_state", index, state_key=f"s{index % 4}",
                state_value={"v": index},
                overload_policy=TelemetryOverloadPolicy.LATEST,
                overload_key=f"s{index % 4}",
            )
            writer.submit(
                "record_bucket", index, bucket_key=f"b{index % 5}",
                event_ts_ms=1_000 * (index % 5),
                overload_policy=TelemetryOverloadPolicy.ADMIT,
            )
        _drain(writer)
    finally:
        writer.stop(drain=True, timeout_s=5.0)

    reconciliation = writer.reconcile()
    assert reconciliation["mismatch"] == 0, reconciliation
    assert reconciliation["submitted"] == reconciliation["accounted"]
    assert writer.snapshot()["accounting_reconciliation_mismatch_rows"] == 0


def test_conservation_holds_while_a_batch_is_in_flight():
    """Rows handed to the sink but not yet acknowledged must stay accounted.

    Found by the live soak: reconciliation transiently reported an 8-10 row
    mismatch because rows removed from the queue for dispatch were subtracted
    from ``buffered`` before ``logical_committed`` grew, leaving them owned by
    nobody.  A transient mismatch is indistinguishable from real loss, so it
    blocked readiness at random.
    """

    released = threading.Event()
    entered = threading.Event()

    class _BlockingSink:
        def submit_telemetry_batch(self, commands, *, timeout_s):
            _ = timeout_s
            entered.set()
            assert released.wait(timeout=10.0), "test deadlock"
            return len(list(commands))

    writer = _writer(_BlockingSink(), capacity=32)
    writer.start()
    try:
        for index in range(24):
            writer.submit("record_event", index)
        assert entered.wait(timeout=5.0)
        # A batch is now sitting inside the sink, neither queued nor committed.
        mid_flight = writer.reconcile()
        assert mid_flight["inflight"] > 0, mid_flight
        assert mid_flight["mismatch"] == 0, mid_flight
        released.set()
        assert writer.flush(timeout_s=5.0)
    finally:
        released.set()
        writer.stop(drain=True, timeout_s=5.0)

    settled = writer.reconcile()
    assert settled["inflight"] == 0, settled
    assert settled["mismatch"] == 0, settled
    assert writer.snapshot()["telemetry_data_safety"] == "HEALTHY"


def test_capacity_state_is_hard_overload_only_when_out_of_control():
    """Shedding by policy is not hard overload; losing rows is."""

    shedding = _writer(_Sink(), capacity=8)
    for index in range(200):
        shedding.submit(
            "record_event", index,
            overload_policy=TelemetryOverloadPolicy.SAMPLE,
            overload_key="stream",
        )
    snapshot = shedding.snapshot()
    assert snapshot["telemetry_capacity_state"] == (
        TelemetryCapacityState.POLICY_SAMPLING_ACTIVE.value)
    assert snapshot["telemetry_data_safety"] == "HEALTHY"

    losing = _writer(_Sink(), capacity=8)
    for index in range(200):
        losing.submit(
            "record_event", index,
            overload_policy=TelemetryOverloadPolicy.ADMIT,
        )
    snapshot = losing.snapshot()
    assert snapshot["telemetry_capacity_state"] == (
        TelemetryCapacityState.HARD_OVERLOAD.value)
    assert snapshot["telemetry_data_safety"] == "UNSAFE"


def test_unreachable_throughput_floor_does_not_block_recovery_forever():
    """Demand above physical capacity must not make the controller "unsafe".

    Found by the live shadow soak.  When offered load exceeds what the deadline
    budget allows, ``throughput_required_chunk`` rises above
    ``deadline_safe_chunk``; the controller correctly clamps to the safe
    maximum and sheds the difference.  Requiring ``selected >= required``
    unconditionally then reported the controller unsafe in exactly the steady
    state it was designed for, pinning the recovery streak at zero forever.
    """

    controller = _controller(maximum=9)
    decision = None
    for tick in range(1, 121):
        # Heavy offered load, shed down to what the sink can actually take.
        # The sink is genuinely the constraint: a 500 ms dispatch sustains two
        # per second, so the deadline budget admits only three rows per
        # transaction while the demand floor asks for six.  That shortfall --
        # not the rate at which the lane happens to be asked to run -- is what
        # lifts the required chunk above the deadline-safe one.
        controller.add(
            float(tick), queue_depth=1,
            incoming=60, offered=60, admitted=9,
            sampled=51, overload_handled=51)
        controller.observe_commit(
            now=float(tick), rows=9, logical_rows=9,
            transaction_ms=500.0, total_ms=500.0, queue_depth=1)
        decision = controller.decide(
            now=float(tick), queue_depth=1,
            transaction_budget_ms=250.0, queued_logical=1)

    assert decision is not None
    # The demand shortfall stays visible rather than being hidden...
    assert decision.throughput_required_chunk >= decision.selected_chunk
    assert decision.selected_chunk <= decision.deadline_safe_chunk
    # ...but a controller pinned at its safe maximum while shedding is safe.
    assert "controller_chunk_unsafe" not in decision.recovery_blockers
    assert "uncontrolled_overload" not in decision.recovery_blockers


def test_chunk_above_deadline_safe_is_always_unsafe():
    """The deadline budget itself is absolute and must never be relaxed."""

    controller = _controller(maximum=32)
    for tick in range(1, 30):
        controller.add(float(tick), queue_depth=0, offered=10, admitted=10)
        controller.observe_commit(
            now=float(tick), rows=10, logical_rows=10,
            transaction_ms=5.0, total_ms=5.0, queue_depth=0)
        controller.decide(
            now=float(tick), queue_depth=0,
            transaction_budget_ms=250.0, queued_logical=0)
    # Force an oversized selection and confirm it is rejected outright.
    controller.selected_chunk = controller.deadline_safe_chunk + 1
    assert controller._controller_safe() is False


def test_recovery_blockers_name_the_failing_condition():
    """The lane explains why it is not recovering rather than just saying no."""

    controller = _controller()
    # A steadily growing queue: the blocker must be named, not implied.
    for tick in range(1, 40):
        controller.add(float(tick), queue_depth=tick, offered=20, admitted=20)
        controller.observe_commit(
            now=float(tick), rows=19, logical_rows=19,
            transaction_ms=20.0, total_ms=20.0, queue_depth=tick)
        decision = controller.decide(
            now=float(tick), queue_depth=tick,
            transaction_budget_ms=250.0, queued_logical=tick)
    assert "queue_accumulating" in decision.recovery_blockers
    assert decision.current_operational_healthy is False

    healthy = _controller()
    for tick in range(1, 121):
        healthy.add(float(tick), queue_depth=0, offered=20, admitted=20)
        healthy.observe_commit(
            now=float(tick), rows=20, logical_rows=20,
            transaction_ms=5.0, total_ms=5.0, queue_depth=0)
        decision = healthy.decide(
            now=float(tick), queue_depth=0,
            transaction_budget_ms=250.0, queued_logical=0)
    assert decision.recovery_blockers == ()


def test_reconciliation_mismatch_blocks_readiness(tmp_path):
    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["persistence"]["telemetry"][
            "accounting_reconciliation_mismatch_rows"] = 3
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY,
        )
        persistence = payload["persistence"]
        assert "telemetry_accounting_mismatch" in (
            persistence["operational_degraded_reasons"])
        assert persistence["operational_ready"] is False
    finally:
        store.close()


# ---------------------------------------------------------------------------
# 11-12. Historical vs current
# ---------------------------------------------------------------------------


def test_lifetime_counters_stay_visible_and_do_not_latch_current_health(tmp_path):
    """A large historical loss total must not pin current readiness false."""

    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        # Historical, ambiguous, and deliberately left as-is.
        state["persistence"]["telemetry"]["raw_telemetry_loss_count"] = 76_056
        state["persistence"]["telemetry"]["rows_dropped"] = 68_589
        state["persistence"]["telemetry"]["failed_batches"] = 85
        # Current window is clean.
        state["persistence"]["telemetry"]["window_unexpected_loss_rows"] = 0
        state["persistence"]["telemetry"]["last_failure_ts_ms"] = 0
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY,
        )
        persistence = payload["persistence"]
        # Lifetime totals stay exposed for audit...
        assert persistence["telemetry_recovery"][
            "lifetime_raw_telemetry_loss"] == 76_056
        # ...but do not block a currently healthy lane.
        assert persistence["operational_ready"] is True
        assert "telemetry_unexpected_noncritical_loss" not in (
            persistence["operational_degraded_reasons"])
    finally:
        store.close()


def test_missing_telemetry_health_model_fails_closed(tmp_path):
    """An older/partial runtime state that cannot report safety is UNKNOWN."""

    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        for field in ("telemetry_data_safety", "telemetry_capacity_state",
                      "queue_bounded"):
            state["persistence"]["telemetry"].pop(field, None)
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY,
        )
        persistence = payload["persistence"]
        assert persistence["operational_ready"] is False
        assert "telemetry_data_safety_unknown" in (
            persistence["operational_degraded_reasons"])
        assert "telemetry_capacity_unknown" in (
            persistence["operational_degraded_reasons"])
    finally:
        store.close()


# ---------------------------------------------------------------------------
# 13-15. Health / readiness semantics
# ---------------------------------------------------------------------------


def test_policy_sampling_active_can_be_operational_ready(tmp_path):
    """The whole point: safe-but-shedding is a ready state, reported honestly."""

    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["persistence"]["telemetry"].update({
            "telemetry_capacity_state":
                TelemetryCapacityState.POLICY_SAMPLING_ACTIVE.value,
            "sampling_keep_ratio": 0.393,
            "overload_reason": "throughput_exceeds_deadline_safe_capacity",
            "window_unexpected_loss_rows": 0,
            "accounting_reconciliation_mismatch_rows": 0,
            "queue_bounded": True,
        })
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY,
        )
        persistence = payload["persistence"]
        assert persistence["operational_ready"] is True
        model = persistence["telemetry_health_model"]
        # Overload is never hidden.
        assert model["reported_state"] == "HEALTHY_WITH_POLICY_SAMPLING"
        assert model["capacity_state"] == "POLICY_SAMPLING_ACTIVE"
        assert model["policy_sampling_active"] is True
        assert model["data_safety"] == "HEALTHY"
        assert model["sampling_keep_ratio"] == 0.393
        assert model["sampling_policy_reason"] == (
            "throughput_exceeds_deadline_safe_capacity")
    finally:
        store.close()


def test_hard_overload_remains_not_ready(tmp_path):
    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["persistence"]["telemetry"]["telemetry_capacity_state"] = (
            TelemetryCapacityState.HARD_OVERLOAD.value)
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY,
        )
        persistence = payload["persistence"]
        assert persistence["operational_ready"] is False
        assert "telemetry_capacity_hard_overload" in (
            persistence["operational_degraded_reasons"])
    finally:
        store.close()


def test_unbounded_queue_blocks_readiness_even_when_sampling(tmp_path):
    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["persistence"]["telemetry"].update({
            "telemetry_capacity_state":
                TelemetryCapacityState.POLICY_SAMPLING_ACTIVE.value,
            "queue_bounded": False,
        })
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY,
        )
        persistence = payload["persistence"]
        assert "telemetry_queue_unbounded" in (
            persistence["operational_degraded_reasons"])
        assert persistence["operational_ready"] is False
    finally:
        store.close()


def test_recent_unexpected_loss_blocks_readiness_even_when_sampling(tmp_path):
    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["persistence"]["telemetry"].update({
            "telemetry_capacity_state":
                TelemetryCapacityState.POLICY_SAMPLING_ACTIVE.value,
            "window_unexpected_loss_rows": 2,
        })
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY,
        )
        persistence = payload["persistence"]
        assert "telemetry_unexpected_noncritical_loss" in (
            persistence["operational_degraded_reasons"])
        assert persistence["operational_ready"] is False
    finally:
        store.close()


# ---------------------------------------------------------------------------
# 16. Recovery window behaviour
# ---------------------------------------------------------------------------


def test_controlled_overload_reaches_ten_healthy_windows():
    """A fully-serviced, bounded lane recovers even while shedding.

    Offered load exceeds capacity, so the controller sheds by policy; every
    admitted row is committed, the queue stays empty and nothing is lost.  This
    is exactly the production shape that previously never recovered.
    """

    controller = _controller()
    decision = None
    for tick in range(1, 121):
        controller.add(
            float(tick), queue_depth=0,
            incoming=40, offered=40, admitted=20,
            sampled=20, overload_handled=20)
        controller.observe_commit(
            now=float(tick), rows=20, logical_rows=20,
            transaction_ms=5.0, total_ms=5.0, queue_depth=0)
        decision = controller.decide(
            now=float(tick), queue_depth=0,
            transaction_budget_ms=250.0, queued_logical=0)
    assert decision is not None
    assert decision.view.lost == 0                 # nothing unexpected was lost
    assert decision.view.sampled > 0               # shedding really is active
    assert decision.recovery_healthy_windows >= 10
    assert decision.current_operational_healthy is True


def test_unexpected_loss_resets_the_recovery_streak():
    controller = _controller()
    for tick in range(1, 121):
        controller.add(
            float(tick), queue_depth=0, incoming=20, offered=20, admitted=20)
        controller.observe_commit(
            now=float(tick), rows=20, logical_rows=20,
            transaction_ms=5.0, total_ms=5.0, queue_depth=0)
        controller.decide(
            now=float(tick), queue_depth=0,
            transaction_budget_ms=250.0, queued_logical=0)
    healthy_before = controller.decide(
        now=121.0, queue_depth=0, transaction_budget_ms=250.0,
        queued_logical=0, advance_state=False)
    assert healthy_before.recovery_healthy_windows >= 10

    # One genuinely lost row resets the streak.
    controller.add(122.0, queue_depth=0, lost=1)
    decision = controller.decide(
        now=123.0, queue_depth=0, transaction_budget_ms=250.0,
        queued_logical=0)
    assert decision.recovery_healthy_windows == 0
    assert decision.current_operational_healthy is False


def test_growing_queue_is_never_healthy_even_below_low_water():
    """Absolute depth is not proof: a steadily climbing queue is unhealthy."""

    controller = _controller()
    decision = None
    for tick in range(1, 46):
        controller.add(
            float(tick), queue_depth=tick, offered=20, admitted=20)
        controller.observe_commit(
            now=float(tick), rows=19, logical_rows=19,
            transaction_ms=20.0, total_ms=20.0, queue_depth=tick)
        decision = controller.decide(
            now=float(tick), queue_depth=tick,
            transaction_budget_ms=250.0, queued_logical=tick)
    assert decision is not None
    assert decision.view.queue_slope_rps > 0
    assert decision.current_operational_healthy is False


def test_empty_queue_does_not_report_phantom_growth():
    """The regression bug: event-sampled depth reported growth for an empty queue.

    Depth is observed only when something happens.  A burst second records a
    mid-burst depth; quiet seconds record nothing.  Regressing over only the
    observed buckets reported a steep positive slope for a queue that returned
    to empty every single second.
    """

    controller = _controller()
    for tick in range(1, 30):
        # A burst is admitted and fully drained within the same second.
        controller.add(float(tick), queue_depth=8, admitted=8)
        controller.observe_commit(
            now=float(tick), rows=8, logical_rows=8,
            transaction_ms=5.0, total_ms=5.0, queue_depth=0)
    decision = controller.decide(
        now=30.0, queue_depth=0, transaction_budget_ms=250.0,
        queued_logical=0, advance_state=False)
    assert decision.view.queue_slope_rps <= 0.0


def test_service_balance_uses_one_consistent_unit():
    """Physical rows committed must not be compared against logical admitted.

    Aggregation merges many logical rows into one pending, which the sink writes
    as one physical row.  Comparing those two numbers made any coalescing look
    like a service deficit forever.
    """

    controller = _controller()
    decision = None
    for tick in range(1, 121):
        # 20 logical rows admitted, aggregated into 4 physical rows, all of
        # which commit.  Nothing is lost and the queue stays empty.
        controller.add(float(tick), queue_depth=0, offered=20, admitted=20)
        controller.observe_commit(
            now=float(tick), rows=4, logical_rows=20,
            transaction_ms=5.0, total_ms=5.0, queue_depth=0)
        decision = controller.decide(
            now=float(tick), queue_depth=0,
            transaction_budget_ms=250.0, queued_logical=0)
    assert decision is not None
    assert decision.view.committed < decision.view.admitted   # physical < logical
    assert decision.view.logical_committed >= decision.view.admitted
    assert decision.current_operational_healthy is True


# ---------------------------------------------------------------------------
# 17. Policy visibility in the export
# ---------------------------------------------------------------------------


def test_keep_ratio_and_policy_reason_appear_in_export(tmp_path):
    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["persistence"]["telemetry"].update({
            "telemetry_capacity_state":
                TelemetryCapacityState.POLICY_SAMPLING_ACTIVE.value,
            "sampling_keep_ratio": 0.25,
            "overload_reason": "throughput_exceeds_deadline_safe_capacity",
            "loss_by_category": {
                TelemetryLossCategory.POLICY_SAMPLED.value: 1_234},
            "policy_outcome_breakdown": {"writer_policy_sampled": 1_234},
            "unexpected_loss_breakdown": {"writer_unexpected_rows": 0},
            "accounting_reconciliation": {"mismatch": 0},
        })
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY,
        )
        model = payload["persistence"]["telemetry_health_model"]
        assert model["sampling_keep_ratio"] == 0.25
        assert model["sampling_policy_reason"] == (
            "throughput_exceeds_deadline_safe_capacity")
        assert model["loss_by_category"][
            TelemetryLossCategory.POLICY_SAMPLED.value] == 1_234
        assert model["policy_outcome_breakdown"]["writer_policy_sampled"] == 1_234
        assert model["accounting_reconciliation"]["mismatch"] == 0
    finally:
        store.close()


def test_snapshot_never_hides_overload():
    """Counters are additive; nothing is removed to make the lane look clean."""

    writer = _writer(_Sink(), capacity=4)
    for index in range(120):
        writer.submit(
            "record_event", index,
            overload_policy=TelemetryOverloadPolicy.SAMPLE,
            overload_key="stream",
        )
    snapshot = writer.snapshot()
    for required in (
        "loss_by_category", "accounting_reconciliation",
        "telemetry_data_safety", "telemetry_capacity_state",
        "noncritical_rows_policy_sampled", "noncritical_rows_unexpectedly_lost",
        "sampling_keep_ratio", "queue_bounded", "rows_dropped", "rows_sampled",
    ):
        assert required in snapshot, required
    # Lifetime drop/sample series remain present for continuity.
    assert snapshot["rows_sampled"] >= snapshot["noncritical_rows_policy_sampled"] - 1


# ---------------------------------------------------------------------------
# 18. The critical lane stays lossless while the noncritical lane sheds
# ---------------------------------------------------------------------------


def test_critical_lane_lossless_under_sustained_noncritical_overload(tmp_path):
    """Sustained shedding on the lossy lane must not touch critical evidence."""

    from poly_alpha_sniper.lite_frequency_v4.persistence import (
        V4PersistenceWriter,
    )

    db = tmp_path / "poly_alpha_frequency_v4.db"
    V4Store(db).close()
    persistence = V4PersistenceWriter(str(db), queue_capacity=512)
    persistence.start()
    telemetry = V4TelemetryWriter(
        persistence, capacity=8, batch_size=4, flush_interval_s=0.01,
        coalescing_interval_s=60.0, submit_timeout_s=2.0,
        heartbeat_interval_s=0.01,
    )
    telemetry.start()
    try:
        for index in range(400):
            telemetry.submit(
                "record_event_count_batch", [],
                overload_policy=TelemetryOverloadPolicy.SAMPLE,
                overload_key=f"stream-{index % 4}",
            )
        telemetry.flush(timeout_s=5.0)
        health = persistence.health()
    finally:
        telemetry.stop(drain=True, timeout_s=5.0)
        persistence.close(timeout_s=5.0)

    snapshot = telemetry.snapshot()
    # The noncritical lane shed rows...
    assert snapshot["noncritical_rows_policy_sampled"] > 0
    # ...and the critical writer lost nothing.
    assert int(health.get("critical_evidence_lost_count") or 0) == 0
    assert int(health.get("unconfirmed_command_count") or 0) == 0


def test_policy_sampling_does_not_mark_the_writer_failed():
    """Sampling must not stamp last_failure_ts_ms or degrade writer health."""

    writer = _writer(_Sink(), capacity=4)
    for index in range(200):
        writer.submit(
            "record_event", index,
            overload_policy=TelemetryOverloadPolicy.SAMPLE,
            overload_key="stream",
        )
    snapshot = writer.snapshot()
    assert snapshot["noncritical_rows_policy_sampled"] > 0
    # Sampling must not stamp a failure or push the writer into a degraded
    # health state; it is a policy decision, not an incident.
    assert not str(snapshot["health"]).startswith("DEGRADED")
    assert int(snapshot["last_failure_ts_ms"] or 0) == 0
    assert snapshot["telemetry_data_safety"] == "HEALTHY"
