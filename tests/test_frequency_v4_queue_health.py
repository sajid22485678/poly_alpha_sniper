"""Queue-health classification, recovery gating and WAL reclaim scheduling.

A 64.7-minute shadow soak showed the previous gates could not distinguish a
shallow oscillating queue from real backlog.  The telemetry queue peaked at 150
rows out of 20 000 (0.75% of capacity), yet ``queue_accumulating`` fired in
87/130 samples -- 65 of them below the low-water mark, one at depth 3 -- while
unexpected loss, reconciliation mismatch and critical loss were all exactly zero
throughout.  Three ``HARD_OVERLOAD`` samples had queue depths of 2-9 rows.

These tests pin the corrected semantics: bounded oscillation is healthy, genuine
accumulation still blocks, and a recovered deadline miss is not loss of control.
"""
from __future__ import annotations

import pytest

from poly_alpha_sniper.lite_frequency_v4.config import FrequencyV4Config
from poly_alpha_sniper.lite_frequency_v4.export import (
    build_frequency_v4_dashboard,
)
from poly_alpha_sniper.lite_frequency_v4.maintenance import (
    CheckpointMode,
    MaintenancePolicy,
    MaintenanceSnapshot,
    decide_checkpoint,
)
from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    TelemetryCapacityState,
    TelemetryLossCategory,
    _AdaptiveTelemetryController,
)
from tests.test_frequency_v4_export import (
    CACHED_OK_INTEGRITY,
    _healthy_runtime_state,
    _store_with_health,
)
from tests.test_frequency_v4_store import NOW


def _controller(*, maximum: int = 32, capacity: int = 20_000):
    return _AdaptiveTelemetryController(
        physical_max_chunk=maximum,
        queue_capacity=capacity,
        flush_interval_s=0.25,
        budgeted_sink=True,
        started_monotonic=0.0,
    )


def _held(controller) -> int:
    """Inventory the controller's own counters imply it is still holding.

    Phases chain: a fixture that assumed every run started from an empty lane
    would mis-attribute the whole carried-over depth on its first tick.  Reading
    it back from the counters keeps a chained fixture exact without every caller
    having to remember where the previous phase left off.
    """

    return controller.window.conservation_now(
        0.0, queued_logical=0, inflight_logical=0).gap


def _resolve_surplus(admitted, committed, depth, held):
    """Close the fixture's own books for one tick.

    A depth series is a statement about inventory, and inventory only moves
    because rows entered or left the lane.  A fixture that admits twenty rows,
    commits twenty, and then asserts the queue went from one to nothing is
    describing a lane that lost a row -- which is precisely what the
    conservation identity now reports, correctly.

    So the surplus is attributed the way the writer attributes it: rows that
    were admitted and then resolved by the shedding policy, which is a
    deliberate, accounted, non-blocking outcome.  Returns the extra admissions
    and the policy resolutions that make the tick balance exactly.
    """

    resolved = admitted - committed - (depth - held)
    return (0, resolved) if resolved >= 0 else (-resolved, 0)


def _drive(controller, depths, *, admitted=20, committed=20, tick0=1,
           overload_handled=0):
    """Run the controller across an explicit depth series and return the last decision.

    ``overload_handled`` mirrors what the writer records whenever the shedding
    policy resolves an admission, which is what marks an overload as *controlled*.
    """

    decision = None
    held = _held(controller)
    for offset, depth in enumerate(depths):
        tick = float(tick0 + offset)
        extra, resolved = _resolve_surplus(admitted, committed, depth, held)
        controller.add(tick, queue_depth=depth, offered=admitted + extra,
                       admitted=admitted + extra,
                       overload_handled=overload_handled + resolved,
                       policy_resolved=resolved)
        controller.observe_commit(
            now=tick, rows=committed, logical_rows=committed,
            transaction_ms=5.0, total_ms=5.0, queue_depth=depth)
        held = depth
        decision = controller.decide(
            now=tick, queue_depth=depth, transaction_budget_ms=250.0,
            queued_logical=depth)
    return decision


# ---------------------------------------------------------------------------
# 1-5. Queue classification
# ---------------------------------------------------------------------------


def test_shallow_oscillation_is_not_accumulating():
    """The exact production shape: shallow, spiky, always draining.

    Depths like 0 -> 5 -> 2 -> 7 -> 1 are the shedding policy doing its job.
    The floor never rises, so this is not backlog.
    """
    controller = _controller()
    pattern = [0, 5, 2, 7, 1, 3, 9, 2, 6, 0, 4, 8, 1, 5, 2] * 6
    decision = _drive(controller, pattern)
    assert "queue_accumulating" not in decision.recovery_blockers


def test_large_spike_with_successful_drain_is_not_accumulating():
    """5 -> 96 -> 5: a burst absorbed and drained is healthy, not backlog."""

    controller = _controller()
    pattern = ([5, 40, 96, 60, 20, 5] * 12)
    decision = _drive(controller, pattern)
    assert "queue_accumulating" not in decision.recovery_blockers


def test_sustained_monotonic_growth_is_accumulating():
    """A queue whose floor climbs every window is real backlog and must block."""

    controller = _controller()
    decision = _drive(controller, list(range(1, 80)), committed=19)
    assert "queue_accumulating" in decision.recovery_blockers
    assert decision.current_operational_healthy is False


def test_queue_approaching_hard_capacity_is_accumulating():
    """Depth near the hard bound blocks regardless of trend."""

    controller = _controller(capacity=1_000)
    # Deep and flat: no rising floor, but more than half of capacity.
    decision = _drive(controller, [600] * 40)
    assert "queue_accumulating" in decision.recovery_blockers


def test_stale_oldest_row_is_accumulating_even_when_shallow():
    """A shallow queue that never drains is still backlog.

    Depth alone cannot see this; residence age can.
    """
    controller = _controller()
    for tick in range(1, 40):
        controller.add(float(tick), queue_depth=4, offered=20, admitted=20)
        controller.observe_commit(
            now=float(tick), rows=20, logical_rows=20,
            transaction_ms=5.0, total_ms=5.0, queue_depth=4)
        decision = controller.decide(
            now=float(tick), queue_depth=4, transaction_budget_ms=250.0,
            queued_logical=4, oldest_age_s=120.0)
    assert "queue_accumulating" in decision.recovery_blockers

    # The same shallow queue that *does* drain is healthy.
    fresh = _controller()
    for tick in range(1, 40):
        fresh.add(float(tick), queue_depth=4, offered=20, admitted=20)
        fresh.observe_commit(
            now=float(tick), rows=20, logical_rows=20,
            transaction_ms=5.0, total_ms=5.0, queue_depth=4)
        ok = fresh.decide(
            now=float(tick), queue_depth=4, transaction_budget_ms=250.0,
            queued_logical=4, oldest_age_s=0.2)
    assert "queue_accumulating" not in ok.recovery_blockers


def test_queue_that_drains_after_policy_response_recovers():
    """Pressure, then a successful drain, must let the streak build again."""

    controller = _controller()
    _drive(controller, list(range(1, 40)), committed=19)     # backlog builds
    decision = _drive(controller, [2, 0, 3, 1, 0, 2] * 25, tick0=60)
    assert "queue_accumulating" not in decision.recovery_blockers
    assert decision.recovery_healthy_windows >= 10
    assert decision.current_operational_healthy is True


# ---------------------------------------------------------------------------
# 6-9. Recovery gating
# ---------------------------------------------------------------------------


def test_ten_healthy_windows_are_still_required():
    controller = _controller()
    seen = []
    for offset in range(30):
        tick = float(1 + offset)
        controller.add(tick, queue_depth=1, offered=20, admitted=20)
        controller.observe_commit(
            now=tick, rows=20, logical_rows=20, transaction_ms=5.0,
            total_ms=5.0, queue_depth=1)
        d = controller.decide(now=tick, queue_depth=1,
                              transaction_budget_ms=250.0, queued_logical=1)
        seen.append((d.recovery_healthy_windows, d.current_operational_healthy))
    # Never healthy before the tenth consecutive window.
    for windows, healthy in seen:
        if healthy:
            assert windows >= 10


def test_unexpected_loss_still_resets_recovery():
    controller = _controller()
    decision = _drive(controller, [1] * 60)
    assert decision.recovery_healthy_windows >= 10
    controller.add(100.0, queue_depth=1, lost=1)
    after = controller.decide(now=101.0, queue_depth=1,
                              transaction_budget_ms=250.0, queued_logical=1)
    assert after.recovery_healthy_windows == 0
    assert "unexpected_loss" in after.recovery_blockers


def test_alternating_policy_sampling_does_not_reset_safe_recovery():
    """Sampling engaging and disengaging is normal under variable load."""

    controller = _controller()
    decision = None
    held = 0
    for offset in range(80):
        tick = float(1 + offset)
        depth = offset % 3
        # Alternate between shedding and not, with a shallow draining queue.
        # ``sampled`` is pre-admission, so those rows never enter the identity;
        # the shallow depth cycle is closed the same way the writer closes it.
        sampled = 20 if offset % 2 else 0
        extra, resolved = _resolve_surplus(20, 20, depth, held)
        controller.add(tick, queue_depth=depth, offered=20 + sampled + extra,
                       admitted=20 + extra, sampled=sampled,
                       overload_handled=sampled + resolved,
                       policy_resolved=resolved)
        controller.observe_commit(
            now=tick, rows=20, logical_rows=20, transaction_ms=5.0,
            total_ms=5.0, queue_depth=depth)
        held = depth
        decision = controller.decide(
            now=tick, queue_depth=depth, transaction_budget_ms=250.0,
            queued_logical=depth)
    assert decision.recovery_healthy_windows >= 10
    assert decision.current_operational_healthy is True


def test_inflight_rows_do_not_look_like_a_service_imbalance():
    """The recovery window excludes the current second.

    A row admitted in the window's last included second and acknowledged in the
    excluded current second belongs to neither the committed sum nor the queue.
    Counting it as a service deficit produced a phantom ``service_imbalance``
    under ordinary bursty load.
    """
    controller = _controller()
    decision = None
    for offset in range(60):
        tick = float(1 + offset)
        # Every admitted row is handed to the sink but acknowledged a beat
        # later, so the queue reads empty while rows are still in flight.
        controller.add(tick, queue_depth=0, offered=20, admitted=20)
        controller.observe_commit(
            now=tick, rows=20, logical_rows=20, transaction_ms=5.0,
            total_ms=5.0, queue_depth=0)
        decision = controller.decide(
            now=tick, queue_depth=0, transaction_budget_ms=250.0,
            queued_logical=0, inflight_logical=20)
    assert "service_imbalance" not in decision.recovery_blockers
    assert decision.current_operational_healthy is True


def test_unaccounted_rows_still_report_a_service_imbalance():
    """Conservation must still bite when rows really are unaccounted."""

    controller = _controller()
    decision = None
    for offset in range(40):
        tick = float(1 + offset)
        # 20 admitted, only 5 ever committed, nothing queued or in flight.
        controller.add(tick, queue_depth=0, offered=20, admitted=20)
        controller.observe_commit(
            now=tick, rows=5, logical_rows=5, transaction_ms=5.0,
            total_ms=5.0, queue_depth=0)
        decision = controller.decide(
            now=tick, queue_depth=0, transaction_budget_ms=250.0,
            queued_logical=0, inflight_logical=0)
    assert "service_imbalance" in decision.recovery_blockers


def test_deduplicated_batch_is_not_a_controller_failure():
    """A content-addressed duplicate is not a sink failure.

    Measured in production the telemetry sink rejected ~116 batches on
    ``UNIQUE constraint failed: book_snapshots.state_hash``: the identical row
    was already stored, so no evidence was lost.  Counting those as controller
    failures reset the recovery settle clock roughly once a minute, which alone
    kept ``settling`` the dominant blocker and held readiness down.
    """
    from poly_alpha_sniper.lite_frequency_v4.telemetry import (
        POLICY_LOSS_CATEGORIES,
        _classify_batch_failure,
    )

    # The exact production error text, as the dispatcher formats it.  On its
    # own it proves nothing: the constraint names four columns and the evidence
    # this table carries mostly sits outside them, so the text alone is now a
    # sink failure and the sink must actually compare the rows.
    duplicate = ("RuntimeError:IntegrityError:UNIQUE constraint failed: "
                 "book_snapshots.market_identity_id, book_snapshots.token_id, "
                 "book_snapshots.state_hash, book_snapshots.receipt_ts_ms")
    assert _classify_batch_failure(
        duplicate, deadline_exceeded=False,
    ) is TelemetryLossCategory.SINK_FAILURE
    category = _classify_batch_failure(
        duplicate, deadline_exceeded=False, verified_duplicate=True)
    assert category is TelemetryLossCategory.POLICY_DEDUPLICATED
    # ...and being a policy outcome is exactly what keeps it out of the
    # controller's failed-batch path, so the settle clock is not reset.
    assert category.value in POLICY_LOSS_CATEGORIES

    # A genuine sink failure is still a controller setback.
    genuine = "RuntimeError:sink unavailable"
    assert _classify_batch_failure(
        genuine, deadline_exceeded=False) is TelemetryLossCategory.SINK_FAILURE
    assert (TelemetryLossCategory.SINK_FAILURE.value
            not in POLICY_LOSS_CATEGORIES)

    # A deduplicated batch must not reset a healthy recovery streak.
    controller = _controller()
    decision = _drive(controller, [1, 0, 2] * 25)
    assert decision.recovery_healthy_windows >= 10

    # A batch failure whose rows are requeued costs nothing and does not reset
    # recovery -- the retry budget bounds how long that can hide a real problem.
    controller.add(200.0, queue_depth=1, failed_batches=1)
    retried = controller.decide(now=201.0, queue_depth=1,
                                transaction_budget_ms=250.0, queued_logical=1)
    assert retried.recovery_healthy_windows >= 10

    # A failure that actually loses rows is terminal and does reset it.
    controller.add(202.0, queue_depth=1, failed_batches=1, lost=4)
    terminal = controller.decide(now=203.0, queue_depth=1,
                                 transaction_budget_ms=250.0, queued_logical=1)
    assert terminal.recovery_healthy_windows == 0
    assert "unexpected_loss" in terminal.recovery_blockers


@pytest.mark.parametrize("sink_error,label", [
    ("IntegrityError:UNIQUE constraint failed: book_snapshots.state_hash",
     "deduplicated"),
    ("sink unavailable", "sink_failure"),
])
def test_conservation_closes_for_every_batch_failure_category(
        sink_error, label):
    """Classifying a batch must never change whether its rows are accounted.

    A deduplicated batch briefly short-circuited the accounting branch, so its
    rows -- already released from the in-flight tally when the batch was taken
    -- were owned by no category at all.  Reconciliation drifted permanently:
    a live 12-minute reproduction reached a mismatch of 88 rows with every loss
    category still reading zero.  Whatever a failure is called, it must land in
    exactly one bucket.
    """
    import threading as _threading
    import time as _time

    from poly_alpha_sniper.lite_frequency_v4.telemetry import (
        V4TelemetryWriter,
    )

    class _FailingSink:
        def __init__(self):
            self.calls = 0
            self.lock = _threading.Lock()

        def submit_telemetry_batch(self, commands, *, timeout_s):
            _ = timeout_s
            with self.lock:
                self.calls += 1
            raise RuntimeError(sink_error)

    sink = _FailingSink()
    writer = V4TelemetryWriter(
        sink, capacity=64, batch_size=8, flush_interval_s=0.01,
        coalescing_interval_s=60.0, submit_timeout_s=1.0,
        heartbeat_interval_s=0.01)
    writer.start()
    try:
        for index in range(24):
            writer.submit("record_book_snapshot", index)
        deadline = _time.monotonic() + 5.0
        while _time.monotonic() < deadline and sink.calls < 2:
            _time.sleep(0.02)
        _time.sleep(0.3)
    finally:
        writer.stop(drain=False, timeout_s=5.0)

    assert sink.calls > 0, label
    reconciliation = writer.reconcile()
    # The identity closes exactly: nothing submitted is unowned.
    assert reconciliation["mismatch"] == 0, (label, reconciliation)
    assert reconciliation["submitted"] == reconciliation["accounted"], label
    # And the rows landed in a category rather than vanishing.
    snapshot = writer.snapshot()
    categorised = sum(snapshot["loss_by_category"].values())
    assert categorised > 0, (label, snapshot["loss_by_category"])


def test_recovered_deadline_miss_does_not_block_recovery():
    """A deadline miss whose rows are requeued cost nothing.

    Measured across a 64.7-minute soak, ``failed_batches`` and
    ``deadline_failures`` advanced while unexpected loss stayed at exactly zero,
    yet each event restarted a 20 s settle clock and blocked a 15 s window --
    roughly 35 s of blocked recovery per miss.  The chunk still shrinks (that is
    the adaptation a miss calls for); recovery is no longer punished.
    """
    controller = _controller()
    decision = _drive(controller, [1, 0, 2] * 25)
    assert decision.recovery_healthy_windows >= 10
    prior_chunk = controller.selected_chunk

    controller.observe_deadline_miss(
        now=200.0, failed_rows=16, transaction_budget_ms=250.0, queue_depth=1)
    after = controller.decide(now=201.0, queue_depth=1,
                              transaction_budget_ms=250.0, queued_logical=1)
    # Adaptation happened...
    assert controller.selected_chunk <= prior_chunk
    # ...but a recovered miss did not restart the settle clock.
    assert "settling" not in after.recovery_blockers
    assert after.recovery_healthy_windows >= 10

    # The same miss that ends in dropped rows is terminal and does block.
    controller.add(202.0, queue_depth=1, deadline_failures=1, lost=3)
    terminal = controller.decide(now=203.0, queue_depth=1,
                                 transaction_budget_ms=250.0, queued_logical=1)
    assert terminal.recovery_healthy_windows == 0
    assert "unexpected_loss" in terminal.recovery_blockers


def test_safe_high_water_entry_does_not_reset_the_settle_clock():
    """Engaging the shedding policy is not a setback when nothing was lost."""

    controller = _controller()
    # Build a healthy streak on a shallow draining queue.
    decision = _drive(controller, [1, 0, 2, 1] * 20)
    assert decision.recovery_healthy_windows >= 10
    established = decision.recovery_healthy_windows

    # Now cross the high-water mark briefly with zero loss, then drain.  Being
    # above low water legitimately pauses the streak while it lasts; what must
    # NOT happen is the 20 s settle clock restarting, because that would keep
    # the lane un-certifiable long after the burst has cleared.
    burst = [controller.high_water + 5, controller.high_water + 2, 3, 1, 0, 2]
    after = _drive(controller, burst * 6, tick0=200, overload_handled=8)
    assert "settling" not in after.recovery_blockers
    assert after.recovery_blockers == ()
    assert established >= 10

    # With the burst cleared, the streak rebuilds promptly rather than waiting
    # out a fresh settle interval.
    calm = _drive(controller, [1, 0, 2] * 8, tick0=400, overload_handled=8)
    assert "settling" not in calm.recovery_blockers
    assert calm.recovery_healthy_windows >= 10
    assert calm.current_operational_healthy is True


def test_lossy_high_water_entry_still_resets_the_settle_clock():
    """A capacity setback that actually cost rows is still a setback."""

    controller = _controller()
    decision = _drive(controller, [1, 0, 2, 1] * 20)
    assert decision.recovery_healthy_windows >= 10
    # Cross high water *and* lose a row in the same window.
    controller.add(300.0, queue_depth=controller.high_water + 5,
                   admitted=20, lost=2)
    after = controller.decide(
        now=301.0, queue_depth=controller.high_water + 5,
        transaction_budget_ms=250.0, queued_logical=0)
    assert after.recovery_healthy_windows == 0


# ---------------------------------------------------------------------------
# 10-11. Overload classification
# ---------------------------------------------------------------------------


def test_recovered_deadline_miss_is_not_hard_overload(tmp_path):
    """All three soak HARD_OVERLOAD samples were this shape: depth 2-9, zero loss."""

    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["persistence"]["telemetry"].update({
            "telemetry_capacity_state":
                TelemetryCapacityState.POLICY_SAMPLING_ACTIVE.value,
            "telemetry_data_safety": "DEGRADED",
            "telemetry_data_safety_reasons": ["recent_deadline_expiry_recovered"],
            "deadline_exceeded_batches": 6,
            "window_unexpected_loss_rows": 0,
            "accounting_reconciliation_mismatch_rows": 0,
            "queue_bounded": True,
            "queue_oldest_age_s": 0.4,
        })
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY)
        persistence = payload["persistence"]
        # Reported honestly...
        model = persistence["telemetry_health_model"]
        assert model["data_safety"] == "DEGRADED"
        assert "recent_deadline_expiry_recovered" in model["data_safety_reasons"]
        # ...but a recovered miss that cost no rows does not block readiness.
        assert persistence["operational_ready"] is True
        assert not any(r.startswith("telemetry_data_safety")
                       for r in persistence["operational_degraded_reasons"])
    finally:
        store.close()


def test_genuine_loss_is_unsafe_and_blocks(tmp_path):
    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["persistence"]["telemetry"].update({
            "telemetry_data_safety": "UNSAFE",
            "telemetry_data_safety_reasons": ["unexpected_noncritical_loss"],
            "window_unexpected_loss_rows": 3,
        })
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY)
        persistence = payload["persistence"]
        assert persistence["operational_ready"] is False
        assert "telemetry_data_safety_unsafe" in (
            persistence["operational_degraded_reasons"])
        assert "telemetry_unexpected_noncritical_loss" in (
            persistence["operational_degraded_reasons"])
    finally:
        store.close()


def test_stale_queue_residence_blocks_readiness(tmp_path):
    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["persistence"]["telemetry"]["queue_oldest_age_s"] = 45.0
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state, integrity=CACHED_OK_INTEGRITY)
        assert "telemetry_queue_residence_stale" in (
            payload["persistence"]["operational_degraded_reasons"])
        assert payload["persistence"]["operational_ready"] is False
    finally:
        store.close()


# ---------------------------------------------------------------------------
# 12-15. WAL reclaim scheduling
# ---------------------------------------------------------------------------


def _snapshot(**overrides) -> MaintenanceSnapshot:
    values = dict(
        now_ms=10_000_000,
        wal_bytes=300 * 1024 * 1024,
        critical_queue_depth=0,
        telemetry_queue_depth=4,
        runtime_active=True,
        runtime_health="RUNNING",
        writer_healthy=True,
        open_positions=3,
        active_readers=0,
        long_reader_count=0,
        consecutive_no_progress_passive=3,
        integrity_ok=True,
        critical_commit_p95_ms=5.0,
        time_to_window_boundary_ms=200_000,
        last_checkpoint_attempt_ts_ms=10_000_000 - 300_000,
        last_successful_checkpoint_ts_ms=10_000_000 - 300_000,
    )
    values.update(overrides)
    return MaintenanceSnapshot(**values)


def test_live_reclaim_runs_when_the_loop_is_responsive():
    decision = decide_checkpoint(_snapshot(event_loop_lag_ms=5.0),
                              MaintenancePolicy())
    assert decision.should_run is True
    assert decision.mode is CheckpointMode.TRUNCATE


def test_live_reclaim_defers_while_the_event_loop_is_lagging():
    """The measured cause of every heartbeat/export breach in the soak."""

    policy = MaintenancePolicy()
    decision = decide_checkpoint(
        _snapshot(event_loop_lag_ms=policy.lag_defer_threshold_ms + 1),
        policy)
    assert decision.mode is not CheckpointMode.TRUNCATE
    # The deferral is explicit and observable, not a silent skip.
    assert decision.reason


def test_deferred_reclaim_still_runs_once_the_wal_ceiling_is_reached():
    """Deferral must never become unbounded WAL growth."""

    policy = MaintenancePolicy()
    decision = decide_checkpoint(
        _snapshot(event_loop_lag_ms=policy.lag_defer_threshold_ms * 10,
                  wal_bytes=policy.lag_defer_max_wal_bytes + 1),
        policy)
    assert decision.should_run is True
    assert decision.mode is CheckpointMode.TRUNCATE


def test_wal_pressure_backfill_runs_far_more_often_than_the_routine_interval():
    """The measured cause of the 300-470 MB WAL regime.

    The definitive soak showed the WAL growing ~110 MB/min while checkpoints ran
    once per ~1.4 min, so every PASSIVE landed on a WAL already hundreds of
    megabytes past the 32 MB trigger and reclaimed exactly zero bytes; the file
    only ever shrank via a 374-425 MB one-shot TRUNCATE that stalled export
    publication for 13-16 s.  A backfill must be allowed while the file is still
    small.
    """
    policy = MaintenancePolicy()
    assert policy.pressure_checkpoint_min_interval_ms < (
        policy.checkpoint_min_interval_ms)
    # Just past the pressure floor, well inside the routine interval.
    snap = _snapshot(
        wal_bytes=policy.wal_trigger_bytes + 1,
        consecutive_no_progress_passive=0,
        now_ms=1_000_000,
        last_checkpoint_attempt_ts_ms=(
            1_000_000 - policy.pressure_checkpoint_min_interval_ms - 1),
    )
    decision = decide_checkpoint(snap, policy)
    assert decision.should_run is True
    assert decision.mode is CheckpointMode.PASSIVE
    # At the documented trigger the file is ~32 MB, not hundreds.
    assert snap.wal_bytes < 64 * 1024 * 1024


def test_backfill_cadence_can_keep_pace_with_measured_wal_growth():
    """A sanity bound tying the cadence to the measured growth rate.

    ~110 MB/min was measured under load.  For the 32 MB trigger to mean
    anything, the backfill interval must be short enough that the WAL cannot
    outrun it by an order of magnitude between attempts.
    """
    policy = MaintenancePolicy()
    measured_growth_bytes_per_ms = (110 * 1024 * 1024) / 60_000.0
    growth_between_attempts = (
        measured_growth_bytes_per_ms
        * policy.pressure_checkpoint_min_interval_ms)
    # Growth between backfills stays within the trigger itself, so the WAL
    # cannot reach the prior failure regime between attempts.
    assert growth_between_attempts <= policy.wal_trigger_bytes
    # The old routine cadence provably could not hold that line: a single
    # interval overshoots the trigger several times over, which is how the file
    # walked up to the 300-470 MB regime across successive no-progress passes.
    old = measured_growth_bytes_per_ms * policy.checkpoint_min_interval_ms
    assert old > policy.wal_trigger_bytes * 3


def test_escalation_is_not_allowed_on_the_pressure_cadence():
    """Only the cheap backfill may run early; reclaim keeps its full interval."""

    policy = MaintenancePolicy()
    snap = _snapshot(
        wal_bytes=policy.restart_trigger_bytes * 3,
        consecutive_no_progress_passive=9,
        now_ms=1_000_000,
        last_checkpoint_attempt_ts_ms=(
            1_000_000 - policy.pressure_checkpoint_min_interval_ms - 1),
    )
    decision = decide_checkpoint(snap, policy)
    assert decision.mode is CheckpointMode.PASSIVE
    assert decision.mode is not CheckpointMode.TRUNCATE


def test_reclaim_remains_reachable_after_the_routine_interval():
    """Bounding the cadence must not make reclaim unreachable."""

    policy = MaintenancePolicy()
    snap = _snapshot(
        wal_bytes=policy.restart_trigger_bytes * 3,
        consecutive_no_progress_passive=9,
        now_ms=1_000_000,
        last_checkpoint_attempt_ts_ms=(
            1_000_000 - policy.checkpoint_min_interval_ms - 1),
        event_loop_lag_ms=0.0,
    )
    decision = decide_checkpoint(snap, policy)
    assert decision.should_run is True
    assert decision.mode is CheckpointMode.TRUNCATE


def test_lag_deferral_default_keeps_prior_behaviour():
    """A caller that does not report lag behaves exactly as before."""

    decision = decide_checkpoint(_snapshot(), MaintenancePolicy())
    assert decision.should_run is True
    assert decision.mode is CheckpointMode.TRUNCATE


def test_the_zero_keep_ratio_clamp_is_keyed_to_the_hard_bound():
    """Going completely dark belongs at the bound, not at the watermark.

    Shedding every sampleable row is the last thing between the queue and its
    hard capacity, where an admission overflows outside policy and the row is
    genuinely lost.  It was keyed to ``high_water + physical_max_chunk``
    instead, which on the shadow configuration is depth 160 in a 20,000-row
    queue -- 0.8% of capacity.  Measured on the definitive soak at 4d8655c, all
    31 zero-keep-ratio ticks sat between depth 160 and 169 with zero blockers
    and zero unexpected loss: a lane in no danger at all, dark for a second.
    """

    controller = _controller(maximum=32, capacity=20_000)
    assert controller.high_water == 128
    clamp = max(controller.high_water + controller.physical_max_chunk,
                controller.queue_capacity - controller.physical_max_chunk)
    assert clamp == 19_968

    def keep_at(depth, tick):
        for step in range(3):
            now = float(tick + step)
            controller.add(now, queue_depth=depth, incoming=200, offered=200,
                           admitted=50, sampled=150, overload_handled=200,
                           policy_resolved=50)
            controller.observe_commit(
                now=now, rows=50, logical_rows=50, transaction_ms=5.0,
                total_ms=5.0, queue_depth=depth)
        return controller.decide(
            now=float(tick + 4), queue_depth=depth,
            transaction_budget_ms=250.0, queued_logical=depth,
        ).sampling_keep_ratio

    # The exact depths the soak went dark at: throttled now, never zero.
    for index, depth in enumerate((160, 165, 169)):
        ratio = keep_at(depth, 100 + index * 10)
        assert ratio > 0.0, depth
        assert ratio <= 0.25, depth       # high-water throttle still applies

    # And at the hard bound it still goes to zero, which is the whole point.
    assert keep_at(clamp, 400) == 0.0
    assert keep_at(controller.queue_capacity, 500) == 0.0


def test_a_small_queue_keeps_the_previous_clamp_threshold():
    """Where a chunk is a large fraction of the queue, nothing changes."""

    controller = _controller(maximum=4, capacity=8)
    assert (max(controller.high_water + controller.physical_max_chunk,
                controller.queue_capacity - controller.physical_max_chunk)
            == controller.high_water + controller.physical_max_chunk)
