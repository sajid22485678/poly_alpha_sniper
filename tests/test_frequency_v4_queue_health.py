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


def _drive(controller, depths, *, admitted=20, committed=20, tick0=1):
    """Run the controller across an explicit depth series and return the last decision."""

    decision = None
    for offset, depth in enumerate(depths):
        tick = float(tick0 + offset)
        controller.add(tick, queue_depth=depth, offered=admitted,
                       admitted=admitted)
        controller.observe_commit(
            now=tick, rows=committed, logical_rows=committed,
            transaction_ms=5.0, total_ms=5.0, queue_depth=depth)
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
    for offset in range(80):
        tick = float(1 + offset)
        # Alternate between shedding and not, with a shallow draining queue.
        sampled = 20 if offset % 2 else 0
        controller.add(tick, queue_depth=offset % 3, offered=20 + sampled,
                       admitted=20, sampled=sampled,
                       overload_handled=sampled)
        controller.observe_commit(
            now=tick, rows=20, logical_rows=20, transaction_ms=5.0,
            total_ms=5.0, queue_depth=offset % 3)
        decision = controller.decide(
            now=tick, queue_depth=offset % 3, transaction_budget_ms=250.0,
            queued_logical=offset % 3)
    assert decision.recovery_healthy_windows >= 10
    assert decision.current_operational_healthy is True


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


def test_lag_deferral_default_keeps_prior_behaviour():
    """A caller that does not report lag behaves exactly as before."""

    decision = decide_checkpoint(_snapshot(), MaintenancePolicy())
    assert decision.should_run is True
    assert decision.mode is CheckpointMode.TRUNCATE
