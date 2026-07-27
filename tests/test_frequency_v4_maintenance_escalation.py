"""Tests for the PASSIVE no-progress -> stronger-mode WAL escalation.

The live escalation exists because a PASSIVE checkpoint under continuous write
load backfills every frame and still reclaims zero bytes: it reports
``frames_checkpointed == frames_total`` while ``after_wal_bytes >=
before_wal_bytes`` because the WAL could not be restarted.  Only TRUNCATE
returns bytes to the filesystem, so that is what the live path escalates to.
Its preconditions are database-level safety properties only.
"""
from __future__ import annotations

import pytest

from lite_frequency_v4.maintenance import (
    EMERGENCY_WAL_REASON, LIVE_RECLAIM_REASON, CheckpointDecision, CheckpointMode, CheckpointResult,
    CheckpointStatus, MaintenancePolicy, MaintenanceSnapshot,
    _invoke_reclaiming_checkpoint, decide_checkpoint, perform_checkpoint,
)


class _BackgroundWriteDeferred(RuntimeError):
    """Mirrors the Store exception raised when the write gate refuses."""


_BackgroundWriteDeferred.__name__ = "V4BackgroundWriteDeferred"


class _GatedStore:
    """Store stub whose background-write gate refuses the first N attempts."""

    def __init__(self, refusals: int, *, wal_bytes: int = 500_000_000) -> None:
        self.refusals = int(refusals)
        self.attempts = 0
        self.busy_timeouts: list = []
        self.wal_bytes = int(wal_bytes)
        self.recorded: list = []

    def checkpoint(self, *, mode: str = "PASSIVE", reason: str = "manual",
                   busy_timeout_ms=None):
        self.attempts += 1
        if self.attempts <= self.refusals:
            raise _BackgroundWriteDeferred("critical write pending")
        self.busy_timeouts.append(busy_timeout_ms)
        before, self.wal_bytes = self.wal_bytes, 0
        return {
            "checkpoint_run_id": self.attempts, "mode": mode,
            "before_wal_bytes": before, "after_wal_bytes": 0,
            "duration_ms": 5.0, "busy_result": 0,
            "frames_total": 1_000, "frames_checkpointed": 1_000,
            "success": True, "failure_reason": None,
        }

    def database_size_bytes(self) -> int:
        return 6_000_000_000


def _snapshot(**overrides):
    base = dict(
        now_ms=1_000, wal_bytes=500_000_000,
        critical_queue_depth=0, telemetry_queue_depth=0,
        runtime_active=True, runtime_health="HEALTHY", writer_healthy=True,
        open_positions=0, active_readers=0, long_reader_count=0,
        critical_commit_p95_ms=15.0, time_to_window_boundary_ms=120_000,
        consecutive_no_progress_passive=0,
    )
    base.update(overrides)
    return MaintenanceSnapshot(**base)


def _result(before: int, after: int, *, frames: int = 1_000,
            mode: CheckpointMode = CheckpointMode.PASSIVE) -> CheckpointResult:
    return CheckpointResult(
        status=CheckpointStatus.SUCCESS, mode=mode, reason="test",
        started_ts_ms=1, completed_ts_ms=2, duration_ms=1.0,
        before_wal_bytes=before, after_wal_bytes=after,
        busy_result=0, frames_total=frames, frames_checkpointed=frames,
    )


# --------------------------------------------------------------------------
# Progress is measured in reclaimed bytes, never in checkpointed frames.
# --------------------------------------------------------------------------

def test_full_frame_backfill_without_byte_reclaim_is_not_progress():
    """The exact production shape: all frames copied, file never shrank."""
    result = _result(376_279_632, 376_279_632)
    assert result.frames_progress is True
    assert result.made_progress is False
    assert result.bytes_reclaimed == 0


def test_growing_wal_despite_checkpointed_frames_is_not_progress():
    result = _result(326_464_712, 326_481_192)
    assert result.frames_progress is True
    assert result.made_progress is False
    assert result.bytes_reclaimed == 0


def test_reclaimed_bytes_are_progress():
    result = _result(300_000_000, 4_096)
    assert result.made_progress is True
    assert result.bytes_reclaimed == 300_000_000 - 4_096


def test_as_dict_exposes_byte_accounting():
    payload = _result(200_000_000, 100_000_000).as_dict()
    assert payload["made_progress"] is True
    assert payload["frames_progress"] is True
    assert payload["bytes_reclaimed"] == 100_000_000
    # The persisted checkpoint_runs row keeps its fixed schema.
    assert "bytes_reclaimed" not in _result(1, 1).checkpoint_record()


# --------------------------------------------------------------------------
# Live escalation.
# --------------------------------------------------------------------------

def test_repeated_no_progress_escalates_to_truncate_when_safe():
    policy = MaintenancePolicy()
    snap = _snapshot(consecutive_no_progress_passive=3, open_positions=0)
    decision = decide_checkpoint(snap, policy)
    assert decision.should_run
    assert decision.mode == CheckpointMode.TRUNCATE
    assert decision.reason == LIVE_RECLAIM_REASON


def test_escalation_fires_at_the_configured_threshold():
    policy = MaintenancePolicy()
    below = decide_checkpoint(
        _snapshot(consecutive_no_progress_passive=1), policy)
    assert below.reason != LIVE_RECLAIM_REASON
    at = decide_checkpoint(
        _snapshot(consecutive_no_progress_passive=2), policy)
    assert at.reason == LIVE_RECLAIM_REASON
    assert at.mode == CheckpointMode.TRUNCATE


def test_open_positions_do_not_block_live_reclamation():
    """Open positions are a business state, not a database-safety condition.

    Coupling reclamation to zero open positions made it unreachable in any
    live shadow run, which is how the WAL grew unbounded in production.
    """
    policy = MaintenancePolicy()
    snap = _snapshot(consecutive_no_progress_passive=5, open_positions=3)
    decision = decide_checkpoint(snap, policy)
    assert decision.mode == CheckpointMode.TRUNCATE
    assert decision.reason == LIVE_RECLAIM_REASON


def test_escalation_runs_even_when_the_maintenance_gate_is_closed():
    """High commit latency closes the gate; it must not starve reclamation.

    An oversized WAL is itself what drives commit latency up, so gating
    reclamation behind the same symptom is self-defeating.
    """
    policy = MaintenancePolicy()
    snap = _snapshot(
        consecutive_no_progress_passive=4,
        critical_commit_p95_ms=policy.max_commit_p95_ms + 500.0,
    )
    decision = decide_checkpoint(snap, policy)
    assert decision.mode == CheckpointMode.TRUNCATE
    assert decision.reason == LIVE_RECLAIM_REASON


@pytest.mark.parametrize(
    "unsafe",
    [
        {"long_reader_count": 1},
        {"critical_queue_depth": 1},
        {"writer_healthy": False},
        {"integrity_ok": False},
    ],
    ids=[
        "long_reader", "critical_queued", "writer_unhealthy",
        "integrity_not_ok",
    ],
)
def test_live_reclamation_requires_database_level_safety(unsafe):
    policy = MaintenancePolicy()
    snap = _snapshot(consecutive_no_progress_passive=5, **unsafe)
    decision = decide_checkpoint(snap, policy)
    assert decision.reason != LIVE_RECLAIM_REASON


@pytest.mark.parametrize(
    "health",
    ["DEGRADED_PARTIAL_CEX", "DEGRADED_NO_FRESH_CEX",
     "DEGRADED_EVENT_LOOP_LAG", "DEGRADED_PERSISTENCE"],
)
def test_trade_gating_health_does_not_starve_reclamation(health):
    """A live runtime is rarely in a pristine overall health state.

    ``runtime_health`` folds in trade-gating conditions: a partial exchange
    feed, event-loop lag, and DEGRADED_PERSISTENCE (which the engine reports
    for any critical_* execution block, including unreconciled maker evidence
    and exposure limits).  None of them describe the database.  Every attempt
    to gate reclamation on this signal made reclamation unreachable in a live
    run and let the WAL grow without bound; the writer and integrity flags are
    the only health inputs that belong here.
    """
    policy = MaintenancePolicy()
    snap = _snapshot(consecutive_no_progress_passive=3, runtime_health=health)
    decision = decide_checkpoint(snap, policy)
    assert decision.mode == CheckpointMode.TRUNCATE
    assert decision.reason == LIVE_RECLAIM_REASON


def test_live_reclamation_requires_the_restart_wal_threshold():
    policy = MaintenancePolicy()
    snap = _snapshot(
        consecutive_no_progress_passive=5,
        wal_bytes=policy.restart_trigger_bytes - 1,
    )
    decision = decide_checkpoint(snap, policy)
    assert decision.reason != LIVE_RECLAIM_REASON


def test_escalation_respects_long_reader():
    """A long reader holding the snapshot blocks any escalation."""
    policy = MaintenancePolicy()
    snap = _snapshot(
        consecutive_no_progress_passive=5, long_reader_count=1)
    decision = decide_checkpoint(snap, policy)
    assert decision.reason not in (
        LIVE_RECLAIM_REASON, "passive_no_progress_escalation")


def test_zero_progress_does_not_escalate():
    policy = MaintenancePolicy()
    snap = _snapshot(consecutive_no_progress_passive=0)
    decision = decide_checkpoint(snap, policy)
    assert decision.reason not in (
        LIVE_RECLAIM_REASON, "passive_no_progress_escalation")


def test_min_interval_still_rate_limits_escalation():
    """The expensive escalation keeps the full routine interval.

    Inside that interval a *cheap* PASSIVE backfill may still run, because
    pacing it at 60 s made the policy's own 32 MB ``wal_trigger_bytes``
    unreachable: measured under load the WAL grows ~110 MB/min, so one attempt
    per minute always landed on a WAL already hundreds of megabytes past the
    trigger, and the only thing that ever reclaimed bytes was a disk-saturating
    one-shot TRUNCATE.  RESTART and TRUNCATE remain rate limited; only the
    backfill that keeps the file small is allowed to run sooner.
    """
    policy = MaintenancePolicy()
    snap = _snapshot(
        consecutive_no_progress_passive=9,
        now_ms=100_000,
        last_checkpoint_attempt_ts_ms=100_000 - policy.checkpoint_min_interval_ms + 1,
    )
    decision = decide_checkpoint(snap, policy)
    # No escalation inside the routine interval, however armed it is.
    assert decision.mode is not CheckpointMode.TRUNCATE
    assert decision.mode is not CheckpointMode.RESTART
    assert decision.should_run is True
    assert decision.mode is CheckpointMode.PASSIVE
    assert decision.reason == EMERGENCY_WAL_REASON


def test_pressure_floor_still_rate_limits_the_passive_backfill():
    """Below the pressure floor nothing runs at all -- it is a real rate limit."""

    policy = MaintenancePolicy()
    snap = _snapshot(
        consecutive_no_progress_passive=9,
        now_ms=100_000,
        last_checkpoint_attempt_ts_ms=(
            100_000 - policy.pressure_checkpoint_min_interval_ms + 1),
    )
    decision = decide_checkpoint(snap, policy)
    assert decision.should_run is False
    assert decision.reason == "checkpoint_min_interval"


def test_snapshot_includes_no_progress_field():
    snap = _snapshot(consecutive_no_progress_passive=7)
    assert snap.consecutive_no_progress_passive == 7


def test_policy_validates_escalation_knobs():
    with pytest.raises(ValueError):
        MaintenancePolicy(no_progress_escalation_threshold=0)
    with pytest.raises(ValueError):
        MaintenancePolicy(live_reclaim_busy_timeout_ms=0)
    with pytest.raises(ValueError):
        MaintenancePolicy(live_reclaim_gate_wait_ms=-1)


# --------------------------------------------------------------------------
# The decided reclamation must actually execute.
# --------------------------------------------------------------------------

def test_short_active_reader_does_not_block_reclamation():
    """An oversized WAL slows reads, keeping a reader perpetually in flight.

    Blocking on any active reader therefore blocks reclamation forever. Only a
    LONG reader is prohibited; a brief overlap returns BUSY and retries.
    """
    policy = MaintenancePolicy()
    snap = _snapshot(consecutive_no_progress_passive=3, active_readers=1)
    decision = decide_checkpoint(snap, policy)
    assert decision.mode == CheckpointMode.TRUNCATE
    assert decision.reason == LIVE_RECLAIM_REASON


def test_refused_write_gate_is_retried_within_a_bounded_window():
    """A single non-blocking attempt is refused nearly every cycle under load."""
    store = _GatedStore(refusals=5)
    decision = CheckpointDecision(
        True, CheckpointMode.TRUNCATE, LIVE_RECLAIM_REASON,
        _snapshot(consecutive_no_progress_passive=3))
    clock = {"t": 0.0}
    policy = MaintenancePolicy(live_reclaim_gate_wait_ms=1_000)
    response = _invoke_reclaiming_checkpoint(
        store, decision, policy,
        monotonic_clock=lambda: clock["t"],
        sleep=lambda seconds: clock.__setitem__("t", clock["t"] + seconds),
    )
    assert store.attempts == 6
    assert response["after_wal_bytes"] == 0
    # The escalated attempt still carries its bounded lock wait.
    assert store.busy_timeouts == [policy.live_reclaim_busy_timeout_ms]


def test_gate_retry_gives_up_at_the_bounded_deadline():
    store = _GatedStore(refusals=10_000)
    decision = CheckpointDecision(
        True, CheckpointMode.TRUNCATE, LIVE_RECLAIM_REASON,
        _snapshot(consecutive_no_progress_passive=3))
    clock = {"t": 0.0}
    with pytest.raises(Exception) as excinfo:
        _invoke_reclaiming_checkpoint(
            store, decision, MaintenancePolicy(live_reclaim_gate_wait_ms=100),
            monotonic_clock=lambda: clock["t"],
            sleep=lambda seconds: clock.__setitem__("t", clock["t"] + seconds),
        )
    assert type(excinfo.value).__name__ == "V4BackgroundWriteDeferred"
    # Bounded: it never spins indefinitely against a gate that stays held.
    assert store.attempts <= 8


def test_emergency_passive_retries_the_gate_so_no_progress_can_advance():
    """A deferred emergency PASSIVE never runs, so it never proves anything.

    The escalation arms only after PASSIVE demonstrably fails to reclaim
    bytes.  If the gate keeps refusing the emergency pass, the counter stalls
    below its threshold and the reclamation never fires while the WAL grows.
    """
    store = _GatedStore(refusals=4)
    decision = CheckpointDecision(
        True, CheckpointMode.PASSIVE, EMERGENCY_WAL_REASON, _snapshot())
    clock = {"t": 0.0}
    _invoke_reclaiming_checkpoint(
        store, decision, MaintenancePolicy(live_reclaim_gate_wait_ms=1_000),
        monotonic_clock=lambda: clock["t"],
        sleep=lambda seconds: clock.__setitem__("t", clock["t"] + seconds),
    )
    assert store.attempts == 5
    # An emergency PASSIVE keeps the connection's own busy timeout; only the
    # escalated reclamation narrows it.
    assert store.busy_timeouts == [None]


def test_ordinary_passive_pass_never_retries_the_gate():
    """Only the escalation retries; a normal pass must yield immediately."""
    store = _GatedStore(refusals=1)
    decision = CheckpointDecision(
        True, CheckpointMode.PASSIVE, "normal_bounded_checkpoint",
        _snapshot())
    with pytest.raises(Exception):
        _invoke_reclaiming_checkpoint(
            store, decision, MaintenancePolicy(),
            monotonic_clock=lambda: 0.0, sleep=lambda _s: None)
    assert store.attempts == 1


def test_deferred_reclamation_is_reported_as_skipped_not_reclaimed():
    store = _GatedStore(refusals=10_000)
    decision = CheckpointDecision(
        True, CheckpointMode.TRUNCATE, LIVE_RECLAIM_REASON,
        _snapshot(consecutive_no_progress_passive=3))
    result = perform_checkpoint(
        store, decision, MaintenancePolicy(live_reclaim_gate_wait_ms=0),
        wal_size_reader=lambda: store.wal_bytes,
    )
    assert result.status is CheckpointStatus.SKIPPED
    assert result.reason == "critical_write_pending"
    assert result.bytes_reclaimed == 0
    assert result.made_progress is False


def test_reclamation_reports_bytes_returned_to_the_filesystem():
    store = _GatedStore(refusals=0, wal_bytes=800_000_000)
    decision = CheckpointDecision(
        True, CheckpointMode.TRUNCATE, LIVE_RECLAIM_REASON,
        _snapshot(consecutive_no_progress_passive=3))
    result = perform_checkpoint(
        store, decision, MaintenancePolicy(),
        wal_size_reader=lambda: store.wal_bytes,
    )
    assert result.status is CheckpointStatus.SUCCESS
    assert result.before_wal_bytes == 800_000_000
    assert result.after_wal_bytes == 0
    assert result.bytes_reclaimed == 800_000_000
    assert result.made_progress is True
