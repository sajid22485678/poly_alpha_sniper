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
    LIVE_RECLAIM_REASON, CheckpointMode, CheckpointResult, CheckpointStatus,
    MaintenancePolicy, MaintenanceSnapshot, decide_checkpoint,
)


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
        {"active_readers": 1},
        {"critical_queue_depth": 1},
        {"writer_healthy": False},
        {"runtime_health": "DEGRADED"},
    ],
)
def test_live_reclamation_requires_database_level_safety(unsafe):
    policy = MaintenancePolicy()
    snap = _snapshot(consecutive_no_progress_passive=5, **unsafe)
    decision = decide_checkpoint(snap, policy)
    assert decision.reason != LIVE_RECLAIM_REASON


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
    policy = MaintenancePolicy()
    snap = _snapshot(
        consecutive_no_progress_passive=9,
        now_ms=100_000,
        last_checkpoint_attempt_ts_ms=100_000 - policy.checkpoint_min_interval_ms + 1,
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
