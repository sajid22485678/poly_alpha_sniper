"""Tests for the PASSIVE no-progress -> RESTART WAL escalation."""
from __future__ import annotations

import pytest

from lite_frequency_v4.maintenance import (
    CheckpointMode, MaintenancePolicy, MaintenanceSnapshot, decide_checkpoint,
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


def test_repeated_no_progress_escalates_to_restart_when_safe():
    policy = MaintenancePolicy()
    snap = _snapshot(consecutive_no_progress_passive=3, open_positions=0)
    decision = decide_checkpoint(snap, policy)
    assert decision.should_run
    assert decision.mode == CheckpointMode.RESTART
    assert decision.reason == "passive_no_progress_escalation"


def test_escalation_respects_open_positions():
    """RESTART waits for readers and must not run with open positions."""
    policy = MaintenancePolicy()
    snap = _snapshot(consecutive_no_progress_passive=5, open_positions=3)
    decision = decide_checkpoint(snap, policy)
    # Falls through to a safe PASSIVE checkpoint instead.
    assert decision.mode == CheckpointMode.PASSIVE


def test_escalation_respects_long_reader():
    """A long reader holding the snapshot blocks RESTART escalation."""
    policy = MaintenancePolicy()
    snap = _snapshot(
        consecutive_no_progress_passive=5, long_reader_count=1)
    decision = decide_checkpoint(snap, policy)
    assert decision.mode != CheckpointMode.RESTART or decision.reason != (
        "passive_no_progress_escalation")


def test_zero_progress_does_not_escalate():
    policy = MaintenancePolicy()
    snap = _snapshot(consecutive_no_progress_passive=0)
    decision = decide_checkpoint(snap, policy)
    assert decision.reason != "passive_no_progress_escalation"


def test_snapshot_includes_no_progress_field():
    snap = _snapshot(consecutive_no_progress_passive=7)
    assert snap.consecutive_no_progress_passive == 7
