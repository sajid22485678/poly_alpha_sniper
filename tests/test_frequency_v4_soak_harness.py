"""Soak-harness identity and lifecycle contract.

A previous soak produced one ``soak_final.jsonl`` spanning two runtime PIDs and
two commits: an old sampler outlived a runtime restart, and the ``Move-Item``
meant to set its file aside failed silently because the sampler still held the
handle.  Headline metrics were then reported against the wrong commit.

These tests pin the contract that makes that impossible: one output file belongs
to exactly one runtime identity, the sampler stops the moment that identity
changes or ends, and it never appends a frozen post-stop tail.

Every test drives the sampler with injected clock/sleep and on-disk fixtures --
no real runtime, no arbitrary sleeps.
"""
from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from poly_alpha_sniper.tools import v4_soak_sampler as sampler


COMMIT = "746eb4f2aa99e8b088631d67743777c658dbc23b"
NONCE = "5fe7220c780e453eb1bb00e754fce898"
SESSION = "2b19cf80f58d4d0aa1b0f2ec2e0a5f11"


def _write_runtime(runtime_dir: Path, *, pid: int, commit: str = COMMIT,
                   session: str = SESSION, nonce: str = NONCE,
                   state: str = "RUNNING") -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "state.json").write_text(json.dumps({
        "current_commit": commit, "pid": pid, "session_id": session,
        "launch_nonce": nonce, "state": state,
        "process_ownership_valid": True, "orphan_processes": 0,
        "dashboard_export_runs": 5, "dashboard_export_ok": True,
        "integrity_check_runs": 1,
    }), encoding="utf-8")
    (runtime_dir / "heartbeat.json").write_text(json.dumps({
        "ts_ms": 1_785_000_000_000, "pid": pid, "launch_nonce": nonce,
    }), encoding="utf-8")


def _write_export(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "generated_ts_ms": 1_785_000_000_000,
        "dry_run": True, "live_enabled": False, "real_orders_possible": False,
        "live_adapter_present": False, "kill_switch_engaged": True,
        "fixed_shares": 5.0,
        "integrity": {"sqlite_integrity": "ok"},
        "runtime": {"open_runtime_session_count": 1},
        "persistence": {
            "operational_ready": True,
            "telemetry": {"queue_depth": 3, "queue_capacity": 20000,
                          "recovery_blockers": []},
            "telemetry_health_model": {
                "reported_state": "HEALTHY_WITH_POLICY_SAMPLING",
                "data_safety": "HEALTHY",
                "capacity_state": "POLICY_SAMPLING_ACTIVE",
                "accounting_reconciliation": {
                    "submitted": 100, "accounted": 100},
                "accounting_reconciliation_mismatch_rows": 0,
                "window_unexpected_loss_rows": 0,
                "queue_oldest_age_s": 0.2,
            },
            "critical": {"unconfirmed_command_count": 0},
            "latest_checkpoint": {"checkpoint_run_id": 1, "mode": "PASSIVE"},
        },
    }), encoding="utf-8")


@pytest.fixture
def env(tmp_path, monkeypatch):
    runtime_dir = tmp_path / "runtime"
    export_path = tmp_path / "export" / "frequency_v4_dashboard.json"
    out_dir = tmp_path / "evidence"
    _write_runtime(runtime_dir, pid=4242)
    _write_export(export_path)
    monkeypatch.setattr(sampler, "pid_alive", lambda pid: int(pid) == 4242)
    monkeypatch.setattr(sampler, "open_runtime_sessions", lambda _p: 1)
    return {"runtime_dir": runtime_dir, "export_path": export_path,
            "out_dir": out_dir, "db": tmp_path / "db.sqlite"}


def _run(env, *, duration_s=100.0, interval_s=10.0, ticks=None):
    """Drive the sampler on a virtual clock; ``ticks`` advances per sleep."""

    clock = {"t": 0.0}
    step = ticks if ticks is not None else interval_s

    def fake_clock():
        return clock["t"]

    def fake_sleep(seconds):
        clock["t"] += float(step)

    return sampler.run_sampler(
        output_dir=env["out_dir"], duration_s=duration_s,
        interval_s=interval_s, export_path=env["export_path"],
        runtime_dir=env["runtime_dir"], db_path=env["db"],
        clock=fake_clock, sleep=fake_sleep, stream=io.StringIO())


def _records(path: Path) -> list[dict]:
    return [json.loads(line) for line in
            path.read_text(encoding="utf-8").splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Output naming and exclusivity
# ---------------------------------------------------------------------------


def test_output_name_carries_commit_pid_and_session(env):
    path, reason, count = _run(env, duration_s=25.0)
    assert reason == sampler.STOP_DURATION_REACHED
    assert count == 3
    assert path.name.startswith("soak_746eb4f2aa99_4242_")
    assert path.suffix == ".jsonl"


def test_sampler_refuses_an_existing_output_path(env):
    path, _, _ = _run(env, duration_s=15.0)
    assert path.exists()
    # A second run for the same identity must not append into that evidence.
    with pytest.raises(FileExistsError):
        _run(env, duration_s=15.0)


def test_second_sampler_cannot_share_one_identity_file(env):
    """Exclusive create is what stops two live samplers colliding."""

    path, _, _ = _run(env, duration_s=15.0)
    with pytest.raises(FileExistsError):
        sampler.run_sampler(
            output_dir=env["out_dir"], duration_s=10.0, interval_s=5.0,
            export_path=env["export_path"], runtime_dir=env["runtime_dir"],
            db_path=env["db"], clock=lambda: 0.0, sleep=lambda _s: None,
            stream=io.StringIO())
    # The first run's evidence is untouched.
    assert len(_records(path)) >= 2


def test_manifest_is_written_before_any_sample(env):
    path, _, _ = _run(env, duration_s=15.0)
    records = _records(path)
    assert records[0]["record"] == "manifest"
    pinned = records[0]["pinned"]
    assert pinned["commit"] == COMMIT
    assert pinned["pid"] == 4242
    assert pinned["session_id"] == SESSION
    assert pinned["launch_nonce"] == NONCE
    assert records[0]["sampler_pid"] > 0


# ---------------------------------------------------------------------------
# Identity contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("field,new", [
    ("commit", "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
    ("session", "ffffffffffffffffffffffffffffffff"),
    ("nonce", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
])
def test_sampler_stops_when_identity_changes(env, field, new):
    """Commit, session and nonce changes each end the run immediately."""

    calls = {"n": 0}
    original = sampler.read_identity

    def drifting(runtime_dir=env["runtime_dir"]):
        calls["n"] += 1
        if calls["n"] > 2:
            kwargs = {"pid": 4242}
            kwargs[{"commit": "commit", "session": "session",
                    "nonce": "nonce"}[field]] = new
            _write_runtime(env["runtime_dir"], **kwargs)
        return original(runtime_dir)

    import poly_alpha_sniper.tools.v4_soak_sampler as mod
    mod.read_identity = drifting
    try:
        path, reason, count = _run(env, duration_s=200.0)
    finally:
        mod.read_identity = original

    assert reason == sampler.STOP_IDENTITY_MISMATCH
    records = _records(path)
    terminal = records[-1]
    assert terminal["record"] == sampler.STOP_IDENTITY_MISMATCH
    assert terminal["identity_changed_fields"]
    # Exactly one terminal record, and nothing after it.
    assert sum(1 for r in records
               if r.get("record") == sampler.STOP_IDENTITY_MISMATCH) == 1


def test_sampler_stops_when_runtime_pid_changes(env):
    """A restarted runtime is a different run and must not join this file."""

    calls = {"n": 0}
    original = sampler.read_identity

    def restarted(runtime_dir=env["runtime_dir"]):
        calls["n"] += 1
        if calls["n"] > 2:
            _write_runtime(env["runtime_dir"], pid=9999)
        return original(runtime_dir)

    import poly_alpha_sniper.tools.v4_soak_sampler as mod
    mod.read_identity = restarted
    try:
        path, reason, _ = _run(env, duration_s=200.0)
    finally:
        mod.read_identity = original

    assert reason == sampler.STOP_IDENTITY_MISMATCH
    assert "pid" in _records(path)[-1]["identity_changed_fields"]


def test_two_runtime_identities_can_never_enter_one_file(env):
    """The exact defect that corrupted the previous soak evidence."""

    calls = {"n": 0}
    original = sampler.read_identity

    def restarted(runtime_dir=env["runtime_dir"]):
        calls["n"] += 1
        if calls["n"] > 2:
            _write_runtime(env["runtime_dir"], pid=9999,
                           commit="0" * 40, session="1" * 32)
        return original(runtime_dir)

    import poly_alpha_sniper.tools.v4_soak_sampler as mod
    mod.read_identity = restarted
    try:
        path, _, _ = _run(env, duration_s=300.0)
    finally:
        mod.read_identity = original

    samples = [r for r in _records(path) if r.get("record") != "manifest"]
    commits = {r["current_commit"] for r in samples
               if r.get("record") == "sample"}
    pids = {r["current_runtime_pid"] for r in samples
            if r.get("record") == "sample"}
    assert commits == {COMMIT}
    assert pids == {4242}


def test_every_sample_carries_full_identity(env):
    path, _, _ = _run(env, duration_s=25.0)
    for row in _records(path):
        if row.get("record") == "manifest":
            continue
        for field in ("expected_commit", "current_commit",
                      "expected_runtime_pid", "current_runtime_pid",
                      "expected_session_id", "runtime_session_id",
                      "expected_launch_nonce", "launch_nonce",
                      "runtime_alive", "sampled_at_utc", "sampled_at_ms"):
            assert field in row, field


def test_verify_identity_tolerates_an_unreadable_observation():
    """A torn read is not proof of a different runtime; a different value is."""

    pinned = {"commit": COMMIT, "pid": 1, "session_id": SESSION,
              "launch_nonce": NONCE}
    assert sampler.verify_identity(pinned, dict(pinned)) == []
    # Empty/missing observation must not trigger a false mismatch.
    assert sampler.verify_identity(
        pinned, {"commit": "", "pid": 0, "session_id": "",
                 "launch_nonce": ""}) == []
    assert sampler.verify_identity(
        pinned, {**pinned, "commit": "0" * 40}) == ["commit"]


# ---------------------------------------------------------------------------
# Lifecycle contract
# ---------------------------------------------------------------------------


def test_sampler_stops_when_the_runtime_process_dies(env, monkeypatch):
    calls = {"n": 0}

    def dying(pid):
        calls["n"] += 1
        return calls["n"] <= 2

    monkeypatch.setattr(sampler, "pid_alive", dying)
    path, reason, _ = _run(env, duration_s=300.0)
    assert reason == sampler.STOP_RUNTIME_GONE
    records = _records(path)
    assert records[-1]["record"] == sampler.STOP_RUNTIME_GONE


def test_sampler_stops_on_terminal_runtime_state(env):
    calls = {"n": 0}
    original = sampler.read_identity

    def stopping(runtime_dir=env["runtime_dir"]):
        calls["n"] += 1
        if calls["n"] > 2:
            _write_runtime(env["runtime_dir"], pid=4242, state="STOPPED")
        return original(runtime_dir)

    import poly_alpha_sniper.tools.v4_soak_sampler as mod
    mod.read_identity = stopping
    try:
        path, reason, _ = _run(env, duration_s=300.0)
    finally:
        mod.read_identity = original
    assert reason == sampler.STOP_RUNTIME_STOPPED


def test_no_frozen_post_stop_tail_is_written(env, monkeypatch):
    """The old harness appended 96 frozen samples against a dead runtime."""

    calls = {"n": 0}

    def dying(pid):
        calls["n"] += 1
        return calls["n"] <= 1

    monkeypatch.setattr(sampler, "pid_alive", dying)
    path, reason, count = _run(env, duration_s=10_000.0)
    records = [r for r in _records(path) if r.get("record") != "manifest"]
    # Exactly one terminal observation confirming cleanup, and nothing more.
    assert len(records) == 1
    assert records[0]["record"] == sampler.STOP_RUNTIME_GONE
    assert count == 1
    assert reason == sampler.STOP_RUNTIME_GONE


def test_sampler_refuses_to_pin_a_dead_runtime(env, monkeypatch):
    monkeypatch.setattr(sampler, "pid_alive", lambda pid: False)
    with pytest.raises(RuntimeError, match="no live V4 runtime"):
        _run(env)


def test_sampler_refuses_to_pin_a_terminal_runtime(env):
    _write_runtime(env["runtime_dir"], pid=4242, state="STOPPED")
    with pytest.raises(RuntimeError, match="terminal runtime"):
        _run(env)


def test_shutdown_is_bounded_by_the_requested_duration(env):
    _, reason, count = _run(env, duration_s=55.0, interval_s=10.0)
    assert reason == sampler.STOP_DURATION_REACHED
    assert count == 6                       # 0,10,20,30,40,50 then deadline


def test_cli_exit_code_marks_an_incomplete_soak(env, monkeypatch, capsys):
    """A soak cut short by a dead runtime must not look successful."""

    monkeypatch.setattr(sampler, "run_sampler",
                        lambda **kw: (Path("x.jsonl"),
                                      sampler.STOP_RUNTIME_GONE, 3))
    assert sampler.main([str(env["out_dir"]), "10", "1"]) == 1
    monkeypatch.setattr(sampler, "run_sampler",
                        lambda **kw: (Path("x.jsonl"),
                                      sampler.STOP_DURATION_REACHED, 3))
    assert sampler.main([str(env["out_dir"]), "10", "1"]) == 0


def test_sampler_records_the_full_safety_tuple(env):
    path, _, _ = _run(env, duration_s=15.0)
    row = [r for r in _records(path) if r.get("record") == "sample"][0]
    assert row["dry_run"] is True
    assert row["live_enabled"] is False
    assert row["real_orders_possible"] is False
    assert row["live_adapter_present"] is False
    assert row["kill_switch_engaged"] is True
    assert row["fixed_shares"] == 5.0
