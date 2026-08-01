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


def _run(env, *, duration_s=100.0, interval_s=10.0, ticks=None,
         tail_timeout_s=0.0, on_sleep=None):
    """Drive the sampler on a virtual clock; ``ticks`` advances per sleep."""

    clock = {"t": 0.0}
    step = ticks if ticks is not None else interval_s

    def fake_clock():
        return clock["t"]

    def fake_sleep(seconds):
        clock["t"] += float(step)
        if on_sleep is not None:
            on_sleep(clock["t"])

    return sampler.run_sampler(
        output_dir=env["out_dir"], duration_s=duration_s,
        interval_s=interval_s, export_path=env["export_path"],
        runtime_dir=env["runtime_dir"], db_path=env["db"],
        clock=fake_clock, sleep=fake_sleep, stream=io.StringIO(),
        tail_timeout_s=tail_timeout_s)


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


# ---------------------------------------------------------------------------
# Measured dashboard liveness, memory, and a run that ends where it really ends
# ---------------------------------------------------------------------------


def test_dashboard_liveness_is_probed_not_asserted(env, monkeypatch):
    """``dashboard_alive`` used to be a parameter defaulting to True.

    No caller ever supplied it, so every sample asserted the dashboard was
    serving without anyone looking, and the evaluator then checked that
    constant.  A fresh export proves the exporter ran inside the bot; it says
    nothing about the separate process that serves the page.
    """

    probes = []

    def fake_probe(*args, **kwargs):
        probes.append(1)
        return {"url": "http://127.0.0.1:8504/", "listening": True,
                "http_status": 200, "latency_ms": 3.5, "error": None}

    monkeypatch.setattr(sampler, "probe_dashboard", fake_probe)
    path, _, count = _run(env, duration_s=25.0)
    rows = [r for r in _records(path) if r.get("record") != "manifest"]

    assert len(probes) == count, "every sample must probe, not assume"
    for row in rows:
        assert row["dashboard_alive"] is True
        assert row["dashboard_listening"] is True
        assert row["dashboard_http_status"] == 200
        assert row["dashboard_probe_ms"] == 3.5
        assert row["dashboard_probe_error"] is None


@pytest.mark.parametrize(
    "probe, alive",
    [
        ({"listening": True, "http_status": 200}, True),
        ({"listening": True, "http_status": 302}, True),
        # A wedged server still holds the port; a listener check alone would
        # call this healthy, which is why both are recorded.
        ({"listening": True, "http_status": 500}, False),
        ({"listening": True, "http_status": None}, False),
        ({"listening": False, "http_status": None}, False),
    ],
)
def test_dashboard_alive_requires_a_listener_and_a_good_status(
        env, monkeypatch, probe, alive):
    monkeypatch.setattr(
        sampler, "probe_dashboard",
        lambda *a, **k: {"url": "u", "latency_ms": 1.0, "error": None, **probe})
    path, _, _ = _run(env, duration_s=15.0)
    rows = [r for r in _records(path) if r.get("record") != "manifest"]
    assert rows and all(row["dashboard_alive"] is alive for row in rows)


def test_a_probe_failure_never_stops_the_sample(env, monkeypatch):
    monkeypatch.setattr(sampler, "probe_dashboard", lambda *a, **k: {
        "url": "u", "listening": False, "http_status": None,
        "latency_ms": None, "error": "ConnectionRefusedError:refused"})
    path, reason, count = _run(env, duration_s=25.0)
    assert reason == sampler.STOP_DURATION_REACHED and count == 3
    rows = [r for r in _records(path) if r.get("record") != "manifest"]
    assert all(row["dashboard_alive"] is False for row in rows)
    assert all("refused" in row["dashboard_probe_error"] for row in rows)


def test_every_sample_carries_runtime_memory(env, monkeypatch):
    """Late-run memory growth is a required criterion; it was never sampled."""

    monkeypatch.setattr(sampler, "process_memory", lambda pid: {
        "rss_bytes": 512 * 2**20 + int(pid), "vms_bytes": 900 * 2**20,
        "num_threads": 31, "num_handles": 640, "error": None})
    path, _, _ = _run(env, duration_s=25.0)
    rows = [r for r in _records(path) if r.get("record") != "manifest"]
    assert rows
    for row in rows:
        assert row["runtime_rss_bytes"] == 512 * 2**20 + 4242
        assert row["runtime_vms_bytes"] == 900 * 2**20
        assert row["runtime_threads"] == 31
        assert row["runtime_handles"] == 640
        assert row["runtime_memory_error"] is None


def test_real_memory_probe_reads_this_process():
    import os

    reading = sampler.process_memory(os.getpid())
    assert reading["error"] is None
    assert isinstance(reading["rss_bytes"], int) and reading["rss_bytes"] > 0
    assert reading["num_threads"] >= 1


def test_missing_process_memory_is_reported_not_invented():
    reading = sampler.process_memory(0)
    assert reading["rss_bytes"] is None
    assert reading["error"] == "no_pid"


def test_a_tail_follows_the_run_through_its_shutdown(env):
    """The gate at 4d8655c sampled 57.1 clean minutes and then ran 6m40s more.

    Four ticks in that unsampled remainder raised ``queue_accumulating`` and
    ``uncontrolled_overload`` with a readiness reset.  A window is not a run:
    with a tail, sampling continues until the runtime actually stops, so the
    stream ends on a real terminal observation.
    """

    def stop_after(now):
        if now >= 40.0:
            _write_runtime(env["runtime_dir"], pid=4242, state="STOPPED")

    path, reason, count = _run(
        env, duration_s=25.0, interval_s=10.0, tail_timeout_s=120.0,
        on_sleep=stop_after)

    assert reason == sampler.STOP_RUNTIME_STOPPED
    rows = [r for r in _records(path) if r.get("record") != "manifest"]
    assert len(rows) == count
    assert rows[-1]["record"] == sampler.STOP_RUNTIME_STOPPED
    assert rows[-1]["state"] == "STOPPED"
    # The tail is labelled, so an evaluator can tell the requested window from
    # the shutdown observation without inferring it from timestamps.
    assert [r["phase"] for r in rows] == (
        ["window"] * 3 + ["tail"] * (len(rows) - 3))
    assert all(r["elapsed_s"] >= 0.0 for r in rows)


def test_a_tail_that_never_sees_a_stop_says_so(env):
    """Ending on an ordinary sample would read as a clean finish; it is not."""

    path, reason, count = _run(
        env, duration_s=25.0, interval_s=10.0, tail_timeout_s=20.0)
    assert reason == sampler.STOP_TAIL_TIMEOUT
    records = _records(path)
    assert records[-1]["record"] == sampler.STOP_TAIL_TIMEOUT
    assert records[-1]["observed_state"] == "RUNNING"
    assert records[-1]["elapsed_s"] >= 25.0


def test_no_tail_keeps_the_previous_duration_bounded_behaviour(env):
    path, reason, count = _run(env, duration_s=25.0, interval_s=10.0)
    assert reason == sampler.STOP_DURATION_REACHED
    assert count == 3
    rows = [r for r in _records(path) if r.get("record") != "manifest"]
    assert all(r["phase"] == "window" for r in rows)
    assert all(r["record"] == "sample" for r in rows)


def test_manifest_records_the_tail_budget(env):
    path, _, _ = _run(env, duration_s=25.0, tail_timeout_s=300.0)
    manifest = _records(path)[0]
    assert manifest["record"] == "manifest"
    assert manifest["tail_timeout_s"] == 300.0
