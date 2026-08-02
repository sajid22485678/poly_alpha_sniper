"""The frozen evaluator must reject bad evidence, not read around it.

The evaluator this replaced skipped unparseable lines, never compared the
identity fields it read to the manifest, filtered the dense trace to the sampled
window, treated a missing conservation term as zero, and checked six of the ten
safety-tuple fields.  Each of those is a way for evidence to look clean because
nothing looked at it.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from poly_alpha_sniper.tools import v4_frozen_gate_evaluator as ev


COMMIT = "4d8655ccef72233200409848574366d96a718d20"
PID = 18292
SESSION = "53dd4921a0034e4f89e95d3c3a6cb9a0"
NONCE = "82e99396702a4ee9a14251c37edd90da"
T0 = 1_785_000_000_000


def _sample(index: int, **overrides) -> dict:
    row = {
        "record": "sample", "phase": "window",
        "sample_index": index,
        "sampled_at_ms": T0 + index * 30_000,
        "expected_commit": COMMIT, "current_commit": COMMIT,
        "expected_runtime_pid": PID, "current_runtime_pid": PID,
        "expected_session_id": SESSION, "runtime_session_id": SESSION,
        "expected_launch_nonce": NONCE, "launch_nonce": NONCE,
        "runtime_alive": True, "ownership_valid": True, "orphan_processes": 0,
        "dashboard_listening": True, "dashboard_http_status": 200,
        "dashboard_probe_ms": 4.0, "dashboard_probe_error": None,
        "runtime_rss_bytes": 500 * 2**20, "runtime_vms_bytes": 700 * 2**20,
        "runtime_threads": 30, "runtime_handles": 500,
        "runtime_memory_error": None,
        "state": "RUNNING", "heartbeat_age_s": 2.0, "export_age_s": 5.0,
        "loop_lag_ms": 90.0, "operational_ready": True,
        "operational_degraded_reasons": [],
        "safety_tuple_sources_agree": True,
        "queue_bounded": True, "queue_depth": 10, "queue_capacity": 20000,
        "queue_oldest_age_s": 1.0, "sampling_keep_ratio": 1.0,
        "live_integrity_max_chunk_ms": 300.0, "live_integrity_health": "OK",
        "live_integrity_chunks_over_bound": 0, "sqlite_integrity": "ok",
        "wal_bytes": 10 * 2**20, "checkpoint_run_id": index,
        "open_runtime_sessions_db": 1, "stray_temp_files": 0,
        "critical_evidence_lost": 0, "critical_evidence_incomplete": 0,
        "unexpected_noncritical_loss": 0, "reconciliation_mismatch": 0,
        "recon_unexpected_loss": 0,
        "recon_submitted": 1000 * index, "recon_accounted": 1000 * index,
    }
    row.update(ev.SAFETY_TUPLE)
    row.update(overrides)
    return row


def _build(tmp_path: Path, *, samples=None, terminal=True) -> Path:
    root = tmp_path / "evidence"
    (root / "sampler").mkdir(parents=True)
    # Long enough that the one not-ready terminal record -- which a graceful
    # stop always produces -- stays inside the unchanged 95% duty-cycle bound,
    # as it does in a real run of a hundred-plus samples.
    rows = samples if samples is not None else [
        _sample(i) for i in range(1, 25)]
    if terminal and rows:
        # A terminal record as the sampler actually writes one for a graceful
        # stop, and as the contract now requires it to read: the runtime has
        # reached STOPPED, is honestly no longer operational_ready, and left no
        # open session, no owned worker, and no unaccounted telemetry behind.
        rows[-1] = dict(rows[-1], record="runtime_stopped", state="STOPPED",
                        runtime_alive=False, operational_ready=False,
                        open_runtime_sessions_db=0,
                        open_runtime_sessions_export=0,
                        active_job_count=0, active_reader_count=0,
                        readers_over_threshold=0, stray_temp_files=0,
                        recovery_blockers=[], queue_depth=0,
                        unresolved_critical_commands=0,
                        telemetry_data_safety="HEALTHY",
                        telemetry_data_safety_reasons=[])
    manifest = {
        "record": "manifest", "interval_s": 30.0,
        "pinned": {"commit": COMMIT, "pid": PID, "session_id": SESSION,
                   "launch_nonce": NONCE},
    }
    (root / "sampler" / f"soak_{COMMIT[:12]}_{PID}_{SESSION[:12]}.jsonl").write_text(
        "\n".join(json.dumps(r) for r in [manifest] + rows) + "\n",
        encoding="utf-8")
    (root / "readiness_trace.jsonl").write_text("\n".join(
        json.dumps({
            "kind": "tick", "seq": i, "mono": float(i),
            "wall_ms": T0 + i * 1000, "blockers": [], "streak_reset": False,
            "sampling_keep_ratio": 1.0, "queue_depth": 5,
            "conservation": {"residual": 0, "absolute_residual": 0},
            "writer": {
                "evidence_conflicts": 0, "batches": i,
                "gate": {"max_consecutive_telemetry_skips": 2,
                         "seconds_since_telemetry_admit": 1.0,
                         "critical_pending": 1, "critical_inflight": 0},
                "sink_cost": {"row_cost_by_method": {
                    "record_cex_observation": {"calls": 40 * i,
                                               "total_ms": 40.0 * i},
                    "record_book_snapshot": {"calls": 20 * i,
                                             "total_ms": 20.0 * i},
                }},
            },
        }) for i in range(1, 40)) + "\n", encoding="utf-8")
    (root / "providers.jsonl").write_text("\n".join(
        json.dumps({"sources": [
            {"source": "okx", "status": "READY", "connected": True,
             "hydrated": True, "future_count": 0},
            {"source": "polymarket", "status": "READY", "connected": True,
             "hydrated": True, "future_count": 0},
        ]}) for _ in range(24)) + "\n", encoding="utf-8")
    (root / "clock.jsonl").write_text("\n".join(
        json.dumps({"offset_median_ms": 3.0, "free_gb_C": 40.0})
        for _ in range(24)) + "\n", encoding="utf-8")
    return root


def _failures(root: Path, *, min_minutes: float = 5.0) -> set[str]:
    return set(ev.evaluate(root, min_minutes=min_minutes)["failures"])


def test_a_clean_run_passes(tmp_path):
    report = ev.evaluate(_build(tmp_path), min_minutes=5.0)
    assert report["verdict"] == "PASS", report["failures"]
    assert report["total"] > 60
    assert report["contract_version"] == ev.CONTRACT_VERSION


def test_a_malformed_line_is_reported_not_skipped(tmp_path):
    root = _build(tmp_path)
    path = next((root / "sampler").glob("*.jsonl"))
    path.write_text(path.read_text(encoding="utf-8") + "{not json\n",
                    encoding="utf-8")
    assert "no malformed or unparseable evidence lines" in _failures(root)


def test_a_duplicate_sample_index_fails(tmp_path):
    rows = [_sample(i) for i in range(1, 13)]
    rows[5] = _sample(5)
    root = _build(tmp_path, samples=rows)
    assert ("sample indices are contiguous from 1, in order, with no duplicates"
            in _failures(root))


def test_reordered_samples_fail(tmp_path):
    rows = [_sample(i) for i in range(1, 13)]
    rows[3], rows[4] = rows[4], rows[3]
    root = _build(tmp_path, samples=rows)
    failures = _failures(root)
    assert ("sample indices are contiguous from 1, in order, with no duplicates"
            in failures)
    assert "sample timestamps are strictly increasing" in failures


def test_a_missing_sample_fails(tmp_path):
    rows = [_sample(i) for i in range(1, 13) if i != 7]
    root = _build(tmp_path, samples=rows)
    failures = _failures(root)
    assert ("sample indices are contiguous from 1, in order, with no duplicates"
            in failures)
    assert "no sampling gap beyond half an interval of slack" in failures


def test_stitched_evidence_fails(tmp_path):
    root = _build(tmp_path)
    (root / "sampler" / "soak_other_1_x.jsonl").write_text("{}\n", encoding="utf-8")
    report = ev.evaluate(root, min_minutes=5.0)
    assert report["verdict"] == "FAIL"
    assert "exactly one sampler stream (no stitched evidence)" in report["failures"]


@pytest.mark.parametrize("field", [
    "current_commit", "current_runtime_pid", "runtime_session_id",
    "launch_nonce",
])
def test_identity_is_compared_to_the_manifest_not_merely_consistent(
        tmp_path, field):
    """Consistently wrong used to pass: expected_key was read and never used."""

    wrong = {"current_commit": "b" * 40, "current_runtime_pid": 999,
             "runtime_session_id": "f" * 32, "launch_nonce": "a" * 32}[field]
    rows = [_sample(i, **{field: wrong}) for i in range(1, 13)]
    root = _build(tmp_path, samples=rows)
    assert f"one {field}, equal to the manifest" in _failures(root)


@pytest.mark.parametrize("field", sorted(ev.SAFETY_TUPLE))
def test_every_safety_field_is_validated(tmp_path, field):
    want = ev.SAFETY_TUPLE[field]
    wrong = 4.0 if isinstance(want, float) else (not want)
    rows = [_sample(i, **{field: wrong}) for i in range(1, 13)]
    root = _build(tmp_path, samples=rows)
    assert ("full safety tuple unchanged throughout (all ten fields)"
            in _failures(root))


@pytest.mark.parametrize("field", sorted(ev.SAFETY_TUPLE))
def test_an_absent_safety_field_fails(tmp_path, field):
    """Four of these were absent from the export and simply never checked."""

    rows = []
    for i in range(1, 13):
        row = _sample(i)
        row.pop(field)
        rows.append(row)
    assert ("full safety tuple unchanged throughout (all ten fields)"
            in _failures(_build(tmp_path, samples=rows)))


def test_a_run_without_a_terminal_record_fails(tmp_path):
    root = _build(tmp_path, terminal=False)
    failures = _failures(root)
    assert "stream ends on a terminal record" in failures
    assert "run was observed through a clean shutdown" in failures


def test_a_tail_timeout_is_not_a_clean_shutdown(tmp_path):
    rows = [_sample(i) for i in range(1, 13)]
    rows[-1] = dict(rows[-1], record="tail_timeout")
    root = _build(tmp_path, samples=rows, terminal=False)
    failures = _failures(root)
    assert "stream ends on a terminal record" not in failures
    assert "run was observed through a clean shutdown" in failures


@pytest.mark.parametrize("probe, failing", [
    ({"dashboard_listening": False}, "dashboard listening in every sample"),
    ({"dashboard_http_status": 500},
     "dashboard answered 2xx/3xx in every sample"),
    ({"dashboard_http_status": None},
     "dashboard answered 2xx/3xx in every sample"),
])
def test_dashboard_evidence_must_be_real(tmp_path, probe, failing):
    rows = [_sample(i, **probe) for i in range(1, 13)]
    assert failing in _failures(_build(tmp_path, samples=rows))


def test_missing_memory_evidence_fails(tmp_path):
    rows = []
    for i in range(1, 13):
        row = _sample(i)
        row.pop("runtime_rss_bytes")
        rows.append(row)
    assert "runtime memory sampled in every sample" in _failures(
        _build(tmp_path, samples=rows))


def test_material_memory_growth_fails(tmp_path):
    rows = [_sample(i, runtime_rss_bytes=(400 + 40 * i) * 2**20)
            for i in range(1, 13)]
    assert ("no material late-run memory growth (< 25% median RSS)"
            in _failures(_build(tmp_path, samples=rows)))


@pytest.mark.parametrize("residual", [1, -1, 7, -90])
def test_a_conservation_residual_of_either_sign_fails(tmp_path, residual):
    """-90 is the exact surplus the superseded predicate reported as zero."""

    root = _build(tmp_path)
    trace = root / "readiness_trace.jsonl"
    rows = [json.loads(l) for l in trace.read_text(encoding="utf-8").splitlines()]
    rows[10]["conservation"]["residual"] = residual
    trace.write_text("\n".join(json.dumps(r) for r in rows) + "\n",
                     encoding="utf-8")
    assert ("window conservation residual is exactly zero on every tick"
            in _failures(root))


def test_a_missing_conservation_term_is_not_read_as_zero(tmp_path):
    root = _build(tmp_path)
    trace = root / "readiness_trace.jsonl"
    rows = [json.loads(l) for l in trace.read_text(encoding="utf-8").splitlines()]
    for row in rows:
        row["conservation"] = {}
    trace.write_text("\n".join(json.dumps(r) for r in rows) + "\n",
                     encoding="utf-8")
    failures = _failures(root)
    assert "every tick carries the conservation identity terms" in failures
    assert "window conservation residual is exactly zero on every tick" in failures
    assert "absolute conservation residual is exactly zero on every tick" in failures


def test_a_single_zero_keep_ratio_tick_fails(tmp_path):
    """Restored original strict rule: never zero, no episode allowance."""

    root = _build(tmp_path)
    trace = root / "readiness_trace.jsonl"
    rows = [json.loads(l) for l in trace.read_text(encoding="utf-8").splitlines()]
    rows[12]["sampling_keep_ratio"] = 0.0
    trace.write_text("\n".join(json.dumps(r) for r in rows) + "\n",
                     encoding="utf-8")
    assert "keep_ratio never reaches zero on any control tick" in _failures(root)


def test_post_window_blockers_are_not_filtered_out(tmp_path):
    """The gate's four post-window overload ticks were censored by design."""

    root = _build(tmp_path)
    trace = root / "readiness_trace.jsonl"
    rows = [json.loads(l) for l in trace.read_text(encoding="utf-8").splitlines()]
    for row in rows[-4:]:
        row["wall_ms"] = T0 + 999_999_999      # far past the sampled window
        row["blockers"] = ["queue_accumulating", "uncontrolled_overload"]
        row["streak_reset"] = True
    trace.write_text("\n".join(json.dumps(r) for r in rows) + "\n",
                     encoding="utf-8")
    failures = _failures(root)
    assert "no blocker or reset after the sampled window" in failures
    assert "queue_accumulating never occurs in the whole session" in failures
    assert "no readiness streak resets in the whole session" in failures


def test_a_non_contiguous_dense_trace_fails(tmp_path):
    root = _build(tmp_path)
    trace = root / "readiness_trace.jsonl"
    rows = [json.loads(l) for l in trace.read_text(encoding="utf-8").splitlines()]
    del rows[15]
    trace.write_text("\n".join(json.dumps(r) for r in rows) + "\n",
                     encoding="utf-8")
    assert "dense trace sequence is contiguous and in order" in _failures(root)


def test_hydration_bound_and_terminal_ready_are_enforced(tmp_path):
    root = _build(tmp_path)
    statuses = (["READY"] + ["HYDRATING"] * 6 + ["READY"] * 4 + ["HYDRATING"])
    (root / "providers.jsonl").write_text("\n".join(
        json.dumps({"sources": [
            {"source": "okx", "status": "READY", "connected": True,
             "hydrated": True, "future_count": 0},
            {"source": "polymarket", "status": status, "connected": True,
             "hydrated": status == "READY", "future_count": 0},
        ]}) for status in statuses) + "\n", encoding="utf-8")
    failures = _failures(root)
    assert (f"polymarket: no HYDRATING episode longer than "
            f"{ev.MAX_HYDRATING_SAMPLES} samples") in failures
    assert "polymarket: terminal sample is READY" in failures
    assert "okx connected in every sample" not in failures


def test_late_first_readiness_fails(tmp_path):
    rows = [_sample(i, operational_ready=(i >= 5),
                    operational_degraded_reasons=(
                        [] if i >= 5 else ["telemetry_recovery_window"]))
            for i in range(1, 20)]
    assert ("lane reaches operational_ready within the first 3 samples"
            in _failures(_build(tmp_path, samples=rows)))


def test_recovery_window_after_certification_fails(tmp_path):
    rows = [_sample(i, operational_degraded_reasons=(
        ["telemetry_recovery_window"] if i == 9 else []))
        for i in range(1, 20)]
    failures = _failures(_build(tmp_path, samples=rows))
    assert "no telemetry_recovery_window after the lane first certifies" in failures


def test_an_evidence_integrity_conflict_fails(tmp_path):
    root = _build(tmp_path)
    trace = root / "readiness_trace.jsonl"
    rows = [json.loads(l) for l in trace.read_text(encoding="utf-8").splitlines()]
    rows[7]["writer"]["evidence_conflicts"] = 1
    trace.write_text("\n".join(json.dumps(r) for r in rows) + "\n",
                     encoding="utf-8")
    assert "no unresolved evidence integrity conflict" in _failures(root)


def test_a_stray_temp_file_fails(tmp_path):
    """Original strict rule restored: not "only if seen consecutively"."""

    rows = [_sample(i, stray_temp_files=(1 if i == 4 else 0))
            for i in range(1, 13)]
    assert "no stray temp files" in _failures(_build(tmp_path, samples=rows))


def test_the_evaluator_hashes_itself(tmp_path):
    digest = ev.sha256_file(Path(ev.__file__))
    assert len(digest) == 64
    assert digest == ev.sha256_file(Path(ev.__file__))
