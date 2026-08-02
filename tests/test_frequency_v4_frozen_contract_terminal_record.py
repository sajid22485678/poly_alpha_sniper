"""Self-tests for the contract correction at `phase2-frozen-2026-08-02b`.

The superseded contract was internally unsatisfiable. It required the stream to
end on a genuine post-shutdown terminal record, and then required the final ten
entries of that same list -- terminal record included -- to be
``operational_ready=true``. A runtime that has correctly reached STOPPED
publishes no ``persistence.operational_ready``, and the export-age bound forces
that terminal reading to be fresh rather than a stale pre-shutdown one, so no
run could satisfy both rules.

These tests pin the correction and its conditions: the readiness streak is
measured over the operating samples, and the record it excludes is held to a
stricter standard of its own.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "tools"))

import v4_frozen_gate_evaluator as ev  # noqa: E402


BASE_MS = 1_785_000_000_000
INTERVAL_MS = 30_000

#: A terminal record exactly as `tools/v4_soak_sampler.py` writes one for a
#: runtime it observed in a terminal state after a graceful stop.
CLEAN_TERMINAL: dict = {
    "record": "runtime_stopped",
    "phase": "tail",
    "state": "STOPPED",
    "runtime_alive": True,
    "operational_ready": False,
    "open_runtime_sessions_db": 0,
    "open_runtime_sessions_export": 0,
    "orphan_processes": 0,
    "active_job_count": 0,
    "active_reader_count": 0,
    "readers_over_threshold": 0,
    "stray_temp_files": 0,
    "telemetry_data_safety": "HEALTHY",
    "telemetry_data_safety_reasons": [],
    "queue_depth": 0,
    "unexpected_noncritical_loss": 0,
    "critical_evidence_lost": 0,
    "critical_evidence_incomplete": 0,
    "unresolved_critical_commands": 0,
    "reconciliation_mismatch": 0,
    "recon_submitted": 457_020,
    "recon_accounted": 457_020,
    "safety_tuple_sources_agree": True,
    **ev.SAFETY_TUPLE,
}

TERMINAL_CRITERIA = (
    "terminal record: runtime state is STOPPED",
    "terminal record: runtime is no longer running",
    "terminal record: honestly reports not operational_ready",
    "terminal record: graceful shutdown completed",
    "terminal record: zero open runtime sessions",
    "terminal record: zero owned process/task/thread leaks",
    "terminal record: no pending unaccounted telemetry",
    "terminal record: safety tuple intact",
)

READY_STREAK = "final ten consecutive operating samples operational_ready=true"
CLEAN_SHUTDOWN = "run was observed through a clean shutdown"


def _operating(index: int, *, ready: bool = True) -> dict:
    return {
        "record": "sample",
        "phase": "window",
        "sample_index": index,
        "sampled_at_ms": BASE_MS + index * INTERVAL_MS,
        "runtime_alive": True,
        "operational_ready": ready,
        "state": "RUNNING",
    }


def _stream(tmp_path: Path, terminal: dict | None, *,
            operating: int = 20, name: str = "case") -> Path:
    root = tmp_path / name
    sampler = root / "sampler"
    sampler.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = [{"record": "manifest", "pinned": {}}]
    rows.extend(_operating(i) for i in range(1, operating + 1))
    if terminal is not None:
        row = dict(terminal)
        row.setdefault("sample_index", operating + 1)
        row.setdefault("sampled_at_ms", BASE_MS + (operating + 1) * INTERVAL_MS)
        rows.append(row)
    (sampler / "soak_case.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return root


def _verdicts(root: Path) -> dict[str, bool]:
    report = ev.evaluate(root, min_minutes=0.0)
    return {str(c["name"]): str(c["status"]) == ev.PASS
            for c in report.get("criteria", [])}


# --- the defect that was corrected -----------------------------------------

def test_superseded_contract_is_recorded_as_superseded() -> None:
    assert ev.CONTRACT_VERSION == "phase2-frozen-2026-08-02b"
    assert ev.SUPERSEDED_CONTRACT_VERSION == "phase2-frozen-2026-08-02"
    assert len(ev.SUPERSEDED_CONTRACT_SHA256) == 64


def test_a_clean_shutdown_no_longer_breaks_the_readiness_streak(tmp_path) -> None:
    """The exact pair that could not both hold under the superseded contract."""

    verdicts = _verdicts(_stream(tmp_path, CLEAN_TERMINAL, name="clean"))
    assert verdicts[CLEAN_SHUTDOWN] is True
    assert verdicts[READY_STREAK] is True
    for name in TERMINAL_CRITERIA:
        assert verdicts[name] is True, name


def test_the_streak_still_fails_when_an_operating_sample_is_not_ready(
        tmp_path) -> None:
    """The exclusion is scoped to the terminal record, not a blanket waiver."""

    root = tmp_path / "not-ready"
    sampler = root / "sampler"
    sampler.mkdir(parents=True)
    rows: list[dict] = [{"record": "manifest", "pinned": {}}]
    rows.extend(_operating(i) for i in range(1, 20))
    rows.append(_operating(20, ready=False))       # last operating sample
    rows.append({**CLEAN_TERMINAL, "sample_index": 21,
                 "sampled_at_ms": BASE_MS + 21 * INTERVAL_MS})
    (sampler / "soak_case.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")

    verdicts = _verdicts(root)
    assert verdicts[READY_STREAK] is False
    assert verdicts[CLEAN_SHUTDOWN] is True


def test_a_short_run_still_fails_the_streak(tmp_path) -> None:
    verdicts = _verdicts(_stream(tmp_path, CLEAN_TERMINAL, operating=9,
                                 name="short"))
    assert verdicts[READY_STREAK] is False


# --- the stricter standard the excluded record is held to -------------------

def test_a_terminal_record_that_falsely_claims_readiness_is_rejected(
        tmp_path) -> None:
    """The exclusion cannot be exploited by faking readiness at shutdown."""

    liar = {**CLEAN_TERMINAL, "operational_ready": True}
    verdicts = _verdicts(_stream(tmp_path, liar, name="liar"))
    assert verdicts["terminal record: honestly reports not operational_ready"] \
        is False


def test_each_terminal_proof_fails_on_its_own_defect(tmp_path) -> None:
    cases = [
        ("terminal record: runtime state is STOPPED", {"state": "FAILED"}),
        ("terminal record: zero open runtime sessions",
         {"open_runtime_sessions_db": 1}),
        ("terminal record: zero open runtime sessions",
         {"open_runtime_sessions_export": 2}),
        ("terminal record: zero owned process/task/thread leaks",
         {"orphan_processes": 1}),
        ("terminal record: zero owned process/task/thread leaks",
         {"active_job_count": 3}),
        ("terminal record: zero owned process/task/thread leaks",
         {"active_reader_count": 1}),
        ("terminal record: zero owned process/task/thread leaks",
         {"stray_temp_files": 4}),
        ("terminal record: no pending unaccounted telemetry",
         {"telemetry_data_safety": "DEGRADED"}),
        ("terminal record: no pending unaccounted telemetry",
         {"telemetry_data_safety_reasons": ["pending_flush"]}),
        ("terminal record: no pending unaccounted telemetry",
         {"queue_depth": 7}),
        ("terminal record: no pending unaccounted telemetry",
         {"unexpected_noncritical_loss": 1}),
        ("terminal record: no pending unaccounted telemetry",
         {"critical_evidence_lost": 1}),
        ("terminal record: no pending unaccounted telemetry",
         {"unresolved_critical_commands": 1}),
        ("terminal record: no pending unaccounted telemetry",
         {"recon_accounted": 457_019}),
        ("terminal record: safety tuple intact", {"dry_run": False}),
        ("terminal record: safety tuple intact", {"live_enabled": True}),
        ("terminal record: safety tuple intact", {"real_orders_possible": True}),
        ("terminal record: safety tuple intact",
         {"authenticated_trading_client": True}),
        ("terminal record: safety tuple intact", {"real_order_placement": True}),
        ("terminal record: safety tuple intact", {"real_wallet_signing": True}),
        ("terminal record: safety tuple intact", {"kill_switch_engaged": False}),
        ("terminal record: safety tuple intact", {"fixed_shares": 10.0}),
        ("terminal record: safety tuple intact",
         {"safety_tuple_sources_agree": False}),
    ]
    for index, (criterion, mutation) in enumerate(cases):
        terminal = {**CLEAN_TERMINAL, **mutation}
        verdicts = _verdicts(_stream(tmp_path, terminal, name=f"mut{index}"))
        assert verdicts[criterion] is False, f"{criterion} survived {mutation}"


def test_a_missing_safety_field_in_the_terminal_record_is_rejected(
        tmp_path) -> None:
    for field in ev.SAFETY_TUPLE:
        terminal = {k: v for k, v in CLEAN_TERMINAL.items() if k != field}
        verdicts = _verdicts(_stream(tmp_path, terminal, name=f"absent-{field}"))
        assert verdicts["terminal record: safety tuple intact"] is False, field


def test_a_stream_that_never_stopped_still_fails_the_clean_shutdown_rule(
        tmp_path) -> None:
    timeout = {"record": "tail_timeout", "phase": "tail", "state": "RUNNING",
               "runtime_alive": True, "operational_ready": True}
    verdicts = _verdicts(_stream(tmp_path, timeout, name="timeout"))
    assert verdicts[CLEAN_SHUTDOWN] is False
    assert verdicts["terminal record: runtime state is STOPPED"] is False
    # ...and its readiness is still excluded from the streak, so the streak
    # cannot be passed by a run that was never seen to stop.
    assert verdicts["stream ends on a terminal record"] is True


def test_terminal_exclusion_does_not_touch_the_duty_cycle_or_first_ready(
        tmp_path) -> None:
    """Only the streak changed; the neighbouring readiness rules are intact."""

    verdicts = _verdicts(_stream(tmp_path, CLEAN_TERMINAL, name="neighbours"))
    assert verdicts["lane reaches operational_ready within the first 3 samples"] \
        is True
    # 20 ready operating samples + 1 not-ready terminal = 95.2% >= 95%.
    assert verdicts["operational_ready duty cycle >= 95%"] is True


def test_unchanged_bounds_are_still_the_ratified_values() -> None:
    assert ev.MAX_HYDRATING_SAMPLES == 5
    assert ev.MAX_SAMPLES_TO_FIRST_READY == 3
    assert ev.ARTIFACT_SETTLE_QUIET_S == 20.0
    assert ev.CLEAN_TERMINAL_RECORDS == frozenset(
        {"runtime_stopped", "runtime_process_gone"})
    assert ev.TERMINAL_RECORDS == frozenset({
        "runtime_stopped", "runtime_process_gone", "identity_mismatch",
        "tail_timeout"})
    assert ev.SAFETY_TUPLE == {
        "dry_run": True,
        "live_enabled": False,
        "real_orders_possible": False,
        "live_adapter_present": False,
        "authenticated_trading_client": False,
        "real_order_placement": False,
        "real_order_cancellation": False,
        "real_wallet_signing": False,
        "kill_switch_engaged": True,
        "fixed_shares": 5.0,
    }
