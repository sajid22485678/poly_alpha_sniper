"""The frozen Phase 2 acceptance contract. One evaluator, committed before use.

WHY THIS EXISTS
===============

The previous evaluator was an uncommitted script in an evidence directory.  It
was edited at 2026-08-01T13:43:36Z -- after Gate 1 failed ten criteria, after
Gate 2's sampling had ended, and after the soak's sampling had ended -- and the
Gate 2 and soak verdicts were then regenerated from the edited rules seconds
later.  It carried no version, no pre-run hash, and no record of what it had
been.  Five criteria were relaxed in that edit:

  * ``keep_ratio does not collapse to unhealthy zero``   -> deleted outright
  * ``keep_ratio never zero on any control tick``        -> episodes up to 5 s
  * ``no stray temp files``                              -> only if consecutive
  * ``subscriptions hydrated once past startup``         -> READY at least once
  * ``no telemetry_recovery_window degradation``         -> after certification

and the conservation check only ever tested ``max(deficit) == 0``, so a window
full of negative surplus reported "deficit = 0".

Acceptance criteria that move after a failure are not acceptance criteria.  This
file is committed before any runtime validation begins, its commit, tree and
SHA-256 are recorded in the evidence, and it is not edited afterwards.  If a run
fails, the run is remediated -- not this file.

WHAT IS FROZEN
==============

The original strict criteria are restored except where a separately recorded
human authority ratified a replacement.  Three were ratified on 2026-08-02, each
with its own recorded reasoning; every one of them is *tighter* than what the
unfrozen evaluator had adopted:

  ``keep_ratio``     RESTORED STRICT.  Must never equal zero on any control
                     tick, anywhere in the session.  No bounded exception, no
                     episode allowance.  The measured cause of the previous zero
                     ticks -- a clamp keyed to the pressure watermark rather
                     than the hard queue bound -- was remediated in the runtime
                     instead of being written around here.

  ``hydration``      RATIFIED REPLACEMENT.  The original "hydrated in every
                     sample past startup" conflicts with designed five-minute
                     market rotation, under which Polymarket's flag cycles.  The
                     frozen rule: connected in every sample, no HYDRATING
                     episode longer than 5 consecutive samples, every episode
                     returns to READY, and the terminal sample is READY.

  ``startup settle`` RATIFIED REPLACEMENT.  The original "no
                     telemetry_recovery_window anywhere" is unsatisfiable: the
                     fail-closed settle window is mandatory before first
                     certification.  The frozen rule: it may appear only before
                     the first ``operational_ready`` sample, that must arrive
                     within the first 3 samples, and it must never appear again.

Everything else is the original Gate 1 contract, plus the criteria the audit
found missing entirely: real dashboard evidence, memory growth, a terminal
record, evaluation through shutdown, strict manifest identity, malformed and
out-of-order sample rejection, and exact conservation.

Usage:
    v4_frozen_gate_evaluator.py <evidence_dir> --min-minutes N [--json OUT]
                               [--seal] [--settle-timeout-s N]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any, Optional

FAIL = "FAIL"
PASS = "PASS"

#: Contract version.  Bound into the recorded hash; a change to any rule below
#: is a change to this file, which is why the file is committed and hashed.
#:
#: ``phase2-frozen-2026-08-02`` was internally unsatisfiable and is superseded.
#: It required the stream to end on a genuine post-shutdown terminal record
#: (criterion "run was observed through a clean shutdown", which reads
#: ``samples[-1]``) *and* required the final ten entries of that same list --
#: terminal record included -- to be ``operational_ready=true``.  A runtime that
#: has correctly reached STOPPED publishes no ``persistence.operational_ready``,
#: and the export-age bound forces that terminal reading to be fresh rather than
#: a stale pre-shutdown one, so the two rules could never both hold.  Proven
#: against the unmodified evaluator with an otherwise-perfect synthetic stream.
#:
#: The correction is scoped exactly: the readiness streak is measured over the
#: operating samples, and the terminal record it excludes is instead held to a
#: stricter, explicit standard of its own (see ``_check_terminal_record``).  No
#: threshold, duration, identity rule, hydration bound, conservation rule,
#: keep_ratio rule or safety requirement changes.
CONTRACT_VERSION = "phase2-frozen-2026-08-02b"
#: The contract this one supersedes, and the SHA-256 of the file that carried it.
SUPERSEDED_CONTRACT_VERSION = "phase2-frozen-2026-08-02"
SUPERSEDED_CONTRACT_SHA256 = (
    "b807580d8c19e78d181cd3bee95ef849025fa14d64f524673d31bb9271b65674")

#: Every field of the Phase 2 safety tuple and its only permitted value.  The
#: previous evaluator checked six of these ten; the four it omitted are exactly
#: the ones asserting no live order path exists.
SAFETY_TUPLE: dict[str, Any] = {
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

#: Records that legitimately terminate a sampler stream.
TERMINAL_RECORDS = frozenset({
    "runtime_stopped", "runtime_process_gone", "identity_mismatch",
    "tail_timeout",
})
#: ...and the subset that represents a run observed through a clean shutdown.
CLEAN_TERMINAL_RECORDS = frozenset({"runtime_stopped", "runtime_process_gone"})

#: Ratified hydration bound: consecutive samples one source may spend HYDRATING.
MAX_HYDRATING_SAMPLES = 5
#: Ratified settle bound: samples allowed before first operational_ready.
MAX_SAMPLES_TO_FIRST_READY = 3
#: Artifacts must be this quiet before they may be sealed.
ARTIFACT_SETTLE_QUIET_S = 20.0


# ---------------------------------------------------------------------------
# Evidence loading: strict, because "skip what does not parse" hides loss
# ---------------------------------------------------------------------------


class EvidenceError(RuntimeError):
    """The evidence itself is unusable; no verdict can be rendered from it."""


def load_jsonl_strict(path: Path) -> tuple[list[dict], list[str]]:
    """Every line parsed, with malformed lines *reported* rather than skipped.

    The previous loader swallowed a ``ValueError`` and continued, so a truncated
    or corrupt line was indistinguishable from a line that was never written.
    """

    rows: list[dict] = []
    problems: list[str] = []
    if not path.exists():
        return rows, [f"{path.name}: missing"]
    with path.open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                parsed = json.loads(stripped)
            except ValueError as exc:
                problems.append(f"{path.name}:{number}: malformed JSON ({exc})")
                continue
            if not isinstance(parsed, dict):
                problems.append(f"{path.name}:{number}: not an object")
                continue
            rows.append(parsed)
    return rows, problems


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def artifact_sizes(root: Path) -> dict[str, tuple[int, float]]:
    """Size and mtime of every evidence file under ``root``."""

    sizes: dict[str, tuple[int, float]] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            stat = path.stat()
            sizes[str(path.relative_to(root))] = (stat.st_size, stat.st_mtime)
    return sizes


def wait_for_quiescence(root: Path, *, quiet_s: float = ARTIFACT_SETTLE_QUIET_S,
                        timeout_s: float = 300.0,
                        sleep=time.sleep) -> dict[str, Any]:
    """Block until nothing under ``root`` has changed for ``quiet_s``.

    The previous soak verdict was written at 13:43:50Z while the dense trace
    kept appending until 13:44:07Z and the session did not end until 13:44:12Z.
    It counted 21,082 ticks; the sealed artifact holds 21,158.  A verdict
    computed from a file that is still being written is a verdict about a
    prefix, and its hashes attest bytes that no longer exist.
    """

    started = time.monotonic()
    previous = artifact_sizes(root)
    stable_since = time.monotonic()
    while True:
        sleep(1.0)
        current = artifact_sizes(root)
        if current != previous:
            previous = current
            stable_since = time.monotonic()
        elif time.monotonic() - stable_since >= quiet_s:
            return {
                "quiesced": True,
                "quiet_s": quiet_s,
                "waited_s": round(time.monotonic() - started, 3),
                "files": len(current),
            }
        if time.monotonic() - started > timeout_s:
            return {
                "quiesced": False,
                "quiet_s": quiet_s,
                "waited_s": round(time.monotonic() - started, 3),
                "files": len(current),
            }


# ---------------------------------------------------------------------------
# The contract
# ---------------------------------------------------------------------------


class Contract:
    def __init__(self) -> None:
        self.results: list[tuple[str, str, str]] = []

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        self.results.append((PASS if ok else FAIL, name, detail))
        return bool(ok)

    @property
    def failures(self) -> list[tuple[str, str, str]]:
        return [row for row in self.results if row[0] == FAIL]


def _check_terminal_record(check, terminal: dict) -> None:
    """Hold the excluded terminal record to a stricter standard of its own.

    The readiness streak no longer reads this record, so it must instead prove,
    positively and from its own fields, that the run ended the way a certifiable
    run must end.  Every assertion here is an addition; none replaces or relaxes
    a rule applied to the operating samples.
    """

    def zero(field: str) -> bool:
        value = terminal.get(field)
        return value is not None and int(value) == 0

    check("terminal record: runtime state is STOPPED",
          str(terminal.get("state")) == "STOPPED",
          f"state={terminal.get('state')!r}")
    # ``runtime_stopped`` is written only when the sampler observed the pinned
    # runtime in a terminal state, which is what "no longer running" means here.
    # ``runtime_alive`` is deliberately not asserted: whether the process object
    # still exists at that instant is a race against its own exit, and the
    # sampler already routes a genuinely vanished process to a different record.
    check("terminal record: runtime is no longer running",
          str(terminal.get("record")) == "runtime_stopped",
          f"record={terminal.get('record')!r}")
    # The inverse of the exclusion above, made explicit so the correction can
    # never be satisfied by a terminal record that falsely claims readiness.
    check("terminal record: honestly reports not operational_ready",
          not bool(terminal.get("operational_ready")),
          f"operational_ready={terminal.get('operational_ready')!r}")
    # Stopped on purpose, rather than died: reached STOPPED (not FAILED, not
    # FATAL, not a vanished process) with nothing left behind.
    check("terminal record: graceful shutdown completed",
          str(terminal.get("record")) == "runtime_stopped"
          and str(terminal.get("state")) == "STOPPED"
          and not str(terminal.get("state")).startswith(("FAILED", "FATAL"))
          and zero("stray_temp_files")
          and not (terminal.get("recovery_blockers") or []),
          f"record={terminal.get('record')!r} state={terminal.get('state')!r} "
          f"stray_temp_files={terminal.get('stray_temp_files')!r} "
          f"blockers={terminal.get('recovery_blockers')!r}")
    check("terminal record: zero open runtime sessions",
          zero("open_runtime_sessions_db") and zero("open_runtime_sessions_export"),
          f"db={terminal.get('open_runtime_sessions_db')!r} "
          f"export={terminal.get('open_runtime_sessions_export')!r}")
    check("terminal record: zero owned process/task/thread leaks",
          all(zero(field) for field in (
              "orphan_processes", "active_job_count", "active_reader_count",
              "readers_over_threshold", "stray_temp_files")),
          "; ".join(f"{f}={terminal.get(f)!r}" for f in (
              "orphan_processes", "active_job_count", "active_reader_count",
              "readers_over_threshold", "stray_temp_files")))
    accounted = terminal.get("recon_accounted")
    submitted = terminal.get("recon_submitted")
    check("terminal record: no pending unaccounted telemetry",
          str(terminal.get("telemetry_data_safety")) == "HEALTHY"
          and not (terminal.get("telemetry_data_safety_reasons") or [])
          and zero("queue_depth")
          and zero("unexpected_noncritical_loss")
          and zero("critical_evidence_lost")
          and zero("critical_evidence_incomplete")
          and zero("unresolved_critical_commands")
          and zero("reconciliation_mismatch")
          and accounted is not None and submitted is not None
          and int(accounted) == int(submitted),
          f"safety={terminal.get('telemetry_data_safety')!r} "
          f"reasons={terminal.get('telemetry_data_safety_reasons')!r} "
          f"queue_depth={terminal.get('queue_depth')!r} "
          f"submitted={submitted!r} accounted={accounted!r}")
    tuple_detail = []
    tuple_ok = True
    for field, want in SAFETY_TUPLE.items():
        value = terminal.get(field)
        if value is None:
            tuple_ok = False
            tuple_detail.append(f"{field}=ABSENT")
            continue
        actual = bool(value) if isinstance(want, bool) else float(value)
        if actual != want:
            tuple_ok = False
        tuple_detail.append(f"{field}={actual}")
    check("terminal record: safety tuple intact",
          tuple_ok and terminal.get("safety_tuple_sources_agree") is not False,
          "; ".join(tuple_detail))


def _series(samples: list[dict], key: str) -> list[float]:
    return [float(row[key]) for row in samples
            if isinstance(row.get(key), (int, float))
            and not isinstance(row.get(key), bool)]


def evaluate(root: Path, *, min_minutes: float) -> dict[str, Any]:
    contract = Contract()
    check = contract.check

    # --- evidence integrity ------------------------------------------------
    sampler_files = sorted((root / "sampler").glob("soak_*.jsonl"))
    if not check("exactly one sampler stream (no stitched evidence)",
                 len(sampler_files) == 1,
                 f"{[f.name for f in sampler_files]}"):
        return _render(contract, root, {}, None)

    rows, problems = load_jsonl_strict(sampler_files[0])
    trace_rows, trace_problems = load_jsonl_strict(root / "readiness_trace.jsonl")
    clock_rows, clock_problems = load_jsonl_strict(root / "clock.jsonl")
    prov_rows, prov_problems = load_jsonl_strict(root / "providers.jsonl")
    all_problems = (problems + trace_problems + clock_problems + prov_problems)
    check("no malformed or unparseable evidence lines",
          not all_problems, "; ".join(all_problems[:5]) or "0 problems")

    manifest = rows[0] if rows and rows[0].get("record") == "manifest" else None
    check("sampler stream opens with a manifest", manifest is not None,
          "" if manifest else "first record is not a manifest")
    samples = [r for r in rows if r.get("record") != "manifest"]
    check("sampler stream contains samples", bool(samples), f"{len(samples)}")
    if manifest is None or not samples:
        return _render(contract, root, {}, manifest)

    pinned = manifest.get("pinned") or {}

    # --- sample ordering and completeness ----------------------------------
    indices = [r.get("sample_index") for r in samples]
    expected = list(range(1, len(samples) + 1))
    check("sample indices are contiguous from 1, in order, with no duplicates",
          indices == expected,
          f"got {indices[:3]}..{indices[-3:]} expected 1..{len(samples)}")
    stamps = [int(r.get("sampled_at_ms") or 0) for r in samples]
    check("sample timestamps are strictly increasing",
          all(b > a for a, b in zip(stamps, stamps[1:])),
          f"{sum(1 for a, b in zip(stamps, stamps[1:]) if b <= a)} out of order")
    gaps = [(b - a) / 1000.0 for a, b in zip(stamps, stamps[1:])]
    interval = float(manifest.get("interval_s") or 30.0)
    # 1.5 intervals, not 2: at exactly two intervals a whole sample has been
    # missed, and a bound that admits it cannot tell a complete stream from one
    # with a hole in it.
    check("no sampling gap beyond half an interval of slack",
          max(gaps, default=0.0) <= interval * 1.5,
          f"max_gap={max(gaps, default=0.0):.2f}s interval={interval:.0f}s")

    # --- identity, compared to the manifest --------------------------------
    # The previous evaluator only checked that each field held ONE value across
    # the run; ``expected_key`` was read into a variable and never used, so a
    # run pinned to the wrong commit passed as long as it was consistently
    # wrong.
    for field, expected_key in (("current_commit", "expected_commit"),
                                ("current_runtime_pid", "expected_runtime_pid"),
                                ("runtime_session_id", "expected_session_id"),
                                ("launch_nonce", "expected_launch_nonce")):
        observed = {str(r.get(field)) for r in samples if r.get(field) is not None}
        declared = {str(r.get(expected_key)) for r in samples
                    if r.get(expected_key) is not None}
        manifest_value = {
            "current_commit": str(pinned.get("commit")),
            "current_runtime_pid": str(pinned.get("pid")),
            "runtime_session_id": str(pinned.get("session_id")),
            "launch_nonce": str(pinned.get("launch_nonce")),
        }[field]
        check(f"one {field}, equal to the manifest",
              len(observed) == 1 and observed == declared == {manifest_value},
              f"observed={sorted(observed)} declared={sorted(declared)} "
              f"manifest={manifest_value}")

    check("no identity mismatch record",
          not [r for r in samples if r.get("record") == "identity_mismatch"],
          f"{len([r for r in samples if r.get('record') == 'identity_mismatch'])}")
    check("ownership valid in every sample",
          all(bool(r.get("ownership_valid")) for r in samples),
          f"{sum(1 for r in samples if not r.get('ownership_valid'))} invalid")
    check("no orphan processes",
          all(int(r.get("orphan_processes") or 0) == 0 for r in samples),
          f"max={max((int(r.get('orphan_processes') or 0)) for r in samples)}")
    check("runtime alive in every sample before the terminal record",
          all(bool(r.get("runtime_alive")) for r in samples[:-1]), "")

    # --- terminal record and evaluation through shutdown -------------------
    terminal = samples[-1]
    check("stream ends on a terminal record",
          str(terminal.get("record")) in TERMINAL_RECORDS,
          f"final record={terminal.get('record')!r}")
    check("run was observed through a clean shutdown",
          str(terminal.get("record")) in CLEAN_TERMINAL_RECORDS,
          f"final record={terminal.get('record')!r} "
          f"state={terminal.get('state')!r}")
    _check_terminal_record(check, terminal)

    window = [r for r in samples if r.get("phase") != "tail"]
    span_s = ((window[-1]["sampled_at_ms"] - window[0]["sampled_at_ms"]) / 1000.0
              if len(window) > 1 else 0.0)
    check(f"continuous run >= {min_minutes:.0f} min",
          span_s >= min_minutes * 60,
          f"{span_s/60:.2f} min across {len(window)} window samples "
          f"(+{len(samples) - len(window)} tail)")

    # --- safety tuple: all ten fields --------------------------------------
    safety_detail = []
    safety_ok = True
    for field, want in SAFETY_TUPLE.items():
        values = {r.get(field) for r in samples if field in r}
        if not values:
            safety_ok = False
            safety_detail.append(f"{field}=ABSENT")
            continue
        if None in values:
            # A field the runtime stopped publishing proves nothing about it.
            safety_ok = False
        if isinstance(want, bool):
            normalised = {bool(v) for v in values if v is not None}
        else:
            normalised = {float(v) for v in values if v is not None}
        if normalised != {want}:
            safety_ok = False
        safety_detail.append(f"{field}={sorted(normalised)}")
    check("full safety tuple unchanged throughout (all ten fields)",
          safety_ok, "; ".join(safety_detail))
    check("safety tuple sources agree in every sample",
          all(r.get("safety_tuple_sources_agree") is not False for r in samples),
          f"{sum(1 for r in samples if r.get('safety_tuple_sources_agree') is False)}"
          " disagreements")

    # --- runtime state ------------------------------------------------------
    states = Counter(str(r.get("state")) for r in samples)
    check("no FAILED/FATAL runtime state",
          not any(s.startswith(("FAILED", "FATAL")) for s in states),
          f"{dict(states)}")

    hb, exp = _series(samples, "heartbeat_age_s"), _series(samples, "export_age_s")
    check("heartbeat age <= 15 s", max(hb, default=0) <= 15.0,
          f"max={max(hb, default=0):.2f}s")
    check("export age <= 10 s", max(exp, default=0) <= 10.0,
          f"max={max(exp, default=0):.2f}s")
    lag = _series(samples, "loop_lag_ms")
    check("no multi-second event-loop freeze", max(lag, default=0) < 2000.0,
          f"max={max(lag, default=0):.0f}ms")

    # --- dashboard: measured, not asserted ---------------------------------
    check("dashboard listening in every sample",
          all(bool(r.get("dashboard_listening")) for r in samples),
          f"{sum(1 for r in samples if not r.get('dashboard_listening'))} down")
    statuses = Counter(r.get("dashboard_http_status") for r in samples)
    check("dashboard answered 2xx/3xx in every sample",
          all(isinstance(r.get("dashboard_http_status"), int)
              and 200 <= int(r["dashboard_http_status"]) < 400
              for r in samples),
          f"{dict(statuses)}")
    probe_ms = _series(samples, "dashboard_probe_ms")
    check("dashboard responded within 5 s", max(probe_ms, default=0) <= 5000.0,
          f"max={max(probe_ms, default=0):.1f}ms "
          f"p50={median(probe_ms) if probe_ms else 0:.1f}ms")

    # --- memory growth ------------------------------------------------------
    rss = _series(samples, "runtime_rss_bytes")
    check("runtime memory sampled in every sample",
          len(rss) == len(samples), f"{len(rss)}/{len(samples)}")
    if len(rss) >= 8:
        half = len(rss) // 2
        first, second = median(rss[:half]), median(rss[half:])
        growth = (second - first) / max(1.0, first)
        check("no material late-run memory growth (< 25% median RSS)",
              growth < 0.25,
              f"{first/2**20:.1f}MB -> {second/2**20:.1f}MB "
              f"({100*growth:+.1f}%), peak={max(rss)/2**20:.1f}MB")
        check("peak RSS stays under 4 GiB", max(rss) < 4 * 2**30,
              f"peak={max(rss)/2**30:.2f}GiB")

    # --- loss / conservation ------------------------------------------------
    for key, label in (("critical_evidence_lost", "critical loss = 0"),
                       ("critical_evidence_incomplete", "critical incomplete = 0"),
                       ("unexpected_noncritical_loss",
                        "unexpected noncritical loss = 0"),
                       ("reconciliation_mismatch", "reconciliation mismatch = 0"),
                       ("recon_unexpected_loss", "recon unexpected loss = 0")):
        values = _series(samples, key)
        check(label, max(values, default=0) == 0, f"max={max(values, default=0):.0f}")
    submitted, accounted = _series(samples, "recon_submitted"), _series(
        samples, "recon_accounted")
    if submitted and accounted:
        check("lifetime reconciliation closes exactly (submitted == accounted)",
              submitted[-1] == accounted[-1],
              f"{submitted[-1]:.0f} vs {accounted[-1]:.0f}")

    # --- queues -------------------------------------------------------------
    check("telemetry queue bounded in every sample",
          all(bool(r.get("queue_bounded")) for r in samples), "")
    depth = _series(samples, "queue_depth")
    capacity = _series(samples, "queue_capacity")
    limit = capacity[-1] if capacity else 20000
    check("telemetry queue depth well inside capacity",
          max(depth, default=0) < 0.5 * limit,
          f"max_depth={max(depth, default=0):.0f} capacity={limit:.0f}")
    oldest = _series(samples, "queue_oldest_age_s")
    check("oldest queued message age bounded (< 15 s)",
          max(oldest, default=0) < 15.0, f"max={max(oldest, default=0):.2f}s")
    for key in ("polymarket_queue_depth", "cex_queue_depth"):
        values = _series(samples, key)
        if values:
            check(f"{key} bounded", max(values) < 10_000, f"max={max(values):.0f}")
    half = len(depth) // 2
    if half >= 4:
        check("no progressive queue-floor growth",
              min(depth[half:]) <= min(depth[:half]) + 8,
              f"first_half_min={min(depth[:half]):.0f} "
              f"second_half_min={min(depth[half:]):.0f}")

    # --- integrity / WAL ----------------------------------------------------
    chunk = _series(samples, "live_integrity_max_chunk_ms")
    check("every live-integrity chunk <= 1000 ms", max(chunk, default=0) <= 1000.0,
          f"max={max(chunk, default=0):.0f}ms")
    health = Counter(str(r.get("live_integrity_health")) for r in samples)
    check("live integrity health OK throughout",
          set(health) <= {"OK", "None"}, f"{dict(health)}")
    over = _series(samples, "live_integrity_chunks_over_bound")
    check("no integrity chunk over bound", max(over, default=0) == 0,
          f"max={max(over, default=0):.0f}")
    integrity = Counter(str(r.get("sqlite_integrity")) for r in samples)
    check("sqlite integrity never bad",
          not any(v.lower() not in ("ok", "none", "unknown") for v in integrity),
          f"{dict(integrity)}")
    wal = _series(samples, "wal_bytes")
    check("WAL bounded (< 512 MB)", max(wal, default=0) < 512 * 2**20,
          f"max={max(wal, default=0)/2**20:.1f}MB")
    checkpoints = {str(r.get("checkpoint_run_id")) for r in samples
                   if r.get("checkpoint_run_id") is not None}
    check("checkpoint progress continuous", len(checkpoints) >= 2,
          f"{len(checkpoints)} distinct checkpoint runs")

    # --- readiness, under the ratified startup-settle policy ---------------
    ready = [bool(r.get("operational_ready")) for r in samples]
    first_ready = next((i for i, value in enumerate(ready) if value), None)
    check("lane reaches operational_ready within the first 3 samples",
          first_ready is not None and first_ready < MAX_SAMPLES_TO_FIRST_READY,
          f"first ready at sample {None if first_ready is None else first_ready + 1}")
    # Measured over the operating samples only.  The stream is *required* to end
    # on a post-shutdown terminal record, and a runtime that has correctly
    # stopped is correctly not operational_ready, so including that record here
    # made this rule and the clean-shutdown rule mutually unsatisfiable.  The
    # excluded record is held to a stricter standard of its own below; this is
    # the only criterion it is excluded from.
    operating_ready = [bool(r.get("operational_ready")) for r in samples
                       if str(r.get("record")) not in TERMINAL_RECORDS]
    check("final ten consecutive operating samples operational_ready=true",
          len(operating_ready) >= 10 and all(operating_ready[-10:]),
          f"{operating_ready[-10:]} over {len(operating_ready)} operating "
          f"samples ({len(samples) - len(operating_ready)} terminal excluded)")
    check("operational_ready duty cycle >= 95%",
          sum(ready) / max(1, len(ready)) >= 0.95,
          f"{sum(ready)}/{len(ready)} = {100*sum(ready)/max(1,len(ready)):.1f}%")
    early = Counter()
    late = Counter()
    for index, row in enumerate(samples):
        target = late if (first_ready is not None and index > first_ready) else early
        for reason in (row.get("operational_degraded_reasons") or []):
            target[reason] += 1
    check("no telemetry_recovery_window after the lane first certifies",
          late.get("telemetry_recovery_window", 0) == 0,
          f"after={dict(late)} before={dict(early)}")
    check("no degradation of any kind after the lane first certifies",
          not late, f"{dict(late)}")

    # --- sessions / stray files (original strict rule restored) ------------
    open_sessions = _series(samples, "open_runtime_sessions_db")
    check("exactly one open runtime session while running",
          set(open_sessions[:-1]) <= {1.0},
          f"observed={sorted(set(open_sessions))}")
    strays = _series(samples, "stray_temp_files")
    check("no stray temp files", max(strays, default=0) == 0,
          f"max={max(strays, default=0):.0f} in "
          f"{sum(1 for v in strays if v > 0)}/{len(strays)} samples")

    # --- dense trace: the WHOLE session, not the sampled window ------------
    ticks = [t for t in trace_rows if t.get("kind") == "tick"]
    check("dense readiness trace present", bool(ticks), f"{len(ticks)} ticks")
    if ticks:
        sequence = [int(t.get("seq") or 0) for t in ticks]
        check("dense trace sequence is contiguous and in order",
              sequence == list(range(sequence[0], sequence[0] + len(sequence))),
              f"seq {sequence[0]}..{sequence[-1]} across {len(sequence)} ticks")
        window_end = window[-1]["sampled_at_ms"]
        post = [t for t in ticks if int(t.get("wall_ms") or 0) > window_end]

        blockers = Counter()
        for tick in ticks:
            for blocker in (tick.get("blockers") or []):
                blockers[blocker] += 1
        # Evaluated over every tick of the session.  The previous evaluator
        # filtered the trace to the sampled window, which is how the gate's
        # four post-window queue_accumulating/uncontrolled_overload ticks and
        # its readiness reset stayed out of the verdict entirely.
        for blocker in ("queue_accumulating", "uncontrolled_overload",
                        "service_imbalance", "unexpected_loss",
                        "admission_overflow", "queue_hard_cap_breached"):
            check(f"{blocker} never occurs in the whole session",
                  blockers.get(blocker, 0) == 0,
                  f"{blockers.get(blocker, 0)} ticks")
        resets = sum(1 for t in ticks if t.get("streak_reset"))
        check("no readiness streak resets in the whole session",
              resets == 0, f"{resets} resets")
        post_blockers = Counter()
        for tick in post:
            for blocker in (tick.get("blockers") or []):
                post_blockers[blocker] += 1
        check("no blocker or reset after the sampled window",
              not post_blockers
              and not any(t.get("streak_reset") for t in post),
              f"{len(post)} post-window ticks, blockers={dict(post_blockers)}, "
              f"resets={sum(1 for t in post if t.get('streak_reset'))}")

        # --- exact conservation, both signs, whole session -----------------
        # A tick that does not carry the terms is not a tick that balanced.
        # Reading a missing key as zero is how "conservation closed" could be
        # reported about evidence that never measured it.
        missing = [t for t in ticks
                   if "residual" not in (t.get("conservation") or {})]
        missing_abs = [t for t in ticks
                       if "absolute_residual" not in (t.get("conservation") or {})]
        residuals = [int((t.get("conservation") or {})["residual"])
                     for t in ticks if "residual" in (t.get("conservation") or {})]
        absolutes = [int((t.get("conservation") or {})["absolute_residual"])
                     for t in ticks
                     if "absolute_residual" in (t.get("conservation") or {})]
        check("every tick carries the conservation identity terms",
              not missing and not missing_abs,
              f"{len(ticks) - len(missing)}/{len(ticks)} carry residual, "
              f"{len(ticks) - len(missing_abs)}/{len(ticks)} carry "
              "absolute_residual")
        nonzero = [v for v in residuals if v != 0]
        check("window conservation residual is exactly zero on every tick",
              not nonzero and not missing,
              f"{len(nonzero)} nonzero and {len(missing)} missing of "
              f"{len(ticks)}; min={min(residuals, default=0)} "
              f"max={max(residuals, default=0)}")
        nonzero_abs = [v for v in absolutes if v != 0]
        check("absolute conservation residual is exactly zero on every tick",
              not nonzero_abs and not missing_abs,
              f"{len(nonzero_abs)} nonzero and {len(missing_abs)} missing of "
              f"{len(ticks)}; min={min(absolutes, default=0)} "
              f"max={max(absolutes, default=0)}")

        # --- keep_ratio: RESTORED ORIGINAL STRICT --------------------------
        zero_ticks = [t for t in ticks if t.get("sampling_keep_ratio") == 0.0]
        episodes: list[list[dict]] = []
        for tick in zero_ticks:
            if episodes and tick["mono"] - episodes[-1][-1]["mono"] <= 0.6:
                episodes[-1].append(tick)
            else:
                episodes.append([tick])
        longest = max((e[-1]["mono"] - e[0]["mono"] for e in episodes), default=0.0)
        # Every episode is reported whether or not any exist, as required.
        check("keep_ratio never reaches zero on any control tick",
              not zero_ticks,
              f"{len(zero_ticks)}/{len(ticks)} ticks in {len(episodes)} episodes, "
              f"longest={longest:.2f}s, depths="
              f"{sorted({int(t.get('queue_depth') or 0) for t in zero_ticks})[:8]}")
        keep_samples = _series(samples, "sampling_keep_ratio")
        check("sample-level keep_ratio never collapses to zero",
              all(v > 0.0 for v in keep_samples),
              f"min={min(keep_samples, default=1.0):.4f}, "
              f"zero in {sum(1 for v in keep_samples if v == 0.0)}/"
              f"{len(keep_samples)}")

        # --- fairness -------------------------------------------------------
        gates = [t["writer"]["gate"] for t in ticks
                 if isinstance((t.get("writer") or {}).get("gate"), dict)
                 and "max_consecutive_telemetry_skips" in t["writer"]["gate"]]
        if gates:
            worst = max(int(g["max_consecutive_telemetry_skips"]) for g in gates)
            check("consecutive priority skips stay bounded (<= 50)", worst <= 50,
                  f"max_consecutive={worst}")
            live = [t["writer"]["gate"] for t in ticks
                    if isinstance((t.get("writer") or {}).get("gate"), dict)
                    and int((t.get("writer") or {}).get("batches") or 0) > 0]
            gap = max((float(g.get("seconds_since_telemetry_admit") or 0.0)
                       for g in live), default=0.0)
            check("normal lane always served within 30 s once running",
                  gap <= 30.0, f"max_since_admit={gap:.2f}s")
            critical = sum(1 for g in gates
                           if int(g.get("critical_pending") or 0) > 0
                           or int(g.get("critical_inflight") or 0) > 0)
            check("critical traffic and priority scheduling actually occur",
                  critical > 0, f"{critical}/{len(gates)} ticks with critical work")

        # --- real ingest ----------------------------------------------------
        costed = [t for t in ticks
                  if (t.get("writer") or {}).get("sink_cost", {}).get(
                      "row_cost_by_method")]
        # Fails closed: the previous evaluator dropped these checks entirely
        # when fewer than three cost samples existed, so a run with no measured
        # ingest at all passed by omission.
        if not check("sink cost sampled enough to judge real ingest",
                     len(costed) > 2, f"{len(costed)} costed ticks"):
            pass
        else:
            head = costed[0]["writer"]["sink_cost"]["row_cost_by_method"]
            tail = costed[-1]["writer"]["sink_cost"]["row_cost_by_method"]
            span = max(1e-9, costed[-1]["mono"] - costed[0]["mono"])

            def rate(method: str) -> float:
                return (int(tail.get(method, {}).get("calls", 0))
                        - int(head.get(method, {}).get("calls", 0))) / span

            def cost(method: str) -> float:
                calls = (int(tail.get(method, {}).get("calls", 0))
                         - int(head.get(method, {}).get("calls", 0)))
                ms = (float(tail.get(method, {}).get("total_ms", 0.0))
                      - float(head.get(method, {}).get("total_ms", 0.0)))
                return ms / max(1, calls)

            check("sustained CEX ingest (record_cex_observation >= 1/s)",
                  rate("record_cex_observation") >= 1.0,
                  f"{rate('record_cex_observation'):.2f}/s at "
                  f"{cost('record_cex_observation'):.4f} ms/row")
            check("sustained Polymarket book ingest "
                  "(record_book_snapshot >= 0.5/s)",
                  rate("record_book_snapshot") >= 0.5,
                  f"{rate('record_book_snapshot'):.2f}/s at "
                  f"{cost('record_book_snapshot'):.4f} ms/row")
            methods = set(head) | set(tail)
            total_calls = sum(
                int(tail.get(m, {}).get("calls", 0))
                - int(head.get(m, {}).get("calls", 0)) for m in methods)
            total_ms = sum(
                float(tail.get(m, {}).get("total_ms", 0.0))
                - float(head.get(m, {}).get("total_ms", 0.0)) for m in methods)
            per_row = total_ms / max(1, total_calls)
            check("mean sink row cost stays under 3 ms", per_row < 3.0,
                  f"{per_row:.4f} ms/row over {total_calls} rows")

        # --- verified duplicates / integrity conflicts ----------------------
        conflicts = max(
            (int((t.get("writer") or {}).get("evidence_conflicts") or 0)
             for t in ticks), default=0)
        check("no unresolved evidence integrity conflict", conflicts == 0,
              f"{conflicts} conflicts")

    # --- providers, under the ratified hydration policy --------------------
    if not check("provider health captured", bool(prov_rows),
                 f"{len(prov_rows)} samples"):
        pass
    else:
        worst_future = 0
        by_source: dict[str, list[dict]] = {}
        for row in prov_rows:
            for entry in (row.get("sources") or []):
                worst_future = max(worst_future,
                                   int(entry.get("future_count") or 0))
                by_source.setdefault(str(entry.get("source")), []).append(entry)
        check("provider future-timestamp rejects at zero", worst_future == 0,
              f"max future_count={worst_future}")
        for source, entries in sorted(by_source.items()):
            check(f"{source} connected in every sample",
                  all(bool(e.get("connected")) for e in entries),
                  f"{sum(1 for e in entries if e.get('connected'))}/{len(entries)}")
            longest, current = 0, 0
            for entry in entries:
                if str(entry.get("status")) == "READY":
                    current = 0
                else:
                    current += 1
                    longest = max(longest, current)
            check(f"{source}: no HYDRATING episode longer than "
                  f"{MAX_HYDRATING_SAMPLES} samples",
                  longest <= MAX_HYDRATING_SAMPLES,
                  f"longest_unready_run={longest} samples "
                  f"(READY in {sum(1 for e in entries if str(e.get('status')) == 'READY')}"
                  f"/{len(entries)})")
            check(f"{source}: terminal sample is READY",
                  str(entries[-1].get("status")) == "READY",
                  f"final={entries[-1].get('status')!r}")

    # --- clock / disk -------------------------------------------------------
    clock_samples = [r for r in clock_rows if r.get("record") != "manifest"]
    if not check("clock monitor captured", bool(clock_samples),
                 f"{len(clock_samples)} samples"):
        pass
    else:
        medians = [r["offset_median_ms"] for r in clock_samples
                   if r.get("offset_median_ms") is not None]
        jumps = [abs(b - a) for a, b in zip(medians, medians[1:])]
        check("host clock offset stays small (|median| <= 50 ms)",
              max((abs(v) for v in medians), default=0) <= 50.0,
              f"max|median|={max((abs(v) for v in medians), default=0):.2f}ms")
        check("no unstable clock jump (< 100 ms between samples)",
              max(jumps, default=0) < 100.0,
              f"max_jump={max(jumps, default=0):.2f}ms")
        free = [r.get("free_gb_C") for r in clock_samples if r.get("free_gb_C")]
        check("C: free space remains safe (>= 15 GB)",
              min(free, default=999) >= 15.0,
              f"min={min(free, default=0):.1f}GB")

    return _render(contract, root, {
        "sampler": str(sampler_files[0]),
        "samples": len(samples),
        "window_samples": len(window),
        "tail_samples": len(samples) - len(window),
        "span_minutes": round(span_s / 60.0, 3),
        "trace_ticks": len(ticks),
    }, manifest)


def _render(contract: Contract, root: Path, summary: dict,
            manifest: Optional[dict]) -> dict[str, Any]:
    pinned = (manifest or {}).get("pinned") or {}
    return {
        "contract_version": CONTRACT_VERSION,
        "evidence_dir": str(root),
        "pinned": pinned,
        "summary": summary,
        "criteria": [
            {"status": status, "name": name, "detail": detail}
            for status, name, detail in contract.results
        ],
        "passed": len(contract.results) - len(contract.failures),
        "total": len(contract.results),
        "verdict": "PASS" if not contract.failures else "FAIL",
        "failures": [name for _s, name, _d in contract.failures],
    }


def seal(root: Path) -> dict[str, str]:
    """SHA-256 of every evidence file, taken after the artifacts stopped growing."""

    return {
        str(path.relative_to(root)): sha256_file(path)
        for path in sorted(root.rglob("*")) if path.is_file()
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence_dir")
    parser.add_argument("--min-minutes", type=float, required=True)
    parser.add_argument("--json")
    parser.add_argument("--seal", action="store_true",
                        help="hash-seal the evidence after it stops growing")
    parser.add_argument("--settle-timeout-s", type=float, default=300.0)
    parser.add_argument("--no-wait", action="store_true",
                        help="skip the quiescence wait (offline re-evaluation "
                             "of already-sealed evidence)")
    args = parser.parse_args(argv)
    root = Path(args.evidence_dir)

    quiescence: dict[str, Any] = {"quiesced": None, "skipped": True}
    if not args.no_wait:
        quiescence = wait_for_quiescence(
            root, timeout_s=args.settle_timeout_s)
        print(f"artifact quiescence: {quiescence}")

    report = evaluate(root, min_minutes=args.min_minutes)
    report["artifact_quiescence"] = quiescence
    report["evaluator"] = {
        "path": str(Path(__file__).resolve()),
        "sha256": sha256_file(Path(__file__).resolve()),
        "contract_version": CONTRACT_VERSION,
    }
    if not quiescence.get("skipped") and not quiescence.get("quiesced"):
        report["criteria"].append({
            "status": FAIL,
            "name": "evidence artifacts stopped growing before evaluation",
            "detail": f"{quiescence}",
        })
        report["total"] += 1
        report["verdict"] = "FAIL"
        report["failures"].append(
            "evidence artifacts stopped growing before evaluation")

    if args.seal:
        # Sealed only after the wait, so the hashes attest the final bytes.
        report["sealed_sha256"] = seal(root)

    print("\n" + "=" * 78)
    for row in report["criteria"]:
        detail = f"  --  {row['detail']}" if row["detail"] else ""
        print(f"[{row['status']}] {row['name']}{detail}")
    print("=" * 78)
    pinned = report["pinned"]
    print(f"contract={CONTRACT_VERSION} "
          f"evaluator_sha256={report['evaluator']['sha256'][:16]}")
    print(f"pinned commit={str(pinned.get('commit'))[:12]} "
          f"pid={pinned.get('pid')} "
          f"session={str(pinned.get('session_id'))[:12]} "
          f"nonce={str(pinned.get('launch_nonce'))[:12]}")
    print(f"{report['passed']}/{report['total']} criteria PASSED")
    print("GATE VERDICT:", report["verdict"]
          if report["verdict"] == "PASS"
          else f"FAIL ({len(report['failures'])})")

    if args.json:
        Path(args.json).write_text(
            json.dumps(report, indent=2, default=str), encoding="utf-8")
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
