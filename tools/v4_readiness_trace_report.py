"""Attribute Frequency V4 readiness-streak resets to their exact cause.

Reads the JSON-lines file produced by the dense readiness observer (see
``lite_frequency_v4/readiness_trace.py``) and answers one question per reset:
*which predicate went false, and which numbers made it go false?*

Read-only.  It never touches the runtime, the database or the repository.

Output
------
``--summary``  one-screen verdict: tick count, reset count and cadence, the
blocker histogram, and the readiness duty cycle.

``--events``   one block per reset with the mission's required window -- the
10 s before, the reset tick itself, and the 20 s after -- printed as a compact
per-tick table plus the exact conservation terms and the cumulative loss
counters that moved across the reset.

``--json PATH`` the same attribution as machine-readable JSON.
"""
from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

#: Mission-specified capture window around each reset, in seconds.
BEFORE_S = 10.0
AFTER_S = 20.0


def load(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split the trace into controller ticks and engine-cadence records."""

    ticks: list[dict[str, Any]] = []
    engine: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("kind") == "engine":
                engine.append(record)
            else:
                ticks.append(record)
    ticks.sort(key=lambda row: (row.get("mono") or 0.0, row.get("seq") or 0))
    engine.sort(key=lambda row: row.get("mono") or 0.0)
    return ticks, engine


def resets(ticks: Sequence[dict[str, Any]]) -> list[int]:
    """Indices of ticks where a non-zero healthy streak was knocked to zero."""

    found = []
    for index, tick in enumerate(ticks):
        if tick.get("streak_reset"):
            found.append(index)
        elif (index > 0
              and int(ticks[index - 1].get("healthy_streak_after") or 0) > 0
              and int(tick.get("healthy_streak_after") or 0) == 0):
            found.append(index)
    return found


def _loss_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, int]:
    left = (before.get("writer") or {}).get("loss_by_category") or {}
    right = (after.get("writer") or {}).get("loss_by_category") or {}
    delta = {}
    for key in sorted(set(left) | set(right)):
        change = int(right.get(key, 0)) - int(left.get(key, 0))
        if change:
            delta[key] = change
    return delta


def _writer_delta(before: dict[str, Any], after: dict[str, Any],
                  fields: Iterable[str]) -> dict[str, int]:
    left = before.get("writer") or {}
    right = after.get("writer") or {}
    delta = {}
    for key in fields:
        change = int(right.get(key, 0) or 0) - int(left.get(key, 0) or 0)
        if change:
            delta[key] = change
    return delta


_DELTA_FIELDS = (
    "submitted", "offered", "admitted", "logical_written", "written",
    "batches", "batch_attempts", "failed_batches", "deadline_exceeded_batches",
    "priority_skipped_batches", "checkpoint_deferral_batches", "requeued_rows",
    "coalesced", "preoverflow_coalesced", "sampled", "deferred",
    "deduplicated", "dropped", "admission_overflow_rows", "overflow_count",
    "aggregated_in_queue",
)


def window(ticks: Sequence[dict[str, Any]], index: int) -> list[dict[str, Any]]:
    centre = float(ticks[index].get("mono") or 0.0)
    return [
        tick for tick in ticks
        if -BEFORE_S <= float(tick.get("mono") or 0.0) - centre <= AFTER_S
    ]


def attribute(ticks: Sequence[dict[str, Any]], index: int) -> dict[str, Any]:
    """Everything needed to explain one reset, with no inference from names."""

    tick = ticks[index]
    prior = ticks[index - 1] if index else tick
    rows = window(ticks, index)
    conservation = tick.get("conservation") or {}
    return {
        "seq": tick.get("seq"),
        "mono": tick.get("mono"),
        "wall_ms": tick.get("wall_ms"),
        "controller_state": tick.get("controller_state"),
        "streak_before": tick.get("healthy_streak_before"),
        "streak_after": tick.get("healthy_streak_after"),
        "blockers": tick.get("blockers"),
        "prior_blockers": prior.get("blockers"),
        "predicates": {
            key: tick.get(key) for key in (
                "service_balanced", "depth_nonincreasing", "controller_safe",
                "within_hard_bound", "policy_consistent", "controlled_overload",
                "overload_active", "overload_reason", "capacity_pressure",
                "high_pressure", "settle_age_s",
            )
        },
        "conservation": conservation,
        "queue_trend": tick.get("queue_trend"),
        "recovery_view": tick.get("recovery_view"),
        "rates": tick.get("rates"),
        "queue_depth": tick.get("queue_depth"),
        "selected_chunk": tick.get("selected_chunk"),
        "deadline_safe_chunk": tick.get("deadline_safe_chunk"),
        "sampling_keep_ratio": tick.get("sampling_keep_ratio"),
        "loss_delta_at_reset": _loss_delta(prior, tick),
        "writer_delta_at_reset": _writer_delta(prior, tick, _DELTA_FIELDS),
        "loss_delta_over_window": (
            _loss_delta(rows[0], rows[-1]) if len(rows) > 1 else {}),
        "window_tick_count": len(rows),
        "ticks_to_recover": _ticks_to_recover(ticks, index),
    }


def _ticks_to_recover(ticks: Sequence[dict[str, Any]], index: int) -> Optional[int]:
    required = int(ticks[index].get("required_healthy_windows") or 10)
    for offset in range(index + 1, len(ticks)):
        if int(ticks[offset].get("healthy_streak_after") or 0) >= required:
            return offset - index
    return None


def summarise(ticks: Sequence[dict[str, Any]],
              engine: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not ticks:
        return {"ticks": 0}
    span = float(ticks[-1]["mono"]) - float(ticks[0]["mono"])
    reset_indices = resets(ticks)
    blocker_counts: Counter[str] = Counter()
    for tick in ticks:
        for name in tick.get("blockers") or []:
            blocker_counts[name] += 1
    reset_blockers: Counter[str] = Counter()
    for index in reset_indices:
        for name in ticks[index].get("blockers") or []:
            reset_blockers[name] += 1
    required = int(ticks[0].get("required_healthy_windows") or 10)
    ready = sum(
        1 for tick in ticks
        if int(tick.get("healthy_streak_after") or 0) >= required)
    gaps = [
        float(ticks[right]["mono"]) - float(ticks[left]["mono"])
        for left, right in zip(reset_indices, reset_indices[1:])
    ]
    # Both signs are failures.  A positive residual is rows the window took in
    # and cannot account for; a negative one is rows accounted for twice, or
    # inventory that appeared without an inflow.  Reporting only the positive
    # side is exactly how a window full of negative surplus was summarised as
    # "deficit = 0".
    residuals = [
        int((tick.get("conservation") or {}).get("residual") or 0)
        for tick in ticks
    ]
    absolute_residuals = [
        int((tick.get("conservation") or {}).get("absolute_residual") or 0)
        for tick in ticks
    ]
    nonzero_residuals = [value for value in residuals if value != 0]
    summary: dict[str, Any] = {
        "ticks": len(ticks),
        "engine_records": len(engine),
        "span_s": round(span, 1),
        "observed_tick_hz": round(len(ticks) / span, 3) if span > 0 else None,
        "required_healthy_windows": required,
        "ready_ticks": ready,
        "ready_duty_cycle": round(ready / len(ticks), 4),
        "reset_count": len(reset_indices),
        "reset_per_minute": (
            round(len(reset_indices) / (span / 60.0), 3) if span > 0 else None),
        "mean_seconds_between_resets": (
            round(statistics.fmean(gaps), 1) if gaps else None),
        "min_seconds_between_resets": round(min(gaps), 1) if gaps else None,
        "blocker_tick_histogram": dict(blocker_counts.most_common()),
        "blocker_at_reset_histogram": dict(reset_blockers.most_common()),
        "conservation_residual_nonzero_ticks": len(nonzero_residuals),
        "conservation_residual_min": min(residuals, default=0),
        "conservation_residual_max": max(residuals, default=0),
        "conservation_residual_mean_abs": (
            round(statistics.fmean(abs(v) for v in nonzero_residuals), 2)
            if nonzero_residuals else 0.0),
        "conservation_absolute_residual_nonzero_ticks": sum(
            1 for value in absolute_residuals if value != 0),
        "conservation_absolute_residual_min": min(absolute_residuals, default=0),
        "conservation_absolute_residual_max": max(absolute_residuals, default=0),
    }
    recoveries = [
        value for value in (
            _ticks_to_recover(ticks, index) for index in reset_indices)
        if value is not None
    ]
    if recoveries:
        summary["ticks_to_recover_median"] = int(statistics.median(recoveries))
        summary["ticks_to_recover_max"] = max(recoveries)
    return summary


def _fmt_tick(tick: dict[str, Any], centre: float) -> str:
    conservation = tick.get("conservation") or {}
    trend = tick.get("queue_trend") or {}
    return (
        f"  t{float(tick.get('mono') or 0.0) - centre:+7.2f}s "
        f"streak={int(tick.get('healthy_streak_after') or 0):>3} "
        f"q={int(tick.get('queue_depth') or 0):>5} "
        f"adm={int(conservation.get('admitted_window') or 0):>7} "
        f"agg={int(conservation.get('aggregated_window') or 0):>5} "
        f"lcom={int(conservation.get('logical_committed_window') or 0):>7} "
        f"lostA={int(conservation.get('lost_admitted_window') or 0):>4} "
        f"inv={int(conservation.get('starting_inventory') or 0):>5}"
        f"->{int(conservation.get('ending_inventory') or 0):<5} "
        f"RES={int(conservation.get('residual') or 0):>6} "
        f"ABS={int(conservation.get('absolute_residual') or 0):>6} "
        f"old={float(trend.get('oldest_age_s') or 0.0):>5.2f}s "
        f"{','.join(tick.get('blockers') or []) or '-'}"
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--events", action="store_true")
    parser.add_argument("--max-events", type=int, default=8)
    parser.add_argument("--json", type=Path, default=None)
    args = parser.parse_args(argv)

    ticks, engine = load(args.trace)
    if not ticks:
        print("no controller ticks in trace")
        return 1
    summary = summarise(ticks, engine)
    reset_indices = resets(ticks)
    events = [attribute(ticks, index) for index in reset_indices]

    if args.summary or not (args.events or args.json):
        print(json.dumps(summary, indent=2))

    if args.events:
        for event in events[: args.max_events]:
            index = reset_indices[events.index(event)]
            centre = float(ticks[index]["mono"])
            print("\n" + "=" * 100)
            print(
                f"RESET seq={event['seq']} mono={event['mono']} "
                f"state={event['controller_state']} "
                f"blockers={event['blockers']} "
                f"streak {event['streak_before']} -> {event['streak_after']} "
                f"recover_in_ticks={event['ticks_to_recover']}")
            print(f"  predicates: {json.dumps(event['predicates'])}")
            if event["loss_delta_at_reset"]:
                print(f"  loss delta AT reset: "
                      f"{json.dumps(event['loss_delta_at_reset'])}")
            if event["writer_delta_at_reset"]:
                print(f"  writer delta AT reset: "
                      f"{json.dumps(event['writer_delta_at_reset'])}")
            if event["loss_delta_over_window"]:
                print(f"  loss delta over window: "
                      f"{json.dumps(event['loss_delta_over_window'])}")
            print("-" * 100)
            for tick in window(ticks, index):
                print(_fmt_tick(tick, centre))

    if args.json is not None:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps({"summary": summary, "events": events}, indent=2,
                       default=str),
            encoding="utf-8")
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
