"""Deterministic CPU benchmark for the Polymarket ingestion path.

Drives the real :class:`PolymarketMarketWS` with synthetic-but-realistic
snapshot and delta traffic, and measures where event-loop CPU actually goes.
No network, no database, no runtime: the adapter is exercised exactly as the
engine exercises it, so the numbers are attributable to the production code and
are reproducible run to run.

Why this exists: a 219-sample stack profile of the live runtime showed the V4
event-loop thread ``active+gil`` 85.8 percent of the time, with no single
hotspot -- the cost is spread across the per-message book path.  A stack profile
says *where* the interpreter was; it cannot say how much work each stage does
per message, which is what an optimization has to move.  This does.

Usage::

    python -m tools.v4_polymarket_ingest_bench --messages 4000 --depth 40

The output is a ranked per-stage attribution table plus the aggregate rates the
acceptance target is expressed in.  ``--json <path>`` writes the same numbers
in machine-readable form for before/after comparison.
"""
from __future__ import annotations

import argparse
import asyncio
import gc
import json
import statistics
import sys
import time
import tracemalloc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lite_frequency_v4.events import (  # noqa: E402
    EventGate,
    canonical_json,
    canonical_payload_hash,
)
from lite_frequency_v4.contracts import BookLevelCache  # noqa: E402
from lite_frequency_v4.polymarket_ws import PolymarketMarketWS  # noqa: E402


# --------------------------------------------------------------------------
# Deterministic clock: the adapter rejects events by age, so the benchmark must
# control time rather than race a wall clock.
# --------------------------------------------------------------------------
class _BenchClock:
    def __init__(self) -> None:
        self.ms = 1_700_000_000_000
        self.ns = 0

    def now_ms(self) -> int:
        return self.ms

    def monotonic_ns(self) -> int:
        return self.ns

    def advance(self, ms: int) -> None:
        self.ms += ms
        self.ns += ms * 1_000_000


@dataclass
class Stage:
    """One measured stage: wall time and call count, aggregated bounded."""

    name: str
    calls: int = 0
    total_s: float = 0.0
    samples: list[float] = field(default_factory=list)

    def add(self, seconds: float) -> None:
        self.calls += 1
        self.total_s += seconds
        # Bounded reservoir: the tail matters, the full series does not.
        if len(self.samples) < 20_000:
            self.samples.append(seconds)

    def summary(self, total_wall_s: float, messages: int) -> dict[str, Any]:
        ordered = sorted(self.samples)

        def pct(fraction: float) -> float:
            if not ordered:
                return 0.0
            return ordered[min(len(ordered) - 1,
                               int(round((len(ordered) - 1) * fraction)))] * 1e6

        return {
            "stage": self.name,
            "calls": self.calls,
            "calls_per_message": round(self.calls / max(1, messages), 3),
            "total_ms": round(self.total_s * 1e3, 2),
            "share_pct": round(100.0 * self.total_s / max(1e-9, total_wall_s), 2),
            "us_p50": round(pct(0.50), 2),
            "us_p90": round(pct(0.90), 2),
            "us_p99": round(pct(0.99), 2),
            "us_max": round((max(ordered) * 1e6) if ordered else 0.0, 2),
            "us_mean": round(1e6 * self.total_s / max(1, self.calls), 2),
        }


class Meter:
    """Bounded stage meter with a context-manager timer."""

    def __init__(self) -> None:
        self.stages: dict[str, Stage] = {}
        self.counters: dict[str, int] = {}

    def bump(self, name: str, amount: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + amount

    def timer(self, name: str) -> "_Timer":
        stage = self.stages.get(name)
        if stage is None:
            stage = self.stages[name] = Stage(name)
        return _Timer(stage)


class _Timer:
    __slots__ = ("_stage", "_started")

    def __init__(self, stage: Stage) -> None:
        self._stage = stage
        self._started = 0.0

    def __enter__(self) -> "_Timer":
        self._started = time.perf_counter()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stage.add(time.perf_counter() - self._started)


# --------------------------------------------------------------------------
# Traffic generation
# --------------------------------------------------------------------------
def build_snapshot(token: str, condition: str, ts_ms: int, depth: int) -> dict[str, Any]:
    """A full book snapshot with ``depth`` levels per side, never crossed."""

    bids = [
        {"price": f"0.{400 - index:03d}", "size": f"{100 + index}.0"}
        for index in range(depth)
    ]
    asks = [
        {"price": f"0.{600 + index:03d}", "size": f"{100 + index}.0"}
        for index in range(depth)
    ]
    return {
        "event_type": "book", "asset_id": token, "market": condition,
        "timestamp": str(ts_ms), "hash": f"h{ts_ms}",
        "bids": bids, "asks": asks,
    }


def build_price_change(token: str, condition: str, ts_ms: int, depth: int,
                       step: int, changes_per_message: int) -> dict[str, Any]:
    """A delta that touches levels away from the top, so the BBO is stable.

    The adapter rejects a delta whose reported best bid/ask disagrees with the
    local book, so a realistic benchmark must keep the touched levels off the
    top of book and report the unchanged BBO.
    """

    changes = []
    for offset in range(changes_per_message):
        index = 1 + ((step + offset) % max(1, depth - 1))
        side = "BUY" if (step + offset) % 2 == 0 else "SELL"
        price = (f"0.{400 - index:03d}" if side == "BUY"
                 else f"0.{600 + index:03d}")
        changes.append({
            "asset_id": token, "price": price,
            "size": f"{100 + ((step + offset) % 50)}.0", "side": side,
            "best_bid": "0.400", "best_ask": "0.600", "hash": f"d{ts_ms}",
        })
    return {
        "event_type": "price_change", "market": condition,
        "timestamp": str(ts_ms), "price_changes": changes,
    }


# --------------------------------------------------------------------------
# Benchmark
# --------------------------------------------------------------------------
async def run_bench(*, messages: int, depth: int, tokens: int,
                    changes_per_message: int, consumers: int,
                    track_alloc: bool) -> dict[str, Any]:
    meter = Meter()
    clock = _BenchClock()
    token_ids = [f"token-{i}" for i in range(tokens)]
    conditions = {token: f"condition-{i}" for i, token in enumerate(token_ids)}

    published: list[Any] = []
    level_pool = BookLevelCache()

    async def on_event(event: Any, decision: Any) -> None:
        published.append(decision.disposition)

    adapter = PolymarketMarketWS(
        conditions, clock=clock, event_gate=EventGate(),
        on_event=on_event, max_event_age_ms=60_000,
    )

    # Hydrate every token with a full snapshot first.
    for token in token_ids:
        clock.advance(1)
        snapshot = build_snapshot(token, conditions[token], clock.ms, depth)
        with meter.timer("00_snapshot_handle"):
            await adapter.handle_message(json.dumps(snapshot))
        meter.bump("snapshots")

    hydrated = len(adapter.hydrated_tokens)
    if hydrated != tokens:
        raise SystemExit(
            f"benchmark setup failed: hydrated {hydrated}/{tokens} tokens")

    if track_alloc:
        gc.collect()
        tracemalloc.start()
    gc_before = gc.get_count()
    started_wall = time.perf_counter()
    started_cpu = time.process_time()

    for step in range(messages):
        token = token_ids[step % tokens]
        clock.advance(5)
        payload = build_price_change(
            token, conditions[token], clock.ms, depth, step, changes_per_message)
        raw = json.dumps(payload)

        # -- stage: transport decode (json.loads of the frame) --------------
        with meter.timer("10_json_loads"):
            json.loads(raw)

        # -- stage: full adapter handling of the delta ----------------------
        with meter.timer("20_handle_message_total"):
            await adapter.handle_message(raw)
        meter.bump("deltas", changes_per_message)

        # -- stage: identity cost, measured standalone ----------------------
        identity_payload = {
            "market": conditions[token], "timestamp": str(clock.ms),
            "event_type": "price_change", "change_index": 0,
            "change": payload["price_changes"][0],
        }
        with meter.timer("30_canonical_json"):
            canonical_json(identity_payload)
        with meter.timer("31_canonical_payload_hash"):
            canonical_payload_hash(identity_payload)

        # -- stage: what the engine does per event, per consumer ------------
        for _ in range(consumers):
            with meter.timer("40_current_book"):
                book = adapter.current_book(token)
            meter.bump("current_book_calls")
            with meter.timer("41_booklevel_build"):
                _build_levels(book, level_pool)

    wall_s = time.perf_counter() - started_wall
    cpu_s = time.process_time() - started_cpu
    alloc_peak = 0
    if track_alloc:
        _current, alloc_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    gc_after = gc.get_count()

    total_stage_s = sum(s.total_s for s in meter.stages.values())
    rows = sorted(
        (s.summary(total_stage_s, messages) for s in meter.stages.values()),
        key=lambda r: -r["total_ms"],
    )
    return {
        "config": {
            "messages": messages, "depth": depth, "tokens": tokens,
            "changes_per_message": changes_per_message,
            "consumers_per_event": consumers,
        },
        "wall_s": round(wall_s, 3),
        "cpu_s": round(cpu_s, 3),
        "cpu_ratio": round(cpu_s / max(1e-9, wall_s), 4),
        "messages_per_s": round(messages / max(1e-9, wall_s), 1),
        "us_cpu_per_message": round(1e6 * cpu_s / max(1, messages), 2),
        "counters": dict(meter.counters),
        "gc_counts_before": list(gc_before),
        "gc_counts_after": list(gc_after),
        "tracemalloc_peak_bytes": alloc_peak,
        "stages": rows,
        "published_decisions": len(published),
        "level_pool": level_pool.stats(),
    }


def _build_levels(book: dict[str, Any], pool: Any) -> tuple[Any, Any]:
    """Mirror of engine._book_from_ws level construction, without the engine.

    Uses the same :class:`BookLevelCache` the engine uses, so the benchmark
    measures the production path rather than a lookalike.
    """

    return (pool.levels(book.get("bids", ())),
            pool.levels(book.get("asks", ())))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=3_000)
    parser.add_argument("--depth", type=int, default=40)
    parser.add_argument("--tokens", type=int, default=8)
    parser.add_argument("--changes-per-message", type=int, default=1)
    parser.add_argument(
        "--consumers", type=int, default=1,
        help="current_book()/BookLevel builds per delta, as the engine does")
    parser.add_argument("--json", type=str, default="")
    parser.add_argument("--track-alloc", action="store_true")
    parser.add_argument("--label", type=str, default="")
    args = parser.parse_args(argv)

    result = asyncio.run(run_bench(
        messages=args.messages, depth=args.depth, tokens=args.tokens,
        changes_per_message=args.changes_per_message,
        consumers=args.consumers, track_alloc=args.track_alloc,
    ))
    result["label"] = args.label

    print(f"=== Polymarket ingest benchmark {args.label} ===")
    print(f"config: {result['config']}")
    print(f"wall={result['wall_s']}s cpu={result['cpu_s']}s "
          f"cpu_ratio={result['cpu_ratio']} "
          f"msg/s={result['messages_per_s']} "
          f"cpu_us_per_message={result['us_cpu_per_message']}")
    print(f"counters: {result['counters']}")
    if result["tracemalloc_peak_bytes"]:
        print(f"tracemalloc peak: {result['tracemalloc_peak_bytes']:,} bytes")
    print()
    header = (f"{'stage':<26}{'calls':>8}{'/msg':>7}{'total_ms':>11}"
              f"{'share%':>8}{'us_p50':>9}{'us_p90':>9}{'us_p99':>9}{'us_max':>10}")
    print(header)
    print("-" * len(header))
    for row in result["stages"]:
        print(f"{row['stage']:<26}{row['calls']:>8}{row['calls_per_message']:>7}"
              f"{row['total_ms']:>11}{row['share_pct']:>8}{row['us_p50']:>9}"
              f"{row['us_p90']:>9}{row['us_p99']:>9}{row['us_max']:>10}")

    if args.json:
        Path(args.json).write_text(
            json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
