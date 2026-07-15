"""Isolated event-driven orchestration for Lite Frequency V4 shadow.

This module intentionally imports no legacy Lite or Advanced runtime code and
contains no authenticated client, signer, order, or cancellation method.  A
``CROSS_SPREAD`` action is an evidence-bound SQLite simulation of exactly five
shares.
"""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import asdict, dataclass, field
import hashlib
from itertools import islice
import json
from pathlib import Path
import time
from typing import Any, Optional
import uuid

from .books import (
    NoBookReason,
    classify_book_pair,
)
from .cex import OkxPublicProvider
from .config import FrequencyV4Config, validate_frequency_v4_config
from .contracts import (
    AnchorStatus,
    BookLevel,
    BookState,
    CexObservation,
    EntrySide,
    MarketIdentity,
    SourceEvent,
)
from .discovery import GammaMarketDiscovery, window_open_ms
from .economics import EconomicCalculation, calculate_economics
from .edge_models import DeterministicEdgeEnsemble, EnsembleResult, ModelContext
from .events import EventDecision, EventDisposition, canonical_json
from .execution import ExecutionRouter, RouterAction, RouterDecision, tier_for_edge
from .export import EXPORT_FILENAME, write_frequency_v4_dashboard
from .features import BookHistoryBuffer, CexFeatureBuffer, FeatureEvidence
from .maintenance import (
    MaintenancePolicy,
    MaintenanceSnapshot,
    run_bounded_maintenance_pass,
)
from .polymarket_ws import PolymarketMarketWS
from .positions import evaluate_exit_vs_hold
from .persistence import (
    V4PersistenceCommand,
    V4PersistenceError,
    V4PersistenceQueueFull,
    V4PersistenceTimeout,
    V4PersistenceWriter,
)
from .resolver import corroborated_resolution
from .rest import ClobPublicClient, GammaPublicClient, hydrate_market_books
from .risk import entry_idempotency_key
from .runtime import V4RuntimeFiles, immutable_safety_state, now_ms
from .store import (
    ExposureLimitExceeded,
    V4StoreError,
    WindowReservationConflict,
)
from .telemetry import V4TelemetryWriter
from .workers import V4MaintenanceWorker, V4ReadWorker, V4RuntimeIOWorker


# Cadence for the heavy, whole-database synchronous maintenance operations.
# A large database makes a full dashboard export or PRAGMA integrity_check cost
# seconds, and they run on the shared event loop; running them every ~2s (the
# cheap state/heartbeat cadence) monopolises the loop and starves the WebSocket
# heartbeat/reconnect path, causing PONG-timeout reconnect storms.  They are
# therefore spaced far apart so a single multi-second scan stays well inside the
# 10s Polymarket PONG window with the loop responsive in between.  The durable
# fix is to run them off-loop against a dedicated read-only connection.
DASHBOARD_EXPORT_INTERVAL_MS = 15_000
INTEGRITY_CHECK_INTERVAL_MS = 300_000
INTEGRITY_MAX_AGE_MS = 600_000
# Non-executable evaluation-state transitions (SKIP/NO_ACTION reason or bucket
# changes) that reverse within this window are threshold flapping around a
# price/score boundary, not decision evidence; they are suppressed and counted
# instead of persisting a full evidence bundle each tick.  Executable actions,
# safety transitions, maker decisions, management samples, and each window's
# first evaluation always persist regardless of this interval.
IMMATERIAL_TRANSITION_MIN_INTERVAL_MS = 5_000


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False, default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _outcome(side: Optional[EntrySide]) -> Optional[str]:
    if side is EntrySide.BUY_YES:
        return "YES"
    if side is EntrySide.BUY_NO:
        return "NO"
    return None


def _anchor_db(status: AnchorStatus) -> str:
    return {
        AnchorStatus.ANCHORED: "ANCHORED",
        AnchorStatus.UNANCHORED: "UNANCHORED",
        AnchorStatus.FIELD_MISSING: "ANCHOR_FIELD_MISSING",
        AnchorStatus.PARSE_FAILED: "ANCHOR_PARSE_FAILED",
        AnchorStatus.NOT_YET_PUBLISHED: "ANCHOR_NOT_YET_PUBLISHED",
    }[status]


@dataclass(slots=True)
class MarketState:
    identity: MarketIdentity
    window_id: int
    db_market_id: int
    market_identity_id: int
    books: dict[str, BookState] = field(default_factory=dict)
    book_snapshot_ids: dict[str, int] = field(default_factory=dict)
    evaluation_seq: int = 0
    last_evaluation_mono_ns: int = 0
    last_trigger: Any = None
    last_fair: Optional[EconomicCalculation] = None
    last_fair_value_id: Optional[int] = None
    last_candidate_id: Optional[int] = None
    last_decision_id: Optional[int] = None
    last_no_book_reason: str = ""
    last_reject_ts_ms: int = 0
    last_candidate_fingerprint: str = ""
    last_candidate_persist_ts_ms: int = 0


@dataclass(frozen=True, slots=True)
class EvaluationTrigger:
    source: str
    receipt_ts_ms: int
    receipt_monotonic_ns: int
    event: SourceEvent | CexObservation | None = None
    decision: EventDecision | None = None


@dataclass(slots=True)
class MakerPersistence:
    maker_observation_id: int
    initial_candidate_id: int
    initial_fair_value_id: int
    update_seq: int = 0


class FrequencyV4Engine:
    """One process, one nonce, one isolated v4 store, and exact window owners."""

    def __init__(
        self, cfg: FrequencyV4Config, runtime: V4RuntimeFiles,
    ) -> None:
        validate_frequency_v4_config(cfg)
        self.cfg = cfg
        self.runtime = runtime
        self.session_id = uuid.uuid4().hex
        self.config_hash = _sha256_json(asdict(cfg))
        self._db_path_resolved = str(Path(cfg.db_path).resolve())
        self._runtime_dir_resolved = str(Path(cfg.runtime_dir).resolve())
        self._export_path_resolved = str(
            (Path(cfg.export_dir) / EXPORT_FILENAME).resolve())
        self.gamma = GammaPublicClient(cfg.gamma_base_url, timeout_s=cfg.rest_timeout_s)
        self.clob = ClobPublicClient(cfg.clob_base_url, timeout_s=cfg.rest_timeout_s)
        self.discovery = GammaMarketDiscovery(self.gamma.get_markets, cfg)
        self.cex_features = CexFeatureBuffer()
        self.book_history = BookHistoryBuffer()
        self.ensemble = DeterministicEdgeEnsemble(
            probability_floor=cfg.fair_probability_floor,
            probability_ceiling=cfg.fair_probability_ceiling,
            max_adjustment=cfg.max_model_adjustment,
        )
        self.router = ExecutionRouter(cfg)
        self.poly_ws = PolymarketMarketWS(
            url=cfg.clob_ws_url,
            on_event=self._on_polymarket_event,
            on_health=self._on_source_health,
            on_hydration_request=self._on_book_hydration_request,
            max_event_age_ms=cfg.book_max_age_ms,
        )
        self.okx = OkxPublicProvider(
            cfg.required_assets, ws_url=cfg.okx_ws_url,
            on_observation=self._on_cex_observation,
            on_health=self._on_source_health,
            on_hydration=self._on_cex_hydration,
            max_event_age_ms=cfg.cex_max_age_ms,
        )
        self.markets: dict[str, MarketState] = {}
        self.token_to_window: dict[str, str] = {}
        self.pending: dict[str, EvaluationTrigger] = {}
        self.pending_event = asyncio.Event()
        self._polymarket_ingest_queue: asyncio.Queue[
            tuple[SourceEvent, EventDecision]
        ] = asyncio.Queue(maxsize=cfg.writer_queue_max)
        # The CEX/OKX ingestion path is decoupled from its WebSocket receive
        # loop by the same bounded-queue + separate-consumer pattern used for
        # Polymarket, so a synchronous SQLite write can never stall the shared
        # event loop (and thus never stall reconnect/heartbeat on either
        # source).  The enqueued monotonic timestamp powers consumer latency.
        self._cex_ingest_queue: asyncio.Queue[
            tuple[CexObservation, EventDecision, int]
        ] = asyncio.Queue(maxsize=cfg.cex_writer_queue_max)
        self._polymarket_queue_high_water = 0
        self._cex_queue_high_water = 0
        self._cex_ingest_latency_ms = 0.0
        self._cex_ingest_latency_max_ms = 0.0
        self._cex_last_enqueue_mono_ns = 0
        self._cex_last_overflow_mono_ns = 0
        self._latest_source_health: dict[str, dict[str, Any]] = {}
        self._loop_lag_ms = 0.0
        self._loop_lag_max_ms = 0.0
        self._loop_lag_p95_ms = 0.0
        self._loop_lag_breaches = 0
        self._loop_lag_last_breach_ms = 0
        self._loop_lag_safety_breach_mono_ns = 0
        self._loop_lag_samples: deque[float] = deque(maxlen=256)
        # Heavy whole-database reporting/maintenance runs off the event loop in
        # bounded single-flight workers against a dedicated read-only (reads) or
        # chunked (writes) path, so a multi-second scan can never stall the
        # WebSocket heartbeat/reconnect path.
        self._export_inflight = False
        self._integrity_inflight = False
        self._maintenance_inflight = False
        self._last_export_duration_ms = 0.0
        self._last_export_ok = True
        self._export_runs = 0
        self._last_integrity_duration_ms = 0.0
        # UNKNOWN is fail-closed until the first dedicated read-worker check.
        self._last_integrity_ok: Optional[bool] = None
        self._last_integrity_ts_ms = 0
        self._integrity_runs = 0
        self._last_maintenance_duration_ms = 0.0
        self._last_maintenance_rows = 0
        self._maintenance_runs = 0
        self._last_published_state: dict[str, Any] = {}
        self._shutdown_drain_timed_out = False
        self._telemetry_shutdown_ok: Optional[bool] = None
        self._polymarket_queue_discarded = 0
        self._cex_queue_discarded = 0
        self._event_count_buffer: dict[
            tuple[int, str, str, str, str, str], list[int]
        ] = {}
        self.maker_persistence: dict[str, MakerPersistence] = {}
        self._recovery_inflight: set[str] = set()
        self._hydration_inflight: set[str] = set()
        self._source_event_ids: dict[str, int] = {}
        self._cex_observation_ids: dict[str, int] = {}
        self._last_raw_persist_ns: dict[str, int] = {}
        self._last_health_persist_ms: dict[str, int] = {}
        self._active_subscription_map: dict[str, str] = {}
        self._active_subscription_windows: frozenset[str] = frozenset()
        self._last_reject_keys: dict[str, int] = {}
        self._tasks: list[asyncio.Task[Any]] = []
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._stopping = asyncio.Event()
        self._started = False
        self._stop_complete = False
        self._last_error = ""
        self._ignored_durations: dict[str, int] = {}
        self._last_discovery_ms = 0
        self._last_event_ms = 0
        self._last_export_ms = 0
        self._last_integrity: dict[str, Any] = {}
        self._last_rest_status = "NOT_RUN"
        self._poly_epoch = 0
        self._poly_connected = False
        self._okx_epoch = 0
        self._okx_connected = False
        # Persistence/reporting/runtime I/O workers are started explicitly in
        # ``start``.  Hot runtime-state publication reads only these in-memory
        # snapshots; it never performs SQLite or filesystem I/O itself.
        self.persistence: Any = None
        self.telemetry: Any = None
        # Operational lifecycle reads and heavyweight reporting have separate
        # read-only connections/queues.  A full export or integrity scan must
        # never strand position management behind a long FIFO report.
        self.read_worker: Any = None
        self.report_worker: Any = None
        self.maintenance_worker: Any = None
        self.runtime_io_worker: Any = None
        self._open_positions_count = 0
        self._entered_window_ids: set[int] = set()
        self._open_position_windows: set[int] = set()
        self._expected_window_keys: set[tuple[str, int]] = set()
        self._position_cache: dict[int, dict[str, Any]] = {}
        self._management_last_ts: dict[int, int] = {}
        # Authoritative capacity rejections (exposure/position caps) latch here
        # so saturated capacity cannot drive a full-rate retry storm of
        # journaled entry commands.  Capacity only frees when a position
        # closes, so every block is cleared exactly there (and on startup
        # reconciliation).  Keys: "global" or "asset:<ASSET>".
        self._entry_capacity_blocks: dict[str, str] = {}
        self._management_seq: dict[int, int] = {}
        self._resolution_state: dict[int, tuple[int, int]] = {}
        self._process_ownership_cache: dict[str, Any] = {
            "process_ownership_valid": False,
            "exact_v4_processes": 0,
            "owned_v4_processes": 0,
            "orphan_processes": 0,
        }
        self._database_size_cache = 0
        self._wal_size_cache = 0
        self._checkpoint_state: dict[str, Any] = {}
        self._maintenance_result: dict[str, Any] = {}
        self._critical_command_sequence = 0
        self._critical_failure_reason = ""
        self.counters: dict[str, int] = {
            "raw_events": 0,
            "accepted_events": 0,
            "rejected_events": 0,
            "coalesced_triggers": 0,
            "evaluations": 0,
            "positive_edge_evaluations": 0,
            "entries": 0,
            "maker_started": 0,
            "maker_updates": 0,
            "rest_recoveries": 0,
            "rest_recovery_failures": 0,
            "resolution_attempts": 0,
            "polymarket_ingest_overflow": 0,
            "cex_ingest_overflow": 0,
            "cex_ingest_admitted": 0,
            "cex_ingest_rejected": 0,
            "telemetry_event_bucket_overflow": 0,
        }

    @property
    def export_path(self) -> Path:
        return Path(self.cfg.export_dir) / EXPORT_FILENAME

    def _runtime_state(self, state: str = "RUNNING") -> dict[str, Any]:
        active = [market for market in self.markets.values()
                  if market.identity.window_open_ms <= now_ms() < market.identity.window_close_ms]
        ownership = dict(self._process_ownership_cache)
        critical = self._writer_health()
        critical["engine_latched_failure_reason"] = (
            self._critical_failure_reason or None
        )
        telemetry = (
            self.telemetry.snapshot() if self.telemetry is not None else {
                "health": "NOT_STARTED", "queue_depth": 0,
                "queue_capacity": self.cfg.telemetry_queue_capacity,
            }
        )
        critical_evidence_incomplete = (
            int(critical.get("unconfirmed_command_count") or 0)
            + int(critical.get("ambiguous_command_count") or 0)
            + int(critical.get("timeout_count") or 0)
        )
        ingress_evidence_loss = (
            int(self.counters.get("polymarket_ingest_overflow") or 0)
            + int(self.counters.get("cex_ingest_overflow") or 0)
            + int(self._polymarket_queue_discarded)
            + int(self._cex_queue_discarded)
        )
        raw_telemetry_loss = (
            int(telemetry.get("dropped") or 0)
            + int(telemetry.get("failed_batches") or 0)
            + int(self.counters.get("telemetry_event_bucket_overflow") or 0)
            + ingress_evidence_loss
        )
        # Stable dashboard aliases keep the external contract independent of
        # the worker implementation's internal counter names.
        telemetry_view = {
            **telemetry,
            "state": telemetry.get("state", telemetry.get("health", "UNKNOWN")),
            "rows_submitted": int(telemetry.get("submitted") or 0),
            "rows_written": int(telemetry.get("written") or 0),
            "rows_coalesced": int(telemetry.get("coalesced") or 0),
            "rows_dropped": int(telemetry.get("dropped") or 0),
            # Raw telemetry is intentionally lossy under pressure.  Complete
            # trade/candidate evidence travels through the critical atomic
            # bundle, so these two concepts must never be conflated.
            "raw_telemetry_loss_count": raw_telemetry_loss,
            "ingress_evidence_loss_count": ingress_evidence_loss,
            "critical_evidence_incomplete_count": critical_evidence_incomplete,
            "incomplete_evidence_count": critical_evidence_incomplete,
        }
        return {
            **immutable_safety_state(),
            "session_id": self.session_id,
            "state": state,
            "config_hash": self.config_hash,
            "db_path": self._db_path_resolved,
            "runtime_dir": self._runtime_dir_resolved,
            "export_path": self._export_path_resolved,
            **ownership,
            "markets_discovered": len(self.markets),
            "active_markets": len(active),
            "active_assets": sorted({row.identity.asset for row in active}),
            "ignored_duration_counts": dict(self._ignored_durations),
            "last_discovery_ts_ms": self._last_discovery_ms,
            "last_event_ts_ms": self._last_event_ms,
            "last_export_ts_ms": self._last_export_ms,
            "rest_recovery_status": self._last_rest_status,
            "polymarket_ingest_queue_depth": self._polymarket_ingest_queue.qsize(),
            "polymarket_ingest_queue_capacity": self.cfg.writer_queue_max,
            "polymarket_ingest_queue_high_water": self._polymarket_queue_high_water,
            "cex_ingest_queue_depth": self._cex_ingest_queue.qsize(),
            "cex_ingest_queue_capacity": self.cfg.cex_writer_queue_max,
            "cex_ingest_queue_high_water": self._cex_queue_high_water,
            "cex_ingest_latency_ms": round(self._cex_ingest_latency_ms, 3),
            "cex_ingest_latency_max_ms": round(self._cex_ingest_latency_max_ms, 3),
            "cex_ingest_oldest_age_ms": self._cex_oldest_pending_age_ms(),
            "cex_data_health": self._cex_data_health(now_ms()),
            "loop_lag_ms": round(self._loop_lag_ms, 3),
            "loop_lag_max_ms": round(self._loop_lag_max_ms, 3),
            "loop_lag_p95_ms": round(self._loop_lag_p95_ms, 3),
            "loop_lag_threshold_ms": self.cfg.loop_lag_threshold_ms,
            "loop_lag_safety_ms": self.cfg.loop_lag_safety_ms,
            "loop_lag_breaches": self._loop_lag_breaches,
            "loop_lag_last_breach_ts_ms": self._loop_lag_last_breach_ms,
            "execution_blocked_reason": self._execution_blocked_reason(),
            "dashboard_export_ms": round(self._last_export_duration_ms, 1),
            "dashboard_export_runs": self._export_runs,
            "dashboard_export_ok": self._last_export_ok,
            "integrity_check_ms": round(self._last_integrity_duration_ms, 1),
            "integrity_check_runs": self._integrity_runs,
            "integrity_ok": self._last_integrity_ok,
            "integrity_checked_ts_ms": self._last_integrity_ts_ms or None,
            "integrity_max_age_ms": INTEGRITY_MAX_AGE_MS,
            "maintenance_ms": round(self._last_maintenance_duration_ms, 1),
            "maintenance_runs": self._maintenance_runs,
            "maintenance_rows_last": self._last_maintenance_rows,
            "persistence": {
                "critical": critical,
                "telemetry": telemetry_view,
                "operational_reads": (
                    self.read_worker.health() if self.read_worker is not None
                    else {"state": "NOT_STARTED"}
                ),
                "reporting": (
                    self.report_worker.health() if self.report_worker is not None
                    else {"state": "NOT_STARTED"}
                ),
                "maintenance": (
                    self.maintenance_worker.health()
                    if self.maintenance_worker is not None
                    else {"state": "NOT_STARTED"}
                ),
                "runtime_io": (
                    self.runtime_io_worker.health()
                    if self.runtime_io_worker is not None
                    else {"state": "NOT_STARTED"}
                ),
                "latest_checkpoint": dict(self._checkpoint_state),
                "latest_maintenance": dict(self._maintenance_result),
                "db_size_bytes": self._database_size_cache,
                "wal_size_bytes": self._wal_size_cache,
            },
            "buffered_event_counter_buckets": len(self._event_count_buffer),
            "shutdown_drain_timed_out": self._shutdown_drain_timed_out,
            "polymarket_ingest_queue_discarded": self._polymarket_queue_discarded,
            "cex_ingest_queue_discarded": self._cex_queue_discarded,
            "telemetry_shutdown_ok": self._telemetry_shutdown_ok,
            "polymarket_ws": self.poly_ws.health(),
            "okx_ws": self.okx.health,
            "counters": dict(self.counters),
            "open_positions": int(self._open_positions_count),
            "last_error": self._last_error,
            "integrity": self._last_integrity,
        }

    def _cex_oldest_pending_age_ms(self) -> float:
        """Age of the oldest CEX evidence still waiting to be persisted."""

        if self._cex_ingest_queue.empty() or not self._cex_last_enqueue_mono_ns:
            return 0.0
        # A non-empty queue means the consumer is behind the producer; the last
        # observed enqueue-to-dequeue latency is the honest lower bound on how
        # long the front-of-queue item has already waited.
        return round(self._cex_ingest_latency_ms, 3)

    async def _refresh_runtime_probe(self) -> None:
        """Refresh ownership and file sizes on the dedicated runtime worker."""

        if self.runtime_io_worker is None:
            raise RuntimeError("runtime I/O worker is not started")
        try:
            ownership, db_size, wal_size = await self.runtime_io_worker.run_io(
                lambda: (
                    self.runtime.process_ownership(),
                    Path(self.cfg.db_path).stat().st_size
                    if Path(self.cfg.db_path).exists() else 0,
                    Path(f"{self.cfg.db_path}-wal").stat().st_size
                    if Path(f"{self.cfg.db_path}-wal").exists() else 0,
                ),
                timeout_s=10.0,
                name="runtime_ownership_and_database_sizes",
            )
        except Exception:
            self._process_ownership_cache = {
                "process_ownership_valid": False,
                "exact_v4_processes": 0,
                "owned_v4_processes": 0,
                "orphan_processes": 0,
                "probe_failed": True,
            }
            raise
        self._process_ownership_cache = dict(ownership)
        self._database_size_cache = int(db_size)
        self._wal_size_cache = int(wal_size)

    def _cex_data_health(self, current: int) -> str:
        """Freshness-aware CEX health that a bare socket flag cannot fake.

        A connected OKX socket that has stopped delivering fresh evidence must
        never be reported healthy; the enumerated states make the difference
        between "connected" and "usable evidence present" observable.
        """

        if not self._okx_connected:
            return "DISCONNECTED"
        if (self._cex_last_overflow_mono_ns
                and time.monotonic_ns() - self._cex_last_overflow_mono_ns
                < 5_000_000_000):
            return "OVERFLOW"
        capacity = max(1, self.cfg.cex_writer_queue_max)
        if self._cex_ingest_queue.qsize() >= capacity // 2:
            return "BACKLOGGED"
        active_assets = {
            state.identity.asset for state in self.markets.values()
            if state.identity.window_open_ms <= int(current)
            < state.identity.window_close_ms
        }
        if not active_assets:
            # No executable window right now: connected and receiving frames but
            # nothing to prove freshness against.
            latest_any = any(
                self.cex_features.latest(asset) is not None
                for asset in self.okx.assets)
            return "RECEIVING" if latest_any else "CONNECTED"
        fresh_assets = {
            asset for asset in active_assets
            if (latest := self.cex_features.latest(asset)) is not None
            and 0 <= int(current) - latest.provider_ts_ms <= self.cfg.cex_max_age_ms
        }
        if not fresh_assets:
            return "STALE"
        if fresh_assets != active_assets:
            return "DEGRADED"
        return "FRESH"

    def _writer_health(self) -> dict[str, Any]:
        if self.persistence is None:
            return {
                "state": "NOT_STARTED",
                "queue_depth": 0,
                "queue_capacity": self.cfg.critical_queue_capacity,
                "unconfirmed_command_count": 0,
            }
        health = getattr(self.persistence, "health", None)
        if callable(health):
            try:
                result = health(stale_after_ms=self.cfg.writer_failure_timeout_ms)
            except TypeError:
                result = health()
        else:
            result = self.persistence.metrics()
        result = dict(result)
        if result.get("state") == "RUNNING":
            result["state"] = "HEALTHY"
        for prefix in ("commit", "queue", "ack"):
            summary = result.get(f"{prefix}_latency_ms")
            if isinstance(summary, dict):
                for statistic in ("avg", "p50", "p95", "max"):
                    result.setdefault(
                        f"{prefix}_latency_{statistic}_ms",
                        float(summary.get(statistic) or 0.0),
                    )
        result.setdefault(
            "transactions_per_minute",
            float(result.get("transaction_rate_per_min") or 0.0),
        )
        return result

    def _next_critical_command_id(self, method: str) -> str:
        self._critical_command_sequence += 1
        return (
            f"{self.session_id}:{self._critical_command_sequence:012d}:"
            f"{str(method).replace('_', '-')[:80]}"
        )

    async def _critical_execute(
        self, method: str, *args: Any,
        ordering_key: str = "global",
        idempotency_key: Optional[str] = None,
        command_type: str = "STORE_CALL",
        priority: int = 50,
        terminal: bool = False,
        associated_asset: Optional[str] = None,
        associated_window_id: Optional[int] = None,
        associated_trade_id: Optional[int] = None,
        expected_store_errors: tuple[str, ...] = (),
        **kwargs: Any,
    ) -> Any:
        """Await one durable commit without running SQLite on the event loop."""

        if self.persistence is None:
            self._critical_failure_reason = "critical_writer_not_started"
            raise V4PersistenceError(self._critical_failure_reason)
        command = V4PersistenceCommand(
            command_id=self._next_critical_command_id(method),
            method=method,
            args=tuple(args),
            kwargs=kwargs,
            ordering_key=ordering_key,
            command_type=command_type,
            idempotency_key=idempotency_key,
            priority=priority,
            terminal=terminal,
            associated_asset=associated_asset,
            associated_window_id=associated_window_id,
            associated_trade_id=associated_trade_id,
        )
        try:
            result = await self.persistence.execute(
                command, timeout_s=self.cfg.critical_command_timeout_s)
            return result
        except (V4PersistenceQueueFull, V4PersistenceTimeout) as exc:
            self._critical_failure_reason = type(exc).__name__
            self._last_error = (
                f"critical_persistence:{type(exc).__name__}:{exc}"
            )[:240]
            raise
        except (WindowReservationConflict, ExposureLimitExceeded) as exc:
            # Deterministic business-invariant rejection is not a writer
            # outage. The caller records the precise risk/economic reason.
            self._last_error = (
                f"critical_rejected:{type(exc).__name__}:{exc}"
            )[:240]
            raise
        except V4StoreError as exc:
            if str(exc) in expected_store_errors:
                self._last_error = (
                    f"critical_rejected:{type(exc).__name__}:{exc}"
                )[:240]
                raise
            self._critical_failure_reason = type(exc).__name__
            self._last_error = (
                f"critical_persistence:{type(exc).__name__}:{exc}"
            )[:240]
            raise
        except Exception as exc:
            # Latch ambiguous/unexpected acknowledgement failures until a
            # process restart reconciles the durable journal. A later unrelated
            # success must never make an unknown commit executable again.
            self._critical_failure_reason = type(exc).__name__
            self._last_error = (
                f"critical_persistence:{type(exc).__name__}:{exc}"
            )[:240]
            raise

    async def _record_session(self) -> None:
        await self._critical_execute("record_runtime_session", {
            "session_id": self.session_id,
            "launch_nonce": self.runtime.launch_nonce,
            "pid": self.runtime.pid,
            "git_commit": self.runtime.commit,
            "config_hash": self.config_hash,
            "started_ts_ms": self.runtime.started_ts_ms,
        }, idempotency_key=f"session-start:{self.session_id}")

    def _stream_key(self, event: SourceEvent | CexObservation) -> str:
        if isinstance(event, CexObservation):
            return f"{event.provider}:{event.event_type}:{event.instrument}"
        identity = event.token_id or event.condition_id or "global"
        return f"{event.source}:{event.channel}:{event.event_type}:{identity}"

    def _buffer_event_count(
        self, event: SourceEvent | CexObservation, decision: EventDecision,
        *, classification: Optional[str] = None,
    ) -> None:
        source = event.source if isinstance(event, SourceEvent) else event.provider
        bucket_start = int(event.receipt_ts_ms) // 1_000 * 1_000
        disposition = str(classification or decision.disposition.value)
        key = (
            bucket_start, str(source), self._stream_key(event),
            str(getattr(event, "asset", "") or ""),
            str(getattr(event, "event_type", "unknown")), disposition,
        )
        if (key not in self._event_count_buffer
                and len(self._event_count_buffer) >= self.cfg.telemetry_queue_capacity):
            self._event_count_buffer.pop(next(iter(self._event_count_buffer)))
            self.counters["telemetry_event_bucket_overflow"] += 1
        counts = self._event_count_buffer.setdefault(key, [0, 0, 0, 0])
        counts[0] += 1
        counts[1] += int(not decision.duplicate)
        counts[2] += int(decision.duplicate)
        counts[3] += int(not decision.accepted or classification is not None)

    def _telemetry_submit(
        self, method: str, *args: Any, kwargs: Optional[dict[str, Any]] = None,
        **policy: Any,
    ) -> bool:
        """Non-blocking admission to the lossy, observable telemetry lane."""

        if self.telemetry is None:
            self._last_error = "telemetry_not_started"
            return False
        disposition = self.telemetry.submit(
            method, *args, kwargs=kwargs or {}, **policy)
        return str(getattr(disposition, "value", disposition)) != "DROPPED"

    def _update_window_funnel(
        self, window_id: int, current: int, **changes: Any,
    ) -> None:
        self._telemetry_submit(
            "update_window_funnel", int(window_id), int(current),
            kwargs=changes,
            state_key=("window-funnel", int(window_id), tuple(sorted(changes))),
            state_value=changes,
        )

    def _flush_event_counts(self) -> bool:
        if not self._event_count_buffer:
            return True
        # Bound hot-loop aggregation work even after a prolonged telemetry
        # outage; remaining buckets stay queued for the next heartbeat.
        limit = max(1, min(self.cfg.telemetry_batch_size, 1_024))
        keys = list(islice(self._event_count_buffer, limit))
        buffered = {
            key: self._event_count_buffer.pop(key)
            for key in keys
        }
        rows = [
            {
                "receipt_ts_ms": key[0],
                "source": key[1],
                "channel": key[2],
                "asset": key[3],
                "event_type": key[4],
                "classification": key[5],
                "raw_count": counts[0],
                "unique_count": counts[1],
                "duplicate_count": counts[2],
                "invalid_count": counts[3],
            }
            for key, counts in buffered.items()
        ]
        try:
            if not self._telemetry_submit(
                    "record_event_count_batch", rows,
                    dedupe_key=("event-count-flush", _sha256_json(rows))):
                raise RuntimeError("telemetry_event_count_admission_failed")
            return True
        except Exception as exc:
            for key, counts in buffered.items():
                target = self._event_count_buffer.setdefault(key, [0, 0, 0, 0])
                for index, count in enumerate(counts):
                    target[index] += count
            self._last_error = (
                f"telemetry_event_count_flush:{type(exc).__name__}:{exc}"
            )[:240]
            return False

    def _should_persist_raw(self, event: SourceEvent | CexObservation,
                            decision: EventDecision) -> bool:
        key = self._stream_key(event)
        current = int(getattr(event, "receipt_monotonic_ns", 0))
        prior = self._last_raw_persist_ns.get(key, 0)
        interval = 250_000_000 if decision.accepted else 1_000_000_000
        if current - prior >= interval:
            self._last_raw_persist_ns[key] = current
            return True
        return False

    def _persist_source_event(self, event: SourceEvent | CexObservation,
                              decision: EventDecision, *, force: bool = False) -> int:
        """Queue non-critical raw evidence; critical bundles persist exact rows.

        Returning zero is intentional: lossy telemetry IDs may never be used as
        authoritative trade evidence.  A material evaluation supplies the full
        source row to the acknowledged critical bundle and receives committed
        IDs from that transaction.
        """
        event_id = (event.event_key if isinstance(event, SourceEvent) else event.event_id)
        if not force and not self._should_persist_raw(event, decision):
            return 0
        wrapper = self._source_event_wrapper(event, decision)
        row = wrapper["value"]
        kwargs = wrapper["kwargs"]
        # Raw/unique/duplicate bucket admission is already known from the
        # transport EventDecision and buffered separately.  This row is only
        # retained raw evidence; SQLite write ordering must not reclassify it.
        kwargs["count_in_bucket"] = False
        kwargs["admitted_at_receipt"] = bool(decision.accepted)
        kwargs["reference_only"] = True
        admitted = self._telemetry_submit(
            "record_source_event", row,
            kwargs=kwargs,
            dedupe_key=("source-event", self.session_id, event_id),
        )
        return 0

    def _source_event_wrapper(
        self, event: SourceEvent | CexObservation, decision: EventDecision,
    ) -> dict[str, Any]:
        if isinstance(event, SourceEvent):
            row = event.to_dict()
            row["channel"] = self._stream_key(event)
            row["market_identity_id"] = self._market_identity_for_token(event.token_id)
            row["accepted"] = int(decision.accepted)
            row["classification"] = decision.disposition.value
            row["invalid_reason"] = decision.reason or None
        else:
            payload = {
                "provider": event.provider, "asset": event.asset,
                "instrument": event.instrument, "event_type": event.event_type,
                "price": event.price, "bid": event.bid, "ask": event.ask,
                "size": event.size, "side": event.side,
                "provider_ts_ms": event.provider_ts_ms,
                "receipt_ts_ms": event.receipt_ts_ms,
                "sequence": event.sequence,
            }
            row = {
                "source": event.provider,
                "channel": self._stream_key(event),
                "event_type": event.event_type,
                "asset": event.asset,
                "event_key": event.event_id,
                "payload_hash": _sha256_json(payload),
                "payload_json": canonical_json(payload),
                "provider_ts_ms": event.provider_ts_ms,
                "receipt_ts_ms": event.receipt_ts_ms,
                "monotonic_ns": event.receipt_monotonic_ns,
                "sequence_no": event.sequence,
                "connection_epoch": event.connection_epoch,
                "accepted": int(decision.accepted),
                "classification": decision.disposition.value,
                "invalid_reason": decision.reason or None,
            }
        return {
            "value": row,
            "kwargs": {
                "session_id": self.session_id,
                "now_ms": event.receipt_ts_ms,
                "future_tolerance_ms": 0,
                "sequence_contiguous": False,
                "count_in_bucket": False,
                "admitted_at_receipt": bool(decision.accepted),
            },
        }

    def _market_identity_for_token(self, token_id: str) -> Optional[int]:
        key = self.token_to_window.get(str(token_id))
        state = self.markets.get(key or "")
        return state.market_identity_id if state is not None else None

    def _persist_cex_observation(self, observation: CexObservation,
                                 decision: EventDecision, *, force: bool = False) -> int:
        source_id = self._persist_source_event(observation, decision, force=force)
        row = observation.to_dict()
        row.update({
            "source_event_id": source_id or None,
            "fresh": int(decision.accepted),
            "invalid_reason": None if decision.accepted else decision.reason,
        })
        self._telemetry_submit(
            "record_cex_observation", row,
            kwargs={"session_id": self.session_id},
            dedupe_key=("cex-observation", self.session_id,
                        observation.event_id),
        )
        return 0

    async def _on_source_health(self, health: dict[str, Any]) -> None:
        """In-memory reconnect-safety only; persistence runs off the callback.

        Invoked directly from a source WebSocket task, so it must never perform
        a synchronous SQLite write.  Connection-scoped evidence is invalidated
        here and the latest snapshot is cached for the heartbeat/export task to
        persist, keeping the receive/heartbeat path free of blocking I/O.
        """

        source = str(health.get("source") or "unknown")
        if source == "polymarket":
            epoch = int(health.get("connection_epoch") or 0)
            connected = bool(health.get("connected"))
            if epoch > self._poly_epoch or (self._poly_connected and not connected):
                # Adapter hydration is connection-scoped.  Do not allow a CEX
                # event to reuse an engine-held pre-reconnect book while the
                # new socket is still hydrating.
                for state in self.markets.values():
                    state.books.clear()
                self._poly_epoch = max(self._poly_epoch, epoch)
            self._poly_connected = connected
        elif source == "okx":
            epoch = int(health.get("connection_epoch") or 0)
            connected = bool(health.get("connected"))
            if epoch > self._okx_epoch or (self._okx_connected and not connected):
                self.cex_features.clear()
                self._okx_epoch = max(self._okx_epoch, epoch)
            self._okx_connected = connected
        self._latest_source_health[source] = dict(health)

    def _persist_pending_source_health(self, current: int) -> None:
        """Persist cached source-health snapshots off the WebSocket path."""

        for source in ("polymarket", "okx"):
            health = self._latest_source_health.get(source)
            if health is None:
                continue
            prior = self._last_health_persist_ms.get(source, 0)
            if current - prior < 2_000:
                continue
            self._last_health_persist_ms[source] = current
            self._record_source_health(source, health, current)

    def _record_source_health(self, source: str, health: dict[str, Any],
                              current: int) -> None:
        counts = health.get("disposition_counts") or {}
        last_provider = int(health.get("last_data_provider_ts_ms") or 0)
        last_receipt = int(health.get("last_data_receipt_ts_ms") or 0)
        pong = int(health.get("last_pong_receipt_ts_ms") or 0)
        desired = int(health.get("desired_subscriptions") or 0)
        hydrated = int(health.get("hydrated_subscriptions") or 0)
        if source == "okx":
            hydration_ok = bool(
                str(health.get("state")) == "READY"
                and int(health.get("acknowledged_subscriptions") or 0) >= desired
                and len(health.get("hydrated_assets") or ()) >= len(health.get("assets") or ())
            )
        else:
            hydration_ok = bool(desired > 0 and hydrated >= desired)
        row = {
            "session_id": self.session_id,
            "source": source,
            "channel": "market" if source == "polymarket" else "public",
            "sample_ts_ms": current,
            "status": str(health.get("state") or "UNKNOWN"),
            "connected": int(bool(health.get("connected"))),
            "hydrated": int(hydration_ok),
            "heartbeat_age_ms": max(0, current - pong) if pong else None,
            "ping_age_ms": max(
                0, current - int(health.get("last_heartbeat_sent_ts_ms") or 0)
            ) if health.get("last_heartbeat_sent_ts_ms") else None,
            "last_provider_ts_ms": last_provider or None,
            "last_receipt_ts_ms": last_receipt or None,
            "freshness_ms": max(0, current - last_provider) if last_provider else None,
            "reconnect_count": int(health.get("reconnect_count") or 0),
            "sequence_gap_count": int(counts.get("REJECT_SEQUENCE") or 0),
            "duplicate_count": int(health.get("duplicate_events") or 0),
            "future_count": int(counts.get("REJECT_FUTURE") or 0),
            "regressed_count": int(counts.get("REJECT_TIMESTAMP_REGRESSION") or 0),
            "rest_recovery_status": self._last_rest_status,
            "last_error": str(health.get("last_error") or "")[:240] or None,
        }
        self._telemetry_submit(
            "record_source_health", row,
            state_key=("source-health", source),
            state_value={
                "status": row["status"], "connected": row["connected"],
                "hydrated": row["hydrated"],
                "reconnect_count": row["reconnect_count"],
                "last_error": row["last_error"],
                "freshness_band": (
                    "STALE" if row["freshness_ms"] is None
                    or int(row["freshness_ms"]) > self.cfg.cex_max_age_ms
                    else "FRESH"
                ),
            },
        )

    async def _on_cex_hydration(self, _asset: str,
                                _decision: Optional[EventDecision]) -> None:
        # Hydration observations are delivered through on_observation too; this
        # callback records transport status only and never creates evidence.
        self._last_rest_status = (
            "OKX_REST_ACCEPTED" if _decision is not None and _decision.accepted
            else "OKX_REST_FAIL_CLOSED"
        )

    async def _on_cex_observation(self, observation: Optional[CexObservation],
                                  decision: EventDecision) -> None:
        """Non-blocking adapter callback; CEX persistence lives in a bounded worker."""

        self.counters["raw_events"] += 1
        if observation is None:
            self.counters["rejected_events"] += 1
            return
        self._last_event_ms = max(self._last_event_ms, observation.receipt_ts_ms)
        if not decision.accepted:
            self.counters["rejected_events"] += 1
            self.counters["cex_ingest_rejected"] += 1
            # Rejected/stale CEX evidence is auditable in aggregate without a
            # synchronous DB write on the WebSocket receive path.
            self._buffer_event_count(observation, decision)
            return
        self.counters["accepted_events"] += 1
        try:
            self._cex_ingest_queue.put_nowait(
                (observation, decision, time.monotonic_ns()))
        except asyncio.QueueFull:
            self.counters["cex_ingest_overflow"] += 1
            self._cex_last_overflow_mono_ns = time.monotonic_ns()
            self._last_error = "cex_ingest_queue_overflow_fail_closed"
            self._buffer_event_count(
                observation, decision, classification="INGEST_QUEUE_OVERFLOW")
            # Fail closed: drop this asset's executable feature history so a
            # later evaluation cannot act on possibly-inconsistent evidence
            # until fresh admitted ticks rebuild it.
            self.cex_features.invalidate(observation.asset)
        else:
            self._cex_last_enqueue_mono_ns = time.monotonic_ns()
            self._cex_queue_high_water = max(
                self._cex_queue_high_water, self._cex_ingest_queue.qsize())

    def _note_cex_latency(self, enqueued_mono: int) -> None:
        latency_ms = max(
            0.0, (time.monotonic_ns() - int(enqueued_mono)) / 1_000_000.0)
        self._cex_ingest_latency_ms = latency_ms
        self._cex_ingest_latency_max_ms = max(
            self._cex_ingest_latency_max_ms, latency_ms)

    async def _process_cex_observation(self, observation: CexObservation,
                                       decision: EventDecision) -> None:
        self._buffer_event_count(observation, decision)
        self._persist_cex_observation(observation, decision, force=False)
        self.counters["cex_ingest_admitted"] += 1
        if observation.connection_epoch < self._okx_epoch:
            # A reconnect superseded this observation while it waited in the
            # queue; it is audited above but must never re-enter the executable
            # feature buffer that the reconnect already cleared.
            return
        # Feature state advances only after successful admission/persistence.
        self.cex_features.append(observation)
        trigger = EvaluationTrigger(
            source="okx", receipt_ts_ms=observation.receipt_ts_ms,
            receipt_monotonic_ns=observation.receipt_monotonic_ns,
            event=observation, decision=decision,
        )
        for key, state in tuple(self.markets.items()):
            market = state.identity
            if market.asset == observation.asset and (
                    market.window_open_ms <= observation.receipt_ts_ms < market.window_close_ms):
                self._schedule(key, trigger)

    async def _drain_cex_ingest_once(self) -> None:
        observation, decision, enqueued_mono = await self._cex_ingest_queue.get()
        self._note_cex_latency(enqueued_mono)
        try:
            await self._process_cex_observation(observation, decision)
        finally:
            self._cex_ingest_queue.task_done()

    async def _cex_ingest_loop(self) -> None:
        while True:
            try:
                observation, decision, enqueued_mono = await asyncio.wait_for(
                    self._cex_ingest_queue.get(), timeout=0.25)
            except asyncio.TimeoutError:
                # Keep draining queued evidence during shutdown; only exit once
                # the queue is fully empty so nothing is dropped silently.
                if self._stopping.is_set():
                    return
                continue
            self._note_cex_latency(enqueued_mono)
            try:
                await self._process_cex_observation(observation, decision)
            except Exception as exc:
                self._last_error = f"cex_ingest:{type(exc).__name__}:{exc}"[:240]
                self._reject(None, "DATA_INVALID", f"cex_ingest_{type(exc).__name__}")
            finally:
                self._cex_ingest_queue.task_done()

    async def _on_polymarket_event(self, event: SourceEvent,
                                   decision: EventDecision) -> None:
        """Non-blocking adapter callback; persistence lives in a bounded worker."""

        self.counters["raw_events"] += 1
        self._last_event_ms = max(self._last_event_ms, event.receipt_ts_ms)
        if decision.accepted:
            self.counters["accepted_events"] += 1
        else:
            self.counters["rejected_events"] += 1
        if not decision.accepted:
            # Rejected transport backlog is auditable in aggregate and source
            # health, without turning each stale frame into a synchronous DB
            # write in the WebSocket receive path.
            self._buffer_event_count(event, decision)
            return
        try:
            self._polymarket_ingest_queue.put_nowait((event, decision))
        except asyncio.QueueFull:
            self.counters["polymarket_ingest_overflow"] += 1
            self._last_error = "polymarket_ingest_queue_overflow_fail_closed"
            self._buffer_event_count(
                event, decision, classification="INGEST_QUEUE_OVERFLOW")
            key = self.token_to_window.get(event.token_id)
            state = self.markets.get(key or "")
            if state is not None:
                side = "YES" if event.token_id == state.identity.yes_token_id else "NO"
                state.books.pop(side, None)
        else:
            self._polymarket_queue_high_water = max(
                self._polymarket_queue_high_water,
                self._polymarket_ingest_queue.qsize())

    async def _process_polymarket_event(self, event: SourceEvent,
                                        decision: EventDecision) -> None:
        self._buffer_event_count(event, decision)
        self._persist_source_event(event, decision, force=False)
        key = self.token_to_window.get(event.token_id)
        state = self.markets.get(key or "")
        if state is None:
            return
        if event.event_type in {"book", "price_change"}:
            book = self._book_from_ws(state, event)
            if book is None:
                return
            side = "YES" if event.token_id == state.identity.yes_token_id else "NO"
            state.books[side] = book
            if "YES" in state.books and "NO" in state.books:
                self.book_history.append_pair(
                    state.identity.window_key, state.books["YES"], state.books["NO"])
        trigger = EvaluationTrigger(
            source="polymarket", receipt_ts_ms=event.receipt_ts_ms,
            receipt_monotonic_ns=event.receipt_monotonic_ns,
            event=event, decision=decision,
        )
        self._schedule(state.identity.window_key, trigger)

    async def _drain_polymarket_ingest_once(self) -> None:
        event, decision = await self._polymarket_ingest_queue.get()
        try:
            await self._process_polymarket_event(event, decision)
        finally:
            self._polymarket_ingest_queue.task_done()

    async def _polymarket_ingest_loop(self) -> None:
        while True:
            try:
                event, decision = await asyncio.wait_for(
                    self._polymarket_ingest_queue.get(), timeout=0.25)
            except asyncio.TimeoutError:
                # Keep draining queued events during shutdown; only exit once
                # the queue is fully empty so nothing is dropped silently.
                if self._stopping.is_set():
                    return
                continue
            try:
                await self._process_polymarket_event(event, decision)
            except Exception as exc:
                self._last_error = f"polymarket_ingest:{type(exc).__name__}:{exc}"[:240]
                self._reject(None, "DATA_INVALID", f"polymarket_ingest_{type(exc).__name__}")
            finally:
                self._polymarket_ingest_queue.task_done()

    def _book_from_ws(self, state: MarketState,
                      event: SourceEvent) -> Optional[BookState]:
        current = self.poly_ws.current_book(event.token_id)
        if not current or not bool(current.get("hydrated")):
            return None
        try:
            bids = tuple(BookLevel(price, shares)
                         for price, shares in current.get("bids", ()))
            asks = tuple(BookLevel(price, shares)
                         for price, shares in current.get("asks", ()))
            side = "YES" if event.token_id == state.identity.yes_token_id else "NO"
            previous = state.books.get(side)
            return BookState(
                token_id=event.token_id,
                condition_id=state.identity.condition_id,
                market_id=state.identity.market_id,
                bids=bids,
                asks=asks,
                provider_ts_ms=int(current["provider_ts_ms"]),
                receipt_ts_ms=event.receipt_ts_ms,
                receipt_monotonic_ns=event.receipt_monotonic_ns,
                source="polymarket_ws",
                event_id=event.event_key,
                payload_hash=str(current.get("hash") or event.payload_hash),
                connection_epoch=int(current.get("connection_epoch") or 0),
                min_order_size=previous.min_order_size if previous else None,
                tick_size=previous.tick_size if previous else None,
                neg_risk=previous.neg_risk if previous else None,
                hydrated=True,
            )
        except (KeyError, TypeError, ValueError):
            return None

    async def _on_book_hydration_request(self, token_id: str,
                                         condition_id: str, reason: str,
                                         _connection_epoch: int) -> None:
        key = self.token_to_window.get(str(token_id))
        state = self.markets.get(key or "")
        if state is None or state.identity.condition_id != str(condition_id):
            return
        self._spawn_background(
            self._recover_token(state, str(token_id), str(reason)),
            name=f"v4-recover-{state.identity.window_key}",
        )

    def _schedule(self, key: str, trigger: EvaluationTrigger) -> None:
        if key in self.pending:
            self.counters["coalesced_triggers"] += 1
        self.pending[key] = trigger
        self.pending_event.set()

    def _spawn_background(self, coroutine: Any, *, name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _sync_active_subscriptions(self, current: int) -> None:
        """Own only active-window tokens and switch at the exact rollover.

        Discovery may retain 30 minutes of verified future identities, but the
        event socket's hydration state must describe executable windows only.
        A dedicated 250 ms scheduler calls this method so excluding future
        tokens never introduces the discovery loop's slower rollover delay.
        """

        active = {
            key: state for key, state in self.markets.items()
            if state.identity.window_open_ms <= int(current)
            < state.identity.window_close_ms
        }
        subscriptions = {
            token: state.identity.condition_id
            for state in active.values()
            for token in (state.identity.yes_token_id, state.identity.no_token_id)
        }
        if subscriptions == self._active_subscription_map:
            return
        prior_windows = self._active_subscription_windows
        await self.poly_ws.set_subscriptions(subscriptions)
        self._active_subscription_map = dict(subscriptions)
        self._active_subscription_windows = frozenset(active)
        for key in sorted(set(active) - set(prior_windows)):
            self._spawn_background(
                self._hydrate_market(active[key]),
                name=f"v4-rollover-hydrate-{key}",
            )

    async def discover_once(self) -> None:
        current = now_ms()
        batch = await self.discovery.discover(current)
        self._last_discovery_ms = current
        self._ignored_durations = dict(batch.ignored_duration_counts)
        for asset in self.cfg.required_assets:
            for offset in (0, 1):
                opening = window_open_ms(current, offset_windows=offset)
                expected_key = (asset, opening)
                if expected_key in self._expected_window_keys:
                    continue
                await self._critical_execute("ensure_asset_window", {
                    "asset": asset,
                    "window_open_ts_ms": opening,
                    "window_close_ts_ms": opening + 300_000,
                    "expected": 1,
                    "lifecycle_status": "DISCOVERING",
                    "created_ts_ms": current,
                    "updated_ts_ms": current,
                }, ordering_key=f"{asset}:{opening}",
                    idempotency_key=(
                        f"expected-window:{self.session_id}:{asset}:{opening}"
                    ),
                    command_type="MARKET_DISCOVERY",
                    associated_asset=asset)
                self._expected_window_keys.add(expected_key)
        discovered: list[MarketState] = []
        for identity in batch.eligible_markets:
            state = await self._persist_market(identity, current)
            discovered.append(state)
        for rejected in batch.rejected:
            reason = str(rejected.reason)
            if reason.startswith("discovery_request_failed"):
                self._reject(None, "DISCOVERY", reason, recoverable=True)
        active_keys = {
            state.identity.window_key for state in discovered
            if state.identity.window_close_ms > current
        }
        for key, state in tuple(self.markets.items()):
            if state.identity.window_close_ms + 300_000 < current:
                self.markets.pop(key, None)
                self.book_history.remove(key)
            elif key not in active_keys and state.identity.window_close_ms > current:
                # A transient discovery miss never remaps a window; retain the
                # verified identity but do not mark a different row selected.
                continue
        self.token_to_window = {
            token: key
            for key, state in self.markets.items()
            for token in (state.identity.yes_token_id, state.identity.no_token_id)
        }
        await self._sync_active_subscriptions(current)
        assets = sorted({state.identity.asset for state in self.markets.values()
                         if state.identity.window_close_ms > current}
                        | set(self.cfg.required_assets))
        await self.okx.set_assets(assets)

    async def _active_subscription_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._sync_active_subscriptions(now_ms())
            except Exception as exc:
                self._last_error = f"active_scheduler:{type(exc).__name__}:{exc}"[:240]
                self._reject(None, "SCHEDULER", f"active_subscription_{type(exc).__name__}",
                             recoverable=True)
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                pass

    async def _persist_market(
        self, identity: MarketIdentity, current: int,
    ) -> MarketState:
        existing_state = self.markets.get(identity.window_key)
        if existing_state is not None:
            prior = existing_state.identity
            if any((
                    prior.market_id != identity.market_id,
                    prior.event_id != identity.event_id,
                    prior.condition_id != identity.condition_id,
                    prior.yes_token_id != identity.yes_token_id,
                    prior.no_token_id != identity.no_token_id,
                    prior.slug != identity.slug)):
                self._reject(existing_state, "DISCOVERY", "duplicate_market_identity")
                return existing_state
        lifecycle = "ACTIVE" if identity.window_open_ms <= current else "UPCOMING"
        persisted = await self._critical_execute(
            "persist_market_bundle", {
            "market": {
                "polymarket_market_id": identity.market_id,
                "asset": identity.asset,
                "slug": identity.slug,
                "question": identity.slug,
                "duration_ms": identity.duration_ms,
                "open_ts_ms": identity.window_open_ms,
                "close_ts_ms": identity.window_close_ms,
                "status": "ACTIVE" if identity.active else "INACTIVE",
                "accepting_orders": int(identity.accepting_orders),
                "first_seen_ts_ms": current,
                "last_seen_ts_ms": current,
            },
            "identity": {
                "event_id": identity.event_id,
                "condition_id": identity.condition_id,
                "yes_token_id": identity.yes_token_id,
                "no_token_id": identity.no_token_id,
                "association_valid": 1,
                "token_pair_valid": 1,
                "ambiguous": 0,
                "verification_reason": "exact_five_minute_identity_verified",
                "verified_ts_ms": current,
            },
            "window": {
                "asset": identity.asset,
                "window_open_ts_ms": identity.window_open_ms,
                "window_close_ts_ms": identity.window_close_ms,
                "expected": 1,
                "lifecycle_status": lifecycle,
                "created_ts_ms": current,
                "updated_ts_ms": current,
            },
            "link": {
                "eligibility_status": "ELIGIBLE",
                "reject_reason": None,
                "selected": 1,
                "linked_ts_ms": current,
            },
            "anchor": ({
                "status": _anchor_db(identity.anchor_status),
                "price_to_beat": identity.price_to_beat,
                "source_field": "priceToBeat" if identity.anchored else None,
                "parse_error": (
                    "parse_failed" if identity.anchor_status is AnchorStatus.PARSE_FAILED else None
                ),
                "provider_ts_ms": None,
                "receipt_ts_ms": current,
            } if existing_state is None else None),
            "funnel": {
                "now_ms": current,
                "changes": {
                    "available": 1,
                    "eligible": 1,
                    "available_ts_ms": current,
                    "eligible_ts_ms": current,
                    "final_blocker": None,
                    "no_book_reason": None,
                    "data_invalid_reason": None,
                },
            },
            },
            ordering_key=identity.window_key,
            idempotency_key=(
                f"market-bundle:{identity.window_key}:"
                f"{identity.condition_id}:{current}"
            ),
            command_type="MARKET_DISCOVERY",
            associated_asset=identity.asset,
        )
        db_market = int(persisted["market_id"])
        market_identity_id = int(persisted["market_identity_id"])
        window_id = int(persisted["window_id"])
        if existing_state is None:
            state = MarketState(identity, window_id, db_market, market_identity_id)
            self.markets[identity.window_key] = state
        else:
            existing_state.identity = identity
            state = existing_state
        self.token_to_window[identity.yes_token_id] = identity.window_key
        self.token_to_window[identity.no_token_id] = identity.window_key
        return state

    async def _hydrate_market(self, state: MarketState) -> None:
        key = state.identity.window_key
        if key in self._hydration_inflight:
            return
        self._hydration_inflight.add(key)
        try:
            results = await hydrate_market_books(
                self.clob, state.identity,
                attempts=self.cfg.rest_recovery_attempts,
                minimum_delay_ms=self.cfg.rest_recovery_min_ms,
                maximum_delay_ms=self.cfg.rest_recovery_max_ms,
                max_age_ms=self.cfg.book_max_age_ms,
            )
            failed: list[str] = []
            for token, result in results.items():
                if result.book is None:
                    failed.append(result.reason.value)
                    continue
                side = "YES" if token == state.identity.yes_token_id else "NO"
                state.books[side] = result.book
                self._persist_book(state, side, result.book, None)
                await self.poly_ws.accept_rest_book(self._rest_book_payload(result.book))
            if failed:
                self._last_rest_status = "CLOB_REST_PARTIAL_FAIL_CLOSED"
                for reason in failed:
                    self._reject(state, "NO_BOOK", reason, recoverable=True)
            else:
                self._last_rest_status = "CLOB_REST_HYDRATED"
            if "YES" in state.books and "NO" in state.books:
                self.book_history.append_pair(
                    state.identity.window_key, state.books["YES"], state.books["NO"])
                self._schedule(state.identity.window_key, EvaluationTrigger(
                    source="clob_rest", receipt_ts_ms=now_ms(),
                    receipt_monotonic_ns=time.monotonic_ns(),
                ))
        except Exception as exc:
            self._last_rest_status = "CLOB_REST_FAILED_CLOSED"
            self._last_error = f"hydrate:{type(exc).__name__}"[:240]
            self._reject(state, "NO_BOOK", "HTTP_FAILURE", recoverable=True)
        finally:
            self._hydration_inflight.discard(key)

    async def _recover_token(self, state: MarketState, token: str,
                             reason: str) -> None:
        guard = f"{state.identity.window_key}:{token}"
        if guard in self._recovery_inflight:
            return
        self._recovery_inflight.add(guard)
        self.counters["rest_recoveries"] += 1
        try:
            results = await hydrate_market_books(
                self.clob, state.identity,
                attempts=self.cfg.rest_recovery_attempts,
                minimum_delay_ms=self.cfg.rest_recovery_min_ms,
                maximum_delay_ms=self.cfg.rest_recovery_max_ms,
                max_age_ms=self.cfg.book_max_age_ms,
            )
            result = results.get(token)
            if result is None or result.book is None:
                self.counters["rest_recovery_failures"] += 1
                recovered_reason = result.reason.value if result else "HTTP_FAILURE"
                self._last_rest_status = "CLOB_REST_RECOVERY_FAILED_CLOSED"
                self._reject(state, "NO_BOOK", recovered_reason,
                             recoverable=True, detail={"trigger": reason})
                return
            side = "YES" if token == state.identity.yes_token_id else "NO"
            state.books[side] = result.book
            self._persist_book(state, side, result.book, None)
            await self.poly_ws.accept_rest_book(self._rest_book_payload(result.book))
            self._last_rest_status = "CLOB_REST_RECOVERY_OK"
        finally:
            self._recovery_inflight.discard(guard)

    @staticmethod
    def _rest_book_payload(book: BookState) -> dict[str, Any]:
        return {
            "asset_id": book.token_id,
            "market": book.condition_id,
            "market_id": book.market_id,
            "timestamp": str(book.provider_ts_ms),
            "hash": book.event_id,
            "bids": [{"price": str(row.price), "size": str(row.shares)}
                     for row in book.bids],
            "asks": [{"price": str(row.price), "size": str(row.shares)}
                     for row in book.asks],
            "min_order_size": book.min_order_size,
            "tick_size": book.tick_size,
            "neg_risk": book.neg_risk,
        }

    @staticmethod
    def _compact_levels(levels: tuple[BookLevel, ...]) -> list[dict[str, float]]:
        kept: list[BookLevel] = []
        cumulative = 0.0
        for level in levels:
            kept.append(level)
            cumulative += level.shares
            if len(kept) >= 10 and cumulative >= 5.0:
                break
        return [{"price": row.price, "shares": row.shares} for row in kept]

    def _persist_book(self, state: MarketState, side: str, book: BookState,
                      source_event_id: Optional[int]) -> int:
        row = self._book_row(state, side, book, source_event_id)
        self._telemetry_submit(
            "record_book_snapshot", row,
            state_key=("book", state.market_identity_id, book.token_id),
            state_value={
                "state_hash": row["state_hash"],
                "hydrated": row["hydrated"], "stale": row["stale"],
                "invalid_reason": row["invalid_reason"],
            },
        )
        # Telemetry IDs are deliberately not exposed to executable state.  The
        # acknowledged evaluation bundle re-inserts/idempotently resolves the
        # exact snapshot and returns its committed identifier.
        return 0

    def _book_row(self, state: MarketState, side: str, book: BookState,
                  source_event_id: Optional[int]) -> dict[str, Any]:
        bids = self._compact_levels(book.bids)
        asks = self._compact_levels(book.asks)
        state_hash = _sha256_json({
            "token": book.token_id, "condition": book.condition_id,
            "provider": book.provider_ts_ms, "epoch": book.connection_epoch,
            "bids": bids, "asks": asks,
        })
        return {
                "source_event_id": source_event_id,
                "market_identity_id": state.market_identity_id,
                "token_id": book.token_id,
                "outcome_side": side,
                "provider_ts_ms": book.provider_ts_ms,
                "receipt_ts_ms": book.receipt_ts_ms,
                "monotonic_ns": book.receipt_monotonic_ns,
                "sequence_no": book.sequence,
                "state_hash": state_hash,
                "best_bid": book.best_bid,
                "best_ask": book.best_ask,
                "spread": book.spread,
                "bid_depth_5": sum(row.shares for row in book.bids[:5]),
                "ask_depth_5": sum(row.shares for row in book.asks[:5]),
                "bids_json": bids,
                "asks_json": asks,
                "hydrated": int(book.hydrated),
                "stale": int(book.age_ms(now_ms()) > self.cfg.book_max_age_ms),
                "invalid_reason": None,
            }

    def _reject(self, state: Optional[MarketState], taxonomy: str, reason: str,
                *, recoverable: bool = False, retry_count: int = 0,
                candidate_id: Optional[int] = None,
                source_event_id: Optional[int] = None,
                detail: Optional[dict[str, Any]] = None) -> int:
        current = now_ms()
        reject_key = "|".join((
            taxonomy, str(reason), str(state.window_id if state else 0),
            str(candidate_id or 0),
        ))
        if current - self._last_reject_keys.get(reject_key, 0) < 5_000:
            return 0
        self._last_reject_keys[reject_key] = current
        if len(self._last_reject_keys) > 10_000:
            self._last_reject_keys.pop(next(iter(self._last_reject_keys)))
        if (state is not None and state.last_reject_ts_ms
                and current - state.last_reject_ts_ms < 500
                and state.last_no_book_reason == f"{taxonomy}:{reason}"):
            return 0
        if state is not None:
            state.last_reject_ts_ms = current
            state.last_no_book_reason = f"{taxonomy}:{reason}"
        try:
            # SCHEDULER is an in-memory subsystem label, not a persisted V1
            # taxonomy.  Persist it truthfully under DATA_INVALID with the
            # original label retained in detail rather than violating CHECK.
            persisted_taxonomy = (
                taxonomy if taxonomy in {
                    "DISCOVERY", "NO_BOOK", "DATA_INVALID", "ECONOMIC",
                    "EXECUTION", "RISK", "RESOLUTION",
                } else "DATA_INVALID"
            )
            row = {
                "session_id": self.session_id,
                "window_id": state.window_id if state else None,
                "candidate_id": candidate_id,
                "source_event_id": source_event_id,
                "reject_ts_ms": current,
                "taxonomy": persisted_taxonomy,
                "reason": str(reason),
                "recoverable": int(recoverable),
                "retry_count": int(retry_count),
                "detail_json": {
                    **(detail or {}),
                    **({"subsystem_taxonomy": taxonomy}
                       if persisted_taxonomy != taxonomy else {}),
                },
            }
            admitted = self._telemetry_submit(
                "record_reject", row,
                state_key=("reject", state.window_id if state else 0,
                           persisted_taxonomy, str(reason)),
                state_value={
                    "recoverable": bool(recoverable),
                    "retry_count": int(retry_count),
                    "candidate_id": candidate_id,
                },
            )
            return int(admitted)
        except Exception as exc:  # noqa: BLE001 - reject telemetry must never crash the engine
            # A transient persistence failure while recording a reject is
            # itself only telemetry; it must never propagate out of a caller's
            # except handler and take down a critical task (e.g. the active
            # subscription scheduler).
            self._last_error = f"reject_persist:{type(exc).__name__}:{exc}"[:240]
            return 0

    async def _evaluation_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(self.pending_event.wait(), timeout=0.025)
            except asyncio.TimeoutError:
                pass
            self.pending_event.clear()
            current_mono = time.monotonic_ns()
            current_wall = now_ms()
            # Monotonic deadline checks continue even if both providers are
            # quiet; no maker state waits for a scanner cadence.
            for key, state in tuple(self.markets.items()):
                if self.router.active(state.identity) is not None and key not in self.pending:
                    self.pending[key] = EvaluationTrigger(
                        source="maker_deadline", receipt_ts_ms=current_wall,
                        receipt_monotonic_ns=current_mono,
                    )
            ready: list[tuple[str, EvaluationTrigger]] = []
            for key, trigger in tuple(self.pending.items()):
                state = self.markets.get(key)
                if state is None:
                    self.pending.pop(key, None)
                    continue
                active = self.router.active(state.identity) is not None
                minimum_ns = 50_000_000 if active else 250_000_000
                if current_mono - state.last_evaluation_mono_ns >= minimum_ns:
                    ready.append((key, self.pending.pop(key)))
            for key, trigger in ready:
                state = self.markets.get(key)
                if state is None:
                    continue
                state.last_evaluation_mono_ns = time.monotonic_ns()
                try:
                    await self._evaluate(state, trigger)
                except Exception as exc:
                    self._last_error = f"evaluate:{type(exc).__name__}:{exc}"[:240]
                    self._reject(state, "DATA_INVALID", f"evaluation_{type(exc).__name__}")

    async def _evaluate(self, state: MarketState,
                        trigger: EvaluationTrigger) -> None:
        current = now_ms()
        identity = state.identity
        if not (identity.window_open_ms <= current < identity.window_close_ms):
            return
        if identity.window_close_ms - current < int(self.cfg.minimum_remaining_s * 1_000):
            return
        self.counters["evaluations"] += 1
        state.evaluation_seq += 1

        feature_evidence = self.cex_features.build(
            identity.asset, now_ms=current,
            window_open_ms=identity.window_open_ms,
            max_age_ms=self.cfg.cex_max_age_ms,
        )
        yes_book, no_book = state.books.get("YES"), state.books.get("NO")
        pair = classify_book_pair(
            identity, yes_book, no_book, now_ms=current,
            max_age_ms=self.cfg.book_max_age_ms,
            max_pair_skew_ms=self.cfg.max_book_pair_skew_ms,
        )
        yes_snapshot: Optional[int] = None
        no_snapshot: Optional[int] = None
        if not pair.valid:
            self._update_window_funnel(
                state.window_id, current, final_blocker=pair.reason.value,
                no_book_reason=pair.reason.value,
            )
            self._reject(state, "NO_BOOK", pair.reason.value, recoverable=True,
                         source_event_id=None)
            if pair.reason in {
                    NoBookReason.YES_BOOK_MISSING, NoBookReason.NO_BOOK_MISSING,
                    NoBookReason.BOTH_BOOKS_MISSING, NoBookReason.STALE_SNAPSHOT,
                    NoBookReason.WS_NOT_HYDRATED, NoBookReason.EMPTY_LEVELS}:
                self._spawn_background(
                    self._hydrate_market(state),
                    name=f"v4-hydrate-{state.identity.window_key}",
                )

        latest_move = feature_evidence.features.latest_move_ts_ms
        book_provider = max(
            (row.provider_ts_ms for row in (yes_book, no_book) if row is not None),
            default=0,
        )
        response_ms = (
            book_provider - latest_move
            if latest_move is not None and book_provider else None
        )
        context = ModelContext(
            market=identity,
            now_ms=current,
            cex=feature_evidence.features,
            yes_book=yes_book,
            no_book=no_book,
            book_history=self.book_history.points(identity.window_key, now_ms=current),
            polymarket_response_ms=response_ms,
        )
        ensemble = self.ensemble.evaluate(context)
        phase = "UPDATE" if self.router.active(identity) is not None else "INITIAL"
        calculation = calculate_economics(
            identity=identity, ensemble=ensemble,
            yes_book=yes_book, no_book=no_book,
            now_ms=current, cfg=self.cfg, phase=phase,
        )
        state.last_fair = calculation

        hard_failure = ""
        blocked = self._execution_blocked_reason()
        if blocked:
            # Fail closed: a degraded event loop or failed integrity scan means
            # evidence and time-sensitive routing cannot be trusted.
            hard_failure = blocked
            self._update_window_funnel(
                state.window_id, current, final_blocker=hard_failure,
                data_invalid_reason=hard_failure,
            )
        elif not self._okx_connected:
            hard_failure = "all_fresh_cex_sources_unavailable"
            self._update_window_funnel(
                state.window_id, current, final_blocker=hard_failure,
                data_invalid_reason=hard_failure,
            )
        elif not feature_evidence.features.valid:
            hard_failure = (
                feature_evidence.features.invalidation_reason
                or "all_fresh_cex_sources_unavailable"
            )
            self._update_window_funnel(
                state.window_id, current, final_blocker=hard_failure,
                data_invalid_reason=hard_failure,
            )
        elif not pair.valid:
            hard_failure = pair.reason.value

        if state.window_id in self._entered_window_ids:
            router_decision = RouterDecision(
                RouterAction.NO_ACTION, "window_entry_already_exists",
                tier_for_edge(calculation.result.selected_net_edge, self.cfg),
                calculation.result.selected_side, calculation, None,
            )
        elif hard_failure and self.router.active(identity) is not None:
            router_decision = self.router.safety_fail(
                identity, calculation, reason=hard_failure,
                monotonic_ns=trigger.receipt_monotonic_ns or time.monotonic_ns(),
            )
        elif hard_failure:
            router_decision = RouterDecision(
                RouterAction.NO_ACTION, hard_failure,
                tier_for_edge(calculation.result.selected_net_edge, self.cfg),
                calculation.result.selected_side, calculation, None,
            )
        else:
            router_decision = self.router.on_candidate(
                identity=identity, calculation=calculation,
                now_ms=current,
                monotonic_ns=trigger.receipt_monotonic_ns or time.monotonic_ns(),
            )

        edge = calculation.result.selected_net_edge
        if edge is None:
            edge_bucket = "NONE"
        elif edge >= self.cfg.strong_cross_edge:
            edge_bucket = "STRONG"
        elif edge >= self.cfg.medium_maker_edge:
            edge_bucket = "MEDIUM"
        elif edge >= self.cfg.weak_observe_edge:
            edge_bucket = "WEAK"
        elif edge > 0.0:
            edge_bucket = "POSITIVE_BELOW_TIER"
        else:
            edge_bucket = "NON_POSITIVE"
        # Materiality fingerprint.  Direction and edge bucket participate only
        # while an actionable positive edge exists: at edge<=0 the selected
        # side and the NONE<->NON_POSITIVE bucket boundary flap on every tick
        # and are score noise, not decision evidence.  Volatile per-tick model
        # inputs (feature classification) are likewise excluded; eligibility
        # is already captured by the action/reason pair, regime, and book
        # pairing state.
        actionable_edge = edge_bucket not in {"NONE", "NON_POSITIVE"}
        fingerprint = "|".join((
            router_decision.action.value, router_decision.reason,
            router_decision.tier,
            (_outcome(router_decision.side) or "NONE")
            if actionable_edge else "NONE",
            edge_bucket if actionable_edge else "NO_EDGE",
            ensemble.regime, pair.reason.value,
            str(bool(decision_state.price_touched) if (
                (decision_state := router_decision.state) is not None
            ) else False),
        ))
        has_open_position = state.window_id in self._open_position_windows
        open_position_id = next(
            (position_id for position_id, row in self._position_cache.items()
             if int(row.get("window_id") or -1) == state.window_id
             and row.get("status") == "OPEN"),
            None,
        )
        management_due = bool(
            has_open_position
            and open_position_id is not None
            and current - self._management_last_ts.get(open_position_id, 0) >= 1_000
        )
        # Persist the full evidence bundle only for material evaluations: an
        # action that will actually advance execution, a decision affecting an
        # active maker observation, a due management sample, a safety-state
        # transition, the first evaluation of a window, or a non-executable
        # state transition that has held long enough to not be threshold
        # flapping.  Repeated identical or rapidly flapping no-edge/skip
        # evaluations are counted and suppressed instead of journaled, per the
        # telemetry aggregation contract.
        maker_active = state.identity.window_key in self.maker_persistence
        capacity_blocked = (
            self._entry_capacity_block(state.identity.asset) is not None)
        transitioned = fingerprint != state.last_candidate_fingerprint
        first_evaluation = not state.last_candidate_fingerprint
        immaterial_interval_ok = (
            current - state.last_candidate_persist_ts_ms
            >= IMMATERIAL_TRANSITION_MIN_INTERVAL_MS
        )
        important = bool(
            (transitioned and (
                first_evaluation
                or immaterial_interval_ok
                or router_decision.action in {
                    RouterAction.START_OBSERVATION,
                    RouterAction.CROSS_SPREAD,
                    RouterAction.SAFETY_FAIL,
                }
            ))
            or (not capacity_blocked
                and state.window_id not in self._entered_window_ids
                and router_decision.action in {
                    RouterAction.START_OBSERVATION,
                    RouterAction.CROSS_SPREAD,
                })
            or (maker_active and router_decision.action in {
                RouterAction.CROSS_SPREAD,
                RouterAction.SKIP,
                RouterAction.SAFETY_FAIL,
            })
            or management_due
        )
        if not important:
            self.counters["suppressed_evaluations"] = (
                self.counters.get("suppressed_evaluations", 0) + 1)
            return

        persisted = await self._persist_evaluation(
            state=state, trigger=trigger,
            feature_evidence=feature_evidence, ensemble=ensemble,
            calculation=calculation, router_decision=router_decision,
            now_ms_value=current,
            monotonic_ns_value=trigger.receipt_monotonic_ns or time.monotonic_ns(),
        )
        # Suppression state advances only after the complete evidence graph is
        # durably committed.  A failed/unknown acknowledgement remains
        # fail-closed and a process restart may safely reconstruct from SQLite.
        state.last_candidate_fingerprint = fingerprint
        state.last_candidate_persist_ts_ms = current
        candidate_id = int(persisted["candidate_id"])
        fair_id = int(persisted["fair_value_calculation_id"])
        decision_id = int(persisted["decision_id"])
        book_ids = list(persisted.get("book_snapshot_ids") or ())
        book_sides = [side for side, book in (("YES", yes_book), ("NO", no_book))
                      if book is not None]
        snapshots = dict(zip(book_sides, book_ids))
        yes_snapshot = snapshots.get("YES")
        no_snapshot = snapshots.get("NO")
        state.last_candidate_id = candidate_id
        state.last_fair_value_id = fair_id
        state.last_decision_id = decision_id

        positive = bool(
            not hard_failure and calculation.result.selected_net_edge is not None
            and calculation.result.selected_net_edge > 0.0
        )
        if positive:
            self.counters["positive_edge_evaluations"] += 1
        await self._advance_execution(
            state, ensemble, calculation, router_decision,
            candidate_id, fair_id, decision_id,
            yes_snapshot, no_snapshot, current,
            trigger.receipt_monotonic_ns or time.monotonic_ns(),
            int(persisted.get("source_event_id") or 0),
        )
        if not hard_failure:
            await self._manage_open_position(
                state, calculation, fair_id, yes_snapshot, no_snapshot, current)

    async def _persist_evaluation(
        self, *, state: MarketState, trigger: EvaluationTrigger,
        feature_evidence: FeatureEvidence, ensemble: EnsembleResult,
        calculation: EconomicCalculation, router_decision: RouterDecision,
        now_ms_value: int, monotonic_ns_value: int,
    ) -> dict[str, Any]:
        fair = calculation.result
        selected = _outcome(fair.selected_side)
        dominant = max(
            ensemble.outputs, key=lambda row: abs(row.contribution), default=None)
        source_wrapper = (
            self._source_event_wrapper(trigger.event, trigger.decision)
            if trigger.event is not None and trigger.decision is not None
            else None
        )
        books: list[dict[str, Any]] = []
        book_indices: dict[str, int] = {}
        for side in ("YES", "NO"):
            book = state.books.get(side)
            if book is None:
                continue
            book_indices[side] = len(books)
            book_row = self._book_row(state, side, book, None)
            book_row.pop("source_event_id", None)
            books.append({
                "value": book_row,
                "side": side,
                "evidence_age_ms": max(0, book.age_ms(now_ms_value)),
            })

        cex_rows: list[dict[str, Any]] = []
        for observation in feature_evidence.observations:
            synthetic = EventDecision(
                EventDisposition.ACCEPT_NO_NEW_TICK if observation.unchanged
                else EventDisposition.ACCEPT_NEW,
                observation, observation.classification,
            )
            observation_row = observation.to_dict()
            observation_row.update({
                "fresh": int(synthetic.accepted),
                "invalid_reason": None if synthetic.accepted else synthetic.reason,
            })
            cex_rows.append({
                "value": observation_row,
                "kwargs": {
                    "session_id": self.session_id,
                    "already_validated_at_receipt": True,
                },
                "source_event": self._source_event_wrapper(observation, synthetic),
                "role": "POINT_IN_TIME_FEATURE",
                "horizon_ms": 0,
                "evidence_age_ms": max(
                    0, now_ms_value - observation.provider_ts_ms),
            })

        models: list[dict[str, Any]] = []
        for output in ensemble.outputs:
            regime_weight = min(
                1.0, abs(output.contribution) / max(abs(output.raw_score), 1e-12))
            models.append({
                "model_name": output.model_name,
                "model_version": self.ensemble.MODEL_VERSION,
                "correlation_group": output.correlation_group or output.family,
                "direction": output.direction.value,
                "raw_score": output.raw_score,
                "estimated_probability": output.estimated_probability_yes,
                "evidence_age_ms": output.evidence_age_ms,
                "confidence": output.confidence,
                "reliability": output.reliability,
                "invalidation_reason": output.invalidation_reason or None,
                "expected_net_edge": output.expected_net_edge,
                "regime_weight": regime_weight,
                "gated": int(bool(output.invalidation_reason)),
                "model_contribution": output.contribution,
                "calibrated": int(output.calibrated),
            })
        phase = (
            "FINAL" if router_decision.action in {
                RouterAction.CROSS_SPREAD, RouterAction.SKIP, RouterAction.SAFETY_FAIL}
            else calculation.result.phase
        )
        selected_side = (
            fair.yes if fair.selected_side is EntrySide.BUY_YES
            else fair.no if fair.selected_side is EntrySide.BUY_NO else None)
        economic = bool(selected_side and selected_side.net_edge is not None
                        and selected_side.net_edge > 0.0)
        completed = now_ms()
        latency_ms = max(
            0.0,
            (time.monotonic_ns() - trigger.receipt_monotonic_ns) / 1_000_000,
        ) if trigger.receipt_monotonic_ns else 0.0
        sides: list[dict[str, Any]] = []
        for side_name, side_result, token_id, is_selected in (
            ("YES", fair.yes, state.identity.yes_token_id,
             fair.selected_side is EntrySide.BUY_YES),
            ("NO", fair.no, state.identity.no_token_id,
             fair.selected_side is EntrySide.BUY_NO),
        ):
            wrapper: dict[str, Any] = {
                "value": self._fair_side_row(
                    side_result, side_name, token_id, None, is_selected,
                    now_ms_value,
                )
            }
            if side_name in book_indices:
                wrapper["book_index"] = book_indices[side_name]
            sides.append(wrapper)

        bundle = {
            "source_event": source_wrapper,
            "books": books,
            "cex": cex_rows,
            "candidate": {
                "session_id": self.session_id,
                "window_id": state.window_id,
                "market_identity_id": state.market_identity_id,
                "evaluation_ts_ms": now_ms_value,
                "monotonic_ns": monotonic_ns_value,
                "evaluation_seq": state.evaluation_seq,
                "status": router_decision.action.value,
                "regime": ensemble.regime,
                "selected_side": selected,
                "fair_probability_yes": fair.fair_probability_yes,
                "fair_probability_no": fair.fair_probability_no,
                "calibrated": int(not ensemble.model_uncalibrated),
                "reliability": ensemble.reliability,
                "positive_edge": int(
                    fair.selected_net_edge is not None
                    and fair.selected_net_edge > 0.0),
                "dominant_model": dominant.model_name if dominant else None,
                "invalidation_reason": (
                    router_decision.reason
                    if router_decision.action in {
                        RouterAction.NO_ACTION, RouterAction.SAFETY_FAIL
                    } else None
                ),
            },
            "models": models,
            "fair_value": {
                "calculation": {
                    "phase": phase,
                    "calculation_seq": 0,
                    "calculated_ts_ms": now_ms_value,
                    "monotonic_ns": monotonic_ns_value,
                    "regime": ensemble.regime,
                    "fair_probability_yes": fair.fair_probability_yes,
                    "fair_probability_no": fair.fair_probability_no,
                    "calibrated": int(not ensemble.model_uncalibrated),
                    "calibration_label": (
                        "FORWARD_CALIBRATED"
                        if not ensemble.model_uncalibrated
                        else "UNCALIBRATED_FORWARD_CANDIDATE"
                    ),
                },
                "sides": sides,
            },
            "decision": {
                "decision_seq": 0,
                "decision_ts_ms": now_ms_value,
                "monotonic_ns": monotonic_ns_value,
                "phase": phase,
                "action": router_decision.action.value,
                "selected_side": selected,
                "selected_net_edge": fair.selected_net_edge,
                "economic_gate_passed": int(economic),
                "exact_depth_passed": int(bool(
                    selected_side and selected_side.valid
                    and selected_side.depth_shares >= self.cfg.fixed_shares)),
                "evidence_fresh": int(bool(
                    selected_side and selected_side.valid
                    and selected_side.evidence_age_ms <= self.cfg.book_max_age_ms
                    and feature_evidence.features.valid)),
                "safety_failure": int(
                    router_decision.action is RouterAction.SAFETY_FAIL),
                "quota_override": 0,
                "reason": router_decision.reason,
            },
            "latency": ({
                "session_id": self.session_id,
                "window_id": state.window_id,
                "measured_ts_ms": completed,
                "stage": "EVENT_TO_DECISION",
                "provider_ts_ms": getattr(trigger.event, "provider_ts_ms", None),
                "receipt_ts_ms": trigger.receipt_ts_ms,
                "completed_ts_ms": completed,
                "latency_ms": latency_ms,
                "within_target": int(latency_ms < 1_000.0),
            } if trigger.receipt_monotonic_ns else None),
            "funnel": ({
                "window_id": state.window_id,
                "now_ms": now_ms_value,
                "changes": {
                    "positive_edge": 1,
                    "first_positive_edge_ts_ms": now_ms_value,
                },
            } if fair.selected_net_edge is not None
                 and fair.selected_net_edge > 0.0 else None),
        }
        return await self._critical_execute(
            "persist_evaluation_bundle",
            bundle,
            ordering_key=state.identity.window_key,
            idempotency_key=(
                f"evaluation:{self.session_id}:{state.window_id}:"
                f"{state.evaluation_seq}"
            ),
            command_type="ENTRY_DECISION_EVIDENCE",
            priority=(20 if router_decision.action in {
                RouterAction.CROSS_SPREAD,
                RouterAction.SKIP,
                RouterAction.SAFETY_FAIL,
            } else 50),
            terminal=router_decision.action in {
                RouterAction.SKIP, RouterAction.SAFETY_FAIL,
            },
            associated_asset=state.identity.asset,
            associated_window_id=state.window_id,
        )

    def _fair_side_row(self, side: Any, outcome: str, token_id: str,
                       snapshot_id: Optional[int], selected: bool,
                       current: int) -> dict[str, Any]:
        exact_depth = bool(side.valid and side.executable_vwap is not None
                           and side.depth_shares >= self.cfg.fixed_shares)
        return {
            "outcome_side": outcome,
            "token_id": token_id,
            "book_snapshot_id": snapshot_id,
            "executable_vwap": side.executable_vwap,
            "worst_consumed_price": side.worst_consumed_price,
            "spread": side.spread,
            "depth_shares": side.depth_shares,
            "exact_five_share_depth": int(exact_depth),
            "estimated_fee": side.estimated_fee,
            "execution_buffer": side.execution_buffer,
            "latency_buffer": side.latency_buffer,
            "uncertainty_buffer": side.uncertainty_buffer,
            "net_edge": side.net_edge,
            "evidence_fresh": int(
                side.valid and side.evidence_age_ms <= self.cfg.book_max_age_ms),
            "selected": int(selected),
        }

    async def _advance_execution(
        self, state: MarketState, ensemble: EnsembleResult,
        calculation: EconomicCalculation, decision: RouterDecision,
        candidate_id: int, fair_id: int, decision_id: int,
        yes_snapshot: Optional[int], no_snapshot: Optional[int],
        current: int, monotonic: int, source_event_id: int,
    ) -> None:
        side = decision.side
        outcome = _outcome(side)
        selected_snapshot = yes_snapshot if side is EntrySide.BUY_YES else no_snapshot
        if decision.action is RouterAction.START_OBSERVATION:
            runtime_state = decision.state
            if (runtime_state is None or outcome is None
                    or selected_snapshot is None or runtime_state.maker_target is None):
                self._reject(state, "EXECUTION", "maker_start_evidence_incomplete",
                             candidate_id=candidate_id)
                return
            maker_id = await self._critical_execute(
                "record_maker_observation", {
                "window_id": state.window_id,
                "candidate_id": candidate_id,
                "decision_id": decision_id,
                "initial_fair_value_calculation_id": fair_id,
                "initial_book_snapshot_id": selected_snapshot,
                "maker_start_ts_ms": runtime_state.maker_start_ts_ms,
                "maker_deadline_ts_ms": runtime_state.maker_deadline_ts_ms,
                "start_monotonic_ns": runtime_state.maker_start_monotonic_ns,
                "maker_target_price": runtime_state.maker_target,
                "chase_cap_price": min(
                    0.999, runtime_state.initial_executable_price
                    + self.cfg.max_chase_worsening),
                "price_touched": 0,
                "maker_fill_assumed": 0,
                "initial_net_edge": runtime_state.initial_edge,
                },
                ordering_key=state.identity.window_key,
                idempotency_key=(
                    f"maker-start:{state.window_id}:{candidate_id}"
                ),
                command_type="MAKER_START",
                associated_asset=state.identity.asset,
                associated_window_id=state.window_id,
            )
            self.maker_persistence[state.identity.window_key] = MakerPersistence(
                maker_id, candidate_id, fair_id)
            self.counters["maker_started"] += 1
            return

        maker = self.maker_persistence.get(state.identity.window_key)
        maker_finished = False
        if decision.action is RouterAction.CONTINUE_OBSERVING and maker is not None:
            maker.update_seq += 1
            await self._critical_execute(
                "record_maker_update", {
                "maker_observation_id": maker.maker_observation_id,
                "update_seq": maker.update_seq,
                "update_ts_ms": current,
                "monotonic_ns": monotonic,
                "source_event_id": source_event_id or None,
                "book_snapshot_id": selected_snapshot,
                "fair_value_calculation_id": fair_id,
                "selected_net_edge": calculation.result.selected_net_edge,
                "price_touched": int(bool(
                    decision.state and decision.state.price_touched)),
                "action": decision.action.value,
                "reason": decision.reason,
                },
                ordering_key=state.identity.window_key,
                idempotency_key=(
                    f"maker-update:{maker.maker_observation_id}:"
                    f"{maker.update_seq}"
                ),
                command_type="MAKER_UPDATE",
                associated_asset=state.identity.asset,
                associated_window_id=state.window_id,
            )
            self.counters["maker_updates"] += 1
            return

        if (maker is not None and decision.action in {
                RouterAction.CROSS_SPREAD, RouterAction.SKIP,
                RouterAction.SAFETY_FAIL}):
            runtime_state = decision.state
            await self._critical_execute(
                "finish_maker_observation",
                maker.maker_observation_id,
                ordering_key=state.identity.window_key,
                idempotency_key=(
                    f"maker-finish:{maker.maker_observation_id}:"
                    f"{decision.action.value}"
                ),
                command_type="MAKER_FINISH",
                priority=20,
                terminal=True,
                associated_asset=state.identity.asset,
                associated_window_id=state.window_id,
                end_ts_ms=current,
                end_monotonic_ns=monotonic,
                final_fair_value_id=fair_id,
                final_book_snapshot_id=selected_snapshot,
                final_net_edge=calculation.result.selected_net_edge,
                price_touched=bool(runtime_state and runtime_state.price_touched),
                outcome=decision.action.value,
                reason=decision.reason,
            )
            maker_finished = True
            self.maker_persistence.pop(state.identity.window_key, None)

        if decision.action is RouterAction.CROSS_SPREAD:
            if decision.state is not None and not maker_finished:
                # A maker-to-cross transition is executable only after both its
                # start and terminal observation records are acknowledged.  We
                # never infer that evidence from an in-memory router state.
                self._critical_failure_reason = "maker_persistence_missing"
                self._reject(
                    state, "DATA_INVALID", "maker_completion_evidence_missing",
                    candidate_id=candidate_id,
                )
                return
            await self._create_shadow_entry(
                state, calculation, decision, candidate_id,
                fair_id, decision_id, selected_snapshot, current)
        elif decision.action is RouterAction.SKIP:
            self._update_window_funnel(
                state.window_id, current,
                missed_opportunity=int(bool(
                    calculation.result.selected_net_edge is not None
                    and calculation.result.selected_net_edge > 0.0)),
                final_blocker=decision.reason,
            )
            self._reject(state, "EXECUTION", decision.reason,
                         candidate_id=candidate_id)
        elif decision.action is RouterAction.SAFETY_FAIL:
            self._reject(state, "DATA_INVALID", decision.reason,
                         candidate_id=candidate_id)

    def _entry_capacity_block(self, asset: str) -> Optional[str]:
        """Latched authoritative capacity rejection for this asset, if any."""

        return (self._entry_capacity_blocks.get("global")
                or self._entry_capacity_blocks.get(f"asset:{asset}"))

    def _clear_entry_capacity_blocks(self) -> None:
        """Capacity may have freed (a position closed); allow fresh attempts."""

        self._entry_capacity_blocks.clear()

    async def _create_shadow_entry(
        self, state: MarketState, calculation: EconomicCalculation,
        decision: RouterDecision, candidate_id: int, fair_id: int,
        decision_id: int, selected_snapshot: Optional[int], current: int,
    ) -> None:
        if state.window_id in self._entered_window_ids:
            return
        capacity_block = self._entry_capacity_block(state.identity.asset)
        if capacity_block is not None:
            # The store rejected capacity authoritatively and nothing has
            # closed since; do not submit another doomed journaled command.
            self._reject(state, "RISK", capacity_block, candidate_id=candidate_id)
            return
        blocked = self._execution_blocked_reason()
        committed_now = now_ms()
        if blocked:
            self._reject(state, "DATA_INVALID", blocked, candidate_id=candidate_id)
            return
        if not (state.identity.window_open_ms <= committed_now
                < state.identity.window_close_ms):
            self._reject(state, "EXECUTION", "window_not_open_at_commit",
                         candidate_id=candidate_id)
            return
        side = decision.side
        outcome = _outcome(side)
        if side is None or outcome is None or selected_snapshot is None:
            self._reject(state, "EXECUTION", "entry_evidence_incomplete",
                         candidate_id=candidate_id)
            return
        selected_result = (
            calculation.result.yes if side is EntrySide.BUY_YES
            else calculation.result.no)
        sweep = calculation.yes_sweep if side is EntrySide.BUY_YES else calculation.no_sweep
        fee_total = (
            calculation.yes_fee_total if side is EntrySide.BUY_YES
            else calculation.no_fee_total)
        if (sweep is None or fee_total is None or sweep.shares != self.cfg.fixed_shares
                or selected_result.net_edge is None or selected_result.net_edge <= 0.0
                or not selected_result.valid):
            self._reject(state, "ECONOMIC", "positive_ev_entry_gate_failed",
                         candidate_id=candidate_id)
            return
        selected_book = state.books.get(outcome)
        latest_cex = self.cex_features.latest(state.identity.asset)
        if (selected_book is None
                or selected_book.age_ms(committed_now) > self.cfg.book_max_age_ms
                or latest_cex is None
                or committed_now - latest_cex.provider_ts_ms > self.cfg.cex_max_age_ms):
            self._reject(state, "DATA_INVALID", "entry_evidence_stale_at_submit",
                         candidate_id=candidate_id)
            return
        commit_deadline = min(
            state.identity.window_close_ms - 1,
            committed_now + max(
                1,
                min(
                    1_000,
                    self.cfg.book_max_age_ms - selected_book.age_ms(committed_now),
                    self.cfg.cex_max_age_ms
                    - (committed_now - latest_cex.provider_ts_ms),
                ),
            ),
        )
        token_id = state.identity.token_for_side(side)
        idempotency = entry_idempotency_key(state.identity, side)
        try:
            committed = await self._critical_execute(
                "reserve_and_create_entry_bundle", {
                    "reservation": {
                        "window_id": state.window_id,
                        "session_id": self.session_id,
                        "market_identity_id": state.market_identity_id,
                        "owner_launch_nonce": self.runtime.launch_nonce,
                        "outcome_side": outcome,
                        "state": "RESERVED",
                        "idempotency_key": idempotency,
                        "candidate_id": candidate_id,
                        "decision_id": decision_id,
                        "reserved_ts_ms": committed_now,
                        "updated_ts_ms": committed_now,
                    },
                    "entry": {
                        "session_id": self.session_id,
                        "window_id": state.window_id,
                        "market_identity_id": state.market_identity_id,
                        "candidate_id": candidate_id,
                        "decision_id": decision_id,
                        "fair_value_calculation_id": fair_id,
                        "book_snapshot_id": selected_snapshot,
                        "outcome_side": outcome,
                        "token_id": token_id,
                        "shares": self.cfg.fixed_shares,
                        "entry_ts_ms": committed_now,
                        "entry_mode": (
                            "CROSS_SPREAD" if decision.state is None
                            else "MAKER_TO_CROSS"),
                        "executable_vwap": sweep.vwap,
                        "worst_consumed_price": sweep.worst_price,
                        "depth_shares": selected_result.depth_shares,
                        "gross_cost": sweep.notional,
                        "estimated_fee": fee_total,
                        "execution_buffer": selected_result.execution_buffer,
                        "latency_buffer": selected_result.latency_buffer,
                        "uncertainty_buffer": selected_result.uncertainty_buffer,
                        "selected_net_edge": selected_result.net_edge,
                        "execution_verified": 1,
                        "maker_fill_assumed": 0,
                        "idempotency_key": idempotency,
                        "status": "OPEN",
                    },
                    "max_concurrent_positions": self.cfg.max_open_positions,
                    "global_exposure_cap_usd": self.cfg.exposure_cap_usd,
                    "per_asset_exposure_cap_usd": self.cfg.exposure_cap_usd,
                    "max_open_per_asset": self.cfg.max_open_per_asset,
                    "commit_deadline_ts_ms": commit_deadline,
                },
                ordering_key=state.identity.window_key,
                # The store-level entry key remains the stable one-entry/window
                # invariant.  The journal key identifies this concrete evidence
                # attempt so a later fresh candidate cannot conflict with a
                # prior deadline-expired payload.
                idempotency_key=(
                    f"entry-command:{idempotency}:{decision_id}"
                ),
                command_type="ENTRY_CREATE",
                priority=5,
                associated_asset=state.identity.asset,
                associated_window_id=state.window_id,
                expected_store_errors=("entry commit deadline expired",),
            )
            entry_id = int(committed["entry_id"])
            self.counters["entries"] += 1
            self._entered_window_ids.add(state.window_id)
            self._open_position_windows.add(state.window_id)
            self._open_positions_count += 1
            self._update_window_funnel(
                state.window_id, committed_now,
                actual_entry=1, entry_ts_ms=committed_now)
            position_row = committed.get("position") or committed.get("position_row")
            if isinstance(position_row, dict):
                self._position_cache[int(position_row["position_id"])] = dict(position_row)
            elif self.read_worker is not None:
                row = await self.read_worker.query_one(
                    """SELECT p.*,e.window_id,e.gross_cost,e.estimated_fee,
                       e.executable_vwap,e.entry_ts_ms
                       FROM positions p JOIN entries e USING(entry_id)
                       WHERE e.entry_id=?""",
                    (entry_id,),
                )
                if row is not None:
                    self._position_cache[int(row["position_id"])] = row
            # The entry ID is deliberately not passed to any adapter: there is
            # no execution surface beyond this atomic shadow record.
            _ = entry_id
        except (WindowReservationConflict, ExposureLimitExceeded) as exc:
            reason = str(getattr(exc, "reason", None)
                         or str(exc) or type(exc).__name__)
            if isinstance(exc, ExposureLimitExceeded):
                # Capacity is a function of open positions only.  Latch the
                # rejection until any position closes instead of resubmitting
                # a full-rate stream of journaled commands into a hard cap.
                scope = (
                    f"asset:{state.identity.asset}"
                    if reason in {"per_asset_exposure_cap", "max_open_per_asset"}
                    else "global"
                )
                self._entry_capacity_blocks[scope] = reason
            self._reject(state, "RISK", reason, candidate_id=candidate_id)
        except V4StoreError as exc:
            if str(exc) != "entry commit deadline expired":
                raise
            state.last_candidate_fingerprint = ""
            self._reject(
                state, "EXECUTION", "entry_commit_deadline_expired",
                candidate_id=candidate_id,
            )

    async def _manage_open_position(
        self, state: MarketState, calculation: EconomicCalculation,
        fair_id: int, yes_snapshot: Optional[int], no_snapshot: Optional[int],
        current: int,
    ) -> None:
        entry = next(
            (row for row in self._position_cache.values()
             if int(row.get("window_id") or -1) == state.window_id
             and row.get("status") == "OPEN"),
            None,
        )
        if entry is None or current >= state.identity.window_close_ms:
            return
        position_id = int(entry["position_id"])
        if current - self._management_last_ts.get(position_id, 0) < 1_000:
            return
        side = EntrySide.BUY_YES if entry["outcome_side"] == "YES" else EntrySide.BUY_NO
        owned_book = state.books.get(str(entry["outcome_side"]))
        management = evaluate_exit_vs_hold(
            entry={
                "id": entry["entry_id"], "side": side.value,
                "entry_price": entry["executable_vwap"],
                "fair_probability": (
                    calculation.result.yes.fair_probability
                    if side is EntrySide.BUY_YES
                    else calculation.result.no.fair_probability),
            },
            identity=state.identity,
            fair_value=calculation.result,
            owned_book=owned_book,
            now_ms=current,
            cfg=self.cfg,
        )
        decision_seq = self._management_seq.get(position_id, 0)
        snapshot = yes_snapshot if side is EntrySide.BUY_YES else no_snapshot
        action = (
            "EXIT_BOOK" if management.action == "EXIT_BOOK"
            else "AWAIT_RESOLUTION" if management.action == "HOLD_OFFICIAL_RESOLUTION"
            else "HOLD"
        )
        decision_row = {
            "position_id": position_id,
            "decision_seq": decision_seq,
            "decision_ts_ms": current,
            "monotonic_ns": time.monotonic_ns(),
            "book_snapshot_id": snapshot,
            "fair_value_calculation_id": fair_id,
            "updated_fair_probability": management.fair_probability,
            "executable_exit_value": management.executable_exit_value,
            "hold_to_resolution_value": management.hold_expected_value,
            "remaining_time_ms": max(0, management.remaining_ms),
            "spread": management.spread,
            "depth_shares": management.depth_shares,
            "estimated_fee": management.exit_fee or 0.0,
            "uncertainty": management.uncertainty_usd,
            "thesis_state": management.thesis_state,
            "action": action,
            "reason": management.reason,
        }
        close_row: Optional[dict[str, Any]] = None
        if management.exit_selected and management.sweep is not None:
            entry_fees = float(entry["estimated_fee"] or 0.0)
            payout = management.sweep.notional
            gross = payout - float(entry["gross_cost"])
            exit_fee = float(management.exit_fee or 0.0)
            close_row = {
                "position_id": position_id,
                "exit_ts_ms": current,
                "exit_source": "BOOK",
                "book_snapshot_id": snapshot,
                "shares": self.cfg.fixed_shares,
                "executable_vwap": management.sweep.vwap,
                "worst_consumed_price": management.sweep.worst_price,
                "payout_usd": payout,
                "gross_pnl": gross,
                "exit_fee": exit_fee,
                "net_pnl": gross - entry_fees - exit_fee,
                "evidence_verified": 1,
                "resolution_outcome": None,
                "reason": management.reason,
            }
        management_payload = {"decision": decision_row, "close": close_row}
        await self._critical_execute(
            "management_bundle",
            management_payload,
            ordering_key=state.identity.window_key,
            idempotency_key=(
                f"management:{position_id}:{decision_seq}:"
                f"{_sha256_json(management_payload)[:20]}"
            ),
            command_type=("POSITION_EXIT" if close_row else "POSITION_MANAGEMENT"),
            priority=10 if close_row else 40,
            terminal=close_row is not None,
            associated_asset=state.identity.asset,
            associated_window_id=state.window_id,
            associated_trade_id=int(entry["entry_id"]),
        )
        self._management_last_ts[position_id] = current
        self._management_seq[position_id] = decision_seq + 1
        if close_row is not None:
            entry["status"] = "CLOSED"
            self._position_cache.pop(position_id, None)
            self._open_position_windows.discard(state.window_id)
            self._open_positions_count = max(0, self._open_positions_count - 1)
            self._clear_entry_capacity_blocks()

    async def _resolution_loop(self) -> None:
        while not self._stopping.is_set():
            current = now_ms()
            rows = (
                await self.read_worker.query(
                """SELECT p.position_id,p.entry_id,p.outcome_side,p.open_shares,
                   e.market_identity_id,e.gross_cost,e.estimated_fee,e.window_id,
                   w.asset,w.window_close_ts_ms
                   FROM positions p JOIN entries e ON e.entry_id=p.entry_id
                   JOIN asset_windows w ON w.window_id=e.window_id
                   WHERE p.status='OPEN' AND w.window_close_ts_ms<=?""",
                (current,),
                ) if self.read_worker is not None else []
            )
            for row in rows:
                try:
                    await self._resolve_position(row, current)
                except Exception as exc:
                    self._last_error = f"resolve:{type(exc).__name__}:{exc}"[:240]
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

    async def _identity_by_db_id(
        self, market_identity_id: int,
    ) -> Optional[MarketIdentity]:
        for state in self.markets.values():
            if state.market_identity_id == int(market_identity_id):
                return state.identity
        row = await self.read_worker.query_one(
            """SELECT m.asset,m.slug,m.polymarket_market_id,m.open_ts_ms,m.close_ts_ms,
               mi.event_id,mi.condition_id,mi.yes_token_id,mi.no_token_id
               FROM market_identities mi JOIN markets m ON m.market_id=mi.market_id
               WHERE mi.market_identity_id=?""",
            (int(market_identity_id),),
        ) if self.read_worker is not None else None
        if row is None:
            return None
        return MarketIdentity(
            asset=row["asset"], slug=row["slug"],
            market_id=row["polymarket_market_id"], event_id=row["event_id"],
            condition_id=row["condition_id"], yes_token_id=row["yes_token_id"],
            no_token_id=row["no_token_id"], window_open_ms=row["open_ts_ms"],
            window_close_ms=row["close_ts_ms"],
            anchor_status=AnchorStatus.FIELD_MISSING,
        )

    async def _resolve_position(self, row: dict[str, Any], current: int) -> None:
        identity = await self._identity_by_db_id(int(row["market_identity_id"]))
        if identity is None:
            self._reject(None, "RESOLUTION", "market_identity_missing")
            return
        entry_id = int(row["entry_id"])
        last_attempt = self._resolution_state.get(entry_id)
        if last_attempt is not None:
            exponent = min(max(0, int(last_attempt[0]) - 1), 6)
            retry_ms = min(300_000, 5_000 * (2 ** exponent))
            if current - int(last_attempt[1]) < retry_ms:
                return
        direct, event = await asyncio.gather(
            self.gamma.get_market(identity.market_id),
            self.gamma.get_event(identity.event_id),
            return_exceptions=True,
        )
        if isinstance(direct, BaseException):
            direct = None
        if isinstance(event, BaseException):
            event = None
        evidence = corroborated_resolution(
            identity=identity,
            direct_market=direct if isinstance(direct, dict) else None,
            event=event if isinstance(event, dict) else None,
        )
        attempt_no = (last_attempt[0] + 1) if last_attempt is not None else 1
        evidence_hash = _sha256_json({"direct": direct, "event": event})
        attempt_row = {
            "entry_id": entry_id,
            "attempt_no": attempt_no,
            "attempt_ts_ms": current,
            "source": "GAMMA_DIRECT_AND_EVENT",
            "result": "RESOLVED" if evidence.verified else "PENDING",
            "observed_outcome": evidence.outcome,
            "evidence_hash": evidence_hash,
            "verified": int(evidence.verified),
            "error": None if evidence.verified else evidence.reason,
        }
        close_row: Optional[dict[str, Any]] = None
        if evidence.verified and evidence.outcome in {"YES", "NO"}:
            payout = (
                self.cfg.fixed_shares
                if evidence.outcome == row["outcome_side"] else 0.0
            )
            gross = payout - float(row["gross_cost"])
            entry_fee = float(row["estimated_fee"] or 0.0)
            close_row = {
                "position_id": row["position_id"],
                "exit_ts_ms": current,
                "exit_source": "OFFICIAL_RESOLUTION",
                "book_snapshot_id": None,
                "shares": self.cfg.fixed_shares,
                "executable_vwap": None,
                "worst_consumed_price": None,
                "payout_usd": payout,
                "gross_pnl": gross,
                "exit_fee": 0.0,
                "net_pnl": gross - entry_fee,
                "evidence_verified": 1,
                "resolution_outcome": evidence.outcome,
                "reason": "official_corroborated_resolution",
            }
        resolution_payload = {"attempt": attempt_row, "close": close_row}
        await self._critical_execute(
            "resolution_bundle",
            resolution_payload,
            ordering_key=identity.window_key,
            idempotency_key=(
                f"resolution:{entry_id}:{attempt_no}:"
                f"{_sha256_json(resolution_payload)[:20]}"
            ),
            command_type=("OFFICIAL_RESOLUTION" if close_row else "RESOLUTION_RETRY"),
            priority=1 if close_row else 25,
            terminal=close_row is not None,
            associated_asset=identity.asset,
            associated_window_id=int(row.get("window_id") or 0) or None,
            associated_trade_id=entry_id,
        )
        self.counters["resolution_attempts"] += 1
        self._resolution_state[entry_id] = (attempt_no, current)
        if close_row is None:
            self._reject(None, "RESOLUTION", evidence.reason, recoverable=True,
                         detail={"entry_id": entry_id})
            return
        position_id = int(row["position_id"])
        window_id = int(row.get("window_id") or 0)
        self._position_cache.pop(position_id, None)
        if window_id:
            self._open_position_windows.discard(window_id)
        self._open_positions_count = max(0, self._open_positions_count - 1)
        self._clear_entry_capacity_blocks()

    async def _discovery_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.discover_once()
            except Exception as exc:
                self._last_error = f"discovery:{type(exc).__name__}:{exc}"[:240]
                self._reject(None, "DISCOVERY", f"loop_{type(exc).__name__}",
                             recoverable=True)
            try:
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=self.cfg.discovery_interval_s)
            except asyncio.TimeoutError:
                pass

    def _execution_blocked_reason(self) -> str:
        """Fail-closed guard for new executable candidates.

        Blocks execution when the runtime cannot be trusted: a failed integrity
        scan (possible corruption) or event-loop starvation (evidence may be
        stale and time-sensitive routing unsafe).  Telemetry and recovery keep
        running; only executable candidate creation is suppressed.
        """

        if self.persistence is None:
            return "critical_writer_not_started"
        ownership = self._process_ownership_cache
        if not bool(ownership.get("process_ownership_valid")):
            return "critical_process_ownership_unverified"
        if int(ownership.get("orphan_processes") or 0) != 0:
            return "critical_orphan_process_detected"
        # A Windows venv launcher runs the real interpreter as its child with
        # an identical exact-module command line, so one owned launch may
        # appear as two exact processes.  Every exact process must belong to
        # the ownership tree (orphans are rejected above).
        exact_count = int(ownership.get("exact_v4_processes") or 0)
        owned_count = int(ownership.get("owned_v4_processes") or 0)
        if exact_count < 1 or owned_count != exact_count:
            return "critical_process_count_mismatch"
        writer = self._writer_health()
        if str(writer.get("state") or "") != "HEALTHY":
            return "critical_writer_unhealthy"
        if self._critical_failure_reason:
            return "critical_command_failed"
        # Only genuinely trade-critical unconfirmed commands (reservations,
        # entries, position transitions, exits, resolution, maker evidence)
        # gate new entries.  Evidence-only bundles are materialized in their
        # authoritative tables and must not throttle execution.  Writers that
        # do not expose the scoped count stay fully fail-closed.
        unconfirmed = writer.get("unconfirmed_trade_critical_count")
        if unconfirmed is None:
            unconfirmed = writer.get("unconfirmed_command_count")
        if int(unconfirmed or 0) > 0:
            return "critical_command_unconfirmed"
        reconciliation = writer.get("recovery_reconciliation")
        reconciliation = reconciliation if isinstance(reconciliation, dict) else {}
        unfinished_makers = int(
            writer.get("unfinished_makers_left_fail_closed")
            or writer.get("orphan_maker_observation_count")
            or reconciliation.get("unfinished_makers_left_fail_closed")
            or 0
        )
        if unfinished_makers > 0:
            return "critical_unfinished_maker_evidence"
        heartbeat_age_ms = writer.get("heartbeat_age_ms")
        if (heartbeat_age_ms is not None
                and int(heartbeat_age_ms) > self.cfg.writer_failure_timeout_ms):
            return "critical_writer_heartbeat_stale"
        for label, worker in (
            ("operational_read_worker", self.read_worker),
            ("reporting_read_worker", self.report_worker),
        ):
            if worker is None:
                return f"{label}_not_started"
            health = worker.health()
            if str(health.get("state") or "") != "RUNNING" or not bool(
                    health.get("thread_alive")):
                return f"{label}_unhealthy"
        if self._last_integrity_ok is None:
            return "sqlite_integrity_unknown"
        if self._last_integrity_ok is False:
            return "sqlite_integrity_degraded"
        if (not self._last_integrity_ts_ms
                or now_ms() - self._last_integrity_ts_ms > INTEGRITY_MAX_AGE_MS):
            return "sqlite_integrity_stale"
        if self._loop_lag_ms > self.cfg.loop_lag_safety_ms:
            return "event_loop_lag_degraded"
        if (self._loop_lag_safety_breach_mono_ns
                and time.monotonic_ns() - self._loop_lag_safety_breach_mono_ns
                < 5_000_000_000):
            return "event_loop_lag_degraded"
        return ""

    def _runtime_state_name(self, current: int) -> str:
        """Truthful runtime state, fail-closed first.

        Integrity or event-loop degradation outrank socket/freshness status so a
        connected-but-unsafe runtime can never read healthy.
        """

        blocked = self._execution_blocked_reason()
        if blocked in {
            "sqlite_integrity_unknown", "sqlite_integrity_degraded",
            "sqlite_integrity_stale", "reporting_read_worker_not_started",
            "reporting_read_worker_unhealthy", "operational_read_worker_not_started",
            "operational_read_worker_unhealthy",
        }:
            return "DEGRADED_INTEGRITY"
        if blocked.startswith("critical_"):
            return "DEGRADED_PERSISTENCE"
        if blocked == "event_loop_lag_degraded":
            return "DEGRADED_EVENT_LOOP_LAG"
        active_assets = {
            state.identity.asset for state in self.markets.values()
            if state.identity.window_open_ms <= current < state.identity.window_close_ms
        }
        fresh_assets = {
            asset for asset in active_assets
            if (latest := self.cex_features.latest(asset)) is not None
            and 0 <= current - latest.provider_ts_ms <= self.cfg.cex_max_age_ms
        }
        if not self._okx_connected or not fresh_assets:
            return "DEGRADED_NO_FRESH_CEX"
        if fresh_assets != active_assets:
            return "DEGRADED_PARTIAL_CEX"
        if not self._poly_connected:
            return "DEGRADED_POLYMARKET_DISCONNECTED"
        return "RUNNING"

    async def _heartbeat_export_loop(self) -> None:
        # Deliberately cheap: only the small state.json/heartbeat publish and
        # runtime-health insert run here every ~2s.  All whole-database scans
        # (dashboard export, integrity) and maintenance run off-loop in their
        # own workers so this cadence stays responsive.
        last_health_ms = 0
        expected_mono = time.monotonic()
        while not self._stopping.is_set():
            loop_lag_ms = max(
                0.0, (time.monotonic() - expected_mono) * 1_000.0,
                self._loop_lag_ms)
            current = now_ms()
            self._flush_event_counts()
            self._persist_pending_source_health(current)
            if current - last_health_ms >= 5_000 and self.runtime_io_worker is not None:
                try:
                    await self._refresh_runtime_probe()
                except Exception as exc:
                    self._last_error = (
                        f"runtime_probe:{type(exc).__name__}:{exc}"
                    )[:240]
            state_name = self._runtime_state_name(current)
            state_payload = self._runtime_state(state_name)
            if self.runtime_io_worker is not None:
                runtime_state = await self.runtime_io_worker.run_io(
                    self.runtime.publish,
                    state_payload,
                    timeout_s=10.0,
                    name="runtime_publish",
                )
            else:
                runtime_state = state_payload
            self._last_published_state = runtime_state
            if current - last_health_ms >= 5_000:
                writer = self._writer_health()
                db_writes_per_min = int(
                    writer.get("transactions_per_minute")
                    or writer.get("transaction_rate_per_min")
                    or 0
                )
                self._telemetry_submit("record_runtime_health", {
                    "session_id": self.session_id,
                    "sample_ts_ms": current,
                    "heartbeat_ts_ms": current,
                    "pid": self.runtime.pid,
                    "state": state_name,
                    "loop_lag_ms": loop_lag_ms,
                    "db_writes_per_min": db_writes_per_min,
                    "db_size_bytes": self._database_size_cache,
                    "open_positions": runtime_state["open_positions"],
                    "last_error": self._last_error or None,
                }, state_key=("runtime-health", self.session_id, state_name),
                    state_value={
                        "loop_lag_bucket": int(loop_lag_ms // 25),
                        "writer_state": writer.get("state"),
                        "open_positions": runtime_state["open_positions"],
                        "last_error": self._last_error or None,
                    })
                last_health_ms = current
            expected_mono = time.monotonic() + 2.0
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass

    async def _run_integrity_check(self) -> None:
        """Off-loop integrity scan on the read-only connection (single-flight)."""
        if self.report_worker is None or self._integrity_inflight:
            return
        self._integrity_inflight = True
        started = time.monotonic()
        try:
            result = await self.report_worker.run_report(
                lambda store: store.integrity_check(),
                timeout_s=self.cfg.reporting_worker_timeout_s,
                name="sqlite_integrity_check",
            )
            self._last_integrity = result
            self._last_integrity_ts_ms = now_ms()
            # A reported problem (corruption / FK violation) fails closed.
            self._last_integrity_ok = bool(
                result.get("integrity") == "ok"
                and not result.get("foreign_key_violations"))
            if not self._last_integrity_ok:
                self._last_error = f"integrity_degraded:{result.get('integrity')}"[:240]
        except Exception as exc:  # noqa: BLE001 - reporting worker must not crash
            self._last_integrity_ok = None
            self._last_integrity = {
                "integrity": "UNKNOWN",
                "foreign_key_violations": [],
                "error": f"{type(exc).__name__}:{exc}"[:200],
            }
            self._last_error = f"integrity:{type(exc).__name__}:{exc}"[:240]
        finally:
            self._last_integrity_duration_ms = (time.monotonic() - started) * 1_000.0
            self._integrity_runs += 1
            self._integrity_inflight = False

    async def _run_dashboard_export(self) -> None:
        """Off-loop whole-database dashboard export (single-flight, atomic write)."""
        if self.report_worker is None or self._export_inflight:
            return
        self._export_inflight = True
        started = time.monotonic()
        current = now_ms()
        state = self._last_published_state or self._runtime_state("RUNNING")
        try:
            await self.report_worker.run_report(
                write_frequency_v4_dashboard,
                self.export_path,
                timeout_s=self.cfg.reporting_worker_timeout_s,
                name="dashboard_export",
                now_ms=current,
                config=self.cfg,
                runtime_state=state,
                session_id=self.session_id,
                integrity=self._last_integrity or None,
            )
            self._last_export_ms = current
            self._last_export_ok = True
        except Exception as exc:  # noqa: BLE001 - reporting worker must not crash
            self._last_export_ok = False
            self._last_error = f"export:{type(exc).__name__}:{exc}"[:240]
        finally:
            self._last_export_duration_ms = (time.monotonic() - started) * 1_000.0
            self._export_runs += 1
            self._export_inflight = False

    async def _reporting_loop(self) -> None:
        """Run integrity then export off-loop, sequentially (never overlapping).

        Each heavy read-only scan executes in a worker thread against the
        dedicated read-only connection; awaiting the thread yields the event
        loop, so the WebSocket heartbeat/reconnect path stays responsive while a
        multi-second scan runs.
        """
        last_export_ms = 0
        # ``start`` already ran one full integrity scan.  Preserve its schedule
        # instead of immediately repeating the expensive 431 MB read.
        last_integrity_ms = self._last_integrity_ts_ms
        while not self._stopping.is_set():
            current = now_ms()
            if current - last_integrity_ms >= INTEGRITY_CHECK_INTERVAL_MS:
                last_integrity_ms = current
                await self._run_integrity_check()
            if current - last_export_ms >= DASHBOARD_EXPORT_INTERVAL_MS:
                last_export_ms = current
                await self._run_dashboard_export()
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    async def _run_maintenance_pass(self) -> None:
        """Run policy-gated checkpoint/retention on its owner connection."""
        if self._maintenance_inflight or self.maintenance_worker is None:
            return
        self._maintenance_inflight = True
        started = time.monotonic()
        try:
            current = now_ms()
            writer = self._writer_health()
            telemetry = self.telemetry.snapshot() if self.telemetry is not None else {}
            reporting = (
                self.report_worker.health()
                if self.report_worker is not None else {}
            )
            reporting_active = bool(reporting.get("current_job"))
            reporting_duration = float(reporting.get("current_duration_ms") or 0.0)
            runtime_health = self._runtime_state_name(current)
            snapshot = MaintenanceSnapshot(
                now_ms=current,
                wal_bytes=max(0, int(self._wal_size_cache)),
                critical_queue_depth=max(0, int(writer.get("queue_depth") or 0)),
                telemetry_queue_depth=max(0, int(telemetry.get("queue_depth") or 0)),
                runtime_active=True,
                runtime_health=(
                    "HEALTHY" if runtime_health == "RUNNING" else runtime_health
                ),
                writer_healthy=writer.get("state") == "HEALTHY",
                open_positions=max(0, int(self._open_positions_count)),
                active_readers=int(reporting_active),
                long_reader_count=int(
                    reporting_active
                    and reporting_duration > self.cfg.reporting_worker_timeout_s * 1_000
                ),
                critical_commit_p95_ms=writer.get("commit_latency_p95_ms"),
                time_to_window_boundary_ms=(
                    300_000 - current % 300_000
                ),
                last_checkpoint_attempt_ts_ms=(
                    self._checkpoint_state.get("started_ts_ms")
                    or self._checkpoint_state.get("completed_ts_ms")
                ),
                last_successful_checkpoint_ts_ms=(
                    self._checkpoint_state.get("completed_ts_ms")
                    if self._checkpoint_state.get("successful") else None
                ),
            )
            policy = MaintenancePolicy(
                wal_trigger_bytes=self.cfg.checkpoint_wal_size_trigger_bytes,
                restart_trigger_bytes=max(
                    self.cfg.checkpoint_wal_size_trigger_bytes * 4,
                    self.cfg.checkpoint_wal_size_trigger_bytes,
                ),
                truncate_trigger_bytes=max(
                    self.cfg.checkpoint_wal_size_trigger_bytes * 4,
                    self.cfg.checkpoint_wal_size_trigger_bytes,
                ),
                checkpoint_min_interval_ms=self.cfg.checkpoint_min_interval_s * 1_000,
                retention_ms=self.cfg.raw_event_retention_hours * 3_600_000,
                retention_chunk_rows=min(
                    self.cfg.retention_chunk_size,
                    self.cfg.maintenance_chunk_rows,
                ),
                retention_row_budget=self.cfg.maintenance_max_rows_per_pass,
                retention_time_budget_ms=min(
                    self.cfg.retention_time_budget_ms,
                    max(1, int(
                        self.cfg.maintenance_max_seconds_per_pass * 1_000
                    )),
                ),
                raw_event_max_rows=self.cfg.raw_event_max_rows,
                event_bucket_detail_retention_ms=min(
                    self.cfg.raw_event_retention_hours * 3_600_000,
                    15 * 60_000,
                ),
                metadata_retention_ms=(
                    self.cfg.raw_event_retention_hours * 3_600_000
                ),
                metadata_max_rows=self.cfg.raw_event_max_rows,
                journal_payload_retention_ms=(
                    self.cfg.raw_event_retention_hours * 3_600_000
                ),
            )
            result = await self.maintenance_worker.run_maintenance(
                run_bounded_maintenance_pass,
                snapshot=snapshot,
                policy=policy,
                timeout_s=self.cfg.maintenance_worker_timeout_s,
                name="bounded_checkpoint_retention",
            )
            result_view = result.as_dict()
            self._maintenance_result = dict(result_view)
            self._last_maintenance_rows = int(result_view.get("rows_deleted") or 0)
            checkpoint = result_view.get("checkpoint")
            if isinstance(checkpoint, dict):
                self._checkpoint_state = dict(checkpoint)
                self._wal_size_cache = int(
                    checkpoint.get("after_wal_bytes") or self._wal_size_cache
                )
        except Exception as exc:  # noqa: BLE001 - maintenance must not crash market-data tasks
            self._maintenance_result = {
                "status": "FAILED",
                "reason": "maintenance_worker_failure",
                "failure_reason": f"{type(exc).__name__}:{exc}"[:200],
            }
            self._last_error = f"maintenance:{type(exc).__name__}:{exc}"[:240]
        finally:
            self._last_maintenance_duration_ms = (time.monotonic() - started) * 1_000.0
            self._maintenance_runs += 1
            self._maintenance_inflight = False

    async def _maintenance_loop(self) -> None:
        while not self._stopping.is_set():
            await self._run_maintenance_pass()
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                pass

    async def _loop_lag_monitor(self) -> None:
        """Sample event-loop scheduling drift to expose blocking regressions.

        A short fixed sleep should return almost exactly on time; the excess is
        time the single event loop spent unable to schedule this coroutine —
        i.e. blocked in synchronous work somewhere.  This is the direct signal
        that the ingestion decoupling is doing its job.
        """

        interval = 0.1
        threshold = float(self.cfg.loop_lag_threshold_ms)
        expected = time.monotonic() + interval
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass
            now = time.monotonic()
            lag_ms = max(0.0, (now - expected) * 1_000.0)
            expected = now + interval
            self._loop_lag_ms = lag_ms
            self._loop_lag_max_ms = max(self._loop_lag_max_ms, lag_ms)
            if lag_ms > threshold:
                self._loop_lag_breaches += 1
                self._loop_lag_last_breach_ms = now_ms()
            if lag_ms > self.cfg.loop_lag_safety_ms:
                # Fail-closed backstop: recent severe starvation blocks new
                # executable candidates until it clears (see
                # _execution_blocked_reason).
                self._loop_lag_safety_breach_mono_ns = time.monotonic_ns()
            self._loop_lag_samples.append(lag_ms)
            ordered = sorted(self._loop_lag_samples)
            index = min(len(ordered) - 1, int(0.95 * len(ordered)))
            self._loop_lag_p95_ms = ordered[index]

    async def _restore_persistence_caches(self) -> None:
        """Load committed lifecycle state through the owner read worker."""

        if self.read_worker is None:
            raise RuntimeError("read worker is not started")
        entered, positions, management, resolutions = await asyncio.gather(
            self.read_worker.query(
                "SELECT entry_id,window_id FROM entries"
            ),
            self.read_worker.query(
                """SELECT p.*,e.window_id,e.market_identity_id,e.gross_cost,
                   e.estimated_fee,e.executable_vwap,e.entry_ts_ms,e.outcome_side
                   FROM positions p JOIN entries e USING(entry_id)
                   WHERE p.status='OPEN'"""
            ),
            self.read_worker.query(
                """SELECT position_id,MAX(decision_seq)+1 AS next_seq,
                   MAX(decision_ts_ms) AS last_ts
                   FROM management_decisions GROUP BY position_id"""
            ),
            self.read_worker.query(
                """SELECT r.entry_id,r.attempt_no,r.attempt_ts_ms
                   FROM resolution_attempts r JOIN (
                     SELECT entry_id,MAX(attempt_no) AS attempt_no
                     FROM resolution_attempts GROUP BY entry_id
                   ) latest ON latest.entry_id=r.entry_id
                   AND latest.attempt_no=r.attempt_no"""
            ),
        )
        self._entered_window_ids = {
            int(row["window_id"]) for row in entered
        }
        self._position_cache = {
            int(row["position_id"]): dict(row) for row in positions
        }
        self._open_position_windows = {
            int(row["window_id"]) for row in positions
        }
        self._open_positions_count = len(positions)
        self._clear_entry_capacity_blocks()
        self._management_seq = {
            int(row["position_id"]): int(row.get("next_seq") or 0)
            for row in management
        }
        self._management_last_ts = {
            int(row["position_id"]): int(row.get("last_ts") or 0)
            for row in management
        }
        self._resolution_state = {
            int(row["entry_id"]): (
                int(row["attempt_no"]), int(row["attempt_ts_ms"])
            ) for row in resolutions
        }

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("Frequency V4 engine already started")
        verified_ownership = getattr(
            self.runtime, "verified_process_ownership", None
        )
        if not isinstance(verified_ownership, dict):
            raise RuntimeError(
                "Frequency V4 process ownership was not verified before DB startup"
            )
        self._process_ownership_cache = dict(verified_ownership)
        self._started = True
        self.persistence = V4PersistenceWriter(
            self.cfg.db_path,
            queue_capacity=self.cfg.critical_queue_capacity,
            busy_timeout_ms=self.cfg.sqlite_busy_timeout_ms,
            sample_interval_s=self.cfg.writer_heartbeat_interval_ms / 1_000.0,
            checkpoint_on_close=False,
            current_launch_nonce=self.runtime.launch_nonce,
            proven_absent_launch_nonces=(
                getattr(self.runtime, "proven_absent_launch_nonces", ())
            ),
        )
        # The writer opens/migrates SQLite on its dedicated thread. Waiting for
        # startup uses one bounded isolated executor call before sockets start.
        await asyncio.to_thread(
            self.persistence.start, self.cfg.critical_command_timeout_s)
        self.telemetry = V4TelemetryWriter(
            self.persistence,
            capacity=self.cfg.telemetry_queue_capacity,
            batch_size=self.cfg.telemetry_batch_size,
            flush_interval_s=self.cfg.telemetry_flush_interval_ms / 1_000.0,
            coalescing_interval_s=(
                self.cfg.telemetry_coalescing_interval_ms / 1_000.0),
            submit_timeout_s=min(5.0, self.cfg.critical_command_timeout_s),
            heartbeat_interval_s=(
                self.cfg.writer_heartbeat_interval_ms / 1_000.0),
        )
        self.telemetry.start()
        self.read_worker = V4ReadWorker(
            self.cfg.db_path,
            worker_name="lite-frequency-v4-operational-read-worker",
            worker_kind="OPERATIONAL_READ",
            queue_capacity=self.cfg.reporting_queue_capacity,
            busy_timeout_ms=self.cfg.sqlite_busy_timeout_ms,
            default_timeout_s=self.cfg.reporting_worker_timeout_s,
        )
        self.report_worker = V4ReadWorker(
            self.cfg.db_path,
            worker_name="lite-frequency-v4-report-read-worker",
            worker_kind="READ_REPORT",
            queue_capacity=self.cfg.reporting_queue_capacity,
            busy_timeout_ms=self.cfg.sqlite_busy_timeout_ms,
            default_timeout_s=self.cfg.reporting_worker_timeout_s,
        )
        self.maintenance_worker = V4MaintenanceWorker(
            self.cfg.db_path,
            queue_capacity=self.cfg.maintenance_queue_capacity,
            busy_timeout_ms=self.cfg.sqlite_busy_timeout_ms,
            default_timeout_s=self.cfg.maintenance_worker_timeout_s,
            background_write_admission=(
                self.persistence.try_acquire_background_write
            ),
            background_write_release=(
                self.persistence.release_background_write
            ),
        )
        self.runtime_io_worker = V4RuntimeIOWorker(
            queue_capacity=max(8, self.cfg.reporting_queue_capacity),
            default_timeout_s=10.0,
        )
        await asyncio.gather(
            self.read_worker.start_async(
                timeout_s=self.cfg.reporting_worker_timeout_s),
            self.report_worker.start_async(
                timeout_s=self.cfg.reporting_worker_timeout_s),
            self.maintenance_worker.start_async(
                timeout_s=self.cfg.maintenance_worker_timeout_s),
            self.runtime_io_worker.start_async(timeout_s=10.0),
        )
        # Ownership is an execution invariant, not delayed telemetry.  Verify it
        # before discovery or source startup can create an executable candidate.
        await self._refresh_runtime_probe()
        await self._record_session()
        await self._restore_persistence_caches()
        # Fail closed until this initial read-worker scan establishes a known
        # integrity state. Sources still start when clean so recovery remains
        # observable; executable candidates are gated by the result.
        await self._run_integrity_check()
        starting_state = self._runtime_state("STARTING")
        self._last_published_state = await self.runtime_io_worker.run_io(
            self.runtime.publish,
            starting_state,
            timeout_s=10.0,
            name="runtime_publish_starting",
        )
        await self.discover_once()
        await self.poly_ws.start()
        await self.okx.start()
        self._tasks = [
            asyncio.create_task(
                self._polymarket_ingest_loop(), name="v4-polymarket-ingest"),
            asyncio.create_task(
                self._cex_ingest_loop(), name="v4-cex-ingest"),
            asyncio.create_task(self._evaluation_loop(), name="v4-evaluation"),
            asyncio.create_task(self._discovery_loop(), name="v4-discovery"),
            asyncio.create_task(
                self._active_subscription_loop(), name="v4-active-subscriptions"),
            asyncio.create_task(self._heartbeat_export_loop(), name="v4-heartbeat-export"),
            asyncio.create_task(self._reporting_loop(), name="v4-reporting"),
            asyncio.create_task(self._loop_lag_monitor(), name="v4-loop-lag"),
            asyncio.create_task(self._resolution_loop(), name="v4-resolution"),
            asyncio.create_task(self._maintenance_loop(), name="v4-maintenance"),
            asyncio.create_task(self.okx.hydrate_all(), name="v4-okx-rest-hydration"),
        ]

    async def run_until_stopped(self) -> None:
        await self.start()
        while not self._stopping.is_set():
            stop_requested = False
            if self.runtime_io_worker is not None:
                stop_requested = bool(await self.runtime_io_worker.run_io(
                    self.runtime.stop_requested,
                    timeout_s=2.0,
                    name="runtime_stop_requested",
                ))
            if stop_requested:
                self._stopping.set()
                break
            for task in tuple(self._tasks):
                if task.done() and task.get_name() != "v4-okx-rest-hydration":
                    error = task.exception()
                    if error is not None:
                        raise RuntimeError(
                            f"critical Frequency V4 task failed: {task.get_name()}") from error
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                pass

    async def stop(self, reason: str = "graceful_stop") -> None:
        if self._stop_complete:
            return
        self._stopping.set()
        drain_timeout = float(self.cfg.shutdown_drain_timeout_s)
        try:
            if self.runtime_io_worker is not None:
                try:
                    await self.runtime_io_worker.run_io(
                        self.runtime.publish,
                        self._runtime_state("STOPPING"),
                        timeout_s=10.0,
                        name="runtime_publish_stopping",
                    )
                except Exception as exc:
                    self._last_error = (
                        f"stopping_publish:{type(exc).__name__}:{exc}"
                    )[:240]

            # Stop producers first, then drain only already-admitted raw data.
            await self.poly_ws.stop()
            await self.okx.stop()
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        self._polymarket_ingest_queue.join(),
                        self._cex_ingest_queue.join(),
                    ),
                    timeout=drain_timeout,
                )
                self._shutdown_drain_timed_out = False
            except asyncio.TimeoutError:
                self._shutdown_drain_timed_out = True
            self._polymarket_queue_discarded = self._polymarket_ingest_queue.qsize()
            self._cex_queue_discarded = self._cex_ingest_queue.qsize()
            if self._shutdown_drain_timed_out and (
                    self._polymarket_queue_discarded
                    or self._cex_queue_discarded):
                self._last_error = (
                    "shutdown_drain_timeout_discarded_"
                    f"poly{self._polymarket_queue_discarded}_"
                    f"cex{self._cex_queue_discarded}"
                )

            for task in self._tasks:
                if not task.done():
                    task.cancel()
            for task in tuple(self._background_tasks):
                if not task.done():
                    task.cancel()
            pending = [*self._tasks, *tuple(self._background_tasks)]
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            self._background_tasks.clear()

            self._flush_event_counts()
            if self.telemetry is not None:
                drained = bool(await asyncio.to_thread(
                    self.telemetry.stop,
                    drain=True,
                    timeout_s=max(drain_timeout, 10.0),
                ))
                self._telemetry_shutdown_ok = drained
                if not drained:
                    # Raw telemetry may be discarded, never critical trade
                    # evidence.  A second bounded non-draining stop is allowed
                    # solely to guarantee that its owner connection is closed
                    # before final DB/export verification.
                    forced = bool(await asyncio.to_thread(
                        self.telemetry.stop,
                        drain=False,
                        timeout_s=max(drain_timeout, 10.0),
                    ))
                    self._last_error = "telemetry_shutdown_forced_after_drain_timeout"
                    if not forced:
                        self._critical_failure_reason = "telemetry_shutdown_timeout"
                        raise V4PersistenceError(
                            "telemetry owner thread did not stop before verification"
                        )

            if self.persistence is not None:
                await self._critical_execute(
                    "end_runtime_session",
                    self.session_id,
                    now_ms(),
                    reason,
                    ordering_key="global",
                    idempotency_key=f"session-end:{self.session_id}",
                    command_type="SESSION_TERMINAL",
                    priority=0,
                    terminal=True,
                )

            final_state = self._runtime_state("STOPPED")
            if self.report_worker is not None:
                try:
                    await self.report_worker.run_report(
                        write_frequency_v4_dashboard,
                        self.export_path,
                        timeout_s=self.cfg.reporting_worker_timeout_s,
                        name="final_dashboard_export",
                        now_ms=now_ms(),
                        config=self.cfg,
                        runtime_state=final_state,
                        session_id=self.session_id,
                        integrity=self._last_integrity or None,
                    )
                except Exception as exc:
                    self._last_error = (
                        f"final_export:{type(exc).__name__}:{exc}"
                    )[:240]
            if self.runtime_io_worker is not None:
                try:
                    self._last_published_state = await self.runtime_io_worker.run_io(
                        self.runtime.publish,
                        final_state,
                        timeout_s=10.0,
                        name="runtime_publish_stopped",
                    )
                except Exception as exc:
                    self._last_error = (
                        f"stopped_publish:{type(exc).__name__}:{exc}"
                    )[:240]
        finally:
            await self.gamma.close()
            await self.clob.close()
            for worker in (
                self.maintenance_worker, self.report_worker, self.read_worker,
            ):
                if worker is not None:
                    try:
                        await worker.stop_async(timeout_s=max(drain_timeout, 10.0))
                    except Exception as exc:
                        self._last_error = (
                            f"worker_stop:{type(exc).__name__}:{exc}"
                        )[:240]
            if self.persistence is not None:
                try:
                    await self.persistence.aclose(
                        timeout_s=max(drain_timeout, 15.0))
                except Exception as exc:
                    self._last_error = (
                        f"persistence_stop:{type(exc).__name__}:{exc}"
                    )[:240]
            if self.runtime_io_worker is not None:
                try:
                    await self.runtime_io_worker.stop_async(
                        timeout_s=max(drain_timeout, 10.0))
                except Exception as exc:
                    self._last_error = (
                        f"runtime_io_stop:{type(exc).__name__}:{exc}"
                    )[:240]
            self._stop_complete = True
