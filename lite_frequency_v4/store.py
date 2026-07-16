"""Normalized, isolated SQLite persistence for Lite Frequency V4.

The store is deliberately fresh-schema-only.  It never imports or migrates the
legacy Lite/Advanced stores and accepts a database path only from its caller.
All trading rows are shadow evidence; there is no order-placement surface.
"""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import asdict, is_dataclass
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional

from .universe import evaluate_persisted_market


STRATEGY_ID = "lite_frequency_v4"
MODE = "lite_frequency_v4_shadow"
FIXED_SHARES = 5.0
SCHEMA_VERSION = 3

# Conservative floor for the per-position exit-fee reservation: the locked
# crypto taker curve 5 * 0.07 * p * (1-p) peaks at p = 0.5 -> $0.0875.
MIN_EXIT_FEE_BUFFER_USD = 0.0875

# Phase 1 cohort identity.  Kept in sync with lite_frequency_v4.config; the
# store keeps its own copies so persistence stays import-light.
ACTIVE_COHORT = "dynamic_universe_phase1_post_activation"
LEGACY_COHORT = "legacy_mixed_universe"

_SECRET_KEY_RE = re.compile(
    r"(?i)(private_key|api_secret|api_key|passphrase|password|bot_token|"
    r"auth_header|signed_payload|wallet|cookie)"
)


class V4StoreError(RuntimeError):
    """Base class for fail-closed persistence errors."""


class V4SchemaError(V4StoreError):
    """Raised when a non-v4 or incompatible database is supplied."""


class WindowReservationConflict(V4StoreError):
    """Raised when one logical asset/window already has an owner or entry."""

    def __init__(self, reason: str, existing: Optional[dict[str, Any]] = None):
        super().__init__(reason)
        self.reason = reason
        self.existing = existing or {}


class ExposureLimitExceeded(V4StoreError):
    """Raised when an atomic shadow entry would exceed a configured cap."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class UniverseEligibilityError(V4StoreError):
    """Raised when the canonical universe policy rejects an entry's market."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class V4BackgroundWriteDeferred(V4StoreError):
    """Raised when a critical-first gate declines a background write."""


def _record(value: Any) -> dict[str, Any]:
    """Return a plain mapping for dicts, dataclasses, and simple objects."""
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, "model_dump"):
        return dict(value.model_dump())
    if hasattr(value, "__dict__"):
        return {key: val for key, val in vars(value).items() if not key.startswith("_")}
    raise TypeError(f"unsupported record type: {type(value).__name__}")


def _clean(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if value is None or isinstance(value, (str, int, bytes)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite numeric evidence is forbidden")
        return value
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if hasattr(value, "value"):
        return _clean(value.value)
    return str(value)


def _canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str,
                     ensure_ascii=True, allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def stable_idempotency_key(*parts: Any) -> str:
    """Stable, non-random idempotency key for persistent reservations."""
    return _canonical_hash(["lite-frequency-v4", *parts])


PERSISTENCE_SCHEMA_V2_SQL = r"""
CREATE TABLE IF NOT EXISTS persistence_commands (
    persistence_command_id INTEGER PRIMARY KEY,
    command_id TEXT NOT NULL UNIQUE,
    command_type TEXT NOT NULL,
    method TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    ordering_key TEXT NOT NULL,
    priority INTEGER NOT NULL CHECK(priority >= 0),
    terminal INTEGER NOT NULL DEFAULT 0 CHECK(terminal IN (0,1)),
    associated_asset TEXT,
    associated_window_id INTEGER,
    associated_trade_id INTEGER,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('SUBMITTED','EXECUTING','COMMITTED','FAILED')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    submitted_ts_ms INTEGER NOT NULL CHECK(submitted_ts_ms >= 0),
    started_ts_ms INTEGER,
    committed_ts_ms INTEGER,
    completed_ts_ms INTEGER,
    worker_thread_id INTEGER CHECK(worker_thread_id IS NULL OR worker_thread_id > 0),
    transaction_reference TEXT,
    result_json TEXT,
    error_type TEXT,
    error TEXT,
    CHECK(started_ts_ms IS NULL OR started_ts_ms >= submitted_ts_ms),
    CHECK(committed_ts_ms IS NULL OR committed_ts_ms >= submitted_ts_ms),
    CHECK(completed_ts_ms IS NULL OR completed_ts_ms >= submitted_ts_ms),
    CHECK((status='COMMITTED' AND committed_ts_ms IS NOT NULL
           AND error_type IS NULL AND error IS NULL)
       OR status<>'COMMITTED'),
    CHECK((status='FAILED' AND error_type IS NOT NULL AND completed_ts_ms IS NOT NULL)
       OR status<>'FAILED')
);

CREATE TABLE IF NOT EXISTS persistence_worker_samples (
    persistence_worker_sample_id INTEGER PRIMARY KEY,
    sample_ts_ms INTEGER NOT NULL CHECK(sample_ts_ms >= 0),
    worker_thread_id INTEGER NOT NULL CHECK(worker_thread_id > 0),
    state TEXT NOT NULL,
    queue_depth INTEGER NOT NULL CHECK(queue_depth >= 0),
    queue_capacity INTEGER NOT NULL CHECK(queue_capacity > 0),
    queue_high_water INTEGER NOT NULL CHECK(queue_high_water >= 0),
    oldest_queue_age_ms INTEGER NOT NULL CHECK(oldest_queue_age_ms >= 0),
    commands_submitted INTEGER NOT NULL CHECK(commands_submitted >= 0),
    commands_committed INTEGER NOT NULL CHECK(commands_committed >= 0),
    commands_failed INTEGER NOT NULL CHECK(commands_failed >= 0),
    commands_retried INTEGER NOT NULL CHECK(commands_retried >= 0),
    idempotent_replays INTEGER NOT NULL CHECK(idempotent_replays >= 0),
    queue_full_count INTEGER NOT NULL CHECK(queue_full_count >= 0),
    timeout_count INTEGER NOT NULL CHECK(timeout_count >= 0),
    transactions_started INTEGER NOT NULL CHECK(transactions_started >= 0),
    transactions_committed INTEGER NOT NULL CHECK(transactions_committed >= 0),
    transactions_rolled_back INTEGER NOT NULL CHECK(transactions_rolled_back >= 0),
    last_commit_ts_ms INTEGER,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS checkpoint_runs (
    checkpoint_run_id INTEGER PRIMARY KEY,
    started_ts_ms INTEGER NOT NULL CHECK(started_ts_ms >= 0),
    completed_ts_ms INTEGER NOT NULL CHECK(completed_ts_ms >= started_ts_ms),
    mode TEXT NOT NULL CHECK(mode IN ('PASSIVE','FULL','RESTART','TRUNCATE')),
    reason TEXT NOT NULL,
    before_wal_bytes INTEGER NOT NULL CHECK(before_wal_bytes >= 0),
    after_wal_bytes INTEGER NOT NULL CHECK(after_wal_bytes >= 0),
    duration_ms REAL NOT NULL CHECK(duration_ms >= 0),
    busy_result INTEGER,
    frames_total INTEGER,
    frames_checkpointed INTEGER,
    database_bytes INTEGER NOT NULL DEFAULT 0 CHECK(database_bytes >= 0),
    success INTEGER NOT NULL CHECK(success IN (0,1)),
    failure_reason TEXT
);

CREATE INDEX IF NOT EXISTS ix_persistence_commands_state_time
    ON persistence_commands(status,completed_ts_ms,command_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_persistence_commands_idempotency
    ON persistence_commands(idempotency_key);
CREATE INDEX IF NOT EXISTS ix_persistence_commands_ordering
    ON persistence_commands(ordering_key,persistence_command_id);
CREATE INDEX IF NOT EXISTS ix_persistence_worker_samples_time
    ON persistence_worker_samples(sample_ts_ms);
CREATE INDEX IF NOT EXISTS ix_checkpoint_runs_time
    ON checkpoint_runs(started_ts_ms);
"""


SCHEMA_SQL = r"""
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_ts_ms INTEGER NOT NULL CHECK(applied_ts_ms >= 0),
    schema_hash TEXT NOT NULL
);

CREATE TABLE runtime_sessions (
    session_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'lite_frequency_v4'),
    mode TEXT NOT NULL CHECK(mode = 'lite_frequency_v4_shadow'),
    launch_nonce TEXT NOT NULL UNIQUE,
    pid INTEGER NOT NULL CHECK(pid > 0),
    git_commit TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    started_ts_ms INTEGER NOT NULL CHECK(started_ts_ms >= 0),
    ended_ts_ms INTEGER CHECK(ended_ts_ms IS NULL OR ended_ts_ms >= started_ts_ms),
    dry_run INTEGER NOT NULL DEFAULT 1 CHECK(dry_run = 1),
    live_enabled INTEGER NOT NULL DEFAULT 0 CHECK(live_enabled = 0),
    real_orders_possible INTEGER NOT NULL DEFAULT 0 CHECK(real_orders_possible = 0),
    live_adapter_present INTEGER NOT NULL DEFAULT 0 CHECK(live_adapter_present = 0),
    kill_switch_engaged INTEGER NOT NULL DEFAULT 1 CHECK(kill_switch_engaged = 1),
    fixed_shares REAL NOT NULL DEFAULT 5.0 CHECK(fixed_shares = 5.0),
    stop_reason TEXT,
    cohort TEXT NOT NULL DEFAULT 'legacy_mixed_universe'
);

CREATE TABLE cohorts (
    cohort TEXT PRIMARY KEY,
    activation_ts_ms INTEGER NOT NULL CHECK(activation_ts_ms >= 0),
    activation_commit TEXT,
    starting_equity_usd REAL NOT NULL CHECK(starting_equity_usd > 0),
    max_exposure_pct REAL NOT NULL CHECK(max_exposure_pct > 0 AND max_exposure_pct <= 1.0),
    fixed_shares REAL NOT NULL DEFAULT 5.0 CHECK(fixed_shares = 5.0),
    authoritative INTEGER NOT NULL DEFAULT 0 CHECK(authoritative IN (0,1)),
    peak_committed_usd REAL NOT NULL DEFAULT 0 CHECK(peak_committed_usd >= 0),
    peak_exposure_pct REAL NOT NULL DEFAULT 0 CHECK(peak_exposure_pct >= 0),
    label TEXT,
    created_ts_ms INTEGER NOT NULL CHECK(created_ts_ms >= 0)
);

CREATE TABLE markets (
    market_id INTEGER PRIMARY KEY,
    polymarket_market_id TEXT NOT NULL UNIQUE,
    asset TEXT NOT NULL,
    slug TEXT NOT NULL,
    question TEXT,
    duration_ms INTEGER NOT NULL CHECK(duration_ms > 0),
    open_ts_ms INTEGER NOT NULL CHECK(open_ts_ms >= 0),
    close_ts_ms INTEGER NOT NULL CHECK(close_ts_ms > open_ts_ms),
    status TEXT NOT NULL,
    accepting_orders INTEGER NOT NULL DEFAULT 0 CHECK(accepting_orders IN (0,1)),
    first_seen_ts_ms INTEGER NOT NULL CHECK(first_seen_ts_ms >= 0),
    last_seen_ts_ms INTEGER NOT NULL CHECK(last_seen_ts_ms >= first_seen_ts_ms),
    raw_identity_hash TEXT NOT NULL
);

CREATE TABLE market_identities (
    market_identity_id INTEGER PRIMARY KEY,
    market_id INTEGER NOT NULL REFERENCES markets(market_id) ON DELETE RESTRICT,
    event_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    yes_token_id TEXT NOT NULL,
    no_token_id TEXT NOT NULL,
    yes_outcome TEXT NOT NULL DEFAULT 'YES',
    no_outcome TEXT NOT NULL DEFAULT 'NO',
    identity_fingerprint TEXT NOT NULL UNIQUE,
    association_valid INTEGER NOT NULL CHECK(association_valid IN (0,1)),
    token_pair_valid INTEGER NOT NULL CHECK(token_pair_valid IN (0,1)),
    ambiguous INTEGER NOT NULL DEFAULT 0 CHECK(ambiguous IN (0,1)),
    verification_reason TEXT,
    verified_ts_ms INTEGER NOT NULL CHECK(verified_ts_ms >= 0),
    CHECK(yes_token_id <> no_token_id),
    UNIQUE(market_id, event_id, condition_id, yes_token_id, no_token_id)
);

CREATE TABLE asset_windows (
    window_id INTEGER PRIMARY KEY,
    asset TEXT NOT NULL,
    window_open_ts_ms INTEGER NOT NULL CHECK(window_open_ts_ms >= 0),
    window_close_ts_ms INTEGER NOT NULL,
    expected INTEGER NOT NULL DEFAULT 1 CHECK(expected IN (0,1)),
    lifecycle_status TEXT NOT NULL DEFAULT 'DISCOVERING',
    created_ts_ms INTEGER NOT NULL CHECK(created_ts_ms >= 0),
    updated_ts_ms INTEGER NOT NULL CHECK(updated_ts_ms >= created_ts_ms),
    CHECK(window_close_ts_ms - window_open_ts_ms = 300000),
    UNIQUE(asset, window_open_ts_ms, window_close_ts_ms)
);

CREATE TABLE window_market_links (
    window_id INTEGER NOT NULL REFERENCES asset_windows(window_id) ON DELETE CASCADE,
    market_identity_id INTEGER NOT NULL REFERENCES market_identities(market_identity_id) ON DELETE RESTRICT,
    eligibility_status TEXT NOT NULL,
    reject_reason TEXT,
    selected INTEGER NOT NULL DEFAULT 0 CHECK(selected IN (0,1)),
    linked_ts_ms INTEGER NOT NULL CHECK(linked_ts_ms >= 0),
    PRIMARY KEY(window_id, market_identity_id)
);
CREATE UNIQUE INDEX ux_window_selected_market
    ON window_market_links(window_id) WHERE selected = 1;

CREATE TABLE window_funnel (
    window_id INTEGER PRIMARY KEY REFERENCES asset_windows(window_id) ON DELETE CASCADE,
    available INTEGER NOT NULL DEFAULT 0 CHECK(available IN (0,1)),
    eligible INTEGER NOT NULL DEFAULT 0 CHECK(eligible IN (0,1)),
    positive_edge INTEGER NOT NULL DEFAULT 0 CHECK(positive_edge IN (0,1)),
    execution_attempts INTEGER NOT NULL DEFAULT 0 CHECK(execution_attempts >= 0),
    actual_entry INTEGER NOT NULL DEFAULT 0 CHECK(actual_entry IN (0,1)),
    terminal INTEGER NOT NULL DEFAULT 0 CHECK(terminal IN (0,1)),
    available_ts_ms INTEGER,
    eligible_ts_ms INTEGER,
    first_positive_edge_ts_ms INTEGER,
    entry_ts_ms INTEGER,
    terminal_ts_ms INTEGER,
    missed_opportunity INTEGER NOT NULL DEFAULT 0 CHECK(missed_opportunity IN (0,1)),
    final_blocker TEXT,
    no_book_reason TEXT,
    data_invalid_reason TEXT,
    updated_ts_ms INTEGER NOT NULL CHECK(updated_ts_ms >= 0)
);

CREATE TABLE anchor_observations (
    anchor_observation_id INTEGER PRIMARY KEY,
    market_identity_id INTEGER NOT NULL REFERENCES market_identities(market_identity_id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK(status IN (
      'ANCHORED','UNANCHORED','ANCHOR_FIELD_MISSING','ANCHOR_PARSE_FAILED','ANCHOR_NOT_YET_PUBLISHED')),
    price_to_beat REAL,
    source_field TEXT,
    parse_error TEXT,
    provider_ts_ms INTEGER,
    receipt_ts_ms INTEGER NOT NULL CHECK(receipt_ts_ms >= 0),
    CHECK(price_to_beat IS NULL OR price_to_beat > 0)
);

CREATE TABLE source_cursors (
    session_id TEXT NOT NULL REFERENCES runtime_sessions(session_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    channel TEXT NOT NULL,
    connection_epoch INTEGER NOT NULL DEFAULT 0 CHECK(connection_epoch >= 0),
    last_provider_ts_ms INTEGER,
    last_receipt_ts_ms INTEGER,
    last_sequence INTEGER,
    updated_ts_ms INTEGER NOT NULL CHECK(updated_ts_ms >= 0),
    PRIMARY KEY(session_id, source, channel, connection_epoch)
);

CREATE TABLE source_events (
    source_event_id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES runtime_sessions(session_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    channel TEXT NOT NULL,
    event_type TEXT NOT NULL,
    asset TEXT,
    market_identity_id INTEGER REFERENCES market_identities(market_identity_id) ON DELETE SET NULL,
    token_id TEXT,
    external_market_id TEXT,
    condition_id TEXT,
    window_open_ts_ms INTEGER,
    connection_epoch INTEGER NOT NULL DEFAULT 0 CHECK(connection_epoch >= 0),
    dedupe_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    payload_json TEXT,
    provider_ts_ms INTEGER,
    receipt_ts_ms INTEGER NOT NULL CHECK(receipt_ts_ms >= 0),
    monotonic_ns INTEGER NOT NULL CHECK(monotonic_ns >= 0),
    sequence_no INTEGER,
    accepted INTEGER NOT NULL CHECK(accepted IN (0,1)),
    classification TEXT NOT NULL,
    invalid_reason TEXT,
    duplicate_count INTEGER NOT NULL DEFAULT 0 CHECK(duplicate_count >= 0),
    last_duplicate_receipt_ts_ms INTEGER,
    retention_class TEXT NOT NULL DEFAULT 'RAW' CHECK(retention_class IN ('RAW','TRADE_EVIDENCE','PERMANENT')),
    pin_count INTEGER NOT NULL DEFAULT 0 CHECK(pin_count >= 0),
    UNIQUE(session_id, source, channel, connection_epoch, dedupe_key)
);

CREATE TABLE event_buckets (
    bucket_start_ts_ms INTEGER NOT NULL CHECK(bucket_start_ts_ms >= 0),
    bucket_ms INTEGER NOT NULL CHECK(bucket_ms > 0),
    source TEXT NOT NULL,
    channel TEXT NOT NULL,
    asset TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL,
    classification TEXT NOT NULL,
    raw_count INTEGER NOT NULL DEFAULT 0 CHECK(raw_count >= 0),
    unique_count INTEGER NOT NULL DEFAULT 0 CHECK(unique_count >= 0),
    duplicate_count INTEGER NOT NULL DEFAULT 0 CHECK(duplicate_count >= 0),
    invalid_count INTEGER NOT NULL DEFAULT 0 CHECK(invalid_count >= 0),
    PRIMARY KEY(bucket_start_ts_ms,bucket_ms,source,channel,asset,event_type,classification)
);

CREATE TABLE book_snapshots (
    book_snapshot_id INTEGER PRIMARY KEY,
    source_event_id INTEGER REFERENCES source_events(source_event_id) ON DELETE SET NULL,
    market_identity_id INTEGER NOT NULL REFERENCES market_identities(market_identity_id) ON DELETE CASCADE,
    token_id TEXT NOT NULL,
    outcome_side TEXT NOT NULL CHECK(outcome_side IN ('YES','NO')),
    provider_ts_ms INTEGER,
    receipt_ts_ms INTEGER NOT NULL CHECK(receipt_ts_ms >= 0),
    monotonic_ns INTEGER NOT NULL CHECK(monotonic_ns >= 0),
    sequence_no INTEGER,
    state_hash TEXT NOT NULL,
    best_bid REAL,
    best_ask REAL,
    spread REAL,
    bid_depth_5 REAL NOT NULL DEFAULT 0 CHECK(bid_depth_5 >= 0),
    ask_depth_5 REAL NOT NULL DEFAULT 0 CHECK(ask_depth_5 >= 0),
    bids_json TEXT NOT NULL DEFAULT '[]',
    asks_json TEXT NOT NULL DEFAULT '[]',
    hydrated INTEGER NOT NULL DEFAULT 0 CHECK(hydrated IN (0,1)),
    stale INTEGER NOT NULL DEFAULT 0 CHECK(stale IN (0,1)),
    invalid_reason TEXT,
    retention_class TEXT NOT NULL DEFAULT 'RAW' CHECK(retention_class IN ('RAW','TRADE_EVIDENCE','PERMANENT')),
    pin_count INTEGER NOT NULL DEFAULT 0 CHECK(pin_count >= 0),
    CHECK(best_bid IS NULL OR (best_bid > 0 AND best_bid < 1)),
    CHECK(best_ask IS NULL OR (best_ask > 0 AND best_ask < 1)),
    UNIQUE(market_identity_id, token_id, state_hash, receipt_ts_ms)
);

CREATE TABLE cex_observations (
    cex_observation_id INTEGER PRIMARY KEY,
    source_event_id INTEGER REFERENCES source_events(source_event_id) ON DELETE SET NULL,
    session_id TEXT NOT NULL REFERENCES runtime_sessions(session_id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    instrument TEXT NOT NULL,
    asset TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL DEFAULT 'ticker',
    trade_side TEXT NOT NULL DEFAULT '' CHECK(trade_side IN ('','buy','sell')),
    size REAL CHECK(size IS NULL OR size > 0),
    connection_epoch INTEGER NOT NULL DEFAULT 0 CHECK(connection_epoch >= 0),
    unchanged INTEGER NOT NULL DEFAULT 0 CHECK(unchanged IN (0,1)),
    price REAL NOT NULL CHECK(price > 0),
    bid REAL,
    ask REAL,
    provider_ts_ms INTEGER NOT NULL CHECK(provider_ts_ms >= 0),
    receipt_ts_ms INTEGER NOT NULL CHECK(receipt_ts_ms >= 0),
    monotonic_ns INTEGER NOT NULL CHECK(monotonic_ns >= 0),
    sequence_no INTEGER,
    observation_hash TEXT NOT NULL,
    classification TEXT NOT NULL CHECK(classification IN ('NEW_TICK','NO_NEW_TICK','INVALID')),
    fresh INTEGER NOT NULL CHECK(fresh IN (0,1)),
    invalid_reason TEXT,
    retention_class TEXT NOT NULL DEFAULT 'RAW' CHECK(retention_class IN ('RAW','TRADE_EVIDENCE','PERMANENT')),
    pin_count INTEGER NOT NULL DEFAULT 0 CHECK(pin_count >= 0),
    CHECK(provider_ts_ms <= receipt_ts_ms OR
          (classification = 'INVALID' AND invalid_reason = 'future_provider_timestamp')),
    UNIQUE(session_id, provider, instrument, connection_epoch, event_id)
);

CREATE TABLE candidates (
    candidate_id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES runtime_sessions(session_id) ON DELETE CASCADE,
    window_id INTEGER NOT NULL REFERENCES asset_windows(window_id) ON DELETE CASCADE,
    market_identity_id INTEGER NOT NULL REFERENCES market_identities(market_identity_id) ON DELETE RESTRICT,
    trigger_source_event_id INTEGER REFERENCES source_events(source_event_id) ON DELETE SET NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    evaluation_ts_ms INTEGER NOT NULL CHECK(evaluation_ts_ms >= 0),
    monotonic_ns INTEGER NOT NULL CHECK(monotonic_ns >= 0),
    evaluation_seq INTEGER NOT NULL CHECK(evaluation_seq >= 0),
    status TEXT NOT NULL,
    regime TEXT NOT NULL,
    selected_side TEXT CHECK(selected_side IS NULL OR selected_side IN ('YES','NO')),
    fair_probability_yes REAL,
    fair_probability_no REAL,
    calibrated INTEGER NOT NULL DEFAULT 0 CHECK(calibrated IN (0,1)),
    reliability REAL CHECK(reliability IS NULL OR (reliability >= 0 AND reliability <= 1)),
    positive_edge INTEGER NOT NULL DEFAULT 0 CHECK(positive_edge IN (0,1)),
    dominant_model TEXT,
    invalidation_reason TEXT,
    CHECK(fair_probability_yes IS NULL OR (fair_probability_yes > 0 AND fair_probability_yes < 1)),
    CHECK(fair_probability_no IS NULL OR (fair_probability_no > 0 AND fair_probability_no < 1)),
    CHECK(fair_probability_yes IS NULL OR fair_probability_no IS NULL OR abs(fair_probability_yes + fair_probability_no - 1.0) <= 0.000001),
    UNIQUE(session_id, window_id, evaluation_seq)
);

CREATE TABLE candidate_book_evidence (
    candidate_id INTEGER NOT NULL REFERENCES candidates(candidate_id) ON DELETE CASCADE,
    outcome_side TEXT NOT NULL CHECK(outcome_side IN ('YES','NO')),
    book_snapshot_id INTEGER NOT NULL REFERENCES book_snapshots(book_snapshot_id) ON DELETE RESTRICT,
    evidence_age_ms INTEGER NOT NULL CHECK(evidence_age_ms >= 0),
    PRIMARY KEY(candidate_id, outcome_side)
);

CREATE TABLE candidate_cex_evidence (
    candidate_id INTEGER NOT NULL REFERENCES candidates(candidate_id) ON DELETE CASCADE,
    cex_observation_id INTEGER NOT NULL REFERENCES cex_observations(cex_observation_id) ON DELETE RESTRICT,
    evidence_role TEXT NOT NULL,
    horizon_ms INTEGER NOT NULL DEFAULT 0 CHECK(horizon_ms >= 0),
    evidence_age_ms INTEGER NOT NULL CHECK(evidence_age_ms >= 0),
    PRIMARY KEY(candidate_id, cex_observation_id, evidence_role, horizon_ms)
);

CREATE TABLE model_contributions (
    model_contribution_id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES candidates(candidate_id) ON DELETE CASCADE,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    correlation_group TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('YES','NO','NONE')),
    raw_score REAL NOT NULL,
    estimated_probability REAL CHECK(estimated_probability IS NULL OR (estimated_probability > 0 AND estimated_probability < 1)),
    evidence_age_ms INTEGER CHECK(evidence_age_ms IS NULL OR evidence_age_ms >= 0),
    confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
    reliability REAL NOT NULL CHECK(reliability >= 0 AND reliability <= 1),
    invalidation_reason TEXT,
    expected_net_edge REAL,
    regime_weight REAL NOT NULL CHECK(regime_weight >= 0 AND regime_weight <= 1),
    gated INTEGER NOT NULL DEFAULT 0 CHECK(gated IN (0,1)),
    model_contribution REAL NOT NULL,
    calibrated INTEGER NOT NULL DEFAULT 0 CHECK(calibrated IN (0,1)),
    UNIQUE(candidate_id, model_name)
);

CREATE TABLE fair_value_calculations (
    fair_value_calculation_id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES candidates(candidate_id) ON DELETE CASCADE,
    phase TEXT NOT NULL CHECK(phase IN ('INITIAL','UPDATE','FINAL','MANAGEMENT')),
    calculation_seq INTEGER NOT NULL CHECK(calculation_seq >= 0),
    calculated_ts_ms INTEGER NOT NULL CHECK(calculated_ts_ms >= 0),
    monotonic_ns INTEGER NOT NULL CHECK(monotonic_ns >= 0),
    regime TEXT NOT NULL,
    fair_probability_yes REAL NOT NULL CHECK(fair_probability_yes > 0 AND fair_probability_yes < 1),
    fair_probability_no REAL NOT NULL CHECK(fair_probability_no > 0 AND fair_probability_no < 1),
    calibrated INTEGER NOT NULL DEFAULT 0 CHECK(calibrated IN (0,1)),
    calibration_label TEXT NOT NULL,
    CHECK(abs(fair_probability_yes + fair_probability_no - 1.0) <= 0.000001),
    UNIQUE(candidate_id, phase, calculation_seq)
);

CREATE TABLE fair_value_sides (
    fair_value_calculation_id INTEGER NOT NULL REFERENCES fair_value_calculations(fair_value_calculation_id) ON DELETE CASCADE,
    outcome_side TEXT NOT NULL CHECK(outcome_side IN ('YES','NO')),
    token_id TEXT NOT NULL,
    book_snapshot_id INTEGER REFERENCES book_snapshots(book_snapshot_id) ON DELETE RESTRICT,
    executable_vwap REAL,
    worst_consumed_price REAL,
    spread REAL,
    depth_shares REAL NOT NULL DEFAULT 0 CHECK(depth_shares >= 0),
    exact_five_share_depth INTEGER NOT NULL CHECK(exact_five_share_depth IN (0,1)),
    estimated_fee REAL NOT NULL CHECK(estimated_fee >= 0),
    execution_buffer REAL NOT NULL CHECK(execution_buffer >= 0),
    latency_buffer REAL NOT NULL CHECK(latency_buffer >= 0),
    uncertainty_buffer REAL NOT NULL CHECK(uncertainty_buffer >= 0),
    net_edge REAL,
    evidence_fresh INTEGER NOT NULL CHECK(evidence_fresh IN (0,1)),
    selected INTEGER NOT NULL DEFAULT 0 CHECK(selected IN (0,1)),
    CHECK(executable_vwap IS NULL OR (executable_vwap > 0 AND executable_vwap < 1)),
    CHECK(worst_consumed_price IS NULL OR (worst_consumed_price > 0 AND worst_consumed_price < 1)),
    PRIMARY KEY(fair_value_calculation_id, outcome_side)
);

CREATE TABLE decisions (
    decision_id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES candidates(candidate_id) ON DELETE CASCADE,
    fair_value_calculation_id INTEGER REFERENCES fair_value_calculations(fair_value_calculation_id) ON DELETE RESTRICT,
    decision_seq INTEGER NOT NULL CHECK(decision_seq >= 0),
    decision_ts_ms INTEGER NOT NULL CHECK(decision_ts_ms >= 0),
    monotonic_ns INTEGER NOT NULL CHECK(monotonic_ns >= 0),
    phase TEXT NOT NULL,
    action TEXT NOT NULL,
    selected_side TEXT CHECK(selected_side IS NULL OR selected_side IN ('YES','NO')),
    selected_net_edge REAL,
    economic_gate_passed INTEGER NOT NULL CHECK(economic_gate_passed IN (0,1)),
    exact_depth_passed INTEGER NOT NULL CHECK(exact_depth_passed IN (0,1)),
    evidence_fresh INTEGER NOT NULL CHECK(evidence_fresh IN (0,1)),
    safety_failure INTEGER NOT NULL DEFAULT 0 CHECK(safety_failure IN (0,1)),
    quota_override INTEGER NOT NULL DEFAULT 0 CHECK(quota_override = 0),
    reason TEXT NOT NULL,
    UNIQUE(candidate_id, decision_seq)
);

CREATE TABLE window_locks (
    window_id INTEGER PRIMARY KEY REFERENCES asset_windows(window_id) ON DELETE CASCADE,
    session_id TEXT NOT NULL REFERENCES runtime_sessions(session_id) ON DELETE CASCADE,
    market_identity_id INTEGER NOT NULL REFERENCES market_identities(market_identity_id) ON DELETE RESTRICT,
    strategy_id TEXT NOT NULL CHECK(strategy_id = 'lite_frequency_v4'),
    mode TEXT NOT NULL CHECK(mode = 'lite_frequency_v4_shadow'),
    owner_launch_nonce TEXT NOT NULL,
    outcome_side TEXT NOT NULL CHECK(outcome_side IN ('YES','NO')),
    state TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    candidate_id INTEGER REFERENCES candidates(candidate_id) ON DELETE SET NULL,
    decision_id INTEGER REFERENCES decisions(decision_id) ON DELETE SET NULL,
    reserved_ts_ms INTEGER NOT NULL CHECK(reserved_ts_ms >= 0),
    updated_ts_ms INTEGER NOT NULL CHECK(updated_ts_ms >= reserved_ts_ms),
    reserved_commitment_usd REAL NOT NULL DEFAULT 0 CHECK(reserved_commitment_usd >= 0)
);

CREATE TABLE maker_observations (
    maker_observation_id INTEGER PRIMARY KEY,
    window_id INTEGER NOT NULL REFERENCES asset_windows(window_id) ON DELETE CASCADE,
    candidate_id INTEGER NOT NULL REFERENCES candidates(candidate_id) ON DELETE CASCADE,
    decision_id INTEGER REFERENCES decisions(decision_id) ON DELETE SET NULL,
    initial_fair_value_calculation_id INTEGER NOT NULL REFERENCES fair_value_calculations(fair_value_calculation_id) ON DELETE RESTRICT,
    final_fair_value_calculation_id INTEGER REFERENCES fair_value_calculations(fair_value_calculation_id) ON DELETE RESTRICT,
    initial_book_snapshot_id INTEGER REFERENCES book_snapshots(book_snapshot_id) ON DELETE RESTRICT,
    final_book_snapshot_id INTEGER REFERENCES book_snapshots(book_snapshot_id) ON DELETE RESTRICT,
    maker_start_ts_ms INTEGER NOT NULL CHECK(maker_start_ts_ms >= 0),
    maker_deadline_ts_ms INTEGER NOT NULL CHECK(maker_deadline_ts_ms >= maker_start_ts_ms),
    maker_end_ts_ms INTEGER,
    start_monotonic_ns INTEGER NOT NULL CHECK(start_monotonic_ns >= 0),
    end_monotonic_ns INTEGER,
    actual_duration_ms INTEGER,
    maker_target_price REAL NOT NULL CHECK(maker_target_price > 0 AND maker_target_price < 1),
    chase_cap_price REAL NOT NULL CHECK(chase_cap_price > 0 AND chase_cap_price < 1),
    price_touched INTEGER NOT NULL DEFAULT 0 CHECK(price_touched IN (0,1)),
    maker_fill_assumed INTEGER NOT NULL DEFAULT 0 CHECK(maker_fill_assumed = 0),
    initial_net_edge REAL NOT NULL,
    final_net_edge REAL,
    outcome TEXT,
    reason TEXT,
    CHECK(actual_duration_ms IS NULL OR actual_duration_ms >= 0)
);

CREATE TABLE maker_updates (
    maker_observation_id INTEGER NOT NULL REFERENCES maker_observations(maker_observation_id) ON DELETE CASCADE,
    update_seq INTEGER NOT NULL CHECK(update_seq >= 0),
    update_ts_ms INTEGER NOT NULL CHECK(update_ts_ms >= 0),
    monotonic_ns INTEGER NOT NULL CHECK(monotonic_ns >= 0),
    source_event_id INTEGER REFERENCES source_events(source_event_id) ON DELETE SET NULL,
    book_snapshot_id INTEGER REFERENCES book_snapshots(book_snapshot_id) ON DELETE RESTRICT,
    fair_value_calculation_id INTEGER REFERENCES fair_value_calculations(fair_value_calculation_id) ON DELETE RESTRICT,
    selected_net_edge REAL,
    price_touched INTEGER NOT NULL DEFAULT 0 CHECK(price_touched IN (0,1)),
    action TEXT NOT NULL,
    reason TEXT,
    PRIMARY KEY(maker_observation_id, update_seq)
);

CREATE TABLE entries (
    entry_id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES runtime_sessions(session_id) ON DELETE CASCADE,
    window_id INTEGER NOT NULL UNIQUE REFERENCES asset_windows(window_id) ON DELETE RESTRICT,
    market_identity_id INTEGER NOT NULL REFERENCES market_identities(market_identity_id) ON DELETE RESTRICT,
    candidate_id INTEGER NOT NULL REFERENCES candidates(candidate_id) ON DELETE RESTRICT,
    decision_id INTEGER NOT NULL REFERENCES decisions(decision_id) ON DELETE RESTRICT,
    fair_value_calculation_id INTEGER NOT NULL REFERENCES fair_value_calculations(fair_value_calculation_id) ON DELETE RESTRICT,
    book_snapshot_id INTEGER NOT NULL REFERENCES book_snapshots(book_snapshot_id) ON DELETE RESTRICT,
    outcome_side TEXT NOT NULL CHECK(outcome_side IN ('YES','NO')),
    token_id TEXT NOT NULL,
    shares REAL NOT NULL DEFAULT 5.0 CHECK(shares = 5.0),
    entry_ts_ms INTEGER NOT NULL CHECK(entry_ts_ms >= 0),
    entry_mode TEXT NOT NULL CHECK(entry_mode IN ('CROSS_SPREAD','MAKER_TO_CROSS')),
    executable_vwap REAL NOT NULL CHECK(executable_vwap > 0 AND executable_vwap < 1),
    worst_consumed_price REAL NOT NULL CHECK(worst_consumed_price > 0 AND worst_consumed_price < 1),
    depth_shares REAL NOT NULL CHECK(depth_shares >= 5.0),
    gross_cost REAL NOT NULL CHECK(gross_cost > 0),
    estimated_fee REAL NOT NULL CHECK(estimated_fee >= 0),
    execution_buffer REAL NOT NULL CHECK(execution_buffer >= 0),
    latency_buffer REAL NOT NULL CHECK(latency_buffer >= 0),
    uncertainty_buffer REAL NOT NULL CHECK(uncertainty_buffer >= 0),
    selected_net_edge REAL NOT NULL CHECK(selected_net_edge > 0),
    execution_verified INTEGER NOT NULL CHECK(execution_verified IN (0,1)),
    maker_fill_assumed INTEGER NOT NULL DEFAULT 0 CHECK(maker_fill_assumed = 0),
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK(status IN ('OPEN','CLOSED','UNRESOLVED_FINAL'))
);

CREATE TABLE positions (
    position_id INTEGER PRIMARY KEY,
    entry_id INTEGER NOT NULL UNIQUE REFERENCES entries(entry_id) ON DELETE CASCADE,
    asset TEXT NOT NULL,
    outcome_side TEXT NOT NULL CHECK(outcome_side IN ('YES','NO')),
    shares REAL NOT NULL CHECK(shares = 5.0),
    open_shares REAL NOT NULL CHECK(open_shares >= 0 AND open_shares <= 5.0),
    committed_exposure_usd REAL NOT NULL CHECK(committed_exposure_usd >= 0),
    status TEXT NOT NULL CHECK(status IN ('OPEN','CLOSED','UNRESOLVED_FINAL')),
    opened_ts_ms INTEGER NOT NULL CHECK(opened_ts_ms >= 0),
    closed_ts_ms INTEGER
);

CREATE TABLE management_decisions (
    management_decision_id INTEGER PRIMARY KEY,
    position_id INTEGER NOT NULL REFERENCES positions(position_id) ON DELETE CASCADE,
    decision_seq INTEGER NOT NULL CHECK(decision_seq >= 0),
    decision_ts_ms INTEGER NOT NULL CHECK(decision_ts_ms >= 0),
    monotonic_ns INTEGER NOT NULL CHECK(monotonic_ns >= 0),
    book_snapshot_id INTEGER REFERENCES book_snapshots(book_snapshot_id) ON DELETE RESTRICT,
    fair_value_calculation_id INTEGER REFERENCES fair_value_calculations(fair_value_calculation_id) ON DELETE RESTRICT,
    updated_fair_probability REAL NOT NULL CHECK(updated_fair_probability > 0 AND updated_fair_probability < 1),
    executable_exit_value REAL CHECK(executable_exit_value IS NULL OR executable_exit_value >= 0),
    hold_to_resolution_value REAL NOT NULL,
    remaining_time_ms INTEGER NOT NULL CHECK(remaining_time_ms >= 0),
    spread REAL,
    depth_shares REAL NOT NULL DEFAULT 0 CHECK(depth_shares >= 0),
    estimated_fee REAL NOT NULL CHECK(estimated_fee >= 0),
    uncertainty REAL NOT NULL CHECK(uncertainty >= 0),
    thesis_state TEXT NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('HOLD','EXIT_BOOK','AWAIT_RESOLUTION')),
    reason TEXT NOT NULL,
    UNIQUE(position_id, decision_seq)
);

CREATE TABLE exits (
    exit_id INTEGER PRIMARY KEY,
    position_id INTEGER NOT NULL REFERENCES positions(position_id) ON DELETE RESTRICT,
    entry_id INTEGER NOT NULL REFERENCES entries(entry_id) ON DELETE RESTRICT,
    exit_ts_ms INTEGER NOT NULL CHECK(exit_ts_ms >= 0),
    exit_source TEXT NOT NULL CHECK(exit_source IN ('BOOK','OFFICIAL_RESOLUTION')),
    book_snapshot_id INTEGER REFERENCES book_snapshots(book_snapshot_id) ON DELETE RESTRICT,
    shares REAL NOT NULL CHECK(shares > 0 AND shares <= 5.0),
    executable_vwap REAL,
    worst_consumed_price REAL,
    payout_usd REAL NOT NULL CHECK(payout_usd >= 0),
    gross_pnl REAL NOT NULL,
    exit_fee REAL NOT NULL CHECK(exit_fee >= 0),
    net_pnl REAL NOT NULL,
    evidence_verified INTEGER NOT NULL CHECK(evidence_verified IN (0,1)),
    resolution_outcome TEXT,
    reason TEXT NOT NULL
);

CREATE TABLE resolution_attempts (
    resolution_attempt_id INTEGER PRIMARY KEY,
    entry_id INTEGER NOT NULL REFERENCES entries(entry_id) ON DELETE CASCADE,
    attempt_no INTEGER NOT NULL CHECK(attempt_no > 0),
    attempt_ts_ms INTEGER NOT NULL CHECK(attempt_ts_ms >= 0),
    source TEXT NOT NULL,
    result TEXT NOT NULL,
    observed_outcome TEXT,
    evidence_hash TEXT,
    verified INTEGER NOT NULL DEFAULT 0 CHECK(verified IN (0,1)),
    error TEXT,
    UNIQUE(entry_id, attempt_no)
);

CREATE TABLE fee_components (
    fee_component_id INTEGER PRIMARY KEY,
    entry_id INTEGER REFERENCES entries(entry_id) ON DELETE CASCADE,
    exit_id INTEGER REFERENCES exits(exit_id) ON DELETE CASCADE,
    component TEXT NOT NULL,
    amount_usd REAL NOT NULL CHECK(amount_usd >= 0),
    rate REAL,
    formula TEXT NOT NULL,
    estimated INTEGER NOT NULL CHECK(estimated IN (0,1)),
    calculated_ts_ms INTEGER NOT NULL CHECK(calculated_ts_ms >= 0),
    CHECK((entry_id IS NOT NULL AND exit_id IS NULL) OR (entry_id IS NULL AND exit_id IS NOT NULL))
);

CREATE TABLE pnl_records (
    pnl_record_id INTEGER PRIMARY KEY,
    entry_id INTEGER NOT NULL UNIQUE REFERENCES entries(entry_id) ON DELETE RESTRICT,
    terminal_ts_ms INTEGER NOT NULL CHECK(terminal_ts_ms >= 0),
    gross_pnl REAL NOT NULL,
    total_fees REAL NOT NULL CHECK(total_fees >= 0),
    net_pnl REAL NOT NULL,
    outcome TEXT NOT NULL,
    exit_source TEXT NOT NULL,
    execution_evidence_complete INTEGER NOT NULL CHECK(execution_evidence_complete IN (0,1)),
    fee_evidence_complete INTEGER NOT NULL CHECK(fee_evidence_complete IN (0,1)),
    resolution_evidence_complete INTEGER NOT NULL CHECK(resolution_evidence_complete IN (0,1)),
    verified INTEGER NOT NULL CHECK(verified IN (0,1))
);

CREATE TABLE compounding_preview (
    compounding_preview_id INTEGER PRIMARY KEY,
    pnl_record_id INTEGER NOT NULL UNIQUE REFERENCES pnl_records(pnl_record_id) ON DELETE CASCADE,
    starting_equity_usd REAL NOT NULL CHECK(starting_equity_usd > 0),
    fixed_share_equity_usd REAL NOT NULL,
    fixed_risk_equity_usd REAL NOT NULL,
    fixed_risk_fraction REAL NOT NULL CHECK(fixed_risk_fraction > 0 AND fixed_risk_fraction < 1),
    drawdown_usd REAL NOT NULL CHECK(drawdown_usd >= 0),
    risk_of_ruin_estimate REAL CHECK(risk_of_ruin_estimate IS NULL OR (risk_of_ruin_estimate >= 0 AND risk_of_ruin_estimate <= 1)),
    assumptions_json TEXT NOT NULL,
    influences_sizing INTEGER NOT NULL DEFAULT 0 CHECK(influences_sizing = 0),
    calculated_ts_ms INTEGER NOT NULL CHECK(calculated_ts_ms >= 0)
);

CREATE TABLE reject_events (
    reject_event_id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES runtime_sessions(session_id) ON DELETE CASCADE,
    window_id INTEGER REFERENCES asset_windows(window_id) ON DELETE CASCADE,
    candidate_id INTEGER REFERENCES candidates(candidate_id) ON DELETE CASCADE,
    source_event_id INTEGER REFERENCES source_events(source_event_id) ON DELETE SET NULL,
    reject_ts_ms INTEGER NOT NULL CHECK(reject_ts_ms >= 0),
    taxonomy TEXT NOT NULL CHECK(taxonomy IN ('DISCOVERY','NO_BOOK','DATA_INVALID','ECONOMIC','EXECUTION','RISK','RESOLUTION')),
    reason TEXT NOT NULL,
    recoverable INTEGER NOT NULL DEFAULT 0 CHECK(recoverable IN (0,1)),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count >= 0),
    detail_json TEXT,
    dedupe_key TEXT NOT NULL UNIQUE
);

CREATE TABLE latency_metrics (
    latency_metric_id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES runtime_sessions(session_id) ON DELETE CASCADE,
    window_id INTEGER REFERENCES asset_windows(window_id) ON DELETE CASCADE,
    candidate_id INTEGER REFERENCES candidates(candidate_id) ON DELETE CASCADE,
    source_event_id INTEGER REFERENCES source_events(source_event_id) ON DELETE SET NULL,
    measured_ts_ms INTEGER NOT NULL CHECK(measured_ts_ms >= 0),
    stage TEXT NOT NULL,
    provider_ts_ms INTEGER,
    receipt_ts_ms INTEGER,
    completed_ts_ms INTEGER,
    latency_ms REAL NOT NULL CHECK(latency_ms >= 0),
    within_target INTEGER NOT NULL CHECK(within_target IN (0,1))
);

CREATE TABLE source_health (
    source_health_id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES runtime_sessions(session_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    channel TEXT NOT NULL,
    sample_ts_ms INTEGER NOT NULL CHECK(sample_ts_ms >= 0),
    status TEXT NOT NULL,
    connected INTEGER NOT NULL CHECK(connected IN (0,1)),
    hydrated INTEGER NOT NULL DEFAULT 0 CHECK(hydrated IN (0,1)),
    heartbeat_age_ms INTEGER,
    ping_age_ms INTEGER,
    last_provider_ts_ms INTEGER,
    last_receipt_ts_ms INTEGER,
    freshness_ms INTEGER,
    reconnect_count INTEGER NOT NULL DEFAULT 0 CHECK(reconnect_count >= 0),
    sequence_gap_count INTEGER NOT NULL DEFAULT 0 CHECK(sequence_gap_count >= 0),
    duplicate_count INTEGER NOT NULL DEFAULT 0 CHECK(duplicate_count >= 0),
    future_count INTEGER NOT NULL DEFAULT 0 CHECK(future_count >= 0),
    regressed_count INTEGER NOT NULL DEFAULT 0 CHECK(regressed_count >= 0),
    rest_recovery_status TEXT,
    last_error TEXT
);

CREATE TABLE runtime_health (
    runtime_health_id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES runtime_sessions(session_id) ON DELETE CASCADE,
    sample_ts_ms INTEGER NOT NULL CHECK(sample_ts_ms >= 0),
    heartbeat_ts_ms INTEGER NOT NULL CHECK(heartbeat_ts_ms >= 0),
    pid INTEGER NOT NULL CHECK(pid > 0),
    state TEXT NOT NULL,
    loop_lag_ms REAL NOT NULL DEFAULT 0 CHECK(loop_lag_ms >= 0),
    db_writes_per_min INTEGER NOT NULL DEFAULT 0 CHECK(db_writes_per_min >= 0),
    db_size_bytes INTEGER NOT NULL DEFAULT 0 CHECK(db_size_bytes >= 0),
    open_positions INTEGER NOT NULL DEFAULT 0 CHECK(open_positions >= 0),
    last_error TEXT
);

CREATE TABLE retention_runs (
    retention_run_id INTEGER PRIMARY KEY,
    started_ts_ms INTEGER NOT NULL CHECK(started_ts_ms >= 0),
    completed_ts_ms INTEGER,
    raw_cutoff_ts_ms INTEGER NOT NULL CHECK(raw_cutoff_ts_ms >= 0),
    requested_batch_size INTEGER NOT NULL CHECK(requested_batch_size > 0),
    source_events_deleted INTEGER NOT NULL DEFAULT 0 CHECK(source_events_deleted >= 0),
    book_snapshots_deleted INTEGER NOT NULL DEFAULT 0 CHECK(book_snapshots_deleted >= 0),
    cex_observations_deleted INTEGER NOT NULL DEFAULT 0 CHECK(cex_observations_deleted >= 0),
    pinned_rows_skipped INTEGER NOT NULL DEFAULT 0 CHECK(pinned_rows_skipped >= 0),
    integrity_result TEXT,
    foreign_key_violations INTEGER
);

CREATE INDEX ix_markets_asset_window ON markets(asset,open_ts_ms,close_ts_ms);
CREATE INDEX ix_windows_time ON asset_windows(window_open_ts_ms,window_close_ts_ms,asset);
CREATE INDEX ix_funnel_positive ON window_funnel(positive_edge,actual_entry,updated_ts_ms);
CREATE INDEX ix_source_events_retention ON source_events(retention_class,pin_count,receipt_ts_ms);
CREATE INDEX ix_event_buckets_time ON event_buckets(bucket_start_ts_ms,source,asset);
CREATE INDEX ix_books_identity_time ON book_snapshots(market_identity_id,token_id,receipt_ts_ms);
CREATE INDEX ix_books_retention ON book_snapshots(retention_class,pin_count,receipt_ts_ms);
CREATE INDEX ix_cex_asset_time ON cex_observations(asset,provider,receipt_ts_ms);
CREATE INDEX ix_cex_retention ON cex_observations(retention_class,pin_count,receipt_ts_ms);
CREATE INDEX ix_candidates_window_time ON candidates(window_id,evaluation_ts_ms);
CREATE INDEX ix_candidates_trigger_source ON candidates(trigger_source_event_id);
CREATE INDEX ix_candidate_cex_evidence_obs ON candidate_cex_evidence(cex_observation_id);
CREATE INDEX ix_candidate_book_evidence_snap ON candidate_book_evidence(book_snapshot_id);
CREATE INDEX ix_decisions_action_time ON decisions(action,decision_ts_ms);
CREATE INDEX ix_entries_time ON entries(entry_ts_ms,status);
CREATE INDEX ix_positions_status ON positions(status,asset);
CREATE INDEX ix_pnl_terminal ON pnl_records(terminal_ts_ms,verified);
CREATE INDEX ix_rejects_time ON reject_events(reject_ts_ms,taxonomy,reason);
CREATE INDEX ix_source_health_latest ON source_health(source,channel,sample_ts_ms);
CREATE INDEX ix_runtime_health_latest ON runtime_health(session_id,sample_ts_ms);
CREATE INDEX ix_runtime_sessions_cohort ON runtime_sessions(cohort);
""" + PERSISTENCE_SCHEMA_V2_SQL


EXPECTED_TABLES = frozenset(
    match.group(1) for match in re.finditer(
        r"^CREATE TABLE(?: IF NOT EXISTS)? ([a-z_][a-z0-9_]*)",
        SCHEMA_SQL, flags=re.MULTILINE,
    )
)


# Additive V2 -> V3 migration: Phase 1 cohort separation and capital-ledger
# reservations.  Every prior row keeps its exact content; existing sessions
# are labelled with the legacy cohort explicitly rather than deleted or
# rewritten.
PHASE1_SCHEMA_V3_SQL = r"""
ALTER TABLE runtime_sessions
    ADD COLUMN cohort TEXT NOT NULL DEFAULT 'legacy_mixed_universe';
ALTER TABLE window_locks
    ADD COLUMN reserved_commitment_usd REAL NOT NULL DEFAULT 0
    CHECK(reserved_commitment_usd >= 0);
CREATE TABLE IF NOT EXISTS cohorts (
    cohort TEXT PRIMARY KEY,
    activation_ts_ms INTEGER NOT NULL CHECK(activation_ts_ms >= 0),
    activation_commit TEXT,
    starting_equity_usd REAL NOT NULL CHECK(starting_equity_usd > 0),
    max_exposure_pct REAL NOT NULL CHECK(max_exposure_pct > 0 AND max_exposure_pct <= 1.0),
    fixed_shares REAL NOT NULL DEFAULT 5.0 CHECK(fixed_shares = 5.0),
    authoritative INTEGER NOT NULL DEFAULT 0 CHECK(authoritative IN (0,1)),
    peak_committed_usd REAL NOT NULL DEFAULT 0 CHECK(peak_committed_usd >= 0),
    peak_exposure_pct REAL NOT NULL DEFAULT 0 CHECK(peak_exposure_pct >= 0),
    label TEXT,
    created_ts_ms INTEGER NOT NULL CHECK(created_ts_ms >= 0)
);
CREATE INDEX IF NOT EXISTS ix_runtime_sessions_cohort ON runtime_sessions(cohort)
"""


class V4Store:
    """V4 SQLite store with explicit and nestable atomic transactions.

    ``enforce_thread_ownership`` defaults to true.  A competing operation must
    construct and close its own store on the worker thread that uses it; a
    connection is never transferable between threads.
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        busy_timeout_ms: int = 10_000,
        enforce_thread_ownership: bool = True,
        background_write_admission: Optional[Callable[[], bool]] = None,
        background_write_release: Optional[Callable[[], None]] = None,
    ):
        if isinstance(busy_timeout_ms, bool) or not isinstance(busy_timeout_ms, int):
            raise ValueError("busy_timeout_ms must be an integer")
        if busy_timeout_ms < 100 or busy_timeout_ms > 120_000:
            raise ValueError("busy_timeout_ms must be within [100, 120000] ms")
        self.busy_timeout_ms = int(busy_timeout_ms)
        if type(enforce_thread_ownership) is not bool:
            raise ValueError("enforce_thread_ownership must be a strict boolean")
        if (background_write_admission is None) != (background_write_release is None):
            raise ValueError("background write gate requires admission and release")
        if (background_write_admission is not None
                and (not callable(background_write_admission)
                     or not callable(background_write_release))):
            raise ValueError("background write gate callbacks must be callable")
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._enforce_thread_ownership = enforce_thread_ownership
        self._owner_thread_id = threading.get_ident()
        self._transaction_depth = 0
        self._transaction_owner_thread_id: Optional[int] = None
        self._transaction_rollback_only = False
        self._transaction_counters = {
            "started": 0,
            "committed": 0,
            "rolled_back": 0,
            "nested": 0,
        }
        self._retention_action_index = 0
        self._background_write_admission = background_write_admission
        self._background_write_release = background_write_release
        self._background_gate_depth = 0
        # The SQLite C-level busy timeout and the Python connect timeout are
        # derived from one configured value so a slow/contended writer waits a
        # bounded, operator-controlled interval rather than a hardcoded literal.
        self._conn = sqlite3.connect(
            str(self.path), timeout=self.busy_timeout_ms / 1000.0,
            check_same_thread=enforce_thread_ownership, isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            # Only the dedicated persistence owner may decide when a checkpoint
            # is safe.  SQLite must never checkpoint implicitly on a hot path.
            self._conn.execute("PRAGMA wal_autocheckpoint=0")
            self._conn.execute("PRAGMA journal_size_limit=67108864")
            journal = str(self._conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
            if journal != "wal":
                self._conn.close()
                raise V4SchemaError(f"WAL unavailable for v4 database: {journal}")
            self._initialize_fresh_schema()
            self._ensure_performance_indexes()

    # Additive, idempotent performance indexes.  These carry no schema-contract
    # change (no new columns/tables), so they are created with IF NOT EXISTS on
    # every open rather than gated behind a schema-version bump — a database
    # created before these indexes existed is upgraded in place on next launch.
    _PERFORMANCE_INDEXES = (
        "CREATE INDEX IF NOT EXISTS ix_cex_retention "
        "ON cex_observations(retention_class,pin_count,receipt_ts_ms)",
        "CREATE INDEX IF NOT EXISTS ix_books_retention "
        "ON book_snapshots(retention_class,pin_count,receipt_ts_ms)",
        # Index the foreign-key child columns so deleting a raw parent row does
        # not full-scan the referencing tables for the ON DELETE RESTRICT check
        # (the dominant cost of a retention/row-cap delete on a large database).
        "CREATE INDEX IF NOT EXISTS ix_candidate_cex_evidence_obs "
        "ON candidate_cex_evidence(cex_observation_id)",
        "CREATE INDEX IF NOT EXISTS ix_candidate_book_evidence_snap "
        "ON candidate_book_evidence(book_snapshot_id)",
        "CREATE INDEX IF NOT EXISTS ix_candidates_trigger_source "
        "ON candidates(trigger_source_event_id)",
        "CREATE INDEX IF NOT EXISTS ix_candidates_retention "
        "ON candidates(evaluation_ts_ms,candidate_id)",
        "CREATE INDEX IF NOT EXISTS ix_retention_runs_time "
        "ON retention_runs(started_ts_ms,retention_run_id)",
        "CREATE INDEX IF NOT EXISTS ix_maker_observations_candidate "
        "ON maker_observations(candidate_id,maker_observation_id)",
        "CREATE INDEX IF NOT EXISTS ix_rejects_candidate "
        "ON reject_events(candidate_id,reject_event_id)",
        "CREATE INDEX IF NOT EXISTS ix_latency_candidate "
        "ON latency_metrics(candidate_id,latency_metric_id)",
    )

    def _ensure_performance_indexes(self) -> None:
        self._assert_owner()
        with self._lock:
            for statement in self._PERFORMANCE_INDEXES:
                self._conn.execute(statement)

    @property
    def connection(self) -> sqlite3.Connection:
        self._assert_owner()
        return self._conn

    @property
    def owner_thread_id(self) -> int:
        return int(self._owner_thread_id)

    @property
    def transaction_counters(self) -> dict[str, int]:
        with self._lock:
            return dict(self._transaction_counters)

    def _assert_owner(self) -> None:
        if self._closed:
            raise V4StoreError("V4 store is closed")
        if (self._enforce_thread_ownership
                and threading.get_ident() != self._owner_thread_id):
            raise V4StoreError(
                "V4 writable connection used outside its owner thread")

    @staticmethod
    def _migration_statements(sql: str) -> Iterator[str]:
        for statement in sql.split(";"):
            cleaned = statement.strip()
            if cleaned:
                yield cleaned

    def _apply_migration_v2(self) -> None:
        """Apply the additive V1 -> V2 persistence schema migration atomically."""

        applied = int(time.time() * 1_000)
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            for statement in self._migration_statements(PERSISTENCE_SCHEMA_V2_SQL):
                self._conn.execute(statement)
            self._conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version,applied_ts_ms,schema_hash) "
                "VALUES(?,?,?)",
                (2, applied, _canonical_hash(PERSISTENCE_SCHEMA_V2_SQL)),
            )
            self._conn.execute("PRAGMA user_version=2")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def _apply_migration_v3(self) -> None:
        """Apply the additive V2 -> V3 cohort/ledger migration atomically.

        Historical rows are preserved exactly; pre-existing sessions receive
        the explicit ``legacy_mixed_universe`` cohort label via the column
        default, and a non-authoritative legacy cohorts row records the
        historical span so reporting can label it honestly.
        """

        applied = int(time.time() * 1_000)
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            existing = {
                (str(table), str(row[1]))
                for table in ("runtime_sessions", "window_locks")
                for row in self._conn.execute(f"PRAGMA table_info({table})")
            }
            for statement in self._migration_statements(PHASE1_SCHEMA_V3_SQL):
                # Every V3 statement is additive; skip ALTERs whose column is
                # already present so a partially-current database migrates
                # idempotently instead of failing mid-transaction.
                match = re.match(
                    r"ALTER TABLE (\w+)\s+ADD COLUMN (\w+)", statement)
                if match and (match.group(1), match.group(2)) in existing:
                    continue
                self._conn.execute(statement)
            self._conn.execute(
                """INSERT OR IGNORE INTO cohorts(
                   cohort,activation_ts_ms,activation_commit,starting_equity_usd,
                   max_exposure_pct,fixed_shares,authoritative,label,created_ts_ms)
                   SELECT ?,COALESCE(MIN(started_ts_ms),0),NULL,13.0,0.75,5.0,0,
                   'NON_AUTHORITATIVE_LEGACY_MIXED_UNIVERSE',?
                   FROM runtime_sessions""",
                (LEGACY_COHORT, applied),
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version,applied_ts_ms,schema_hash) "
                "VALUES(?,?,?)",
                (3, applied, _canonical_hash(PHASE1_SCHEMA_V3_SQL)),
            )
            self._conn.execute("PRAGMA user_version=3")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def _initialize_fresh_schema(self) -> None:
        self._assert_owner()
        tables = {
            str(row[0]) for row in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        if tables and "schema_migrations" not in tables:
            raise V4SchemaError(
                "refusing non-v4 database: fresh schema or v4 schema_migrations required"
            )
        if not tables:
            self._conn.executescript(SCHEMA_SQL)
            self._conn.execute(
                "INSERT INTO schema_migrations(version,applied_ts_ms,schema_hash) VALUES(?,?,?)",
                (SCHEMA_VERSION, 0, _canonical_hash(SCHEMA_SQL)),
            )
            self._conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        else:
            row = self._conn.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()
            version = int(row[0]) if row and row[0] is not None else 0
            if version == 1:
                self._apply_migration_v2()
                version = 2
            if version == 2:
                self._apply_migration_v3()
                version = 3
            if version >= 2:
                tables = {
                    str(found[0]) for found in self._conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name NOT LIKE 'sqlite_%'"
                    ).fetchall()
                }
            if version != SCHEMA_VERSION:
                raise V4SchemaError(
                    f"unsupported v4 schema version {version}; expected {SCHEMA_VERSION}"
                )
            missing = EXPECTED_TABLES - tables
            if missing:
                raise V4SchemaError(f"incomplete v4 schema; missing {sorted(missing)}")

    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        self._assert_owner()
        with self._lock:
            thread_id = threading.get_ident()
            outer = self._transaction_depth == 0
            gate = self._background_write_gate() if outer else nullcontext()
            with gate:
                if outer:
                    self._conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
                    self._transaction_depth = 1
                    self._transaction_owner_thread_id = thread_id
                    self._transaction_rollback_only = False
                    self._transaction_counters["started"] += 1
                else:
                    if self._transaction_owner_thread_id != thread_id:
                        raise V4StoreError("nested transaction changed owner thread")
                    self._transaction_depth += 1
                    self._transaction_counters["nested"] += 1
                try:
                    yield self._conn
                    if outer:
                        if self._transaction_rollback_only:
                            raise V4StoreError(
                                "nested transaction marked outer transaction rollback-only")
                        self._conn.commit()
                        self._transaction_counters["committed"] += 1
                except Exception:
                    if outer:
                        self._conn.rollback()
                        self._transaction_counters["rolled_back"] += 1
                    else:
                        self._transaction_rollback_only = True
                    raise
                finally:
                    self._transaction_depth -= 1
                    if outer:
                        self._transaction_depth = 0
                        self._transaction_owner_thread_id = None
                        self._transaction_rollback_only = False

    @contextmanager
    def _background_write_gate(self) -> Iterator[None]:
        admission = self._background_write_admission
        release = self._background_write_release
        if admission is None or self._background_gate_depth > 0:
            yield
            return
        admitted = bool(admission())
        if not admitted:
            raise V4BackgroundWriteDeferred(
                "background write deferred while critical persistence is pending")
        self._background_gate_depth = 1
        try:
            yield
        finally:
            self._background_gate_depth = 0
            assert release is not None
            release()

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        self._assert_owner()
        with self._lock:
            return [dict(row) for row in self._conn.execute(sql, tuple(params)).fetchall()]

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> Optional[dict[str, Any]]:
        self._assert_owner()
        with self._lock:
            row = self._conn.execute(sql, tuple(params)).fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    def _safe_payload(value: Any) -> dict[str, Any]:
        payload = _record(value)

        def inspect(item: Any, path: str = "") -> None:
            if isinstance(item, Mapping):
                for key, nested in item.items():
                    field = f"{path}.{key}" if path else str(key)
                    if _SECRET_KEY_RE.search(str(key)):
                        raise ValueError(f"refusing secret-like field {field!r}")
                    inspect(nested, field)
            elif isinstance(item, (list, tuple)):
                for index, nested in enumerate(item):
                    inspect(nested, f"{path}[{index}]")

        inspect(payload)
        return payload

    @staticmethod
    def _insert_sql(table: str, payload: Mapping[str, Any]) -> tuple[str, tuple[Any, ...]]:
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", table):
            raise ValueError("unsafe table name")
        columns = list(payload)
        if not columns:
            raise ValueError("empty insert payload")
        if any(not re.fullmatch(r"[a-z_][a-z0-9_]*", column) for column in columns):
            raise ValueError("unsafe column name")
        marks = ",".join("?" for _ in columns)
        sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({marks})"
        return sql, tuple(_clean(payload[column]) for column in columns)

    def _insert(self, table: str, value: Any, *, conn: Optional[sqlite3.Connection] = None) -> int:
        payload = self._safe_payload(value)
        sql, params = self._insert_sql(table, payload)
        target = conn or self._conn
        cursor = target.execute(sql, params)
        return int(cursor.lastrowid)

    def record_runtime_session(self, value: Any) -> str:
        row = self._safe_payload(value)
        row.setdefault("strategy_id", STRATEGY_ID)
        row.setdefault("mode", MODE)
        row.setdefault("dry_run", 1)
        row.setdefault("live_enabled", 0)
        row.setdefault("real_orders_possible", 0)
        row.setdefault("live_adapter_present", 0)
        row.setdefault("kill_switch_engaged", 1)
        row.setdefault("fixed_shares", FIXED_SHARES)
        required = ("session_id", "launch_nonce", "pid", "git_commit", "config_hash", "started_ts_ms")
        missing = [key for key in required if row.get(key) in (None, "")]
        if missing:
            raise ValueError(f"runtime session missing {missing}")
        with self.transaction(immediate=True) as conn:
            self._insert("runtime_sessions", row, conn=conn)
        return str(row["session_id"])

    def end_runtime_session(self, session_id: str, ended_ts_ms: int, reason: str) -> None:
        with self.transaction(immediate=True) as conn:
            conn.execute(
                "UPDATE runtime_sessions SET ended_ts_ms=?,stop_reason=? WHERE session_id=?",
                (int(ended_ts_ms), str(reason), str(session_id)),
            )

    def ensure_cohort(self, value: Any) -> dict[str, Any]:
        """Idempotently create a cohort row; the first insert wins forever.

        The activation timestamp of the authoritative forward cohort is the
        first startup of the activating commit; later restarts return the
        original row unchanged so the cohort boundary never drifts.
        """
        row = self._safe_payload(value)
        required = ("cohort", "activation_ts_ms", "starting_equity_usd",
                    "max_exposure_pct")
        missing = [key for key in required if row.get(key) in (None, "")]
        if missing:
            raise ValueError(f"cohort row missing {missing}")
        row.setdefault("fixed_shares", FIXED_SHARES)
        row.setdefault("authoritative", 0)
        row.setdefault("created_ts_ms", row["activation_ts_ms"])
        with self.transaction(immediate=True) as conn:
            existing = conn.execute(
                "SELECT * FROM cohorts WHERE cohort=?", (str(row["cohort"]),)
            ).fetchone()
            if existing is None:
                self._insert("cohorts", row, conn=conn)
                existing = conn.execute(
                    "SELECT * FROM cohorts WHERE cohort=?", (str(row["cohort"]),)
                ).fetchone()
        return dict(existing)

    def upsert_market(self, value: Any) -> int:
        row = self._safe_payload(value)
        row.setdefault("raw_identity_hash", _canonical_hash(row))
        with self.transaction(immediate=True) as conn:
            conn.execute(
                """INSERT INTO markets(
                   polymarket_market_id,asset,slug,question,duration_ms,open_ts_ms,
                   close_ts_ms,status,accepting_orders,first_seen_ts_ms,last_seen_ts_ms,
                   raw_identity_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(polymarket_market_id) DO UPDATE SET
                   asset=excluded.asset,slug=excluded.slug,question=excluded.question,
                   duration_ms=excluded.duration_ms,open_ts_ms=excluded.open_ts_ms,
                   close_ts_ms=excluded.close_ts_ms,status=excluded.status,
                   accepting_orders=excluded.accepting_orders,
                   last_seen_ts_ms=MAX(markets.last_seen_ts_ms,excluded.last_seen_ts_ms),
                   raw_identity_hash=excluded.raw_identity_hash""",
                tuple(_clean(row.get(key)) for key in (
                    "polymarket_market_id", "asset", "slug", "question", "duration_ms",
                    "open_ts_ms", "close_ts_ms", "status", "accepting_orders",
                    "first_seen_ts_ms", "last_seen_ts_ms", "raw_identity_hash"
                )),
            )
            market = conn.execute(
                "SELECT market_id FROM markets WHERE polymarket_market_id=?",
                (str(row["polymarket_market_id"]),),
            ).fetchone()
        return int(market[0])

    def record_market_identity(self, value: Any) -> int:
        row = self._safe_payload(value)
        row.setdefault("identity_fingerprint", stable_idempotency_key(
            row.get("market_id"), row.get("event_id"), row.get("condition_id"),
            row.get("yes_token_id"), row.get("no_token_id")
        ))
        with self.transaction(immediate=True) as conn:
            return self._insert("market_identities", row, conn=conn)

    def ensure_asset_window(self, value: Any) -> int:
        row = self._safe_payload(value)
        row.setdefault("expected", 1)
        row.setdefault("lifecycle_status", "DISCOVERING")
        with self.transaction(immediate=True) as conn:
            conn.execute(
                """INSERT INTO asset_windows(asset,window_open_ts_ms,window_close_ts_ms,
                   expected,lifecycle_status,created_ts_ms,updated_ts_ms)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(asset,window_open_ts_ms,window_close_ts_ms) DO UPDATE SET
                   expected=MAX(asset_windows.expected,excluded.expected),
                   lifecycle_status=excluded.lifecycle_status,
                   updated_ts_ms=MAX(asset_windows.updated_ts_ms,excluded.updated_ts_ms)""",
                tuple(_clean(row.get(key)) for key in (
                    "asset", "window_open_ts_ms", "window_close_ts_ms", "expected",
                    "lifecycle_status", "created_ts_ms", "updated_ts_ms"
                )),
            )
            found = conn.execute(
                """SELECT window_id FROM asset_windows
                   WHERE asset=? AND window_open_ts_ms=? AND window_close_ts_ms=?""",
                (str(row["asset"]), int(row["window_open_ts_ms"]),
                 int(row["window_close_ts_ms"])),
            ).fetchone()
            conn.execute(
                "INSERT OR IGNORE INTO window_funnel(window_id,updated_ts_ms) VALUES(?,?)",
                (int(found[0]), int(row["updated_ts_ms"])),
            )
        return int(found[0])

    def link_window_market(self, value: Any) -> None:
        row = self._safe_payload(value)
        with self.transaction(immediate=True) as conn:
            self._insert("window_market_links", row, conn=conn)

    def record_anchor_observation(self, value: Any) -> int:
        with self.transaction() as conn:
            return self._insert("anchor_observations", value, conn=conn)

    def update_window_funnel(self, window_id: int, now_ms: int, **changes: Any) -> None:
        allowed = {
            "available", "eligible", "positive_edge", "execution_attempts",
            "actual_entry", "terminal", "available_ts_ms", "eligible_ts_ms",
            "first_positive_edge_ts_ms", "entry_ts_ms", "terminal_ts_ms",
            "missed_opportunity", "final_blocker", "no_book_reason",
            "data_invalid_reason",
        }
        unsafe = set(changes) - allowed
        if unsafe:
            raise ValueError(f"unsupported funnel fields: {sorted(unsafe)}")
        changes["updated_ts_ms"] = int(now_ms)
        assignments = ",".join(f"{key}=?" for key in changes)
        with self.transaction(immediate=True) as conn:
            conn.execute(
                f"UPDATE window_funnel SET {assignments} WHERE window_id=?",
                (*(_clean(value) for value in changes.values()), int(window_id)),
            )

    def _record_event_bucket(
        self, conn: sqlite3.Connection, *, receipt_ts_ms: int, source: str,
        channel: str, asset: str, event_type: str, classification: str,
        inserted: int | bool, duplicate: int | bool, invalid: int | bool,
        raw_count: int = 1, bucket_ms: int = 1000,
    ) -> None:
        raw = int(raw_count)
        unique_count = int(inserted)
        duplicate_count = int(duplicate)
        invalid_count = int(invalid)
        if (raw <= 0 or min(unique_count, duplicate_count, invalid_count) < 0
                or max(unique_count, duplicate_count, invalid_count) > raw):
            raise ValueError("invalid event bucket counts")
        start = int(receipt_ts_ms) // int(bucket_ms) * int(bucket_ms)
        conn.execute(
            """INSERT INTO event_buckets(
               bucket_start_ts_ms,bucket_ms,source,channel,asset,event_type,
               classification,raw_count,unique_count,duplicate_count,invalid_count)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(bucket_start_ts_ms,bucket_ms,source,channel,asset,event_type,classification)
               DO UPDATE SET raw_count=raw_count+excluded.raw_count,
                 unique_count=unique_count+excluded.unique_count,
                 duplicate_count=duplicate_count+excluded.duplicate_count,
                 invalid_count=invalid_count+excluded.invalid_count""",
            (start, int(bucket_ms), source, channel, asset or "", event_type,
             classification, raw, unique_count, duplicate_count, invalid_count),
        )

    def record_event_count(
        self, *, receipt_ts_ms: int, source: str, channel: str,
        asset: str, event_type: str, classification: str,
        unique: bool, duplicate: bool, invalid: bool,
    ) -> None:
        """Aggregate a raw frame that was intentionally not stored row-wise.

        High-burst public channels can advance hundreds of times per second.
        Candidate and trade evidence is always persisted exactly; unrelated
        frames are counted in bounded one-second buckets instead of flooding
        SQLite with unbounded raw rows.
        """

        with self.transaction() as conn:
            self._record_event_bucket(
                conn, receipt_ts_ms=int(receipt_ts_ms), source=str(source),
                channel=str(channel), asset=str(asset or ""),
                event_type=str(event_type), classification=str(classification),
                inserted=bool(unique), duplicate=bool(duplicate),
                invalid=bool(invalid),
            )

    def record_event_count_batch(self, values: Iterable[Mapping[str, Any]]) -> None:
        """Atomically flush pre-aggregated one-second source counters."""

        rows = [dict(value) for value in values]
        if not rows:
            return
        with self.transaction() as conn:
            for row in rows:
                required = {
                    "receipt_ts_ms", "source", "channel", "asset", "event_type",
                    "classification", "raw_count", "unique_count",
                    "duplicate_count", "invalid_count",
                }
                if set(row) != required:
                    raise ValueError("invalid event count batch fields")
                self._record_event_bucket(
                    conn,
                    receipt_ts_ms=int(row["receipt_ts_ms"]),
                    source=str(row["source"]),
                    channel=str(row["channel"]),
                    asset=str(row["asset"] or ""),
                    event_type=str(row["event_type"]),
                    classification=str(row["classification"]),
                    raw_count=int(row["raw_count"]),
                    inserted=int(row["unique_count"]),
                    duplicate=int(row["duplicate_count"]),
                    invalid=int(row["invalid_count"]),
                )

    def record_source_event(
        self, value: Any, *, session_id: Optional[str] = None,
        now_ms: Optional[int] = None, future_tolerance_ms: int = 0,
        sequence_contiguous: Optional[bool] = None,
        count_in_bucket: bool = True,
        admitted_at_receipt: bool = False,
        reference_only: bool = False,
    ) -> dict[str, Any]:
        row = self._safe_payload(value)
        if session_id is not None:
            if row.get("session_id") not in (None, str(session_id)):
                raise ValueError("source event session_id disagrees with runtime owner")
            row["session_id"] = str(session_id)
        if not row.get("session_id"):
            raise ValueError("source event requires runtime-owned session_id")
        if isinstance(future_tolerance_ms, bool) or int(future_tolerance_ms) < 0:
            raise ValueError("future_tolerance_ms must be a non-negative integer")
        row_flag = row.pop("sequence_contiguous", False)
        contiguous = row_flag if sequence_contiguous is None else sequence_contiguous
        if type(contiguous) is not bool:
            raise ValueError("sequence_contiguous must be a strict boolean")
        if type(count_in_bucket) is not bool:
            raise ValueError("count_in_bucket must be a strict boolean")
        if type(admitted_at_receipt) is not bool:
            raise ValueError("admitted_at_receipt must be a strict boolean")
        if type(reference_only) is not bool:
            raise ValueError("reference_only must be a strict boolean")
        if reference_only and count_in_bucket:
            raise ValueError("reference-only evidence must already be bucket-counted")
        if admitted_at_receipt and count_in_bucket:
            raise ValueError("late admitted evidence must already be bucket-counted")
        admitted_at_receipt = admitted_at_receipt or reference_only
        event_key = row.pop("event_key", None)
        if "receipt_monotonic_ns" in row:
            row.setdefault("monotonic_ns", row.pop("receipt_monotonic_ns"))
        if "sequence" in row:
            row.setdefault("sequence_no", row.pop("sequence"))
        if "market_id" in row:
            row.setdefault("external_market_id", row.pop("market_id"))
        if "window_open_ms" in row:
            row.setdefault("window_open_ts_ms", row.pop("window_open_ms"))
        row.setdefault("connection_epoch", 0)
        payload = row.pop("payload", None)
        row.setdefault("payload_hash", _canonical_hash(payload if payload is not None else row))
        row.setdefault("payload_json", payload)
        row.setdefault("dedupe_key", event_key or stable_idempotency_key(
            row.get("source"), row.get("channel"), row.get("event_type"),
            row.get("connection_epoch"), row.get("sequence_no"),
            row.get("provider_ts_ms"), row["payload_hash"]
        ))
        row.setdefault("retention_class", "RAW")
        row.setdefault("pin_count", 0)
        provider_ts = row.get("provider_ts_ms")
        receipt_ts = int(row["receipt_ts_ms"])
        invalid_reason = row.get("invalid_reason")
        classification = str(row.get("classification") or "ACCEPTED")
        accepted = bool(row.get("accepted", True))
        evidence_now_ms = receipt_ts if now_ms is None else int(now_ms)
        with self.transaction(immediate=True) as conn:
            cursor = conn.execute(
                """SELECT last_provider_ts_ms,last_receipt_ts_ms,last_sequence
                   FROM source_cursors WHERE session_id=? AND source=? AND channel=?
                   AND connection_epoch=?""",
                (str(row["session_id"]), str(row["source"]), str(row["channel"]),
                 int(row["connection_epoch"])),
            ).fetchone()
            if provider_ts is not None and int(provider_ts) > evidence_now_ms + int(future_tolerance_ms):
                accepted, classification, invalid_reason = False, "FUTURE_EVENT", "future_provider_timestamp"
            elif (not admitted_at_receipt and provider_ts is not None
                  and cursor is not None and cursor[0] is not None
                  and int(provider_ts) < int(cursor[0])):
                accepted, classification, invalid_reason = False, "REGRESSED_EVENT", "regressed_provider_timestamp"
            elif (not admitted_at_receipt and row.get("sequence_no") is not None
                  and cursor is not None and cursor[2] is not None):
                sequence = int(row["sequence_no"])
                last_sequence = int(cursor[2])
                if sequence < last_sequence:
                    accepted, classification, invalid_reason = False, "REGRESSED_SEQUENCE", "regressed_sequence"
                elif contiguous and sequence == last_sequence:
                    accepted, classification, invalid_reason = False, "OUT_OF_ORDER", "non_increasing_contiguous_sequence"
                elif contiguous and sequence > last_sequence + 1:
                    accepted, classification, invalid_reason = False, "SEQUENCE_GAP", "dropped_sequence"
            row["accepted"] = int(accepted)
            row["classification"] = classification
            row["invalid_reason"] = invalid_reason
            sql, params = self._insert_sql("source_events", row)
            try:
                result = conn.execute(sql, params)
                event_id = int(result.lastrowid)
                inserted, duplicate = True, False
            except sqlite3.IntegrityError as exc:
                if "UNIQUE constraint failed: source_events" not in str(exc):
                    raise
                existing = conn.execute(
                    """SELECT source_event_id,classification,accepted,invalid_reason
                       FROM source_events
                       WHERE session_id=? AND source=? AND channel=?
                       AND connection_epoch=? AND dedupe_key=?""",
                    (str(row["session_id"]), str(row["source"]), str(row["channel"]),
                     int(row["connection_epoch"]), str(row["dedupe_key"])),
                ).fetchone()
                if existing is None:
                    raise
                event_id = int(existing[0])
                inserted, duplicate = False, not reference_only
                if reference_only:
                    classification = str(existing[1])
                    accepted = bool(existing[2])
                    invalid_reason = existing[3]
                else:
                    conn.execute(
                        """UPDATE source_events SET duplicate_count=duplicate_count+1,
                           last_duplicate_receipt_ts_ms=MAX(
                             COALESCE(last_duplicate_receipt_ts_ms,0),?)
                           WHERE source_event_id=?""",
                        (receipt_ts, event_id),
                    )
                    classification = "DUPLICATE"
            if count_in_bucket:
                self._record_event_bucket(
                    conn, receipt_ts_ms=receipt_ts, source=str(row["source"]),
                    channel=str(row["channel"]), asset=str(row.get("asset") or ""),
                    event_type=str(row["event_type"]), classification=classification,
                    inserted=inserted, duplicate=duplicate, invalid=not accepted,
                )
            if inserted and accepted and not admitted_at_receipt:
                conn.execute(
                    """INSERT INTO source_cursors(session_id,source,channel,connection_epoch,
                       last_provider_ts_ms,last_receipt_ts_ms,last_sequence,updated_ts_ms)
                       VALUES(?,?,?,?,?,?,?,?)
                       ON CONFLICT(session_id,source,channel,connection_epoch) DO UPDATE SET
                       last_provider_ts_ms=excluded.last_provider_ts_ms,
                       last_receipt_ts_ms=excluded.last_receipt_ts_ms,
                       last_sequence=COALESCE(excluded.last_sequence,source_cursors.last_sequence),
                       updated_ts_ms=excluded.updated_ts_ms""",
                    (str(row["session_id"]), str(row["source"]), str(row["channel"]),
                     int(row["connection_epoch"]), provider_ts, receipt_ts,
                     row.get("sequence_no"), receipt_ts),
                )
        return {
            "source_event_id": event_id, "inserted": inserted,
            "duplicate": duplicate,
            "accepted": bool(accepted and (inserted or reference_only)),
            "classification": classification, "invalid_reason": invalid_reason,
            "sequence_contiguous": contiguous,
            "admitted_at_receipt": admitted_at_receipt,
            "reference_only": reference_only,
        }

    def record_book_snapshot(self, value: Any) -> int:
        row = self._safe_payload(value)
        row.setdefault("state_hash", _canonical_hash({
            key: row.get(key) for key in ("token_id", "bids_json", "asks_json", "best_bid", "best_ask")
        }))
        row.setdefault("retention_class", "RAW")
        with self.transaction() as conn:
            return self._insert("book_snapshots", row, conn=conn)

    def record_cex_observation(
        self, value: Any, *, session_id: Optional[str] = None,
        already_validated_at_receipt: bool = False,
    ) -> dict[str, Any]:
        if type(already_validated_at_receipt) is not bool:
            raise ValueError("already_validated_at_receipt must be a strict boolean")
        row = self._safe_payload(value)
        if session_id is not None:
            if row.get("session_id") not in (None, str(session_id)):
                raise ValueError("CEX observation session_id disagrees with runtime owner")
            row["session_id"] = str(session_id)
        if not row.get("session_id"):
            raise ValueError("CEX observation requires runtime-owned session_id")
        if "receipt_monotonic_ns" in row:
            row.setdefault("monotonic_ns", row.pop("receipt_monotonic_ns"))
        if "sequence" in row:
            row.setdefault("sequence_no", row.pop("sequence"))
        if "side" in row:
            row.setdefault("trade_side", row.pop("side"))
        row.setdefault("connection_epoch", 0)
        row.setdefault("event_type", "ticker")
        row.setdefault("observation_hash", _canonical_hash({
            key: row.get(key) for key in (
                "provider", "instrument", "event_type", "event_id", "price",
                "bid", "ask", "size", "provider_ts_ms", "receipt_ts_ms",
                "sequence_no", "connection_epoch",
            )
        }))
        row.setdefault("event_id", row["observation_hash"])
        row["unchanged"] = int(bool(row.get("unchanged", False)))
        row.setdefault("retention_class", "RAW")
        with self.transaction(immediate=True) as conn:
            prior = None
            if not already_validated_at_receipt:
                prior = conn.execute(
                    """SELECT price,bid,ask,provider_ts_ms,receipt_ts_ms
                       FROM cex_observations WHERE session_id=? AND provider=? AND instrument=?
                       ORDER BY provider_ts_ms DESC,cex_observation_id DESC LIMIT 1""",
                    (str(row["session_id"]), str(row["provider"]),
                     str(row["instrument"])),
                ).fetchone()
            classification = str(row.get("classification") or "NEW_TICK")
            invalid_reason = row.get("invalid_reason")
            if int(row["provider_ts_ms"]) > int(row["receipt_ts_ms"]):
                classification, invalid_reason = "INVALID", "future_provider_timestamp"
            elif (not already_validated_at_receipt and prior is not None
                  and int(row["provider_ts_ms"]) < int(prior[3])):
                classification, invalid_reason = "INVALID", "regressed_provider_timestamp"
            elif not already_validated_at_receipt and (bool(row["unchanged"]) or (
                prior is not None and all(
                    row.get(key) == prior[index]
                    for index, key in enumerate(("price", "bid", "ask"))
                )
            )):
                classification = "NO_NEW_TICK"
            if classification not in {"NEW_TICK", "NO_NEW_TICK", "INVALID"}:
                raise ValueError("invalid CEX observation classification")
            row["classification"] = classification
            row["fresh"] = int(
                classification != "INVALID" and bool(row.get("fresh", True)))
            row["invalid_reason"] = invalid_reason
            try:
                observation_id = self._insert("cex_observations", row, conn=conn)
                inserted = True
            except sqlite3.IntegrityError as exc:
                if "UNIQUE constraint failed: cex_observations" not in str(exc):
                    raise
                existing = conn.execute(
                    """SELECT cex_observation_id,classification,fresh
                       FROM cex_observations
                       WHERE session_id=? AND provider=? AND instrument=?
                       AND connection_epoch=? AND event_id=?""",
                    (str(row["session_id"]), str(row["provider"]), str(row["instrument"]),
                     int(row["connection_epoch"]), str(row["event_id"])),
                ).fetchone()
                observation_id = int(existing[0])
                classification = str(existing[1])
                row["fresh"] = int(existing[2])
                inserted = False
        return {"cex_observation_id": observation_id, "inserted": inserted,
                "classification": classification, "fresh": bool(row["fresh"])}

    def record_candidate(self, value: Any) -> int:
        row = self._safe_payload(value)
        row.setdefault("idempotency_key", stable_idempotency_key(
            row.get("session_id"), row.get("window_id"), row.get("evaluation_seq")
        ))
        with self.transaction(immediate=True) as conn:
            try:
                candidate_id = self._insert("candidates", row, conn=conn)
            except sqlite3.IntegrityError as exc:
                if "UNIQUE constraint failed" not in str(exc):
                    raise
                existing = conn.execute(
                    "SELECT candidate_id FROM candidates WHERE idempotency_key=?",
                    (str(row["idempotency_key"]),),
                ).fetchone()
                if existing is None:
                    raise
                candidate_id = int(existing[0])
        return candidate_id

    def link_candidate_book(self, candidate_id: int, side: str, snapshot_id: int,
                            evidence_age_ms: int) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO candidate_book_evidence(
                   candidate_id,outcome_side,book_snapshot_id,evidence_age_ms) VALUES(?,?,?,?)""",
                (int(candidate_id), str(side), int(snapshot_id), int(evidence_age_ms)),
            )

    def link_candidate_cex(self, candidate_id: int, observation_id: int, role: str,
                           horizon_ms: int, evidence_age_ms: int) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO candidate_cex_evidence(
                   candidate_id,cex_observation_id,evidence_role,horizon_ms,evidence_age_ms)
                   VALUES(?,?,?,?,?)""",
                (int(candidate_id), int(observation_id), str(role), int(horizon_ms),
                 int(evidence_age_ms)),
            )

    def record_model_contribution(self, value: Any) -> int:
        with self.transaction() as conn:
            return self._insert("model_contributions", value, conn=conn)

    def record_fair_value(self, calculation: Any, sides: Iterable[Any]) -> int:
        with self.transaction(immediate=True) as conn:
            calculation_id = self._insert("fair_value_calculations", calculation, conn=conn)
            selected = 0
            for side_value in sides:
                side = self._safe_payload(side_value)
                side["fair_value_calculation_id"] = calculation_id
                selected += int(bool(side.get("selected")))
                self._insert("fair_value_sides", side, conn=conn)
            if selected > 1:
                raise ValueError("only one fair-value side may be selected")
        return calculation_id

    def record_decision(self, value: Any) -> int:
        row = self._safe_payload(value)
        if bool(row.get("economic_gate_passed")) and not (
            row.get("selected_net_edge") is not None and float(row["selected_net_edge"]) > 0
        ):
            raise ValueError("economic gate cannot pass without positive net edge")
        with self.transaction(immediate=True) as conn:
            return self._insert("decisions", row, conn=conn)

    def reserve_window(self, value: Any) -> dict[str, Any]:
        row = self._safe_payload(value)
        row.setdefault("strategy_id", STRATEGY_ID)
        row.setdefault("mode", MODE)
        row.setdefault("idempotency_key", stable_idempotency_key(
            row.get("session_id"), row.get("window_id"), row.get("market_identity_id"),
            row.get("outcome_side")
        ))
        with self.transaction(immediate=True) as conn:
            session = conn.execute(
                """SELECT launch_nonce,dry_run,live_enabled,real_orders_possible,
                   live_adapter_present,kill_switch_engaged,fixed_shares
                   FROM runtime_sessions WHERE session_id=?""",
                (str(row["session_id"]),),
            ).fetchone()
            if session is None:
                raise ValueError("unknown runtime session")
            if str(session[0]) != str(row["owner_launch_nonce"]):
                raise WindowReservationConflict("launch_nonce_mismatch")
            if tuple(session[1:]) != (1, 0, 0, 0, 1, FIXED_SHARES):
                raise V4StoreError("runtime session safety invariant failed")
            existing = conn.execute(
                "SELECT * FROM window_locks WHERE window_id=?", (int(row["window_id"]),)
            ).fetchone()
            if existing is not None:
                result = dict(existing)
                if str(result["idempotency_key"]) == str(row["idempotency_key"]):
                    return {"created": False, "idempotent": True, "lock": result}
                reason = (
                    "opposite_side_blocked" if str(result["outcome_side"]) != str(row["outcome_side"])
                    else "duplicate_window_owner"
                )
                raise WindowReservationConflict(reason, result)
            self._insert("window_locks", row, conn=conn)
            created = conn.execute(
                "SELECT * FROM window_locks WHERE window_id=?", (int(row["window_id"]),)
            ).fetchone()
        return {"created": True, "idempotent": False, "lock": dict(created)}

    def release_window_reservation(
        self, *, window_id: int, session_id: str,
        owner_launch_nonce: str, idempotency_key: str,
    ) -> bool:
        """Release only this owner's unentered failed reservation."""

        with self.transaction(immediate=True) as conn:
            entry = conn.execute(
                "SELECT 1 FROM entries WHERE window_id=?", (int(window_id),)
            ).fetchone()
            if entry is not None:
                return False
            cursor = conn.execute(
                """DELETE FROM window_locks WHERE window_id=? AND session_id=?
                   AND owner_launch_nonce=? AND idempotency_key=? AND state='RESERVED'""",
                (int(window_id), str(session_id), str(owner_launch_nonce),
                 str(idempotency_key)),
            )
        return bool(cursor.rowcount)

    def create_entry(
        self, value: Any, *, max_concurrent_positions: int,
        cohort: str = ACTIVE_COHORT,
        starting_equity_usd: float = 13.0,
        max_exposure_pct: float = 1.0,
        exit_fee_buffer_usd: float = MIN_EXIT_FEE_BUFFER_USD,
        max_open_per_asset: int = 1,
    ) -> int:
        """Atomically validate universe, reservation, ledger, and create one entry.

        Capital is enforced against the single authoritative cohort ledger:
        equity = cohort starting equity + realized cohort net PnL; committed
        capital = open-position cost (incl. entry fees) + unresolved-position
        cost + conservative exit-fee buffers + live reservations.  The entry
        fails closed unless the projected committed total stays within
        ``equity * max_exposure_pct`` — with the Phase 1 cap of 100% this is
        exactly the invariant ``total committed capital <= current equity``.
        """
        row = self._safe_payload(value)
        if bool(row.get("maker_fill_assumed")):
            raise ValueError("maker fill may never be assumed")
        row["shares"] = FIXED_SHARES
        row["maker_fill_assumed"] = 0
        row.setdefault("idempotency_key", stable_idempotency_key(
            row.get("session_id"), row.get("window_id"), row.get("outcome_side"), "entry"
        ))
        if float(row.get("selected_net_edge") or 0) <= 0:
            raise ValueError("entry requires positive fee-net edge")
        if float(row.get("depth_shares") or 0) < FIXED_SHARES:
            raise ValueError("entry requires exact five-share executable depth")
        if not bool(row.get("execution_verified")):
            raise ValueError("entry requires verified execution evidence")
        if abs(float(row.get("gross_cost") or 0) - FIXED_SHARES * float(
                row.get("executable_vwap") or 0)) > 1e-8:
            raise ValueError("entry gross cost must equal five-share VWAP cost")
        with self.transaction(immediate=True) as conn:
            existing = conn.execute(
                "SELECT entry_id,idempotency_key FROM entries WHERE window_id=?",
                (int(row["window_id"]),),
            ).fetchone()
            if existing is not None:
                if str(existing[1]) == str(row["idempotency_key"]):
                    return int(existing[0])
                raise WindowReservationConflict("one_entry_per_asset_window")
            lock = conn.execute(
                "SELECT * FROM window_locks WHERE window_id=?", (int(row["window_id"]),)
            ).fetchone()
            if lock is None:
                raise WindowReservationConflict("window_not_reserved")
            lock_row = dict(lock)
            if str(lock_row["session_id"]) != str(row["session_id"]):
                raise WindowReservationConflict("wrong_window_owner", lock_row)
            if str(lock_row["outcome_side"]) != str(row["outcome_side"]):
                raise WindowReservationConflict("opposite_side_blocked", lock_row)
            decision = conn.execute(
                """SELECT economic_gate_passed,exact_depth_passed,evidence_fresh,
                   selected_side,selected_net_edge,quota_override FROM decisions WHERE decision_id=?""",
                (int(row["decision_id"]),),
            ).fetchone()
            if decision is None or not all(bool(decision[index]) for index in (0, 1, 2)):
                raise ValueError("entry decision did not pass economic/depth/freshness gates")
            if str(decision[3]) != str(row["outcome_side"]):
                raise ValueError("entry side differs from selected decision side")
            if float(decision[4] or 0) <= 0 or bool(decision[5]):
                raise ValueError("entry decision is not positive-EV quota-free")
            if abs(float(decision[4]) - float(row["selected_net_edge"])) > 1e-8:
                raise ValueError("entry edge differs from final decision edge")
            fair_side = conn.execute(
                """SELECT token_id,book_snapshot_id,executable_vwap,depth_shares,
                   exact_five_share_depth,net_edge,evidence_fresh,selected
                   FROM fair_value_sides WHERE fair_value_calculation_id=?
                   AND outcome_side=?""",
                (int(row["fair_value_calculation_id"]), str(row["outcome_side"])),
            ).fetchone()
            if fair_side is None or not all(bool(fair_side[index]) for index in (4, 6, 7)):
                raise ValueError("entry lacks selected fresh exact-depth fair-value evidence")
            if str(fair_side[0]) != str(row["token_id"]):
                raise ValueError("entry token differs from fair-value token")
            if int(fair_side[1]) != int(row["book_snapshot_id"]):
                raise ValueError("entry book differs from fair-value evidence book")
            if abs(float(fair_side[2]) - float(row["executable_vwap"])) > 1e-8:
                raise ValueError("entry VWAP differs from final fair-value evidence")
            if float(fair_side[3]) < FIXED_SHARES or float(fair_side[5] or 0) <= 0:
                raise ValueError("entry fair-value side is not positive-EV executable depth")
            window = conn.execute(
                "SELECT asset,window_close_ts_ms FROM asset_windows WHERE window_id=?",
                (int(row["window_id"]),),
            ).fetchone()
            if window is None:
                raise ValueError("unknown asset window")
            asset = str(window[0])
            if int(row["entry_ts_ms"]) >= int(window[1]):
                raise ValueError("cannot enter at or after window close")

            # Defensive execution boundary: re-derive the canonical dynamic
            # universe decision from previously committed market rows.  A
            # directly injected or corrupted candidate cannot open a position
            # for a market the policy rejects, regardless of caller claims.
            identity_row = conn.execute(
                "SELECT * FROM market_identities WHERE market_identity_id=?",
                (int(row["market_identity_id"]),),
            ).fetchone()
            market_row = (
                conn.execute(
                    "SELECT * FROM markets WHERE market_id=?",
                    (int(identity_row["market_id"]),),
                ).fetchone()
                if identity_row is not None else None
            )
            anchor_row = (
                conn.execute(
                    """SELECT status,price_to_beat FROM anchor_observations
                       WHERE market_identity_id=?
                       ORDER BY anchor_observation_id DESC LIMIT 1""",
                    (int(row["market_identity_id"]),),
                ).fetchone()
                if identity_row is not None else None
            )
            universe_decision = evaluate_persisted_market(
                dict(market_row) if market_row is not None else None,
                dict(identity_row) if identity_row is not None else None,
                dict(anchor_row) if anchor_row is not None else None,
            )
            if not universe_decision.eligible:
                raise UniverseEligibilityError(
                    universe_decision.reject_reason or "universe_not_eligible")

            # Authoritative cohort ledger, all inside this transaction.  The
            # persisted cohorts row is the source of truth; caller parameters
            # must agree with it exactly or the entry fails closed.
            cohort_name = str(cohort)
            cohort_row = conn.execute(
                """SELECT starting_equity_usd,max_exposure_pct FROM cohorts
                   WHERE cohort=?""", (cohort_name,),
            ).fetchone()
            if cohort_row is None:
                raise V4StoreError("unknown capital cohort")
            if (abs(float(cohort_row[0]) - float(starting_equity_usd)) > 1e-9
                    or abs(float(cohort_row[1]) - float(max_exposure_pct)) > 1e-9):
                raise V4StoreError("cohort ledger parameters mismatch")
            session_cohort = conn.execute(
                "SELECT cohort FROM runtime_sessions WHERE session_id=?",
                (str(row["session_id"]),),
            ).fetchone()
            if session_cohort is None or str(session_cohort[0]) != cohort_name:
                raise V4StoreError("entry session does not belong to the cohort")
            exit_buffer = float(exit_fee_buffer_usd)
            if (not math.isfinite(exit_buffer)
                    or exit_buffer < MIN_EXIT_FEE_BUFFER_USD - 1e-9):
                raise V4StoreError("exit fee buffer below conservative floor")

            open_count, open_cost = conn.execute(
                """SELECT COUNT(*),COALESCE(SUM(p.committed_exposure_usd),0)
                   FROM positions p JOIN entries e ON e.entry_id=p.entry_id
                   JOIN runtime_sessions s ON s.session_id=e.session_id
                   WHERE p.status='OPEN' AND s.cohort=?""", (cohort_name,),
            ).fetchone()
            open_count = int(open_count or 0)
            open_cost = float(open_cost or 0)
            if open_count >= int(max_concurrent_positions):
                raise ExposureLimitExceeded("max_concurrent_positions")
            asset_open_count = int(conn.execute(
                """SELECT COUNT(*) FROM positions p
                   JOIN entries e ON e.entry_id=p.entry_id
                   JOIN runtime_sessions s ON s.session_id=e.session_id
                   WHERE p.status='OPEN' AND p.asset=? AND s.cohort=?""",
                (asset, cohort_name),
            ).fetchone()[0])
            if asset_open_count >= int(max_open_per_asset):
                raise ExposureLimitExceeded("max_open_per_asset")
            realized = float(conn.execute(
                """SELECT COALESCE(SUM(pr.net_pnl),0) FROM pnl_records pr
                   JOIN entries e ON e.entry_id=pr.entry_id
                   JOIN runtime_sessions s ON s.session_id=e.session_id
                   WHERE s.cohort=?""", (cohort_name,),
            ).fetchone()[0] or 0)
            unresolved_cost = float(conn.execute(
                """SELECT COALESCE(SUM(p.committed_exposure_usd),0)
                   FROM positions p JOIN entries e ON e.entry_id=p.entry_id
                   JOIN runtime_sessions s ON s.session_id=e.session_id
                   WHERE p.status='UNRESOLVED_FINAL' AND s.cohort=?""",
                (cohort_name,),
            ).fetchone()[0] or 0)
            reserved = float(conn.execute(
                """SELECT COALESCE(SUM(l.reserved_commitment_usd),0)
                   FROM window_locks l
                   JOIN runtime_sessions s ON s.session_id=l.session_id
                   WHERE l.state='RESERVED' AND s.cohort=? AND l.window_id!=?""",
                (cohort_name, int(row["window_id"])),
            ).fetchone()[0] or 0)
            equity = round(float(starting_equity_usd) + realized, 10)
            if equity <= 0.0:
                raise ExposureLimitExceeded("cohort_equity_depleted")
            # Position rows carry cost basis only (gross cost + entry fee);
            # the conservative exit-fee buffer is committed per open position
            # at check time so it is never double-counted into cost sums.
            position_cost = round(
                float(row["gross_cost"]) + float(row.get("estimated_fee") or 0), 10)
            proposed = round(position_cost + exit_buffer, 10)
            committed_existing = round(
                open_cost + unresolved_cost + reserved
                + open_count * exit_buffer, 10)
            projected = round(committed_existing + proposed, 10)
            capital_cap = round(equity * float(max_exposure_pct), 10)
            if projected > capital_cap + 1e-9:
                raise ExposureLimitExceeded("insufficient_capital")
            entry_id = self._insert("entries", row, conn=conn)
            conn.execute(
                """INSERT INTO positions(entry_id,asset,outcome_side,shares,open_shares,
                   committed_exposure_usd,status,opened_ts_ms)
                   VALUES(?,?,?,?,? ,?,'OPEN',?)""",
                (entry_id, asset, str(row["outcome_side"]), FIXED_SHARES, FIXED_SHARES,
                 position_cost, int(row["entry_ts_ms"])),
            )
            conn.execute(
                """UPDATE cohorts SET
                   peak_committed_usd=MAX(peak_committed_usd,?),
                   peak_exposure_pct=MAX(peak_exposure_pct,?) WHERE cohort=?""",
                (projected, round(projected / equity, 10), cohort_name),
            )
            if float(row.get("estimated_fee") or 0) > 0:
                conn.execute(
                    """INSERT INTO fee_components(entry_id,component,amount_usd,rate,
                       formula,estimated,calculated_ts_ms) VALUES(?,?,?,?,?,?,?)""",
                    (entry_id, "ENTRY_FEE", float(row["estimated_fee"]), None,
                     "provided_fee_model", 1, int(row["entry_ts_ms"])),
                )
            conn.execute(
                """UPDATE window_locks SET state='ENTERED',decision_id=?,candidate_id=?,
                   updated_ts_ms=?,reserved_commitment_usd=0 WHERE window_id=?""",
                (int(row["decision_id"]), int(row["candidate_id"]),
                 int(row["entry_ts_ms"]), int(row["window_id"])),
            )
            conn.execute(
                """UPDATE window_funnel SET actual_entry=1,entry_ts_ms=?,
                   execution_attempts=execution_attempts+1,updated_ts_ms=? WHERE window_id=?""",
                (int(row["entry_ts_ms"]), int(row["entry_ts_ms"]), int(row["window_id"])),
            )
            self._pin_trade_evidence(conn, row)
        return entry_id

    reserve_and_create_entry = create_entry
    record_entry = create_entry

    @staticmethod
    def _pin_trade_evidence(conn: sqlite3.Connection, entry: Mapping[str, Any]) -> None:
        conn.execute(
            """UPDATE book_snapshots SET retention_class='TRADE_EVIDENCE',pin_count=pin_count+1
               WHERE book_snapshot_id=?""", (int(entry["book_snapshot_id"]),)
        )
        candidate_id = int(entry["candidate_id"])
        conn.execute(
            """UPDATE book_snapshots SET retention_class='TRADE_EVIDENCE',pin_count=pin_count+1
               WHERE book_snapshot_id IN (
                 SELECT book_snapshot_id FROM candidate_book_evidence WHERE candidate_id=?)""",
            (candidate_id,),
        )
        conn.execute(
            """UPDATE cex_observations SET retention_class='TRADE_EVIDENCE',pin_count=pin_count+1
               WHERE cex_observation_id IN (
                 SELECT cex_observation_id FROM candidate_cex_evidence WHERE candidate_id=?)""",
            (candidate_id,),
        )
        conn.execute(
            """UPDATE source_events SET retention_class='TRADE_EVIDENCE',pin_count=pin_count+1
               WHERE source_event_id=(SELECT trigger_source_event_id FROM candidates WHERE candidate_id=?)""",
            (candidate_id,),
        )

    def record_maker_observation(self, value: Any) -> int:
        row = self._safe_payload(value)
        if bool(row.get("maker_fill_assumed")):
            raise ValueError("maker fill may never be assumed")
        row["maker_fill_assumed"] = 0
        with self.transaction(immediate=True) as conn:
            return self._insert("maker_observations", row, conn=conn)

    def record_maker_update(self, value: Any) -> None:
        with self.transaction() as conn:
            self._insert("maker_updates", value, conn=conn)

    def finish_maker_observation(self, maker_observation_id: int, *, end_ts_ms: int,
                                 end_monotonic_ns: int, final_fair_value_id: int,
                                 final_book_snapshot_id: Optional[int],
                                 final_net_edge: Optional[float],
                                 price_touched: bool, outcome: str, reason: str) -> None:
        with self.transaction(immediate=True) as conn:
            row = conn.execute(
                "SELECT start_monotonic_ns FROM maker_observations WHERE maker_observation_id=?",
                (int(maker_observation_id),),
            ).fetchone()
            if row is None:
                raise ValueError("unknown maker observation")
            duration = max(0, (int(end_monotonic_ns) - int(row[0])) // 1_000_000)
            conn.execute(
                """UPDATE maker_observations SET maker_end_ts_ms=?,end_monotonic_ns=?,
                   actual_duration_ms=?,final_fair_value_calculation_id=?,
                   final_book_snapshot_id=?,final_net_edge=?,price_touched=?,outcome=?,
                   reason=?,maker_fill_assumed=0 WHERE maker_observation_id=?""",
                (int(end_ts_ms), int(end_monotonic_ns), duration, int(final_fair_value_id),
                 (int(final_book_snapshot_id) if final_book_snapshot_id is not None else None),
                 (float(final_net_edge) if final_net_edge is not None else None),
                 int(bool(price_touched)),
                 str(outcome), str(reason), int(maker_observation_id)),
            )

    def open_positions(self) -> list[dict[str, Any]]:
        return self.query(
            """SELECT p.*,e.window_id,e.market_identity_id,e.candidate_id,e.decision_id,
               e.token_id,e.entry_ts_ms,e.entry_mode,e.executable_vwap,e.selected_net_edge,
               w.window_close_ts_ms FROM positions p JOIN entries e ON e.entry_id=p.entry_id
               JOIN asset_windows w ON w.window_id=e.window_id
               WHERE p.status='OPEN' ORDER BY p.opened_ts_ms"""
        )

    def record_management_decision(self, value: Any) -> int:
        row = self._safe_payload(value)
        with self.transaction(immediate=True) as conn:
            position = conn.execute(
                """SELECT p.status,w.window_close_ts_ms FROM positions p
                   JOIN entries e ON e.entry_id=p.entry_id
                   JOIN asset_windows w ON w.window_id=e.window_id WHERE p.position_id=?""",
                (int(row["position_id"]),),
            ).fetchone()
            if position is None or str(position[0]) != "OPEN":
                raise ValueError("management requires an open position")
            if str(row["action"]) == "EXIT_BOOK" and int(row["decision_ts_ms"]) >= int(position[1]):
                raise ValueError("post-close book exit is forbidden")
            return self._insert("management_decisions", row, conn=conn)

    def record_resolution_attempt(self, value: Any) -> int:
        with self.transaction(immediate=True) as conn:
            return self._insert("resolution_attempts", value, conn=conn)

    def close_position(self, value: Any) -> int:
        row = self._safe_payload(value)
        with self.transaction(immediate=True) as conn:
            position = conn.execute(
                """SELECT p.*,e.window_id,e.gross_cost,e.execution_verified,
                   w.window_close_ts_ms FROM positions p JOIN entries e ON e.entry_id=p.entry_id
                   JOIN asset_windows w ON w.window_id=e.window_id WHERE p.position_id=?""",
                (int(row["position_id"]),),
            ).fetchone()
            if position is None or str(position["status"]) != "OPEN":
                raise ValueError("position is not open")
            if str(row["exit_source"]) == "BOOK":
                if int(row["exit_ts_ms"]) >= int(position["window_close_ts_ms"]):
                    raise ValueError("post-close book exit is forbidden")
                if row.get("book_snapshot_id") is None:
                    raise ValueError("book exit requires exact book evidence")
                management = conn.execute(
                    """SELECT action FROM management_decisions WHERE position_id=?
                       ORDER BY decision_seq DESC LIMIT 1""",
                    (int(row["position_id"]),),
                ).fetchone()
                if management is None or str(management[0]) != "EXIT_BOOK":
                    raise ValueError("book exit requires an EXIT_BOOK management decision")
            else:
                if not bool(row.get("evidence_verified")):
                    raise ValueError("official resolution must be verified")
                resolution = conn.execute(
                    """SELECT observed_outcome FROM resolution_attempts
                       WHERE entry_id=? AND verified=1 AND result='RESOLVED'
                       ORDER BY attempt_no DESC LIMIT 1""",
                    (int(position["entry_id"]),),
                ).fetchone()
                if resolution is None:
                    raise ValueError("official close requires a verified resolution attempt")
                if str(resolution[0]) != str(row.get("resolution_outcome")):
                    raise ValueError("official resolution outcome mismatch")
                expected_payout = FIXED_SHARES if str(resolution[0]) == str(position["outcome_side"]) else 0.0
                if abs(float(row["payout_usd"]) - expected_payout) > 1e-8:
                    raise ValueError("official payout is inconsistent with verified outcome")
            if abs(float(row["shares"]) - float(position["open_shares"])) > 1e-8:
                raise ValueError("terminal close must account for all open shares")
            expected_gross = float(row["payout_usd"]) - float(position["gross_cost"])
            if abs(expected_gross - float(row["gross_pnl"])) > 1e-8:
                raise ValueError("gross PnL does not reconcile to payout minus entry cost")
            row["entry_id"] = int(position["entry_id"])
            exit_id = self._insert("exits", row, conn=conn)
            if float(row.get("exit_fee") or 0) > 0:
                conn.execute(
                    """INSERT INTO fee_components(exit_id,component,amount_usd,rate,
                       formula,estimated,calculated_ts_ms) VALUES(?,?,?,?,?,?,?)""",
                    (exit_id, "EXIT_FEE", float(row["exit_fee"]), None,
                     "provided_fee_model", 0, int(row["exit_ts_ms"])),
                )
            total_entry_fees = float(conn.execute(
                "SELECT COALESCE(SUM(amount_usd),0) FROM fee_components WHERE entry_id=?",
                (int(position["entry_id"]),),
            ).fetchone()[0] or 0)
            total_fees = total_entry_fees + float(row.get("exit_fee") or 0)
            expected_net = float(row["gross_pnl"]) - total_fees
            if abs(expected_net - float(row["net_pnl"])) > 1e-8:
                raise ValueError("fee/PnL reconciliation failed")
            verified = bool(position["execution_verified"]) and bool(row["evidence_verified"])
            conn.execute(
                """INSERT INTO pnl_records(entry_id,terminal_ts_ms,gross_pnl,total_fees,
                   net_pnl,outcome,exit_source,execution_evidence_complete,
                   fee_evidence_complete,resolution_evidence_complete,verified)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (int(position["entry_id"]), int(row["exit_ts_ms"]), float(row["gross_pnl"]),
                 total_fees, float(row["net_pnl"]), str(row.get("resolution_outcome") or "EXIT"),
                 str(row["exit_source"]), int(bool(position["execution_verified"])), 1,
                 int(bool(row["evidence_verified"])), int(verified)),
            )
            conn.execute(
                "UPDATE positions SET open_shares=0,status='CLOSED',closed_ts_ms=? WHERE position_id=?",
                (int(row["exit_ts_ms"]), int(row["position_id"])),
            )
            conn.execute(
                "UPDATE entries SET status='CLOSED' WHERE entry_id=?",
                (int(position["entry_id"]),),
            )
            conn.execute(
                """UPDATE window_locks SET state='COMPLETE',updated_ts_ms=? WHERE window_id=?""",
                (int(row["exit_ts_ms"]), int(position["window_id"])),
            )
            conn.execute(
                """UPDATE window_funnel SET terminal=1,terminal_ts_ms=?,updated_ts_ms=?
                   WHERE window_id=?""",
                (int(row["exit_ts_ms"]), int(row["exit_ts_ms"]), int(position["window_id"])),
            )
        return exit_id

    def mark_unresolved_final(self, entry_id: int, ts_ms: int, reason: str) -> None:
        with self.transaction(immediate=True) as conn:
            row = conn.execute(
                "SELECT position_id,window_id FROM positions JOIN entries USING(entry_id) WHERE entry_id=?",
                (int(entry_id),),
            ).fetchone()
            if row is None:
                raise ValueError("unknown entry")
            conn.execute("UPDATE entries SET status='UNRESOLVED_FINAL' WHERE entry_id=?", (int(entry_id),))
            conn.execute(
                """UPDATE positions SET status='UNRESOLVED_FINAL',closed_ts_ms=?
                   WHERE position_id=?""", (int(ts_ms), int(row[0]))
            )
            conn.execute(
                """UPDATE window_funnel SET terminal=0,final_blocker=?,updated_ts_ms=?
                   WHERE window_id=?""", (str(reason), int(ts_ms), int(row[1]))
            )

    def record_reject(self, value: Any) -> int:
        row = self._safe_payload(value)
        row.setdefault("dedupe_key", stable_idempotency_key(
            row.get("session_id"), row.get("window_id"), row.get("candidate_id"),
            row.get("taxonomy"), row.get("reason"), row.get("reject_ts_ms")
        ))
        with self.transaction(immediate=True) as conn:
            try:
                return self._insert("reject_events", row, conn=conn)
            except sqlite3.IntegrityError as exc:
                if "UNIQUE constraint failed" not in str(exc):
                    raise
                existing = conn.execute(
                    "SELECT reject_event_id FROM reject_events WHERE dedupe_key=?",
                    (str(row["dedupe_key"]),),
                ).fetchone()
                return int(existing[0])

    def record_latency(self, value: Any) -> int:
        with self.transaction() as conn:
            return self._insert("latency_metrics", value, conn=conn)

    def record_source_health(self, value: Any) -> int:
        with self.transaction() as conn:
            return self._insert("source_health", value, conn=conn)

    def record_runtime_health(self, value: Any) -> int:
        with self.transaction() as conn:
            return self._insert("runtime_health", value, conn=conn)

    def record_persistence_worker_sample(self, value: Any) -> int:
        with self.transaction() as conn:
            return self._insert("persistence_worker_samples", value, conn=conn)

    def record_compounding_preview(self, value: Any) -> int:
        row = self._safe_payload(value)
        row["influences_sizing"] = 0
        with self.transaction(immediate=True) as conn:
            return self._insert("compounding_preview", row, conn=conn)

    @staticmethod
    def _bundle(value: Optional[Mapping[str, Any]], parts: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(value or {})
        result.update(parts)
        return result

    @staticmethod
    def _wrapped_row(value: Any) -> tuple[dict[str, Any], dict[str, Any]]:
        wrapper = dict(value) if isinstance(value, Mapping) else {"value": value}
        if "value" in wrapper:
            row = _record(wrapper.pop("value"))
            return row, wrapper
        return dict(wrapper), {}

    def persist_market_bundle(
        self, value: Optional[Mapping[str, Any]] = None, **parts: Any,
    ) -> dict[str, Any]:
        """Atomically upsert one market, identity, exact window and metadata.

        Bundle keys are ``market``, ``identity``, ``window`` and optional
        ``link``, ``anchor`` and ``funnel``.  ``funnel`` accepts either
        ``{"now_ms": ..., "changes": {...}}`` or fields plus ``updated_ts_ms``.
        """

        bundle = self._safe_payload(self._bundle(value, parts))
        for required in ("market", "identity", "window"):
            if required not in bundle:
                raise ValueError(f"market bundle missing {required}")
        with self.transaction(immediate=True) as conn:
            market_id = self.upsert_market(bundle["market"])
            identity = self._safe_payload(bundle["identity"])
            identity["market_id"] = market_id
            identity.setdefault("identity_fingerprint", stable_idempotency_key(
                identity.get("market_id"), identity.get("event_id"),
                identity.get("condition_id"), identity.get("yes_token_id"),
                identity.get("no_token_id"),
            ))
            found = conn.execute(
                "SELECT * FROM market_identities WHERE identity_fingerprint=?",
                (str(identity["identity_fingerprint"]),),
            ).fetchone()
            if found is None:
                market_identity_id = self.record_market_identity(identity)
            else:
                found_row = dict(found)
                for name in ("market_id", "event_id", "condition_id",
                             "yes_token_id", "no_token_id"):
                    if str(found_row[name]) != str(identity[name]):
                        raise V4StoreError("market identity fingerprint conflict")
                market_identity_id = int(found_row["market_identity_id"])

            window_id = self.ensure_asset_window(bundle["window"])
            link_idempotent = False
            if bundle.get("link") is not None:
                link = self._safe_payload(bundle["link"])
                link.update({
                    "window_id": window_id,
                    "market_identity_id": market_identity_id,
                })
                existing_link = conn.execute(
                    "SELECT * FROM window_market_links WHERE window_id=? AND market_identity_id=?",
                    (window_id, market_identity_id),
                ).fetchone()
                if existing_link is None:
                    self.link_window_market(link)
                else:
                    link_idempotent = True

            anchor_id: Optional[int] = None
            if bundle.get("anchor") is not None:
                anchor = self._safe_payload(bundle["anchor"])
                anchor["market_identity_id"] = market_identity_id
                existing_anchor = conn.execute(
                    """SELECT anchor_observation_id FROM anchor_observations
                       WHERE market_identity_id=? AND status=?
                       AND price_to_beat IS ? AND source_field IS ?
                       AND parse_error IS ? AND provider_ts_ms IS ? AND receipt_ts_ms=?""",
                    (market_identity_id, anchor.get("status"), anchor.get("price_to_beat"),
                     anchor.get("source_field"), anchor.get("parse_error"),
                     anchor.get("provider_ts_ms"), int(anchor["receipt_ts_ms"])),
                ).fetchone()
                anchor_id = (int(existing_anchor[0]) if existing_anchor is not None
                             else self.record_anchor_observation(anchor))

            if bundle.get("funnel") is not None:
                funnel = self._safe_payload(bundle["funnel"])
                changes = dict(funnel.pop("changes", funnel))
                now_value = int(funnel.get("now_ms", changes.pop(
                    "now_ms", changes.pop("updated_ts_ms", 0))))
                if now_value <= 0:
                    raise ValueError("market bundle funnel requires now_ms")
                self.update_window_funnel(window_id, now_value, **changes)
        return {
            "market_id": market_id, "market_identity_id": market_identity_id,
            "window_id": window_id, "anchor_observation_id": anchor_id,
            "link_idempotent": link_idempotent,
        }

    def _record_book_snapshot_idempotent(self, value: Any) -> int:
        row = self._safe_payload(value)
        row.setdefault("state_hash", _canonical_hash({
            key: row.get(key) for key in (
                "token_id", "bids_json", "asks_json", "best_bid", "best_ask")
        }))
        found = self.query_one(
            """SELECT book_snapshot_id FROM book_snapshots
               WHERE market_identity_id=? AND token_id=? AND state_hash=?
               AND receipt_ts_ms=?""",
            (row["market_identity_id"], row["token_id"], row["state_hash"],
             row["receipt_ts_ms"]),
        )
        return (int(found["book_snapshot_id"]) if found is not None
                else self.record_book_snapshot(row))

    def persist_evaluation_bundle(
        self, value: Optional[Mapping[str, Any]] = None, **parts: Any,
    ) -> dict[str, Any]:
        """Persist a complete candidate evidence graph in one transaction.

        Supported keys: ``source_event`` (optional wrapper with ``value`` and
        ``kwargs``), ``books``, ``cex`` (observation wrappers), ``candidate``,
        ``candidate_book_links``, ``candidate_cex_links``, ``models``,
        ``fair_value`` (``calculation`` and ``sides``), ``decision``, optional
        ``latency`` and ``funnel``.
        """

        bundle = self._safe_payload(self._bundle(value, parts))
        for required in ("candidate", "fair_value", "decision"):
            if required not in bundle:
                raise ValueError(f"evaluation bundle missing {required}")
        with self.transaction(immediate=True) as conn:
            source_event_id: Optional[int] = None
            if bundle.get("source_event") is not None:
                source_row, source_meta = self._wrapped_row(bundle["source_event"])
                source_kwargs = dict(source_meta.get("kwargs") or {})
                source_kwargs.update({
                    "reference_only": True,
                    "count_in_bucket": False,
                    "admitted_at_receipt": True,
                })
                source_result = self.record_source_event(
                    source_row, **source_kwargs)
                source_event_id = int(source_result["source_event_id"])

            book_ids: list[int] = []
            book_meta: list[dict[str, Any]] = []
            for raw_book in bundle.get("books") or ():
                book, meta = self._wrapped_row(raw_book)
                # A candidate trigger is not necessarily the event that
                # produced either book. Never misattribute a CEX or opposite-
                # side trigger as the snapshot's Polymarket source evidence.
                # Callers may supply an exact per-book source_event_id; absent
                # that evidence, NULL is the only honest value.
                book_ids.append(self._record_book_snapshot_idempotent(book))
                book_meta.append(meta)

            cex_ids: list[int] = []
            cex_meta: list[dict[str, Any]] = []
            cex_source_ids: list[Optional[int]] = []
            for raw_cex in bundle.get("cex") or ():
                observation, meta = self._wrapped_row(raw_cex)
                cex_source_id: Optional[int] = None
                if meta.get("source_event") is not None:
                    cex_source, cex_source_meta = self._wrapped_row(meta["source_event"])
                    cex_source_kwargs = dict(cex_source_meta.get("kwargs") or {})
                    cex_source_kwargs.update({
                        "reference_only": True,
                        "count_in_bucket": False,
                        "admitted_at_receipt": True,
                    })
                    source_result = self.record_source_event(
                        cex_source, **cex_source_kwargs)
                    cex_source_id = int(source_result["source_event_id"])
                    observation.setdefault("source_event_id", cex_source_id)
                result = self.record_cex_observation(
                    observation, **dict(meta.get("kwargs") or {}))
                cex_ids.append(int(result["cex_observation_id"]))
                cex_source_ids.append(cex_source_id)
                cex_meta.append(meta)

            candidate = self._safe_payload(bundle["candidate"])
            if source_event_id is not None:
                candidate.setdefault("trigger_source_event_id", source_event_id)
            candidate_id = self.record_candidate(candidate)

            explicit_book_links = list(bundle.get("candidate_book_links") or ())
            if not explicit_book_links:
                explicit_book_links = [
                    {
                        "book_index": index,
                        "side": meta.get("side", meta.get("outcome_side",
                                 self.query_one(
                                     "SELECT outcome_side FROM book_snapshots WHERE book_snapshot_id=?",
                                     (book_id,),
                                 )["outcome_side"])),
                        "evidence_age_ms": int(meta.get("evidence_age_ms") or 0),
                    }
                    for index, (book_id, meta) in enumerate(zip(book_ids, book_meta))
                ]
            for raw_link in explicit_book_links:
                link = dict(raw_link)
                index = int(link.pop("book_index"))
                self.link_candidate_book(
                    candidate_id, str(link.pop("side", link.pop("outcome_side", ""))),
                    book_ids[index], int(link.pop("evidence_age_ms", 0)))
                if link:
                    raise ValueError(f"unsupported candidate book link fields: {sorted(link)}")

            explicit_cex_links = list(bundle.get("candidate_cex_links") or ())
            if not explicit_cex_links:
                explicit_cex_links = [
                    {"cex_index": index, "role": meta.get("role", "POINT_IN_TIME_FEATURE"),
                     "horizon_ms": int(meta.get("horizon_ms") or 0),
                     "evidence_age_ms": int(meta.get("evidence_age_ms") or 0)}
                    for index, meta in enumerate(cex_meta)
                ]
            for raw_link in explicit_cex_links:
                link = dict(raw_link)
                index = int(link.pop("cex_index"))
                self.link_candidate_cex(
                    candidate_id, cex_ids[index],
                    str(link.pop("role", link.pop("evidence_role", "POINT_IN_TIME_FEATURE"))),
                    int(link.pop("horizon_ms", 0)),
                    int(link.pop("evidence_age_ms", 0)))
                if link:
                    raise ValueError(f"unsupported candidate CEX link fields: {sorted(link)}")

            model_ids: list[int] = []
            for raw_model in bundle.get("models") or ():
                model = self._safe_payload(raw_model)
                model["candidate_id"] = candidate_id
                existing_model = conn.execute(
                    "SELECT model_contribution_id FROM model_contributions "
                    "WHERE candidate_id=? AND model_name=?",
                    (candidate_id, str(model["model_name"])),
                ).fetchone()
                model_ids.append(int(existing_model[0]) if existing_model is not None
                                 else self.record_model_contribution(model))

            fair_spec = dict(bundle["fair_value"])
            calculation = self._safe_payload(fair_spec.get("calculation") or {})
            calculation["candidate_id"] = candidate_id
            sides: list[dict[str, Any]] = []
            for raw_side in fair_spec.get("sides") or ():
                side, meta = self._wrapped_row(raw_side)
                if meta.get("book_index") is not None:
                    side["book_snapshot_id"] = book_ids[int(meta["book_index"])]
                sides.append(side)
            existing_fair = conn.execute(
                """SELECT fair_value_calculation_id FROM fair_value_calculations
                   WHERE candidate_id=? AND phase=? AND calculation_seq=?""",
                (candidate_id, str(calculation["phase"]),
                 int(calculation.get("calculation_seq") or 0)),
            ).fetchone()
            if existing_fair is None:
                fair_id = self.record_fair_value(calculation, sides)
            else:
                fair_id = int(existing_fair[0])
                existing_sides = {
                    str(row[0]) for row in conn.execute(
                        "SELECT outcome_side FROM fair_value_sides "
                        "WHERE fair_value_calculation_id=?", (fair_id,)).fetchall()
                }
                for side in sides:
                    if str(side["outcome_side"]) not in existing_sides:
                        side = dict(side)
                        side["fair_value_calculation_id"] = fair_id
                        self._insert("fair_value_sides", side, conn=conn)

            decision = self._safe_payload(bundle["decision"])
            decision.update({
                "candidate_id": candidate_id,
                "fair_value_calculation_id": fair_id,
            })
            existing_decision = conn.execute(
                "SELECT decision_id FROM decisions WHERE candidate_id=? AND decision_seq=?",
                (candidate_id, int(decision.get("decision_seq") or 0)),
            ).fetchone()
            decision_id = (int(existing_decision[0]) if existing_decision is not None
                           else self.record_decision(decision))

            latency_id: Optional[int] = None
            if bundle.get("latency") is not None:
                latency = self._safe_payload(bundle["latency"])
                latency.setdefault("candidate_id", candidate_id)
                if source_event_id is not None:
                    latency.setdefault("source_event_id", source_event_id)
                existing_latency = conn.execute(
                    """SELECT latency_metric_id FROM latency_metrics
                       WHERE candidate_id=? AND stage=? AND measured_ts_ms=?
                       AND completed_ts_ms IS ?""",
                    (candidate_id, str(latency["stage"]), int(latency["measured_ts_ms"]),
                     latency.get("completed_ts_ms")),
                ).fetchone()
                latency_id = (int(existing_latency[0]) if existing_latency is not None
                              else self.record_latency(latency))

            if bundle.get("funnel") is not None:
                funnel = self._safe_payload(bundle["funnel"])
                window_id = int(funnel.pop("window_id", candidate["window_id"]))
                changes = dict(funnel.pop("changes", funnel))
                now_value = int(funnel.get("now_ms", changes.pop(
                    "now_ms", changes.pop("updated_ts_ms", 0))))
                if now_value <= 0:
                    raise ValueError("evaluation bundle funnel requires now_ms")
                self.update_window_funnel(window_id, now_value, **changes)
        return {
            "source_event_id": source_event_id,
            "book_snapshot_ids": book_ids,
            "cex_source_event_ids": cex_source_ids,
            "cex_observation_ids": cex_ids,
            "candidate_id": candidate_id,
            "model_contribution_ids": model_ids,
            "fair_value_calculation_id": fair_id,
            "decision_id": decision_id,
            "latency_metric_id": latency_id,
        }

    def reserve_and_create_entry_bundle(
        self, reservation: Any, entry: Optional[Any] = None, *,
        max_concurrent_positions: Optional[int] = None,
        cohort: str = ACTIVE_COHORT,
        starting_equity_usd: float = 13.0,
        max_exposure_pct: float = 1.0,
        exit_fee_buffer_usd: float = MIN_EXIT_FEE_BUFFER_USD,
        max_open_per_asset: int = 1,
        commit_deadline_ts_ms: Optional[int] = None,
    ) -> dict[str, Any]:
        """Reserve and create an entry under one outer IMMEDIATE transaction.

        The optional wall-clock deadline is checked *after* SQLite grants the
        write transaction.  A command that waited behind another writer can
        therefore never reserve a window or create an entry using evidence
        that expired while it was queued or blocked on ``BEGIN IMMEDIATE``.
        """

        if entry is None and isinstance(reservation, Mapping) and "reservation" in reservation:
            bundle = dict(reservation)
            entry = bundle["entry"]
            reservation = bundle["reservation"]
            max_concurrent_positions = int(bundle["max_concurrent_positions"])
            cohort = str(bundle.get("cohort", cohort))
            starting_equity_usd = float(
                bundle.get("starting_equity_usd", starting_equity_usd))
            max_exposure_pct = float(
                bundle.get("max_exposure_pct", max_exposure_pct))
            exit_fee_buffer_usd = float(
                bundle.get("exit_fee_buffer_usd", exit_fee_buffer_usd))
            max_open_per_asset = int(bundle.get("max_open_per_asset", max_open_per_asset))
            if bundle.get("commit_deadline_ts_ms") is not None:
                commit_deadline_ts_ms = int(bundle["commit_deadline_ts_ms"])
        if entry is None or max_concurrent_positions is None:
            raise ValueError("entry bundle is incomplete")
        entry_payload = self._safe_payload(entry)
        reservation_payload = self._safe_payload(reservation)
        # The reservation carries the full proposed commitment so that a
        # crash between reservation and entry keeps the capital committed
        # (released only by explicit, evidence-based reconciliation).
        reservation_payload.setdefault("reserved_commitment_usd", round(
            float(entry_payload.get("gross_cost") or 0)
            + float(entry_payload.get("estimated_fee") or 0)
            + float(exit_fee_buffer_usd), 10))
        with self.transaction(immediate=True):
            if (commit_deadline_ts_ms is not None
                    and int(time.time() * 1_000) > int(commit_deadline_ts_ms)):
                raise V4StoreError("entry commit deadline expired")
            reservation_result = self.reserve_window(reservation_payload)
            entry_id = self.create_entry(
                entry_payload,
                max_concurrent_positions=int(max_concurrent_positions),
                cohort=str(cohort),
                starting_equity_usd=float(starting_equity_usd),
                max_exposure_pct=float(max_exposure_pct),
                exit_fee_buffer_usd=float(exit_fee_buffer_usd),
                max_open_per_asset=int(max_open_per_asset),
            )
            position = self.query_one(
                "SELECT * FROM positions WHERE entry_id=?", (int(entry_id),))
            if position is None:
                raise V4StoreError("entry bundle committed without a position")
        return {
            "reservation": reservation_result,
            "entry_id": entry_id,
            "position_id": int(position["position_id"]),
            "position": position,
        }

    def management_bundle(self, value: Any, close: Optional[Any] = None) -> dict[str, Any]:
        """Atomically persist management authorization and optional book close."""

        if close is None and isinstance(value, Mapping) and "decision" in value:
            bundle = dict(value)
            decision, close = bundle["decision"], bundle.get("close")
        else:
            decision = value
        row = self._safe_payload(decision)
        with self.transaction(immediate=True) as conn:
            existing = conn.execute(
                "SELECT management_decision_id,action FROM management_decisions "
                "WHERE position_id=? AND decision_seq=?",
                (int(row["position_id"]), int(row["decision_seq"])),
            ).fetchone()
            if existing is None:
                management_id = self.record_management_decision(row)
            else:
                if str(existing[1]) != str(row["action"]):
                    raise V4StoreError("management decision idempotency conflict")
                management_id = int(existing[0])
            exit_id: Optional[int] = None
            if close is not None:
                position = conn.execute(
                    "SELECT status FROM positions WHERE position_id=?",
                    (int(row["position_id"]),),
                ).fetchone()
                if position is not None and str(position[0]) == "OPEN":
                    exit_id = self.close_position(close)
                else:
                    prior_exit = conn.execute(
                        "SELECT exit_id FROM exits WHERE position_id=? ORDER BY exit_id LIMIT 1",
                        (int(row["position_id"]),),
                    ).fetchone()
                    if prior_exit is None:
                        raise ValueError("closed management position has no exit")
                    exit_id = int(prior_exit[0])
        return {"management_decision_id": management_id, "exit_id": exit_id}

    def resolution_bundle(self, value: Any, close: Optional[Any] = None) -> dict[str, Any]:
        """Atomically persist an official attempt and optional terminal close."""

        if close is None and isinstance(value, Mapping) and "attempt" in value:
            bundle = dict(value)
            attempt, close = bundle["attempt"], bundle.get("close")
        else:
            attempt = value
        row = self._safe_payload(attempt)
        with self.transaction(immediate=True) as conn:
            existing = conn.execute(
                "SELECT resolution_attempt_id,evidence_hash FROM resolution_attempts "
                "WHERE entry_id=? AND attempt_no=?",
                (int(row["entry_id"]), int(row["attempt_no"])),
            ).fetchone()
            if existing is None:
                attempt_id = self.record_resolution_attempt(row)
            else:
                if str(existing[1] or "") != str(row.get("evidence_hash") or ""):
                    raise V4StoreError("resolution attempt idempotency conflict")
                attempt_id = int(existing[0])
            exit_id: Optional[int] = None
            if close is not None:
                position = conn.execute(
                    "SELECT position_id,status FROM positions WHERE entry_id=?",
                    (int(row["entry_id"]),),
                ).fetchone()
                if position is None:
                    raise ValueError("resolution entry has no position")
                if str(position[1]) == "OPEN":
                    exit_id = self.close_position(close)
                else:
                    prior_exit = conn.execute(
                        "SELECT exit_id FROM exits WHERE position_id=? ORDER BY exit_id LIMIT 1",
                        (int(position[0]),),
                    ).fetchone()
                    if prior_exit is None:
                        raise ValueError("resolved position has no exit")
                    exit_id = int(prior_exit[0])
        return {"resolution_attempt_id": attempt_id, "exit_id": exit_id}

    def reconcile_startup_state(
        self, *, current_launch_nonce: Optional[str] = None,
        proven_absent_launch_nonces: Iterable[str] = (),
        reconciled_ts_ms: Optional[int] = None,
    ) -> dict[str, int]:
        """Validate lifecycle consistency and conservatively release stale locks.

        A RESERVED lock is released only when its nonce is explicitly supplied
        as externally proven absent, it is not the current nonce, and no entry
        exists.  No entry, position, exit, or execution evidence is replayed or
        synthesized by recovery.
        """

        absent = {str(value) for value in proven_absent_launch_nonces if str(value)}
        current = str(current_launch_nonce or "")
        if current and current in absent:
            raise ValueError("current launch nonce cannot be proven absent")
        timestamp = int(reconciled_ts_ms or int(time.time() * 1_000))
        if timestamp < 0:
            raise ValueError("invalid reconciliation timestamp")
        with self.transaction(immediate=True) as conn:
            counts = {
                "entries_without_positions": int(conn.execute(
                    """SELECT COUNT(*) FROM entries e LEFT JOIN positions p USING(entry_id)
                       WHERE p.position_id IS NULL""").fetchone()[0]),
                "open_entry_position_mismatch": int(conn.execute(
                    """SELECT COUNT(*) FROM entries e JOIN positions p USING(entry_id)
                       WHERE (e.status='OPEN') <> (p.status='OPEN')""").fetchone()[0]),
                "closed_positions_without_exit": int(conn.execute(
                    """SELECT COUNT(*) FROM positions p LEFT JOIN exits x USING(position_id)
                       WHERE p.status='CLOSED' AND x.exit_id IS NULL""").fetchone()[0]),
                "exits_with_nonclosed_position": int(conn.execute(
                    """SELECT COUNT(*) FROM exits x JOIN positions p USING(position_id)
                       WHERE p.status<>'CLOSED'""").fetchone()[0]),
                "duplicate_position_exits": int(conn.execute(
                    """SELECT COUNT(*) FROM (SELECT position_id FROM exits
                       GROUP BY position_id HAVING COUNT(*)>1)""").fetchone()[0]),
                "reserved_without_entry": int(conn.execute(
                    """SELECT COUNT(*) FROM window_locks wl LEFT JOIN entries e USING(window_id)
                       WHERE wl.state='RESERVED' AND e.entry_id IS NULL""").fetchone()[0]),
                "unfinished_maker_observations": int(conn.execute(
                    """SELECT COUNT(*) FROM maker_observations
                       WHERE maker_end_ts_ms IS NULL""").fetchone()[0]),
            }
            releasable = [
                (int(row[0]), str(row[1])) for row in conn.execute(
                    """SELECT wl.window_id,wl.owner_launch_nonce FROM window_locks wl
                       LEFT JOIN entries e USING(window_id)
                       WHERE wl.state='RESERVED' AND e.entry_id IS NULL""").fetchall()
                if str(row[1]) in absent and str(row[1]) != current
            ]
            released = 0
            for window_id, nonce in releasable:
                cursor = conn.execute(
                    """DELETE FROM window_locks WHERE window_id=?
                       AND owner_launch_nonce=? AND state='RESERVED'
                       AND NOT EXISTS(SELECT 1 FROM entries WHERE window_id=?)""",
                    (window_id, nonce, window_id),
                )
                released += int(cursor.rowcount)
            counts["released_proven_stale_reservations"] = released
            counts["reserved_left_fail_closed"] = max(
                0, counts["reserved_without_entry"] - released)
            # A maker observation cannot outlive its owning session.  Two
            # deterministic proofs finish one as ABANDONED: the session's
            # launch nonce is externally proven absent, or the session
            # durably journaled its own termination (ended_ts_ms) via the
            # terminal end_runtime_session command.  Reservation release
            # above intentionally keeps the stricter external-nonce rule.
            abandoned_makers = conn.execute(
                """UPDATE maker_observations SET
                   maker_end_ts_ms=MAX(maker_start_ts_ms,?),
                   outcome='ABANDONED',reason='STARTUP_RECONCILED',
                   maker_fill_assumed=0
                   WHERE maker_end_ts_ms IS NULL AND candidate_id IN (
                     SELECT c.candidate_id FROM candidates c
                     JOIN runtime_sessions rs ON rs.session_id=c.session_id
                     WHERE rs.ended_ts_ms IS NOT NULL
                        OR rs.launch_nonce IN ({})
                   )""".format(
                    ",".join("?" for _ in absent) if absent else "NULL"
                ),
                (timestamp, *sorted(absent)) if absent else (timestamp,),
            ).rowcount
            counts["reconciled_abandoned_maker_observations"] = int(
                abandoned_makers)
            counts["unfinished_makers_left_fail_closed"] = max(
                0,
                counts["unfinished_maker_observations"]
                - counts["reconciled_abandoned_maker_observations"],
            )
            # Authoritative-cohort ledger invariant: committed capital may
            # never exceed current cohort equity.  Balances are derived from
            # committed rows, so recovery "restores" them by construction —
            # this verifies no corruption slipped in while the writer was
            # down.  A missing cohorts row (pre-activation) verifies nothing.
            ledger_violations = 0
            cohort_row = conn.execute(
                """SELECT starting_equity_usd,max_exposure_pct FROM cohorts
                   WHERE cohort=?""", (ACTIVE_COHORT,),
            ).fetchone()
            if cohort_row is not None:
                realized = float(conn.execute(
                    """SELECT COALESCE(SUM(pr.net_pnl),0) FROM pnl_records pr
                       JOIN entries e ON e.entry_id=pr.entry_id
                       JOIN runtime_sessions s ON s.session_id=e.session_id
                       WHERE s.cohort=?""", (ACTIVE_COHORT,),
                ).fetchone()[0] or 0)
                held = float(conn.execute(
                    """SELECT COALESCE(SUM(p.committed_exposure_usd),0)
                       FROM positions p JOIN entries e ON e.entry_id=p.entry_id
                       JOIN runtime_sessions s ON s.session_id=e.session_id
                       WHERE p.status IN ('OPEN','UNRESOLVED_FINAL')
                       AND s.cohort=?""", (ACTIVE_COHORT,),
                ).fetchone()[0] or 0)
                reserved = float(conn.execute(
                    """SELECT COALESCE(SUM(l.reserved_commitment_usd),0)
                       FROM window_locks l
                       JOIN runtime_sessions s ON s.session_id=l.session_id
                       WHERE l.state='RESERVED' AND s.cohort=?""",
                    (ACTIVE_COHORT,),
                ).fetchone()[0] or 0)
                equity = float(cohort_row[0]) + realized
                cap = equity * float(cohort_row[1])
                if held + reserved > cap + 1e-6:
                    ledger_violations = 1
            counts["cohort_ledger_violations"] = ledger_violations
            counts["consistency_errors"] = sum(
                counts[name] for name in (
                    "entries_without_positions", "open_entry_position_mismatch",
                    "closed_positions_without_exit", "exits_with_nonclosed_position",
                    "duplicate_position_exits", "cohort_ledger_violations",
                ))
            counts["reconciled_ts_ms"] = timestamp
        return counts

    def latest_source_health(self, session_id: Optional[str] = None) -> list[dict[str, Any]]:
        """Return each channel's latest row, optionally scoped to one session."""

        if session_id is None:
            return self.query(
                """SELECT sh.* FROM source_health sh JOIN (
                   SELECT source,channel,MAX(source_health_id) latest_id FROM source_health
                   GROUP BY source,channel) latest ON latest.latest_id=sh.source_health_id
                   ORDER BY sh.source,sh.channel"""
            )
        return self.query(
            """SELECT sh.* FROM source_health sh JOIN (
               SELECT source,channel,MAX(source_health_id) latest_id FROM source_health
               WHERE session_id=? GROUP BY source,channel
               ) latest ON latest.latest_id=sh.source_health_id
               WHERE sh.session_id=? ORDER BY sh.source,sh.channel""",
            (str(session_id), str(session_id)),
        )

    def pin_source_event(self, source_event_id: int) -> None:
        with self.transaction(immediate=True) as conn:
            conn.execute(
                """UPDATE source_events SET retention_class='TRADE_EVIDENCE',
                   pin_count=pin_count+1 WHERE source_event_id=?""",
                (int(source_event_id),),
            )

    @contextmanager
    def _sqlite_deadline(self, deadline_monotonic: Optional[float]) -> Iterator[None]:
        """Interrupt a maintenance statement once its monotonic budget expires."""

        self._assert_owner()
        if deadline_monotonic is None:
            yield
            return
        deadline = float(deadline_monotonic)
        if not math.isfinite(deadline) or deadline <= 0:
            raise ValueError("deadline_monotonic must be finite and positive")
        if time.monotonic() >= deadline:
            raise TimeoutError("maintenance deadline exhausted")
        remaining_ms = max(
            1, int(math.ceil((deadline - time.monotonic()) * 1_000.0)))
        bounded_busy_timeout = min(self.busy_timeout_ms, remaining_ms)
        prior_busy_timeout = int(
            self._conn.execute("PRAGMA busy_timeout").fetchone()[0])
        self._conn.execute(f"PRAGMA busy_timeout={bounded_busy_timeout}")
        self._conn.set_progress_handler(
            lambda: int(time.monotonic() >= deadline), 1_000)
        try:
            yield
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if ("interrupted" in message
                    or (any(token in message for token in ("locked", "busy"))
                        and time.monotonic() >= deadline)):
                raise TimeoutError("maintenance deadline exhausted") from exc
            raise
        finally:
            self._conn.set_progress_handler(None, 0)
            self._conn.execute(f"PRAGMA busy_timeout={prior_busy_timeout}")

    @staticmethod
    def _retention_result(
        action: str,
        *,
        rows_deleted: int = 0,
        rows_compacted: int = 0,
        protected_rows_skipped: int = 0,
        deadline_exhausted: bool = False,
        metrics: Optional[Mapping[str, int]] = None,
    ) -> dict[str, Any]:
        deleted = max(0, int(rows_deleted))
        compacted = max(0, int(rows_compacted))
        protected = max(0, int(protected_rows_skipped))
        return {
            "action": str(action),
            "rows_deleted": deleted,
            "rows_compacted": compacted,
            "budget_units": deleted + compacted,
            "protected_rows_skipped": protected,
            "deadline_exhausted": bool(deadline_exhausted),
            "trade_evidence_deleted": 0,
            "permanent_rows_deleted": 0,
            "protected_rows_deleted": 0,
            "metrics": {
                str(key): max(0, int(value))
                for key, value in dict(metrics or {}).items()
            },
        }

    @staticmethod
    def _raw_unlinked_predicate(table: str, alias: str = "raw") -> str:
        """SQL predicate proving that a raw row is outside every evidence graph."""

        if table == "cex_observations":
            return (
                f"NOT EXISTS(SELECT 1 FROM candidate_cex_evidence e "
                f"WHERE e.cex_observation_id={alias}.cex_observation_id)"
            )
        if table == "book_snapshots":
            return " AND ".join((
                f"NOT EXISTS(SELECT 1 FROM candidate_book_evidence e WHERE "
                f"e.book_snapshot_id={alias}.book_snapshot_id)",
                f"NOT EXISTS(SELECT 1 FROM fair_value_sides e WHERE "
                f"e.book_snapshot_id={alias}.book_snapshot_id)",
                f"NOT EXISTS(SELECT 1 FROM maker_observations e WHERE "
                f"e.initial_book_snapshot_id={alias}.book_snapshot_id OR "
                f"e.final_book_snapshot_id={alias}.book_snapshot_id)",
                f"NOT EXISTS(SELECT 1 FROM maker_updates e WHERE "
                f"e.book_snapshot_id={alias}.book_snapshot_id)",
                f"NOT EXISTS(SELECT 1 FROM entries e WHERE "
                f"e.book_snapshot_id={alias}.book_snapshot_id)",
                f"NOT EXISTS(SELECT 1 FROM management_decisions e WHERE "
                f"e.book_snapshot_id={alias}.book_snapshot_id)",
                f"NOT EXISTS(SELECT 1 FROM exits e WHERE "
                f"e.book_snapshot_id={alias}.book_snapshot_id)",
            ))
        if table == "source_events":
            return " AND ".join((
                f"NOT EXISTS(SELECT 1 FROM candidates e WHERE "
                f"e.trigger_source_event_id={alias}.source_event_id)",
                f"NOT EXISTS(SELECT 1 FROM book_snapshots e WHERE "
                f"e.source_event_id={alias}.source_event_id)",
                f"NOT EXISTS(SELECT 1 FROM cex_observations e WHERE "
                f"e.source_event_id={alias}.source_event_id)",
                f"NOT EXISTS(SELECT 1 FROM maker_updates e WHERE "
                f"e.source_event_id={alias}.source_event_id)",
                f"NOT EXISTS(SELECT 1 FROM latency_metrics e WHERE "
                f"e.source_event_id={alias}.source_event_id)",
                f"NOT EXISTS(SELECT 1 FROM reject_events e WHERE "
                f"e.source_event_id={alias}.source_event_id)",
            ))
        raise ValueError("unsupported raw retention table")

    def _delete_old_raw_step(
        self,
        table: str,
        *,
        cutoff_ts_ms: int,
        max_rows: int,
        deadline_monotonic: Optional[float],
    ) -> Optional[dict[str, Any]]:
        id_col = {
            "source_events": "source_event_id",
            "book_snapshots": "book_snapshot_id",
            "cex_observations": "cex_observation_id",
        }[table]
        predicate = self._raw_unlinked_predicate(table)
        with self._sqlite_deadline(deadline_monotonic):
            with self.transaction(immediate=True) as conn:
                cursor = conn.execute(
                    f"""DELETE FROM {table} WHERE {id_col} IN (
                       SELECT raw.{id_col} FROM {table} raw
                       WHERE raw.receipt_ts_ms<? AND raw.retention_class='RAW'
                       AND raw.pin_count=0 AND {predicate}
                       ORDER BY raw.receipt_ts_ms,raw.{id_col} LIMIT ?
                       )""",
                    (int(cutoff_ts_ms), int(max_rows)),
                )
                deleted = int(cursor.rowcount)
        if not deleted:
            return None
        return self._retention_result(
            f"raw_age:{table}", rows_deleted=deleted,
            metrics={f"raw_age_{table}_deleted": deleted})

    def _delete_nontrade_candidate_graph_step(
        self,
        *,
        cutoff_ts_ms: int,
        max_rows: int,
        deadline_monotonic: Optional[float],
    ) -> Optional[dict[str, Any]]:
        """Delete one old graph only when its complete cascade fits the budget."""

        oversize = 0
        with self._sqlite_deadline(deadline_monotonic):
            with self.transaction(immediate=True) as conn:
                candidate_ids = [int(row[0]) for row in conn.execute(
                    """SELECT c.candidate_id FROM candidates c
                       WHERE c.evaluation_ts_ms<?
                       AND NOT EXISTS(SELECT 1 FROM entries e
                                      WHERE e.candidate_id=c.candidate_id)
                       AND NOT EXISTS(SELECT 1 FROM maker_observations m
                                      WHERE m.candidate_id=c.candidate_id
                                      AND m.maker_end_ts_ms IS NULL)
                       AND NOT EXISTS(SELECT 1 FROM window_locks wl
                                      WHERE wl.candidate_id=c.candidate_id
                                      AND wl.state IN ('RESERVED','ENTERED'))
                       AND NOT EXISTS(SELECT 1 FROM persistence_commands pc
                                      WHERE pc.associated_window_id=c.window_id
                                      AND pc.status<>'COMMITTED')
                       ORDER BY c.evaluation_ts_ms,c.candidate_id LIMIT 4""",
                    (int(cutoff_ts_ms),),
                ).fetchall()]
                for candidate_id in candidate_ids:
                    if (deadline_monotonic is not None
                            and time.monotonic() >= float(deadline_monotonic)):
                        raise TimeoutError("maintenance deadline exhausted")
                    conn.execute("SAVEPOINT v4_candidate_retention")
                    before = int(conn.total_changes)
                    cursor = conn.execute(
                        """DELETE FROM candidates WHERE candidate_id=?
                           AND NOT EXISTS(SELECT 1 FROM entries
                                          WHERE candidate_id=?)""",
                        (candidate_id, candidate_id),
                    )
                    changed = int(conn.total_changes) - before
                    if cursor.rowcount == 1 and changed <= int(max_rows):
                        conn.execute("RELEASE v4_candidate_retention")
                        return self._retention_result(
                            "candidate_graph", rows_deleted=changed,
                            metrics={
                                "candidate_graphs_deleted": 1,
                                "candidate_graph_rows_deleted": changed,
                            },
                        )
                    conn.execute("ROLLBACK TO v4_candidate_retention")
                    conn.execute("RELEASE v4_candidate_retention")
                    if cursor.rowcount == 1:
                        oversize += 1
        if oversize:
            return self._retention_result(
                "candidate_graph_oversize",
                protected_rows_skipped=oversize,
                metrics={"candidate_graphs_over_budget": oversize},
            )
        return None

    def _compact_journal_payload_step(
        self,
        *,
        cutoff_ts_ms: int,
        max_rows: int,
        deadline_monotonic: Optional[float],
    ) -> Optional[dict[str, Any]]:
        """Tombstone replay payloads while retaining the durable idempotency record."""

        with self._sqlite_deadline(deadline_monotonic):
            with self.transaction(immediate=True) as conn:
                rows = conn.execute(
                    """SELECT pc.persistence_command_id,pc.payload_hash
                       FROM persistence_commands pc
                       WHERE pc.status='COMMITTED' AND pc.terminal=0
                       AND pc.associated_trade_id IS NULL
                       AND pc.completed_ts_ms<?
                       AND pc.payload_json NOT LIKE '{\"compacted\":true,%'
                       AND UPPER(pc.command_type) NOT IN
                           ('ENTRY','MANAGEMENT','RESOLUTION','TERMINAL')
                       AND pc.method NOT IN (
                         'reserve_and_create_entry_bundle','create_entry',
                         'management_bundle','record_management_decision',
                         'resolution_bundle','record_resolution_attempt',
                         'close_position','mark_unresolved_final',
                         'record_maker_observation','record_maker_update',
                         'finish_maker_observation')
                       AND (pc.associated_window_id IS NULL OR NOT EXISTS(
                         SELECT 1 FROM entries e
                         WHERE e.window_id=pc.associated_window_id))
                       ORDER BY pc.completed_ts_ms,pc.persistence_command_id
                       LIMIT ?""",
                    (int(cutoff_ts_ms), int(max_rows)),
                ).fetchall()
                compacted = 0
                for row in rows:
                    if (deadline_monotonic is not None
                            and time.monotonic() >= float(deadline_monotonic)):
                        raise TimeoutError("maintenance deadline exhausted")
                    tombstone = json.dumps(
                        {"compacted": True, "payload_hash": str(row[1])},
                        sort_keys=True, separators=(",", ":"), allow_nan=False,
                    )
                    compacted += int(conn.execute(
                        """UPDATE persistence_commands SET payload_json=?
                           WHERE persistence_command_id=? AND status='COMMITTED'
                           AND terminal=0 AND payload_hash=?""",
                        (tombstone, int(row[0]), str(row[1])),
                    ).rowcount)
        if not compacted:
            return None
        return self._retention_result(
            "journal_payload", rows_compacted=compacted,
            metrics={"journal_payloads_compacted": compacted})

    def _delete_metadata_step(
        self,
        table: str,
        *,
        cutoff_ts_ms: int,
        maximum_rows: int,
        max_delete_rows: int,
        deadline_monotonic: Optional[float],
    ) -> Optional[dict[str, Any]]:
        id_col, time_col = {
            "persistence_worker_samples": (
                "persistence_worker_sample_id", "sample_ts_ms"),
            "checkpoint_runs": ("checkpoint_run_id", "started_ts_ms"),
            "retention_runs": ("retention_run_id", "started_ts_ms"),
        }[table]
        with self._sqlite_deadline(deadline_monotonic):
            with self.transaction(immediate=True) as conn:
                boundary = conn.execute(
                    f"""SELECT {id_col} FROM {table}
                       ORDER BY {id_col} DESC LIMIT 1 OFFSET ?""",
                    (int(maximum_rows) - 1,),
                ).fetchone()
                boundary_id = int(boundary[0]) if boundary is not None else None
                conditions = [f"meta.{time_col}<?"]
                params: list[Any] = [int(cutoff_ts_ms)]
                if boundary_id is not None:
                    conditions.append(f"meta.{id_col}<?")
                    params.append(boundary_id)
                params.append(int(max_delete_rows))
                cursor = conn.execute(
                    f"""DELETE FROM {table} WHERE {id_col} IN (
                       SELECT meta.{id_col} FROM {table} meta
                       WHERE ({' OR '.join(conditions)})
                       AND meta.{id_col}<>(SELECT MAX({id_col}) FROM {table})
                       ORDER BY meta.{id_col} LIMIT ?
                       )""",
                    tuple(params),
                )
                deleted = int(cursor.rowcount)
        if not deleted:
            return None
        return self._retention_result(
            f"metadata:{table}", rows_deleted=deleted,
            metrics={f"{table}_deleted": deleted})

    def _raw_cap_step(
        self,
        table: str,
        *,
        maximum_rows: int,
        max_delete_rows: int,
        deadline_monotonic: Optional[float],
    ) -> Optional[dict[str, Any]]:
        result = self.enforce_raw_row_cap(
            maximum_rows,
            max_delete_rows=max_delete_rows,
            deadline_monotonic=deadline_monotonic,
            _only_table=table,
        )
        deleted = int(result.get(table, 0))
        if not deleted:
            return None
        return self._retention_result(
            f"raw_cap:{table}", rows_deleted=deleted,
            metrics={f"raw_cap_{table}_deleted": deleted})

    def bounded_retention_step(
        self,
        *,
        cutoff_ts_ms: int,
        max_rows: int,
        deadline_monotonic: float,
        protect_trade_evidence: bool = True,
        raw_event_max_rows: int = 250_000,
        event_bucket_detail_retention_ms: int = 15 * 60 * 1_000,
        metadata_retention_ms: int = 24 * 60 * 60 * 1_000,
        metadata_max_rows: int = 100_000,
        journal_payload_retention_ms: int = 24 * 60 * 60 * 1_000,
        now_ms: Optional[int] = None,
    ) -> dict[str, Any]:
        """Execute at most one small retention transaction before ``deadline``.

        Actions rotate so a journal backlog cannot starve raw caps, bucket
        compaction, or worker/checkpoint metadata retention. Trade-linked
        candidates, terminal commands, ambiguous commands, and pinned evidence
        are excluded structurally rather than deleted and reconstructed later.
        """

        self._assert_owner()
        if type(protect_trade_evidence) is not bool or not protect_trade_evidence:
            raise ValueError("trade evidence protection must remain enabled")
        limit = int(max_rows)
        if not 1 <= limit <= 50_000:
            raise ValueError("max_rows must be in [1, 50000]")
        if int(raw_event_max_rows) < 1_000:
            raise ValueError("raw_event_max_rows must be at least 1000")
        if int(metadata_max_rows) < 1_000:
            raise ValueError("metadata_max_rows must be at least 1000")
        current = int(now_ms if now_ms is not None else time.time() * 1_000)
        if current < 0 or int(cutoff_ts_ms) < 0:
            raise ValueError("retention timestamps must be nonnegative")
        if time.monotonic() >= float(deadline_monotonic):
            return self._retention_result(
                "deadline", deadline_exhausted=True)

        bucket_cutoff = max(0, current - int(event_bucket_detail_retention_ms))
        metadata_cutoff = max(0, current - int(metadata_retention_ms))
        journal_cutoff = max(0, current - int(journal_payload_retention_ms))
        actions = (
            lambda: self._delete_nontrade_candidate_graph_step(
                cutoff_ts_ms=int(cutoff_ts_ms), max_rows=limit,
                deadline_monotonic=deadline_monotonic),
            lambda: self._compact_journal_payload_step(
                cutoff_ts_ms=journal_cutoff, max_rows=limit,
                deadline_monotonic=deadline_monotonic),
            lambda: self._compact_event_bucket_step(
                cutoff_ts_ms=bucket_cutoff, max_rows=limit,
                deadline_monotonic=deadline_monotonic),
            *(lambda table=table: self._delete_old_raw_step(
                table, cutoff_ts_ms=int(cutoff_ts_ms), max_rows=limit,
                deadline_monotonic=deadline_monotonic)
              for table in ("cex_observations", "book_snapshots", "source_events")),
            *(lambda table=table: self._raw_cap_step(
                table, maximum_rows=int(raw_event_max_rows),
                max_delete_rows=limit, deadline_monotonic=deadline_monotonic)
              for table in ("cex_observations", "book_snapshots", "source_events")),
            *(lambda table=table: self._delete_metadata_step(
                table, cutoff_ts_ms=metadata_cutoff,
                maximum_rows=int(metadata_max_rows), max_delete_rows=limit,
                deadline_monotonic=deadline_monotonic)
              for table in (
                  "persistence_worker_samples", "checkpoint_runs", "retention_runs")),
        )
        start = self._retention_action_index % len(actions)
        for offset in range(len(actions)):
            if time.monotonic() >= float(deadline_monotonic):
                return self._retention_result(
                    "deadline", deadline_exhausted=True)
            index = (start + offset) % len(actions)
            try:
                result = actions[index]()
            except TimeoutError:
                return self._retention_result(
                    "deadline", deadline_exhausted=True)
            self._retention_action_index = (index + 1) % len(actions)
            if result is not None and (
                    int(result.get("budget_units", 0)) > 0
                    or int(result.get("protected_rows_skipped", 0)) > 0):
                return result
        return self._retention_result("no_eligible_rows")

    def compact_raw_evidence(
        self, now_ms: int, *, retention_ms: int = 6 * 60 * 60 * 1000,
        batch_size: int = 5000, run_integrity: bool = True,
    ) -> dict[str, Any]:
        """Delete only old unpinned raw rows, in a bounded atomic batch.

        ``run_integrity`` may be set False by a chunked off-loop maintenance
        worker that verifies integrity once per cycle rather than paying a full
        ``PRAGMA integrity_check`` (seconds on a large database) for every small
        delete chunk.
        """
        if retention_ms < 60_000:
            raise ValueError("raw retention must be at least one minute")
        if not 1 <= int(batch_size) <= 50_000:
            raise ValueError("batch_size must be in [1, 50000]")
        cutoff = max(0, int(now_ms) - int(retention_ms))
        deleted = {
            "source_events": 0,
            "book_snapshots": 0,
            "cex_observations": 0,
        }
        # Children are removed before their source event so evidence lineage is
        # never nulled merely to make a parent row deletable.
        for table in ("cex_observations", "book_snapshots", "source_events"):
            result = self._delete_old_raw_step(
                table, cutoff_ts_ms=cutoff, max_rows=int(batch_size),
                deadline_monotonic=None,
            )
            if result is not None:
                deleted[table] = int(result["rows_deleted"])
        pinned = 0
        with self.transaction(immediate=True) as conn:
            run_id = self._insert("retention_runs", {
                "started_ts_ms": int(now_ms), "raw_cutoff_ts_ms": cutoff,
                "requested_batch_size": int(batch_size),
                "completed_ts_ms": int(now_ms),
                "source_events_deleted": deleted["source_events"],
                "book_snapshots_deleted": deleted["book_snapshots"],
                "cex_observations_deleted": deleted["cex_observations"],
                "pinned_rows_skipped": pinned,
            }, conn=conn)
        if not run_integrity:
            return {"retention_run_id": run_id, "cutoff_ts_ms": cutoff,
                    "pinned_rows_skipped": pinned, **deleted}
        integrity = self.integrity_check()
        with self.transaction() as conn:
            conn.execute(
                """UPDATE retention_runs SET integrity_result=?,foreign_key_violations=?
                   WHERE retention_run_id=?""",
                (integrity["integrity"], len(integrity["foreign_key_violations"]), run_id),
            )
        return {"retention_run_id": run_id, "cutoff_ts_ms": cutoff,
                "pinned_rows_skipped": pinned, **deleted, **integrity}

    def enforce_raw_row_cap(
        self,
        maximum_rows: int,
        *,
        max_delete_rows: int = 5_000,
        deadline_monotonic: Optional[float] = None,
        _only_table: Optional[str] = None,
    ) -> dict[str, int]:
        """Delete a bounded number of oldest, proven-unlinked raw rows.

        The cap is evaluated with an indexed ``OFFSET`` boundary instead of a
        full-table count. Each table uses its own small transaction and the
        combined delete never exceeds ``max_delete_rows``.
        """

        limit = int(maximum_rows)
        delete_budget = int(max_delete_rows)
        if limit < 1_000:
            raise ValueError("raw row cap must be at least 1000")
        if not 1 <= delete_budget <= 50_000:
            raise ValueError("max_delete_rows must be in [1, 50000]")
        id_columns = {
            "source_events": "source_event_id",
            "book_snapshots": "book_snapshot_id",
            "cex_observations": "cex_observation_id",
        }
        if _only_table is not None and _only_table not in id_columns:
            raise ValueError("unsupported raw row-cap table")
        tables = ((_only_table,) if _only_table is not None else
                  ("source_events", "book_snapshots", "cex_observations"))
        deleted = {table: 0 for table in id_columns}
        remaining = delete_budget
        for table in tables:
            if remaining <= 0:
                break
            id_col = id_columns[table]
            predicate = self._raw_unlinked_predicate(table)
            with self._sqlite_deadline(deadline_monotonic):
                with self.transaction(immediate=True) as conn:
                    boundary = conn.execute(
                        f"""SELECT raw.receipt_ts_ms,raw.{id_col} FROM {table} raw
                           WHERE raw.retention_class='RAW' AND raw.pin_count=0
                           ORDER BY raw.receipt_ts_ms DESC,raw.{id_col} DESC
                           LIMIT 1 OFFSET ?""",
                        (limit - 1,),
                    ).fetchone()
                    if boundary is None:
                        continue
                    cursor = conn.execute(
                        f"""DELETE FROM {table} WHERE {id_col} IN (
                           SELECT raw.{id_col} FROM {table} raw
                           WHERE raw.retention_class='RAW' AND raw.pin_count=0
                           AND {predicate}
                           AND (raw.receipt_ts_ms<? OR
                                (raw.receipt_ts_ms=? AND raw.{id_col}<?))
                           ORDER BY raw.receipt_ts_ms,raw.{id_col} LIMIT ?
                           )""",
                        (int(boundary[0]), int(boundary[0]), int(boundary[1]),
                         remaining),
                    )
                    changed = int(cursor.rowcount)
                    deleted[table] = changed
                    remaining -= changed
        return deleted

    def _compact_event_bucket_step(
        self,
        *,
        cutoff_ts_ms: int,
        max_rows: int,
        deadline_monotonic: Optional[float],
    ) -> Optional[dict[str, Any]]:
        """Roll at most ``max_rows`` one-second buckets into minute buckets."""

        with self._sqlite_deadline(deadline_monotonic):
            with self.transaction(immediate=True) as conn:
                rows = conn.execute(
                    """SELECT rowid,bucket_start_ts_ms,source,channel,asset,
                       event_type,classification,raw_count,unique_count,
                       duplicate_count,invalid_count FROM event_buckets
                       WHERE bucket_ms=1000 AND bucket_start_ts_ms<?
                       ORDER BY bucket_start_ts_ms,rowid LIMIT ?""",
                    (int(cutoff_ts_ms), int(max_rows)),
                ).fetchall()
                if not rows:
                    return None
                aggregates: dict[tuple[Any, ...], list[int]] = {}
                rowids: list[int] = []
                for row in rows:
                    if (deadline_monotonic is not None
                            and time.monotonic() >= float(deadline_monotonic)):
                        raise TimeoutError("maintenance deadline exhausted")
                    rowids.append(int(row[0]))
                    key = (
                        (int(row[1]) // 60_000) * 60_000,
                        str(row[2]), str(row[3]), str(row[4]), str(row[5]),
                        str(row[6]),
                    )
                    totals = aggregates.setdefault(key, [0, 0, 0, 0])
                    for index in range(4):
                        totals[index] += int(row[7 + index])
                for key, totals in aggregates.items():
                    conn.execute(
                        """INSERT INTO event_buckets(
                           bucket_start_ts_ms,bucket_ms,source,channel,asset,event_type,
                           classification,raw_count,unique_count,duplicate_count,invalid_count)
                           VALUES(?,60000,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT(bucket_start_ts_ms,bucket_ms,source,channel,asset,event_type,classification)
                           DO UPDATE SET raw_count=raw_count+excluded.raw_count,
                             unique_count=unique_count+excluded.unique_count,
                             duplicate_count=duplicate_count+excluded.duplicate_count,
                             invalid_count=invalid_count+excluded.invalid_count""",
                        (*key, *totals),
                    )
                marks = ",".join("?" for _ in rowids)
                deleted = int(conn.execute(
                    f"DELETE FROM event_buckets WHERE rowid IN ({marks})", rowids
                ).rowcount)
                if deleted != len(rowids):
                    raise V4StoreError("event bucket compaction lost row ownership")
        return self._retention_result(
            "event_buckets", rows_compacted=deleted,
            metrics={
                "event_bucket_detail_rows_compacted": deleted,
                "event_bucket_minute_groups_updated": len(aggregates),
            },
        )

    def compact_event_buckets(
        self,
        now_ms: int,
        *,
        detail_retention_ms: int,
        max_rows: int = 5_000,
        deadline_monotonic: Optional[float] = None,
    ) -> int:
        """Boundedly roll old one-second counters into minute counters."""

        if not 1 <= int(max_rows) <= 50_000:
            raise ValueError("max_rows must be in [1, 50000]")
        cutoff = max(0, int(now_ms) - int(detail_retention_ms))
        result = self._compact_event_bucket_step(
            cutoff_ts_ms=cutoff, max_rows=int(max_rows),
            deadline_monotonic=deadline_monotonic,
        )
        return int(result["rows_compacted"]) if result is not None else 0

    def integrity_check(self) -> dict[str, Any]:
        self._assert_owner()
        with self._lock:
            result = [str(row[0]) for row in self._conn.execute("PRAGMA integrity_check").fetchall()]
            fk = [dict(row) for row in self._conn.execute("PRAGMA foreign_key_check").fetchall()]
        return {"integrity": "ok" if result == ["ok"] else "; ".join(result),
                "foreign_key_violations": fk}

    def database_size_bytes(self) -> int:
        return sum(
            candidate.stat().st_size
            for suffix in ("", "-wal", "-shm")
            if (candidate := Path(f"{self.path}{suffix}")).exists()
        )

    def checkpoint(self, *, mode: str = "PASSIVE", reason: str = "manual") -> dict[str, Any]:
        """Run and journal one explicit WAL checkpoint.

        Automatic checkpoints are disabled.  This method is deliberately not
        called by :meth:`close`; the persistence owner schedules it explicitly.
        """

        self._assert_owner()
        parsed_mode = str(mode).upper()
        if parsed_mode not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise ValueError("unsupported WAL checkpoint mode")
        if self._transaction_depth:
            raise V4StoreError("cannot checkpoint inside an active transaction")
        with self._background_write_gate():
            started_ts = int(time.time() * 1_000)
            started_mono = time.monotonic()
            wal_path = Path(f"{self.path}-wal")
            before = wal_path.stat().st_size if wal_path.exists() else 0
            busy = total = checkpointed = None
            failure: Optional[str] = None
            try:
                with self._lock:
                    result = self._conn.execute(
                        f"PRAGMA wal_checkpoint({parsed_mode})").fetchone()
                if result is not None:
                    busy, total, checkpointed = (
                        int(result[0]), int(result[1]), int(result[2]))
                success = int((busy or 0) == 0)
                if not success:
                    failure = "checkpoint_busy"
            except Exception as exc:
                success = 0
                failure = f"{type(exc).__name__}:{exc}"[:500]
            completed_ts = max(started_ts, int(time.time() * 1_000))
            after = wal_path.stat().st_size if wal_path.exists() else 0
            duration = max(0.0, (time.monotonic() - started_mono) * 1_000.0)
            with self.transaction(immediate=True) as conn:
                run_id = self._insert("checkpoint_runs", {
                    "started_ts_ms": started_ts,
                    "completed_ts_ms": completed_ts,
                    "mode": parsed_mode,
                    "reason": str(reason),
                    "before_wal_bytes": int(before),
                    "after_wal_bytes": int(after),
                    "duration_ms": duration,
                    "busy_result": busy,
                    "frames_total": total,
                    "frames_checkpointed": checkpointed,
                    "database_bytes": (
                        self.path.stat().st_size if self.path.exists() else 0),
                    "success": success,
                    "failure_reason": failure,
                }, conn=conn)
        return {
            "checkpoint_run_id": run_id, "mode": parsed_mode,
            "before_wal_bytes": int(before), "after_wal_bytes": int(after),
            "duration_ms": duration, "busy_result": busy,
            "frames_total": total, "frames_checkpointed": checkpointed,
            "success": bool(success), "failure_reason": failure,
        }

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            if (self._enforce_thread_ownership
                    and threading.get_ident() != self._owner_thread_id):
                raise V4StoreError(
                    "V4 writable connection closed outside its owner thread")
            self._conn.close()
            self._closed = True

    def __enter__(self) -> "V4Store":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class V4ReadOnlyStore(V4Store):
    """Read-only view over an existing V4 database on its own connection.

    WAL mode lets this reader run concurrently with the writer without
    contending on the writer's lock, so heavy dashboard/integrity reads can be
    offloaded to a worker thread and never stall the event loop.  Only the
    inherited *read* methods (query/query_one/integrity_check/open_positions/
    latest_source_health/database_size_bytes) are used; the connection is opened
    ``mode=ro`` with ``PRAGMA query_only`` so any inherited write method raises
    instead of mutating the database.
    """

    def __init__(self, db_path: str | Path, *, busy_timeout_ms: int = 5_000,
                 enforce_thread_ownership: bool = True):
        if isinstance(busy_timeout_ms, bool) or not isinstance(busy_timeout_ms, int):
            raise ValueError("busy_timeout_ms must be an integer")
        if busy_timeout_ms < 100 or busy_timeout_ms > 120_000:
            raise ValueError("busy_timeout_ms must be within [100, 120000] ms")
        self.busy_timeout_ms = int(busy_timeout_ms)
        if type(enforce_thread_ownership) is not bool:
            raise ValueError("enforce_thread_ownership must be a strict boolean")
        self.path = Path(db_path)
        if not self.path.exists():
            raise V4SchemaError(f"read-only store requires an existing database: {self.path}")
        self._lock = threading.RLock()
        self._closed = False
        self._enforce_thread_ownership = enforce_thread_ownership
        self._owner_thread_id = threading.get_ident()
        self._transaction_depth = 0
        self._transaction_owner_thread_id = None
        self._transaction_rollback_only = False
        self._transaction_counters = {
            "started": 0, "committed": 0, "rolled_back": 0, "nested": 0,
        }
        self._retention_action_index = 0
        self._background_write_admission = None
        self._background_write_release = None
        self._background_gate_depth = 0
        self._conn = sqlite3.connect(
            f"file:{self.path.as_posix()}?mode=ro", uri=True,
            timeout=self.busy_timeout_ms / 1000.0,
            check_same_thread=enforce_thread_ownership, isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            self._conn.execute("PRAGMA query_only=ON")
            self._conn.execute("PRAGMA wal_autocheckpoint=0")

    def close(self) -> None:
        # A read-only connection must not attempt a WAL checkpoint (a write).
        with self._lock:
            if self._closed:
                return
            if (self._enforce_thread_ownership
                    and threading.get_ident() != self._owner_thread_id):
                raise V4StoreError(
                    "V4 read-only connection closed outside its owner thread")
            self._conn.close()
            self._closed = True
