"""Atomic, read-only dashboard export for Lite Frequency V4 only."""
from __future__ import annotations

import hashlib
import json
import math
import os
import uuid
from pathlib import Path
from typing import Any, Mapping, Optional

from .config import ACTIVE_COHORT, LEGACY_COHORT, RUNTIME_LABEL
from .config import FREQUENCY_V4_ROOT
from .export_profile import EXPORT_PROFILE, stage
from .ledger import compute_capital_ledger
from .metrics import build_metrics
from .runtime import _discard_temporary, atomic_replace
from .store import FIXED_SHARES, MODE, STRATEGY_ID
from .universe import UNIVERSE_POLICY_VERSION


EXPORT_FILENAME = "frequency_v4_dashboard.json"
WARNING = (
    "LITE FREQUENCY V4 SHADOW ONLY - RESEARCH CANDIDATE - "
    "NO REAL ORDERS OR CANCELLATIONS"
)


def canonical_export_dir() -> Path:
    """The canonical, source-root-derived export directory.

    Re-derived from ``FREQUENCY_V4_ROOT`` so it tracks whichever checkout
    the module was imported from.  When the source root is the live
    deployment this is the existing live path
    ``D:/claude/agent_readonly/poly_alpha_frequency_v4``; when the source
    root is an isolated staging checkout it is isolated under that
    staging parent.

    This is the **single source of truth** for the canonical export path.
    C1.H mandates strict canonical containment (design a): the export
    writer accepts writes only under this directory and refuses every
    other path.  Tests repoint the canonical root at ``tmp_path`` by
    monkeypatching this one function rather than loosening the guard, so
    no test-only override flag lives in production code.
    """
    return Path(FREQUENCY_V4_ROOT).parent / "agent_readonly" / "poly_alpha_frequency_v4"


def _is_within(path: Path, root: Path) -> bool:
    """True if ``path`` is ``root`` itself or a descendant of ``root``.

    Callers pass already-resolved absolute paths.  This helper compares their
    case-normalized, separator-normalized parts so the containment check is
    robust on Windows (case-insensitive, both ``\\`` and ``/``).  Any symlink
    or junction dereferencing has therefore already occurred in the caller's
    ``Path.resolve()`` step.
    """
    path_parts = tuple(_normalize_part(p) for p in path.parts)
    root_parts = tuple(_normalize_part(p) for p in root.parts)
    if len(path_parts) < len(root_parts):
        return False
    return path_parts[:len(root_parts)] == root_parts


def _normalize_part(part: str) -> str:
    # Drive letters and directory names are case-insensitive on Windows;
    # collapse separators so ``/`` and ``\\`` compare equal.
    return part.replace("\\", "/").lower()


def assert_canonical_export_path(output_path: str | Path) -> Path:
    """Fail closed unless ``output_path`` is inside the canonical export dir.

    C1.H isolation guard (strict canonical containment, design a): the
    Frequency V4 export writer accepts writes **only** under
    :func:`canonical_export_dir` and refuses every other path -- the
    production live export directory, the live production source tree,
    arbitrary temp directories, sibling/parent directories, and any
    path-normalization or relative-escape variant.  Tests that need to
    write under ``tmp_path`` monkeypatch :func:`canonical_export_dir` to
    return their temp root; no production-code override flag exists.

    A directory input resolves to ``<dir>/EXPORT_FILENAME``; a non-``.json``
    file input likewise.  The resolved (absolute, normalized) path must
    be the canonical dir or a descendant of it.
    """
    target = Path(output_path)
    if target.exists() and target.is_dir():
        target = target / EXPORT_FILENAME
    elif target.suffix.lower() != ".json":
        target = target / EXPORT_FILENAME
    resolved = Path(os.path.normpath(str(target.resolve())))
    canonical = Path(os.path.normpath(str(canonical_export_dir().resolve())))

    if not _is_within(resolved, canonical):
        raise ValueError(
            "frequency v4 export refused: path is outside the canonical "
            f"export directory {canonical}: {resolved}"
        )
    return resolved


def _mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "model_dump"):
        return dict(value.model_dump())
    if hasattr(value, "to_dict"):
        return dict(value.to_dict())
    if hasattr(value, "__dict__"):
        return {key: val for key, val in vars(value).items() if not key.startswith("_")}
    return {}


def _value(value: Any, *names: str, default: Any = None) -> Any:
    data = _mapping(value)
    for name in names:
        if name in data:
            found = data[name]
            return found.value if hasattr(found, "value") else found
        if hasattr(value, name):
            found = getattr(value, name)
            return found.value if hasattr(found, "value") else found
    return default


def assert_v4_safety(config: Any = None) -> dict[str, Any]:
    """Fail closed if a caller tries to export a non-v4 or unsafe runtime."""
    if config is not None:
        required_aliases = (
            ("strategy_id", "strategy"), ("mode",), ("dry_run",),
            ("live_enabled",), ("real_orders_possible",),
            ("live_adapter_present",),
            ("kill_switch_engaged", "live_kill_switch_engaged"),
            ("fixed_shares", "fixed_order_shares"),
        )
        mapped = _mapping(config)
        missing = [
            aliases[0] for aliases in required_aliases
            if not any(alias in mapped or hasattr(config, alias) for alias in aliases)
        ]
        if missing:
            raise RuntimeError(f"v4 safety config missing required fields: {missing}")
    mode = str(_value(config, "mode", default=MODE))
    strategy_id = str(_value(config, "strategy_id", "strategy", default=STRATEGY_ID))
    dry_run = _value(config, "dry_run", default=True)
    live_enabled = _value(config, "live_enabled", default=False)
    real_orders = _value(config, "real_orders_possible", default=False)
    live_adapter = _value(config, "live_adapter_present", default=False)
    kill_switch = _value(
        config, "kill_switch_engaged", "live_kill_switch_engaged", default=True
    )
    fixed_shares = float(_value(
        config, "fixed_shares", "fixed_order_shares", default=FIXED_SHARES
    ))
    actual = {
        "strategy_id": strategy_id,
        "mode": mode,
        "dry_run": dry_run,
        "live_enabled": live_enabled,
        "real_orders_possible": real_orders,
        "live_adapter_present": live_adapter,
        "kill_switch_engaged": kill_switch,
        "fixed_shares": fixed_shares,
    }
    expected = {
        "strategy_id": STRATEGY_ID,
        "mode": MODE,
        "dry_run": True,
        "live_enabled": False,
        "real_orders_possible": False,
        "live_adapter_present": False,
        "kill_switch_engaged": True,
        "fixed_shares": FIXED_SHARES,
    }
    for key, expected_value in expected.items():
        if type(actual[key]) is not type(expected_value) and key not in ("fixed_shares",):
            raise RuntimeError(f"v4 safety value {key} has unsafe type")
        if actual[key] != expected_value:
            raise RuntimeError(
                f"v4 safety lock failed for {key}: {actual[key]!r} != {expected_value!r}"
            )
    return expected


def _latest_runtime_health(store: Any, session_id: Optional[str]) -> Optional[dict[str, Any]]:
    if session_id:
        return store.query_one(
            """SELECT * FROM runtime_health WHERE session_id=?
               ORDER BY sample_ts_ms DESC,runtime_health_id DESC LIMIT 1""",
            (session_id,),
        )
    return store.query_one(
        "SELECT * FROM runtime_health ORDER BY sample_ts_ms DESC,runtime_health_id DESC LIMIT 1"
    )


def _universe(store: Any, now_ms: int) -> dict[str, Any]:
    with stage("durations"):
        duration_rows = store.query(
            """SELECT duration_ms,COUNT(*) markets,COUNT(DISTINCT asset) assets
               FROM markets GROUP BY duration_ms ORDER BY duration_ms"""
        )
    ignored = [row for row in duration_rows if int(row["duration_ms"]) != 300_000]
    with stage("active_windows"):
        active = store.query(
            """SELECT w.window_id,w.asset,w.window_open_ts_ms,w.window_close_ts_ms,
               f.available,f.eligible,f.positive_edge,f.execution_attempts,
               f.actual_entry,
               l.eligibility_status,l.reject_reason universe_reject_reason,
               m.polymarket_market_id,m.slug,mi.event_id,mi.condition_id,
               mi.yes_token_id,mi.no_token_id,mi.association_valid,
               mi.token_pair_valid,
               a.status anchor_status,a.price_to_beat
               FROM asset_windows w JOIN window_funnel f ON f.window_id=w.window_id
               LEFT JOIN window_market_links l
                 ON l.window_id=w.window_id AND l.selected=1
               LEFT JOIN market_identities mi
                 ON mi.market_identity_id=l.market_identity_id
               LEFT JOIN markets m ON m.market_id=mi.market_id
               LEFT JOIN anchor_observations a ON a.anchor_observation_id=(
                 SELECT MAX(a2.anchor_observation_id) FROM anchor_observations a2
                 WHERE a2.market_identity_id=mi.market_identity_id)
               WHERE w.window_open_ts_ms<=? AND w.window_close_ts_ms>?
               ORDER BY w.asset""",
            (int(now_ms), int(now_ms)),
        )
    exact_count = sum(int(row["markets"]) for row in duration_rows
                      if int(row["duration_ms"]) == 300_000)
    discovered_assets = sorted({str(row["asset"]) for row in active})
    eligible_rows = [
        row for row in active
        if str(row.get("eligibility_status") or "") == "ELIGIBLE"
    ]
    observed_rows = [
        row for row in active
        if row.get("eligibility_status") is not None
        and str(row["eligibility_status"]) != "ELIGIBLE"
    ]
    rejection_details = [
        {
            "asset": str(row["asset"]),
            "slug": row.get("slug"),
            "polymarket_market_id": row.get("polymarket_market_id"),
            "eligibility_status": row.get("eligibility_status"),
            "reject_reason": row.get("universe_reject_reason"),
        }
        for row in observed_rows
    ]
    with stage("universe_rejects"):
        universe_rejects = {
            str(row["reason"]): int(row["count"])
            for row in store.query(
                """SELECT reason,COUNT(*) count FROM reject_events
                   WHERE reason LIKE 'universe%' AND reject_ts_ms>=?
                   GROUP BY reason ORDER BY count DESC""",
                (int(now_ms) - 12 * 3_600_000,),
            )
        }
    return {
        "exact_duration_ms": 300_000,
        "exact_five_minute_markets": exact_count,
        "active_exact_five_minute_windows": active,
        "ignored_other_durations": ignored,
        "dynamic_universe_enabled": True,
        "universe_policy_version": UNIVERSE_POLICY_VERSION,
        "automatic_universe": True,
        # Required assets guarantee direct discovery queries and CEX
        # pre-subscription only; they never restrict execution eligibility.
        "required_assets": ["BTC", "ETH", "SOL"],
        "hardcoded_to_required_assets_only": False,
        "execution_fail_closed": True,
        "discovered_assets_active": discovered_assets,
        "eligible_assets_active": sorted({
            str(row["asset"]) for row in eligible_rows}),
        "observed_only_assets_active": sorted({
            str(row["asset"]) for row in observed_rows}),
        "eligible_market_count_active": len(eligible_rows),
        "observed_only_market_count_active": len(observed_rows),
        "universe_rejections_active": rejection_details,
        "universe_reject_reasons_12h": universe_rejects,
    }


def _latest_candidates(store: Any, limit: int = 12) -> list[dict[str, Any]]:
    with stage("head"):
        candidates = store.query(
            """SELECT c.*,w.asset,w.window_open_ts_ms,w.window_close_ts_ms,
               m.slug,m.polymarket_market_id FROM candidates c
               JOIN asset_windows w ON w.window_id=c.window_id
               JOIN market_identities mi
                 ON mi.market_identity_id=c.market_identity_id
               JOIN markets m ON m.market_id=mi.market_id
               ORDER BY c.evaluation_ts_ms DESC,c.candidate_id DESC LIMIT ?""",
            (int(limit),),
        )
    with stage("detail"):
        for candidate in candidates:
            candidate["model_contributions"] = store.query(
                """SELECT model_name,model_version,correlation_group,direction,
                   raw_score,estimated_probability,evidence_age_ms,confidence,
                   reliability,invalidation_reason,expected_net_edge,
                   regime_weight,gated,model_contribution,calibrated
                   FROM model_contributions
                   WHERE candidate_id=? ORDER BY model_name""",
                (int(candidate["candidate_id"]),),
            )
            candidate["fair_value_calculations"] = store.query(
                """SELECT fv.*,s.outcome_side,s.executable_vwap,
                   s.worst_consumed_price,s.spread,s.depth_shares,
                   s.exact_five_share_depth,s.estimated_fee,s.execution_buffer,
                   s.latency_buffer,s.uncertainty_buffer,s.net_edge,
                   s.evidence_fresh,s.selected FROM fair_value_calculations fv
                   JOIN fair_value_sides s USING(fair_value_calculation_id)
                   WHERE fv.candidate_id=?
                   ORDER BY fv.calculation_seq,s.outcome_side""",
                (int(candidate["candidate_id"]),),
            )
    return candidates


_PERSISTENCE_CONFIG_FIELDS = (
    "critical_queue_capacity", "telemetry_queue_capacity",
    "critical_command_timeout_s", "telemetry_batch_size",
    "telemetry_flush_interval_ms", "telemetry_coalescing_interval_ms",
    "writer_heartbeat_interval_ms", "writer_failure_timeout_ms",
    "checkpoint_wal_size_trigger_bytes", "checkpoint_min_interval_s",
    "retention_chunk_size", "retention_time_budget_ms",
    "reporting_worker_timeout_s", "maintenance_worker_timeout_s",
    "reporting_queue_capacity", "maintenance_queue_capacity",
    "raw_event_retention_hours", "raw_event_max_rows",
    "maintenance_chunk_rows", "maintenance_max_rows_per_pass",
    "maintenance_max_seconds_per_pass", "writer_queue_max",
    "cex_writer_queue_max", "shutdown_drain_timeout_s",
    "sqlite_busy_timeout_ms",
    "model_health_quarantine_enabled", "model_health_min_observations",
    "model_health_min_profit_factor", "model_health_min_expectancy",
    "model_health_max_loss_asymmetry", "model_health_cooldown_s",
)


def _effective_persistence_config(config: Any) -> dict[str, Any]:
    """Return only sanitized numeric persistence controls for operators."""

    result = {
        name: _value(config, name)
        for name in _PERSISTENCE_CONFIG_FIELDS
        if _value(config, name) is not None
    }
    retention_ms = int(
        _value(config, "raw_event_retention_hours", default=24)
    ) * 3_600_000
    result.update({
        "effective_retention_time_budget_ms": min(
            int(_value(config, "retention_time_budget_ms", default=1_000)),
            max(1, int(float(_value(
                config, "maintenance_max_seconds_per_pass", default=1.0
            )) * 1_000)),
        ),
        "effective_retention_chunk_rows": min(
            int(_value(config, "retention_chunk_size", default=250)),
            int(_value(config, "maintenance_chunk_rows", default=250)),
        ),
        "event_bucket_detail_retention_ms": min(retention_ms, 15 * 60_000),
        "metadata_retention_ms": retention_ms,
        "metadata_max_rows": int(_value(
            config, "raw_event_max_rows", default=250_000
        )),
        "journal_payload_retention_ms": retention_ms,
    })
    return result


def build_frequency_v4_dashboard(
    store: Any, *, now_ms: int, config: Any = None,
    runtime_state: Any = None, session_id: Optional[str] = None,
    integrity: Any = None,
) -> dict[str, Any]:
    safety = assert_v4_safety(config)
    runtime = _mapping(runtime_state)
    effective_session_id = session_id or runtime.get("session_id")
    starting_equity = float(_value(
        config, "starting_equity_usd", "research_equity_usd", default=130.0
    ))
    with stage("metrics"):
        metrics = build_metrics(
            store, int(now_ms), session_id=effective_session_id,
            starting_equity_usd=starting_equity, cohort=ACTIVE_COHORT,
        )
    fee_buffer = float(_value(config, "fee_buffer_usd", default=0.02))
    fee_rate = float(_value(config, "crypto_taker_fee_rate", default=0.07))
    try:
        with stage("ledger"):
            ledger = compute_capital_ledger(
                store.query, cohort=ACTIVE_COHORT,
                fee_rate=fee_rate, fee_buffer_usd=fee_buffer,
            ).to_dict()
        with stage("cohort_row"):
            cohort_row = store.query_one(
                "SELECT * FROM cohorts WHERE cohort=?", (ACTIVE_COHORT,))
        with stage("insufficient_rejects"):
            insufficient_rejects = int((store.query_one(
                """SELECT COUNT(*) count FROM reject_events re
                   JOIN runtime_sessions rs ON rs.session_id=re.session_id
                   WHERE re.reason IN
                     ('insufficient_capital','cohort_equity_depleted')
                   AND rs.cohort=?""", (ACTIVE_COHORT,)) or {}).get("count") or 0)
    except Exception:
        # A pre-cohort read-only fixture cannot report the ledger; the
        # authoritative runtime always can.
        ledger, cohort_row, insufficient_rejects = None, None, 0
    authoritative_capital = {
        "label": RUNTIME_LABEL,
        "cohort": ACTIVE_COHORT,
        "legacy_cohort": LEGACY_COHORT,
        "activation_ts_ms": (cohort_row or {}).get("activation_ts_ms"),
        "activation_commit": (cohort_row or {}).get("activation_commit"),
        "insufficient_capital_rejects": insufficient_rejects,
        "fixed_shares": FIXED_SHARES,
        "ledger": ledger,
        "ledger_available": ledger is not None,
    }
    # The export path never scans the database.  Integrity is verified off-loop
    # on its own worker and connection; the caller passes that cached result in.
    # Measured on the production evidence store (6.50 GB): ``PRAGMA quick_check``
    # takes ~17s and the full ``PRAGMA integrity_check`` ~66s.  Either one on this
    # path would monopolise the reporting worker and stall the 5s export cadence,
    # so neither is called here -- not even as a "first export" fallback.
    # With no cached result the payload reports an honest UNKNOWN, which fails
    # closed through ``sqlite_integrity_unhealthy`` below.  Callers that need a
    # healthy integrity verdict must supply the cached payload explicitly.
    if integrity is None:
        integrity = {
            "integrity": "UNKNOWN",
            "foreign_key_violations": [],
            "error": "no_cached_integrity_result",
        }
    else:
        integrity = dict(integrity)
    with stage("open_positions"):
        open_positions = store.open_positions()
    exposure = sum(float(row.get("committed_exposure_usd") or 0) for row in open_positions)
    with stage("runtime_health"):
        latest_health = _latest_runtime_health(store, effective_session_id)
    runtime_session_query_error: Optional[str] = None
    try:
        with stage("runtime_sessions"):
            open_runtime_session_count = int((store.query_one(
                """SELECT COUNT(*) AS count FROM runtime_sessions
                   WHERE strategy_id=? AND mode=? AND ended_ts_ms IS NULL""",
                (STRATEGY_ID, MODE),
            ) or {}).get("count") or 0)
            current_session_open = int((store.query_one(
                """SELECT COUNT(*) AS count FROM runtime_sessions
                   WHERE session_id=? AND ended_ts_ms IS NULL""",
                (effective_session_id,),
            ) or {}).get("count") or 0)
    except Exception as exc:
        # Unknown is not zero.  Session evidence is part of the runtime safety
        # contract and must fail closed if the query cannot be completed.
        open_runtime_session_count = None
        current_session_open = None
        runtime_session_query_error = (
            f"{type(exc).__name__}:{exc}")[:240]
    with stage("source_health"):
        try:
            source_health = store.latest_source_health(
                session_id=effective_session_id)
        except TypeError:  # Compatibility for a pre-v2 read-only store in tests.
            source_health = store.latest_source_health()
    nonce = runtime.get("launch_nonce")
    nonce_fingerprint = (
        hashlib.sha256(str(nonce).encode("utf-8")).hexdigest()[:12]
        if nonce else None
    )
    conflicts = metrics["acceptance_gate"]["conflicts"]
    duplicates = metrics["acceptance_gate"]["duplicates"]
    unresolved = metrics["acceptance_gate"]["unresolved_final"]
    with stage("universe"):
        universe = _universe(store, int(now_ms))
    with stage("candidates"):
        candidates = _latest_candidates(store)
    performance = metrics["performance"]
    compound = metrics["compound_preview"]
    commit = runtime.get("current_commit", runtime.get("git_commit", "UNKNOWN"))
    heartbeat_ts_ms = runtime.get(
        "heartbeat_ts_ms", latest_health.get("heartbeat_ts_ms") if latest_health else None
    )
    ledger_cap = (ledger or {}).get("max_committed_usd")
    positions = {
        "open": open_positions,
        "open_count": len(open_positions),
        "active_exposure_usd": round(exposure, 10),
        "exposure_cap_usd": (
            float(ledger_cap) if ledger_cap is not None
            else float(_value(config, "exposure_cap_usd", default=130.0))),
        "max_exposure_pct": (ledger or {}).get("max_exposure_pct", 1.0),
        "fixed_shares": FIXED_SHARES,
    }
    pnl = {
        **performance["verified_terminal"],
        "all_terminal": performance["all_terminal"],
        "excluded_unverifiable_rows": performance["excluded_unverifiable_rows"],
    }
    breakdowns = {
        key: performance[key] for key in (
            "by_asset", "by_side", "by_model", "by_edge_bucket",
            "by_entry_mode", "by_exit_source",
        )
    }
    reject_reasons: dict[str, int] = {}
    for reasons in metrics["rejects"]["all"].values():
        for reason, count in reasons.items():
            reject_reasons[reason] = reject_reasons.get(reason, 0) + int(count)
    with stage("recent_entries"):
        recent_entries = store.query(
            """SELECT e.*,w.asset,w.window_open_ts_ms,w.window_close_ts_ms
               FROM entries e JOIN asset_windows w ON w.window_id=e.window_id
               ORDER BY e.entry_ts_ms DESC,e.entry_id DESC LIMIT 20"""
        )
    with stage("terminal_trades"):
        terminal_trades = store.query(
            """SELECT p.*,w.asset,e.outcome_side,e.entry_mode
               FROM pnl_records p JOIN entries e ON e.entry_id=p.entry_id
               JOIN asset_windows w ON w.window_id=e.window_id
               ORDER BY p.terminal_ts_ms DESC,p.pnl_record_id DESC LIMIT 20"""
        )
    persistence = _mapping(runtime.get("persistence"))
    critical = _mapping(persistence.get("critical"))
    telemetry = _mapping(persistence.get("telemetry"))
    operational_reads = _mapping(persistence.get("operational_reads"))
    reporting = _mapping(persistence.get("reporting"))
    integrity_reads = _mapping(
        persistence.get("integrity_reads") or persistence.get("reporting"))
    maintenance = _mapping(persistence.get("maintenance"))
    latest_maintenance = _mapping(persistence.get("latest_maintenance"))
    runtime_io = _mapping(persistence.get("runtime_io"))
    writer_state = str(critical.get("state") or "UNKNOWN")
    critical_incomplete = int(
        telemetry.get("critical_evidence_incomplete_count")
        or telemetry.get("incomplete_evidence_count") or 0
    )
    critical_lost = int(
        telemetry.get("true_lost_critical_rows")
        or telemetry.get("critical_evidence_lost_count") or 0
    )
    raw_telemetry_loss = int(
        telemetry.get("raw_telemetry_loss_count")
        or telemetry.get("rows_dropped") or 0
    )
    telemetry_failures = int(telemetry.get("failed_batches") or 0)
    # Raw telemetry counters are cumulative-lifetime, so a single historical
    # pressure event would otherwise latch the dashboard degraded forever.
    # Operational degradation fires only while the lossy lane is *currently*
    # unhealthy: the writer is in a degraded health state or a failure/drop
    # landed inside the recent failure window.  Lifetime totals remain exported
    # below for auditability; they no longer pin operational_ready.
    telemetry_health = str(
        telemetry.get("state") or telemetry.get("health") or ""
    )
    telemetry_recent_window_ms = int(_value(
        config, "writer_failure_timeout_ms", default=5_000) or 5_000)
    # An accepted critical command that is merely in flight is normal
    # pipelining; only one still unconfirmed past the acknowledgement deadline
    # is an unresolved command.  An age the writer cannot report is treated as
    # overdue so the dashboard stays fail-closed.
    unconfirmed_oldest_age_ms = critical.get(
        "unconfirmed_command_oldest_age_ms")
    unconfirmed_overdue = (
        unconfirmed_oldest_age_ms is None
        or int(unconfirmed_oldest_age_ms) > telemetry_recent_window_ms
    )
    telemetry_last_failure_ts_ms = int(
        telemetry.get("last_failure_ts_ms") or 0)
    telemetry_last_overflow_ts_ms = int(
        telemetry.get("last_overflow_ts_ms") or 0)
    telemetry_recent_failure = (
        telemetry_last_failure_ts_ms > 0
        and int(now_ms) - telemetry_last_failure_ts_ms <= telemetry_recent_window_ms
    )
    telemetry_recent_overflow = (
        telemetry_last_overflow_ts_ms > 0
        and int(now_ms) - telemetry_last_overflow_ts_ms <= telemetry_recent_window_ms
    )
    # Recovery is an explicit writer-owned, fixed-cadence contract.  Missing or
    # stale state is unknown/unhealthy; legacy timestamp heuristics must not
    # turn an empty telemetry mapping into operational readiness.
    explicit_current_health = telemetry.get("current_operational_healthy")
    recovery_sample_age_ms = telemetry.get("recovery_sample_age_ms")
    recovery_sample_fresh = (
        isinstance(recovery_sample_age_ms, (int, float))
        and not isinstance(recovery_sample_age_ms, bool)
        and math.isfinite(float(recovery_sample_age_ms))
        and 0 <= float(recovery_sample_age_ms) <= telemetry_recent_window_ms
    )
    recovery_healthy_windows = int(
        telemetry.get("recovery_healthy_windows") or 0)
    recovery_required_windows = int(
        telemetry.get("recovery_required_windows") or 0)
    telemetry_recovery_healthy = (
        explicit_current_health is True
        and recovery_sample_fresh
        and recovery_required_windows > 0
        and recovery_healthy_windows >= recovery_required_windows
    )
    telemetry_currently_unhealthy = (
        telemetry_health != "HEALTHY"
        or not telemetry_recovery_healthy
    )
    # Data safety and capacity are separate questions.  A lane that is shedding
    # noncritical rows under an approved, fully accounted policy is honestly not
    # within capacity, but it is not losing evidence either -- and only the
    # latter may block readiness.  Missing fields stay fail-closed via UNKNOWN.
    telemetry_data_safety = str(
        telemetry.get("telemetry_data_safety") or "UNKNOWN")
    telemetry_capacity_state = str(
        telemetry.get("telemetry_capacity_state") or "UNKNOWN")
    telemetry_unexpected_loss = int(
        telemetry.get("window_unexpected_loss_rows")
        or telemetry.get("noncritical_rows_unexpectedly_lost") or 0)
    telemetry_reconciliation_mismatch = int(
        telemetry.get("accounting_reconciliation_mismatch_rows") or 0)
    telemetry_queue_bounded = bool(telemetry.get("queue_bounded", False))
    telemetry_queue_oldest_age_s = float(
        telemetry.get("queue_oldest_age_s") or 0.0)
    telemetry_capacity_blocking = telemetry_capacity_state in {
        "HARD_OVERLOAD", "UNKNOWN"}
    telemetry_policy_sampling = telemetry_capacity_state in {
        "POLICY_SAMPLING_ACTIVE", "POLICY_COALESCING_ACTIVE",
        "POLICY_DEFER_ACTIVE", "RECOVERING",
    }
    # Only UNSAFE (evidence actually lost) and UNKNOWN block.  DEGRADED means a
    # deadline miss or a failed batch happened and the bounded retry recovered
    # it with zero rows lost: an operator must see it, but it is not evidence
    # loss and must not hold readiness down while the lane is provably intact.
    telemetry_data_safety_blocking = telemetry_data_safety in {
        "UNSAFE", "UNKNOWN"}
    # The honest combined verdict: safe, but explicitly not within capacity.
    telemetry_reported_state = (
        "HEALTHY_WITH_POLICY_SAMPLING"
        if telemetry_data_safety == "HEALTHY" and telemetry_policy_sampling
        else telemetry_data_safety
        if telemetry_data_safety != "HEALTHY"
        else "HEALTHY" if telemetry_capacity_state == "WITHIN_CAPACITY"
        else telemetry_capacity_state
    )
    execution_blocked_reason = str(
        runtime.get("execution_blocked_reason")
        or critical.get("engine_latched_failure_reason") or ""
    )
    runtime_state_name = str(runtime.get("state") or "").upper()
    # Every nonterminal state owns exactly one open session, including
    # STARTING, STOPPING, and DEGRADED_* states.  Terminal exports must prove
    # that the session was closed.  Unknown state is deliberately treated as
    # nonterminal so missing lifecycle data cannot bypass the check.
    terminal_runtime_state = runtime_state_name in {"STOPPED", "FAILED"}
    expected_open_runtime_sessions = 0 if terminal_runtime_state else 1
    runtime_session_count_mismatch = (
        open_runtime_session_count != expected_open_runtime_sessions
        or current_session_open != expected_open_runtime_sessions
    )
    critical_blocked_reasons = [
        reason for reason, present in (
            ("critical_writer_unhealthy", writer_state != "HEALTHY"),
            ("unconfirmed_critical_command", int(
                critical.get("unconfirmed_command_count") or 0) > 0
                and unconfirmed_overdue),
            ("critical_evidence_incomplete", critical_incomplete > 0),
            ("critical_evidence_lost", critical_lost > 0),
            (f"engine_execution_blocked:{execution_blocked_reason}", bool(
                execution_blocked_reason)),
            ("process_ownership_unverified", not bool(
                runtime.get("process_ownership_valid"))),
            ("orphan_v4_process_detected", int(
                runtime.get("orphan_processes") or 0) > 0),
            ("runtime_session_query_failed", bool(
                runtime_session_query_error)),
            ("runtime_session_count_mismatch", bool(
                runtime_session_count_mismatch)),
            ("operational_read_worker_unhealthy", str(
                operational_reads.get("state") or "") != "RUNNING"),
            ("reporting_read_worker_unhealthy", str(
                reporting.get("state") or "") != "RUNNING"),
            ("integrity_read_worker_unhealthy", bool(
                persistence.get("integrity_reads")) and str(
                    integrity_reads.get("state") or "") != "RUNNING"),
            ("sqlite_integrity_unhealthy", not (
                integrity.get("integrity") == "ok"
                and not integrity.get("foreign_key_violations"))),
        ) if present
    ]
    critical_execution_ready = not critical_blocked_reasons
    operational_degraded_reasons = [
        *critical_blocked_reasons,
        *(["telemetry_writer_unhealthy"] if (
            telemetry_health != "HEALTHY") else []),
        # Evidence safety, not throughput: unexpected loss, an unclosed
        # conservation identity, or an unbounded queue all stay blocking.
        *([f"telemetry_data_safety_{telemetry_data_safety.lower()}"] if (
            telemetry_data_safety_blocking) else []),
        *(["telemetry_queue_residence_stale"] if (
            telemetry_queue_oldest_age_s > 30.0) else []),
        *(["telemetry_unexpected_noncritical_loss"] if (
            telemetry_unexpected_loss > 0) else []),
        *(["telemetry_accounting_mismatch"] if (
            telemetry_reconciliation_mismatch != 0) else []),
        *(["telemetry_queue_unbounded"] if (
            not telemetry_queue_bounded) else []),
        # Capacity blocks only when the lane is genuinely out of control.
        # POLICY_SAMPLING_ACTIVE is reported, not penalised.
        *([f"telemetry_capacity_{telemetry_capacity_state.lower()}"] if (
            telemetry_capacity_blocking) else []),
        *(["telemetry_recovery_window"] if (
            not telemetry_recovery_healthy) else []),
        *(["telemetry_recovery_sample_stale"] if (
            not recovery_sample_fresh) else []),
        *(["maintenance_worker_unhealthy"] if str(
            maintenance.get("state") or "") != "RUNNING" else []),
        *(["maintenance_pass_failed"] if str(
            latest_maintenance.get("status") or ""
        ) == "FAILED" else []),
        *(["runtime_io_worker_unhealthy"] if str(
            runtime_io.get("state") or "") != "RUNNING" else []),
        # A stuck runtime/export publish is recoverable rather than fatal, so it
        # does not block execution -- but it must not be hidden either.  While
        # the engine reports the degradation, operational readiness fails closed
        # until the bounded healthy-publish recovery window clears it.
        *(["reporting_export_degraded"] if bool(
            runtime.get("reporting_export_degraded")) else []),
    ]
    persistence_ready = bool(
        critical_execution_ready and not operational_degraded_reasons
    )
    db_path = Path(store.path)
    wal_path = Path(f"{db_path}-wal")
    shm_path = Path(f"{db_path}-shm")
    with stage("latest_checkpoint"):
        try:
            latest_checkpoint = store.query_one(
                "SELECT * FROM checkpoint_runs "
                "ORDER BY checkpoint_run_id DESC LIMIT 1"
            )
        except Exception:  # A v1 fixture can be exported before migrations run.
            latest_checkpoint = None
    with stage("database_size"):
        database_size_bytes = store.database_size_bytes()
    # Freshness must distinguish the runtime heartbeat from the export itself.
    # A stale export must not make a fresh runtime heartbeat appear stale, so
    # the runtime heartbeat is the freshest independently-observed signal:
    # the published runtime heartbeat, the latest runtime_health row, or the
    # critical writer heartbeat (which the dedicated writer thread touches every
    # writer_heartbeat_interval_ms).  ``export_age_ms`` is this payload's age.
    runtime_heartbeat_ts_ms = max(int(value or 0) for value in (
        heartbeat_ts_ms,
        (latest_health or {}).get("heartbeat_ts_ms"),
        (latest_health or {}).get("sample_ts_ms"),
        critical.get("heartbeat_ts_ms"),
        operational_reads.get("heartbeat_ts_ms"),
    ))
    if runtime_heartbeat_ts_ms <= 0:
        runtime_heartbeat_ts_ms = int(heartbeat_ts_ms or 0)
    # export_age_ms is 0 at generation (this payload is brand new).  A reader
    # that fetches this file later computes the displayed age as
    # now - generated_ts_ms; exporting it explicitly keeps the contract honest
    # and lets the dashboard distinguish export freshness from runtime heartbeat
    # freshness without re-deriving the semantics.
    export_age_ms = 0
    runtime_heartbeat_age_ms = (
        max(0, int(now_ms) - runtime_heartbeat_ts_ms)
        if runtime_heartbeat_ts_ms > 0 else None)
    heartbeat_age_ms = (
        max(0, int(now_ms) - int(heartbeat_ts_ms or 0))
        if heartbeat_ts_ms else None)
    payload = {
        "schema_version": 3,
        "generated_ts_ms": int(now_ms),
        "export_age_ms": export_age_ms,
        "runtime_heartbeat_age_ms": runtime_heartbeat_age_ms,
        "heartbeat_age_ms": heartbeat_age_ms,
        "runtime_heartbeat_ts_ms": runtime_heartbeat_ts_ms,
        "strategy_id": STRATEGY_ID,
        "mode": MODE,
        **safety,
        "lineage_fingerprint": runtime.get("lineage_fingerprint"),
        "runtime_label": RUNTIME_LABEL,
        "cohort": {
            "authoritative": ACTIVE_COHORT,
            "legacy": LEGACY_COHORT,
            "activation_ts_ms": (cohort_row or {}).get("activation_ts_ms"),
            "activation_commit": (cohort_row or {}).get("activation_commit"),
            "starting_equity_usd": (cohort_row or {}).get(
                "starting_equity_usd", starting_equity),
            "max_exposure_pct": (cohort_row or {}).get("max_exposure_pct", 1.0),
            "legacy_metrics_are_non_authoritative": True,
        },
        "authoritative_capital": authoritative_capital,
        "current_commit": commit,
        "heartbeat_ts_ms": heartbeat_ts_ms,
        "fixed_shares": FIXED_SHARES,
        "warning": WARNING,
        "safety": {
            **safety,
            "exact_one_entry_per_asset_window": True,
            "one_side_only": True,
            "authenticated_trading_client_present": False,
            "wallet_signing_present": False,
            "real_order_placement_present": False,
            "real_order_cancellation_present": False,
            "quota_can_override_economic_gate": False,
        },
        "runtime": {
            "session_id": effective_session_id,
            "pid": runtime.get("pid", latest_health.get("pid") if latest_health else None),
            "launch_nonce_fingerprint": nonce_fingerprint,
            "current_commit": commit,
            "config_hash": runtime.get("config_hash"),
            "lineage_fingerprint": runtime.get("lineage_fingerprint"),
            "heartbeat_ts_ms": heartbeat_ts_ms,
            "state": runtime.get("state", latest_health.get("state") if latest_health else None),
            "orphan_processes": int(runtime.get("orphan_processes") or 0),
            "process_ownership_valid": bool(runtime.get("process_ownership_valid", False)),
            "open_runtime_session_count": open_runtime_session_count,
            "expected_open_runtime_session_count": (
                expected_open_runtime_sessions),
            "current_session_open": (
                bool(current_session_open)
                if current_session_open is not None else None),
            "runtime_session_query_error": runtime_session_query_error,
            "latest_health": latest_health,
        },
        "effective_config": {
            "config_hash": runtime.get("config_hash"),
            **_effective_persistence_config(config),
        },
        "persistence": {
            "critical": critical,
            "telemetry": telemetry,
            "operational_reads": operational_reads,
            "reporting": reporting,
            "integrity_reads": integrity_reads,
            "maintenance": maintenance,
            "latest_maintenance": latest_maintenance,
            "runtime_io": runtime_io,
            "latest_checkpoint": latest_checkpoint,
            # Reader-lifetime diagnostics: which SQLite read operations are
            # live right now and how old their snapshots are.  Sourced from
            # the published runtime state so the export never re-derives it.
            "sqlite_readers": _mapping(persistence.get("sqlite_readers")),
            # Named-stage export cost profile (p50/p90/p99/max per stage and
            # per statement).  Sourced from the published runtime state so the
            # export never re-derives it, and reflecting the *previous* builds
            # -- this build's own timings are only complete after it returns.
            "export_profile": _mapping(persistence.get("export_profile")),
            "connection_ownership": {
                "critical_writer": "dedicated_writer_thread",
                "telemetry": "dedicated_aggregator_and_writer_connection",
                "operational_reads": "dedicated_operational_read_only_worker",
                "reporting": "dedicated_report_read_only_worker",
                "maintenance": "dedicated_maintenance_worker",
            },
            "operational_ready": persistence_ready,
            "critical_execution_ready": critical_execution_ready,
            "critical_blocked_reasons": critical_blocked_reasons,
            "operational_degraded_reasons": operational_degraded_reasons,
            # Telemetry recovery context: lifetime totals stay exposed for
            # audit, plus the recent-loss flags that actually drive the
            # operational decision, so an operator can tell a recovered lane
            # (lifetime loss > 0, recent = false) from an actively failing one.
            # Separated health model.  ``reported_state`` is the honest combined
            # verdict; the two components stay individually visible so an
            # operator can always tell "safe but shedding" from "losing rows".
            "telemetry_health_model": {
                "reported_state": telemetry_reported_state,
                "data_safety": telemetry_data_safety,
                "data_safety_reasons": telemetry.get(
                    "telemetry_data_safety_reasons") or [],
                "capacity_state": telemetry_capacity_state,
                "capacity_blocking": telemetry_capacity_blocking,
                "policy_sampling_active": telemetry_policy_sampling,
                "sampling_keep_ratio": telemetry.get("sampling_keep_ratio"),
                "sampling_policy_reason": telemetry.get("overload_reason"),
                "overload_policy_reasons": telemetry.get(
                    "overload_policy_reasons"),
                "window_unexpected_loss_rows": telemetry_unexpected_loss,
                "accounting_reconciliation_mismatch_rows": (
                    telemetry_reconciliation_mismatch),
                "accounting_reconciliation": telemetry.get(
                    "accounting_reconciliation"),
                "loss_by_category": telemetry.get("loss_by_category"),
                "policy_outcome_breakdown": telemetry.get(
                    "policy_outcome_breakdown"),
                "unexpected_loss_breakdown": telemetry.get(
                    "unexpected_loss_breakdown"),
                "queue_bounded": telemetry_queue_bounded,
                "queue_max_depth_window": telemetry.get(
                    "queue_max_depth_window"),
                "queue_oldest_age_s": telemetry_queue_oldest_age_s,
                "queue_floor_before": telemetry.get("queue_floor_before"),
                "queue_floor_after": telemetry.get("queue_floor_after"),
                "queue_danger_depth": telemetry.get("queue_danger_depth"),
                "data_safety_blocking": telemetry_data_safety_blocking,
                "queue_depth_slope_per_second": telemetry.get(
                    "queue_depth_slope_per_second"),
            },
            "telemetry_recovery": {
                "lifetime_raw_telemetry_loss": raw_telemetry_loss,
                "lifetime_telemetry_failures": telemetry_failures,
                "recent_failure": telemetry_recent_failure,
                "recent_overflow": telemetry_recent_overflow,
                "currently_unhealthy": telemetry_currently_unhealthy,
                "current_operational_healthy": telemetry_recovery_healthy,
                "healthy_windows": recovery_healthy_windows,
                "required_healthy_windows": recovery_required_windows,
                "sample_age_ms": recovery_sample_age_ms,
                "sample_fresh": recovery_sample_fresh,
                "last_failure_ts_ms": telemetry_last_failure_ts_ms or None,
                "last_overflow_ts_ms": telemetry_last_overflow_ts_ms or None,
                "recent_window_ms": telemetry_recent_window_ms,
            },
            # Backward-compatible alias, now explicitly operational rather
            # than a claim that lossy raw telemetry blocked safe shadow entry.
            "blocked_reasons": operational_degraded_reasons,
        },
        "sources": source_health,
        "source_policy": {
            "primary_cex": "OKX",
            "all_fresh_cex_unavailable_blocks_entry": True,
            "stale_cex_reuse_allowed": False,
        },
        # Automatic model-health quarantine state.  Data-driven from realized
        # fee-net performance per dominant_model; a quarantined model's ensemble
        # contribution is zeroed and its entries are rejected (fail-closed).
        "model_health": _mapping(runtime.get("model_health")),
        "universe": universe,
        "market_universe": universe,
        "frequency": metrics["frequency"],
        "funnel": metrics["funnel"],
        "latest_candidates": candidates,
        "model_contributions": [
            {"candidate_id": row["candidate_id"], "asset": row["asset"],
             "contributions": row["model_contributions"]}
            for row in candidates
        ],
        "execution": metrics["execution"],
        "reject_taxonomy": metrics["rejects"],
        "no_book": metrics["rejects"]["no_book"],
        "data_invalid": metrics["rejects"]["data_invalid"],
        "reject_reasons": dict(sorted(reject_reasons.items())),
        "positions": positions,
        "exposure": positions,
        "open_positions": open_positions,
        "recent_entries": recent_entries,
        "terminal_trades": terminal_trades,
        "performance": performance,
        "legacy_non_authoritative": metrics.get("legacy_non_authoritative"),
        "pnl": pnl,
        "breakdowns": breakdowns,
        "compound_preview": compound,
        "compounding_preview": compound,
        "integrity": {
            "sqlite_integrity": integrity["integrity"],
            "foreign_key_violations": len(integrity["foreign_key_violations"]),
            "conflicts": conflicts,
            "duplicates": duplicates,
            "unresolved_final": unresolved,
            "maker_fill_assumed_count": metrics["execution"]["maker_fill_assumed_count"],
        },
        "database": {
            "path_name": db_path.name,
            "size_bytes": database_size_bytes,
            "db_bytes": db_path.stat().st_size if db_path.exists() else 0,
            "wal_bytes": wal_path.stat().st_size if wal_path.exists() else 0,
            "shm_bytes": shm_path.stat().st_size if shm_path.exists() else 0,
            "wal_enabled": True,
            "wal_autocheckpoint": 0,
            "foreign_keys_enabled": True,
            "legacy_data_included": False,
        },
        "acceptance_gate": metrics["acceptance_gate"],
    }
    with stage("json_safe"):
        return _json_safe(payload)


build_v4_dashboard = build_frequency_v4_dashboard


def _json_safe(value: Any) -> Any:
    """Replace non-finite floats with ``None`` throughout the payload.

    The export is written with ``allow_nan=False`` because ``Infinity`` and
    ``NaN`` are not valid JSON and no browser will parse them.  A single
    non-finite number anywhere therefore aborted the whole write, leaving the
    dashboard frozen on its last good file while the runtime kept running --
    the operator sees a stale page with no indication that anything failed.

    Non-finite values are legitimate here (a model with no losing trade has an
    infinite profit factor), so they are exported as ``null`` rather than
    allowed to blank the entire view.
    """

    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _atomic_write(path: Path, content: str) -> None:
    """Publish the dashboard export atomically.

    This path has the same Windows exposure as the runtime heartbeat publish and
    has already failed in production with
    ``export:PermissionError:[WinError 5] Access is denied`` on this very temp
    file: the dashboard polls the exported document continuously, so a reader or
    an antivirus scan can hold the destination at the instant of the replace.
    It shares the runtime's bounded-retry replace so a transient lock costs a few
    milliseconds instead of a failed export and a growing export age.

    The temp name carries a uuid as well as the pid so two writers can never
    collide on one temp path; the canonical destination is still single-writer
    (the dedicated reporting worker is single-flight).
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(content, encoding="utf-8", newline="\n")
        atomic_replace(temporary, path)
    finally:
        _discard_temporary(temporary)


def write_frequency_v4_dashboard(
    store: Any, output_path: str | Path, *, now_ms: int,
    config: Any = None, runtime_state: Any = None,
    session_id: Optional[str] = None, integrity: Any = None,
) -> dict[str, Any]:
    # C1 isolation guard: refuse any path outside the canonical source-root-
    # derived export directory before building or writing anything.
    path = assert_canonical_export_path(output_path)
    with EXPORT_PROFILE.build():
        payload = build_frequency_v4_dashboard(
            store, now_ms=int(now_ms), config=config,
            runtime_state=runtime_state, session_id=session_id,
            integrity=integrity,
        )
        with stage("serialize"):
            encoded = json.dumps(
                payload, ensure_ascii=False, sort_keys=True, indent=2,
                allow_nan=False,
            ) + "\n"
            encoded_bytes = len(encoded.encode("utf-8"))
        with stage("publish"):
            _atomic_write(path, encoded)
        EXPORT_PROFILE.observe_payload_bytes(encoded_bytes)
    return {"path": str(path), "payload": payload, "bytes": encoded_bytes}


write_v4_dashboard = write_frequency_v4_dashboard
