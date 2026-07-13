"""Atomic, read-only dashboard export for Lite Frequency V4 only."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional

from .metrics import build_metrics
from .store import FIXED_SHARES, MODE, STRATEGY_ID


EXPORT_FILENAME = "frequency_v4_dashboard.json"
WARNING = (
    "LITE FREQUENCY V4 SHADOW ONLY - RESEARCH CANDIDATE - "
    "NO REAL ORDERS OR CANCELLATIONS"
)


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
    duration_rows = store.query(
        """SELECT duration_ms,COUNT(*) markets,COUNT(DISTINCT asset) assets
           FROM markets GROUP BY duration_ms ORDER BY duration_ms"""
    )
    ignored = [row for row in duration_rows if int(row["duration_ms"]) != 300_000]
    active = store.query(
        """SELECT w.window_id,w.asset,w.window_open_ts_ms,w.window_close_ts_ms,
           f.available,f.eligible,f.positive_edge,f.execution_attempts,f.actual_entry,
           m.polymarket_market_id,m.slug,mi.event_id,mi.condition_id,
           mi.yes_token_id,mi.no_token_id,mi.association_valid,mi.token_pair_valid,
           a.status anchor_status,a.price_to_beat
           FROM asset_windows w JOIN window_funnel f ON f.window_id=w.window_id
           LEFT JOIN window_market_links l ON l.window_id=w.window_id AND l.selected=1
           LEFT JOIN market_identities mi ON mi.market_identity_id=l.market_identity_id
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
    return {
        "exact_duration_ms": 300_000,
        "exact_five_minute_markets": exact_count,
        "active_exact_five_minute_windows": active,
        "ignored_other_durations": ignored,
        "automatic_universe": True,
        "required_assets": ["BTC", "ETH", "SOL"],
        "hardcoded_to_required_assets_only": False,
    }


def _latest_candidates(store: Any, limit: int = 12) -> list[dict[str, Any]]:
    candidates = store.query(
        """SELECT c.*,w.asset,w.window_open_ts_ms,w.window_close_ts_ms,
           m.slug,m.polymarket_market_id FROM candidates c
           JOIN asset_windows w ON w.window_id=c.window_id
           JOIN market_identities mi ON mi.market_identity_id=c.market_identity_id
           JOIN markets m ON m.market_id=mi.market_id
           ORDER BY c.evaluation_ts_ms DESC,c.candidate_id DESC LIMIT ?""",
        (int(limit),),
    )
    for candidate in candidates:
        candidate["model_contributions"] = store.query(
            """SELECT model_name,model_version,correlation_group,direction,raw_score,
               estimated_probability,evidence_age_ms,confidence,reliability,
               invalidation_reason,expected_net_edge,regime_weight,gated,
               model_contribution,calibrated FROM model_contributions
               WHERE candidate_id=? ORDER BY model_name""",
            (int(candidate["candidate_id"]),),
        )
        candidate["fair_value_calculations"] = store.query(
            """SELECT fv.*,s.outcome_side,s.executable_vwap,s.worst_consumed_price,
               s.spread,s.depth_shares,s.exact_five_share_depth,s.estimated_fee,
               s.execution_buffer,s.latency_buffer,s.uncertainty_buffer,s.net_edge,
               s.evidence_fresh,s.selected FROM fair_value_calculations fv
               JOIN fair_value_sides s USING(fair_value_calculation_id)
               WHERE fv.candidate_id=? ORDER BY fv.calculation_seq,s.outcome_side""",
            (int(candidate["candidate_id"]),),
        )
    return candidates


def build_frequency_v4_dashboard(
    store: Any, *, now_ms: int, config: Any = None,
    runtime_state: Any = None, session_id: Optional[str] = None,
) -> dict[str, Any]:
    safety = assert_v4_safety(config)
    runtime = _mapping(runtime_state)
    effective_session_id = session_id or runtime.get("session_id")
    starting_equity = float(_value(
        config, "starting_equity_usd", "research_equity_usd", default=13.0
    ))
    metrics = build_metrics(
        store, int(now_ms), session_id=effective_session_id,
        starting_equity_usd=starting_equity,
    )
    integrity = store.integrity_check()
    open_positions = store.open_positions()
    exposure = sum(float(row.get("committed_exposure_usd") or 0) for row in open_positions)
    latest_health = _latest_runtime_health(store, effective_session_id)
    source_health = store.latest_source_health()
    nonce = runtime.get("launch_nonce")
    nonce_fingerprint = (
        hashlib.sha256(str(nonce).encode("utf-8")).hexdigest()[:12]
        if nonce else None
    )
    conflicts = metrics["acceptance_gate"]["conflicts"]
    duplicates = metrics["acceptance_gate"]["duplicates"]
    unresolved = metrics["acceptance_gate"]["unresolved_final"]
    universe = _universe(store, int(now_ms))
    candidates = _latest_candidates(store)
    performance = metrics["performance"]
    compound = metrics["compound_preview"]
    commit = runtime.get("current_commit", runtime.get("git_commit", "UNKNOWN"))
    heartbeat_ts_ms = runtime.get(
        "heartbeat_ts_ms", latest_health.get("heartbeat_ts_ms") if latest_health else None
    )
    positions = {
        "open": open_positions,
        "open_count": len(open_positions),
        "active_exposure_usd": round(exposure, 10),
        "exposure_cap_usd": float(_value(config, "exposure_cap_usd", default=9.75)),
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
    recent_entries = store.query(
        """SELECT e.*,w.asset,w.window_open_ts_ms,w.window_close_ts_ms
           FROM entries e JOIN asset_windows w ON w.window_id=e.window_id
           ORDER BY e.entry_ts_ms DESC,e.entry_id DESC LIMIT 20"""
    )
    terminal_trades = store.query(
        """SELECT p.*,w.asset,e.outcome_side,e.entry_mode
           FROM pnl_records p JOIN entries e ON e.entry_id=p.entry_id
           JOIN asset_windows w ON w.window_id=e.window_id
           ORDER BY p.terminal_ts_ms DESC,p.pnl_record_id DESC LIMIT 20"""
    )
    return {
        "schema_version": 1,
        "generated_ts_ms": int(now_ms),
        "strategy_id": STRATEGY_ID,
        "mode": MODE,
        **safety,
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
            "heartbeat_ts_ms": heartbeat_ts_ms,
            "state": runtime.get("state", latest_health.get("state") if latest_health else None),
            "orphan_processes": int(runtime.get("orphan_processes") or 0),
            "process_ownership_valid": bool(runtime.get("process_ownership_valid", False)),
            "latest_health": latest_health,
        },
        "sources": source_health,
        "source_policy": {
            "primary_cex": "OKX",
            "all_fresh_cex_unavailable_blocks_entry": True,
            "stale_cex_reuse_allowed": False,
        },
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
            "path_name": Path(store.path).name,
            "size_bytes": store.database_size_bytes(),
            "wal_enabled": True,
            "foreign_keys_enabled": True,
            "legacy_data_included": False,
        },
        "acceptance_gate": metrics["acceptance_gate"],
    }


build_v4_dashboard = build_frequency_v4_dashboard


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(content, encoding="utf-8", newline="\n")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def write_frequency_v4_dashboard(
    store: Any, output_path: str | Path, *, now_ms: int,
    config: Any = None, runtime_state: Any = None,
    session_id: Optional[str] = None,
) -> dict[str, Any]:
    path = Path(output_path)
    if path.exists() and path.is_dir():
        path = path / EXPORT_FILENAME
    elif path.suffix.lower() != ".json":
        path = path / EXPORT_FILENAME
    payload = build_frequency_v4_dashboard(
        store, now_ms=int(now_ms), config=config,
        runtime_state=runtime_state, session_id=session_id,
    )
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False
    ) + "\n"
    _atomic_write(path, encoded)
    return {"path": str(path), "payload": payload, "bytes": len(encoded.encode("utf-8"))}


write_v4_dashboard = write_frequency_v4_dashboard
