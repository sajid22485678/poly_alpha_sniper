"""Deterministic V4 frequency, performance, and acceptance metrics.

Rates are never extrapolated from a partial requested horizon.  A rolling
``entries_per_hour`` is populated only after the runtime has observed the
entire 1/3/6/12-hour interval; partial intervals expose counts and elapsed
minutes with an explicit insufficient-duration status instead.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Optional


ROLLING_HOURS = (1, 3, 6, 12)
TARGET_MIN_ENTRIES_PER_HOUR = 19.0
TARGET_MAX_ENTRIES_PER_HOUR = 36.0
FORWARD_TERMINAL_REQUIREMENT = 300


def _query(store: Any, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    return store.query(sql, tuple(params))


def _one(store: Any, sql: str, params: Iterable[Any] = ()) -> dict[str, Any]:
    row = store.query_one(sql, tuple(params))
    return row or {}


def _safe_rate(numerator: float, hours: float, complete: bool) -> Optional[float]:
    if not complete or hours <= 0:
        return None
    return round(float(numerator) / float(hours), 6)


def _session_start(store: Any, now_ms: int, session_id: Optional[str]) -> int:
    if session_id:
        row = _one(
            store, "SELECT started_ts_ms FROM runtime_sessions WHERE session_id=?",
            (session_id,),
        )
    else:
        row = _one(store, "SELECT MIN(started_ts_ms) started_ts_ms FROM runtime_sessions")
    value = row.get("started_ts_ms")
    return min(int(now_ms), int(value)) if value is not None else int(now_ms)


def cohort_activation_ts_ms(store: Any, cohort: str) -> Optional[int]:
    """Persisted activation timestamp of a cohort; None before activation."""
    try:
        row = _one(
            store, "SELECT activation_ts_ms FROM cohorts WHERE cohort=?",
            (str(cohort),),
        )
    except Exception:
        return None
    value = row.get("activation_ts_ms")
    return int(value) if value is not None else None


def frequency_window(
    store: Any, now_ms: int, *, hours: float, session_id: Optional[str] = None,
    full_session: bool = False, not_before_ts_ms: Optional[int] = None,
) -> dict[str, Any]:
    """Return window-level funnel metrics for one observed interval.

    ``not_before_ts_ms`` clamps the observation start (used to exclude
    pre-activation legacy activity from the authoritative cohort's funnel).
    """
    now_ms = int(now_ms)
    start = _session_start(store, now_ms, session_id)
    if not_before_ts_ms is not None:
        start = min(now_ms, max(start, int(not_before_ts_ms)))
    if full_session:
        cutoff = start
        requested_hours = max(0.0, (now_ms - start) / 3_600_000.0)
        complete = requested_hours >= 1.0
        label = "session"
    else:
        requested_hours = float(hours)
        horizon_ms = int(requested_hours * 3_600_000)
        cutoff = max(start, now_ms - horizon_ms)
        complete = start <= now_ms - horizon_ms
        label = f"{int(hours)}h"
    observed_hours = max(0.0, (now_ms - cutoff) / 3_600_000.0)
    # A runtime commonly starts part-way through the already-open five-minute
    # window.  Capacity is window-aligned, while entries/positive edges and
    # terminals are admitted by their own evidence timestamps.  Filtering all
    # funnel fields by window-open time would silently drop genuine activity
    # from that first partial window.
    window_cutoff = cutoff // 300_000 * 300_000
    window_filter = "w.window_open_ts_ms>=? AND w.window_open_ts_ms<?"
    window_params = (window_cutoff, now_ms)
    funnel = _one(
        store,
        f"""SELECT COUNT(*) available_asset_windows,
          COALESCE(SUM(f.available),0) available_marked,
          COALESCE(SUM(f.eligible),0) eligible_windows,
          COALESCE(SUM(CASE WHEN f.positive_edge=1
            AND f.first_positive_edge_ts_ms>=? AND f.first_positive_edge_ts_ms<?
            THEN 1 ELSE 0 END),0) positive_edge_windows,
          COALESCE(SUM(f.execution_attempts),0) execution_attempts,
          COALESCE(SUM(CASE WHEN f.actual_entry=1
            AND f.entry_ts_ms>=? AND f.entry_ts_ms<? THEN 1 ELSE 0 END),0)
            actual_entries,
          COALESCE(SUM(CASE WHEN f.terminal=1
            AND f.terminal_ts_ms>=? AND f.terminal_ts_ms<? THEN 1 ELSE 0 END),0)
            terminal_trades,
          COALESCE(SUM(CASE WHEN f.positive_edge=1
            AND f.first_positive_edge_ts_ms>=? AND f.first_positive_edge_ts_ms<?
            AND NOT (f.actual_entry=1 AND f.entry_ts_ms>=? AND f.entry_ts_ms<?)
            THEN 1 ELSE 0 END),0)
            missed_opportunities
          FROM asset_windows w JOIN window_funnel f ON f.window_id=w.window_id
          WHERE {window_filter}""",
        (
            cutoff, now_ms,
            cutoff, now_ms,
            cutoff, now_ms,
            cutoff, now_ms, cutoff, now_ms,
            *window_params,
        ),
    )
    # Expected logical windows are the honest capacity denominator.  If a
    # market was not discovered, that window remains available-but-ineligible
    # and its blocker explains the shortfall rather than silently vanishing.
    expected_windows = int(funnel.get("available_asset_windows") or 0)
    available = int(funnel.get("available_marked") or 0)
    eligible = int(funnel.get("eligible_windows") or 0)
    positive = int(funnel.get("positive_edge_windows") or 0)
    attempts = int(funnel.get("execution_attempts") or 0)
    entries = int(funnel.get("actual_entries") or 0)
    terminal = int(funnel.get("terminal_trades") or 0)
    missed = int(funnel.get("missed_opportunities") or 0)
    raw = _one(
        store,
        """SELECT COALESCE(SUM(raw_count),0) raw_events,
           COALESCE(SUM(unique_count),0) unique_source_events,
           COALESCE(SUM(duplicate_count),0) duplicate_events,
           COALESCE(SUM(invalid_count),0) invalid_events
           FROM event_buckets WHERE bucket_start_ts_ms>=? AND bucket_start_ts_ms<?""",
        (cutoff, now_ms),
    )
    assets = _query(
        store,
        f"""SELECT DISTINCT w.asset FROM asset_windows w JOIN window_funnel f
           ON f.window_id=w.window_id WHERE {window_filter} AND f.available=1
           ORDER BY w.asset""",
        window_params,
    )
    eligible_assets = [str(row["asset"]) for row in assets]
    theoretical_capacity_per_hour = len(eligible_assets) * 12
    required_coverage = (
        round(TARGET_MIN_ENTRIES_PER_HOUR / theoretical_capacity_per_hour * 100.0, 6)
        if theoretical_capacity_per_hour else None
    )
    coverage_denominator = expected_windows
    coverage = (
        round(entries / coverage_denominator * 100.0, 6)
        if coverage_denominator else None
    )
    eligible_coverage = round(entries / eligible * 100.0, 6) if eligible else None
    positive_conversion = round(entries / positive * 100.0, 6) if positive else None
    blockers = {
        str(row["reason"]): int(row["count"])
        for row in _query(
            store,
            f"""SELECT COALESCE(NULLIF(f.final_blocker,''),'UNCLASSIFIED') reason,
               COUNT(*) count FROM asset_windows w JOIN window_funnel f
               ON f.window_id=w.window_id WHERE {window_filter}
               AND NOT (f.actual_entry=1 AND f.entry_ts_ms>=? AND f.entry_ts_ms<?)
               GROUP BY reason ORDER BY count DESC,reason""",
            (*window_params, cutoff, now_ms),
        )
    }
    complete_rate = _safe_rate(entries, requested_hours, complete)
    if complete_rate is None:
        rate_status = "INSUFFICIENT_DURATION_NO_EXTRAPOLATION"
    elif TARGET_MIN_ENTRIES_PER_HOUR <= complete_rate <= TARGET_MAX_ENTRIES_PER_HOUR:
        rate_status = "SOFT_TARGET_RANGE"
    elif complete_rate < TARGET_MIN_ENTRIES_PER_HOUR:
        rate_status = "BELOW_SOFT_TARGET"
    else:
        rate_status = "ABOVE_SOFT_TARGET"
    return {
        "label": label,
        "from_ts_ms": cutoff,
        "to_ts_ms": now_ms,
        "requested_hours": round(requested_hours, 6),
        "observed_hours": round(observed_hours, 6),
        "complete_interval": bool(complete),
        "rate_status": rate_status,
        "available_asset_windows": expected_windows,
        "available_marked_windows": available,
        "eligible_windows": eligible,
        "positive_edge_windows": positive,
        "unique_positive_edge_opportunities": positive,
        "execution_attempts": attempts,
        "actual_entries": entries,
        "terminal_trades": terminal,
        "missed_opportunities": missed,
        "entries_per_hour": complete_rate,
        "coverage_pct": coverage,
        "eligible_coverage_pct": eligible_coverage,
        "positive_edge_conversion_pct": positive_conversion,
        "eligible_assets": eligible_assets,
        "theoretical_max_trades_per_hour": theoretical_capacity_per_hour,
        "soft_target_min_per_hour": TARGET_MIN_ENTRIES_PER_HOUR,
        "soft_target_max_per_hour": TARGET_MAX_ENTRIES_PER_HOUR,
        "coverage_required_for_19_per_hour_pct": required_coverage,
        "raw_events": int(raw.get("raw_events") or 0),
        "unique_source_events": int(raw.get("unique_source_events") or 0),
        "duplicate_events": int(raw.get("duplicate_events") or 0),
        "invalid_events": int(raw.get("invalid_events") or 0),
        "bottlenecks": blockers,
        "quota_override": False,
        "economic_gate": "estimated_fee_net_EV > 0",
    }


def rolling_frequency(
    store: Any, now_ms: int, *, session_id: Optional[str] = None,
    not_before_ts_ms: Optional[int] = None,
) -> dict[str, dict[str, Any]]:
    result = {
        f"{hours}h": frequency_window(
            store, now_ms, hours=hours, session_id=session_id,
            not_before_ts_ms=not_before_ts_ms,
        )
        for hours in ROLLING_HOURS
    }
    result["session"] = frequency_window(
        store, now_ms, hours=0, session_id=session_id, full_session=True,
        not_before_ts_ms=not_before_ts_ms,
    )
    return result


def _performance(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: (int(row["terminal_ts_ms"]), int(row["entry_id"])))
    values = [float(row["net_pnl"]) for row in ordered]
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    cumulative = peak = drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    gross_profit = sum(wins)
    gross_loss = -sum(losses)
    return {
        "count": len(values),
        "net_pnl": round(sum(values), 10),
        "fees": round(sum(float(row.get("total_fees") or 0) for row in ordered), 10),
        "wins": len(wins),
        "losses": len(losses),
        "winrate": round(len(wins) / len(values), 6) if values else None,
        "profit_factor": round(gross_profit / gross_loss, 6) if gross_loss > 0 else None,
        "expectancy": round(sum(values) / len(values), 10) if values else None,
        "max_drawdown": round(drawdown, 10),
        "average_win": round(gross_profit / len(wins), 10) if wins else None,
        "average_loss": round(sum(losses) / len(losses), 10) if losses else None,
        "gross_profit": round(gross_profit, 10),
        "gross_loss": round(gross_loss, 10),
    }


def _group_performance(rows: list[dict[str, Any]], field: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(field) or "UNKNOWN")].append(row)
    return {key: _performance(group) for key, group in sorted(grouped.items())}


def performance_metrics(store: Any, *, cohort: Optional[str] = None) -> dict[str, Any]:
    """Terminal-trade performance, optionally scoped to one capital cohort.

    With ``cohort`` set, only trades whose owning runtime session belongs to
    that cohort are counted — the authoritative Phase 1 view.  Without it,
    every historical row is aggregated (legacy, non-authoritative).
    """
    cohort_join = (
        "JOIN runtime_sessions rs ON rs.session_id=e.session_id "
        if cohort is not None else ""
    )
    cohort_where = "WHERE rs.cohort=? " if cohort is not None else ""
    params: tuple[Any, ...] = (str(cohort),) if cohort is not None else ()
    rows = _query(
        store,
        f"""SELECT p.*,e.outcome_side,e.entry_mode,e.selected_net_edge,
           w.asset,c.dominant_model,x.exit_source
           FROM pnl_records p JOIN entries e ON e.entry_id=p.entry_id
           JOIN asset_windows w ON w.window_id=e.window_id
           JOIN candidates c ON c.candidate_id=e.candidate_id
           {cohort_join}
           LEFT JOIN exits x ON x.entry_id=e.entry_id
           {cohort_where}
           ORDER BY p.terminal_ts_ms,p.entry_id""",
        params,
    )
    for row in rows:
        edge = float(row.get("selected_net_edge") or 0)
        row["edge_bucket"] = (
            ">=0.020" if edge >= 0.020 else "0.010-0.020" if edge >= 0.010
            else "0.005-0.010" if edge >= 0.005 else "0-0.005"
        )
    verified = [row for row in rows if bool(row.get("verified"))]
    excluded = len(rows) - len(verified)
    return {
        "all_terminal": _performance(rows),
        "verified_terminal": _performance(verified),
        "excluded_unverifiable_rows": excluded,
        "by_asset": _group_performance(verified, "asset"),
        "by_side": _group_performance(verified, "outcome_side"),
        "by_model": _group_performance(verified, "dominant_model"),
        "by_edge_bucket": _group_performance(verified, "edge_bucket"),
        "by_entry_mode": _group_performance(verified, "entry_mode"),
        "by_exit_source": _group_performance(verified, "exit_source"),
    }


def _risk_of_ruin_estimate(values: list[float], risk_fraction: float) -> Optional[float]:
    """Simple bounded empirical estimate; suppressed for tiny samples."""
    if len(values) < 30:
        return None
    wins = [value for value in values if value > 0]
    losses = [-value for value in values if value < 0]
    if not wins or not losses:
        return None
    p = len(wins) / len(values)
    q = 1.0 - p
    payoff = (sum(wins) / len(wins)) / (sum(losses) / len(losses))
    edge = p - q / payoff if payoff > 0 else -1.0
    if edge <= 0:
        return 1.0
    # Classical fixed-fraction approximation, intentionally labelled estimate.
    base = max(0.0, min(1.0, (1.0 - edge) / (1.0 + edge)))
    units = max(1.0, 1.0 / float(risk_fraction))
    return round(max(0.0, min(1.0, base ** units)), 8)


def compound_preview(
    store: Any, *, starting_equity_usd: float = 130.0,
    fixed_risk_fraction: float = 0.02, cohort: Optional[str] = None,
) -> dict[str, Any]:
    cohort_join = (
        "JOIN runtime_sessions rs ON rs.session_id=e.session_id "
        if cohort is not None else ""
    )
    cohort_filter = "AND rs.cohort=? " if cohort is not None else ""
    params: tuple[Any, ...] = (str(cohort),) if cohort is not None else ()
    rows = _query(
        store,
        f"""SELECT p.entry_id,p.terminal_ts_ms,p.net_pnl,e.gross_cost
           FROM pnl_records p JOIN entries e ON e.entry_id=p.entry_id
           {cohort_join}
           WHERE p.verified=1 {cohort_filter}
           ORDER BY p.terminal_ts_ms,p.entry_id""",
        params,
    )
    fixed_share_equity = float(starting_equity_usd)
    fixed_risk_equity = float(starting_equity_usd)
    peak = fixed_share_equity
    max_drawdown = 0.0
    values: list[float] = []
    series: list[dict[str, Any]] = []
    for row in rows:
        pnl = float(row["net_pnl"])
        exposure = float(row.get("gross_cost") or 0)
        fixed_share_equity += pnl
        scale = (fixed_risk_equity * float(fixed_risk_fraction) / exposure) if exposure > 0 else 0.0
        fixed_risk_equity += pnl * scale
        peak = max(peak, fixed_share_equity)
        max_drawdown = max(max_drawdown, peak - fixed_share_equity)
        values.append(pnl)
        series.append({
            "entry_id": int(row["entry_id"]),
            "terminal_ts_ms": int(row["terminal_ts_ms"]),
            "fixed_share_equity_usd": round(fixed_share_equity, 10),
            "fixed_risk_equity_usd": round(fixed_risk_equity, 10),
        })
    return {
        "label": "READ_ONLY_THEORETICAL_PREVIEW_DOES_NOT_INFLUENCE_EXECUTION",
        "cohort": cohort,
        "starting_equity_usd": float(starting_equity_usd),
        "fixed_shares": 5.0,
        "fixed_share_equity_usd": round(fixed_share_equity, 10),
        "fixed_risk_fraction": float(fixed_risk_fraction),
        "fixed_risk_equity_usd": round(fixed_risk_equity, 10),
        "max_drawdown_usd": round(max_drawdown, 10),
        "risk_of_ruin_estimate": _risk_of_ruin_estimate(values, fixed_risk_fraction),
        "risk_of_ruin_label": "EMPIRICAL_ESTIMATE_NOT_A_GUARANTEE",
        "sample_size": len(values),
        "influences_sizing": False,
        "series": series[-50:],
    }


def execution_metrics(store: Any, now_ms: int) -> dict[str, Any]:
    since = int(now_ms) - 12 * 3_600_000
    actions = {
        str(row["action"]): int(row["count"])
        for row in _query(
            store,
            """SELECT action,COUNT(*) count FROM decisions WHERE decision_ts_ms>=?
               GROUP BY action ORDER BY count DESC,action""",
            (since,),
        )
    }
    maker = _one(
        store,
        """SELECT COUNT(*) maker_wait_count,
           SUM(CASE WHEN outcome='CROSS_SPREAD' THEN 1 ELSE 0 END) maker_to_cross_count,
           SUM(CASE WHEN reason LIKE '%edge%expired%' OR reason='edge_expired' THEN 1 ELSE 0 END) edge_expired,
           SUM(CASE WHEN reason LIKE '%chase%' THEN 1 ELSE 0 END) chase_rejected,
           AVG(actual_duration_ms) average_duration_ms,
           MIN(actual_duration_ms) min_duration_ms,MAX(actual_duration_ms) max_duration_ms,
           SUM(maker_fill_assumed) assumed_maker_fills
           FROM maker_observations WHERE maker_start_ts_ms>=?""",
        (since,),
    )
    waits = int(maker.get("maker_wait_count") or 0)
    conversions = int(maker.get("maker_to_cross_count") or 0)
    return {
        "decision_actions_12h": actions,
        "immediate_cross_count": actions.get("CROSS_SPREAD", 0),
        "maker_wait_count": waits,
        "maker_to_cross_count": conversions,
        "maker_to_cross_conversion_pct": round(conversions / waits * 100, 6) if waits else None,
        "edge_expired": int(maker.get("edge_expired") or 0),
        "chase_rejected": int(maker.get("chase_rejected") or 0),
        "average_maker_duration_ms": maker.get("average_duration_ms"),
        "min_maker_duration_ms": maker.get("min_duration_ms"),
        "max_maker_duration_ms": maker.get("max_duration_ms"),
        "maker_fill_assumed_count": int(maker.get("assumed_maker_fills") or 0),
    }


def reject_taxonomy(store: Any, now_ms: int) -> dict[str, Any]:
    since = int(now_ms) - 12 * 3_600_000
    by_taxonomy: dict[str, dict[str, int]] = defaultdict(dict)
    for row in _query(
        store,
        """SELECT taxonomy,reason,COUNT(*) count FROM reject_events
           WHERE reject_ts_ms>=? GROUP BY taxonomy,reason ORDER BY count DESC""",
        (since,),
    ):
        by_taxonomy[str(row["taxonomy"])][str(row["reason"])] = int(row["count"])
    return {
        "all": dict(sorted(by_taxonomy.items())),
        "no_book": by_taxonomy.get("NO_BOOK", {}),
        "data_invalid": by_taxonomy.get("DATA_INVALID", {}),
        "economic": by_taxonomy.get("ECONOMIC", {}),
        "risk": by_taxonomy.get("RISK", {}),
    }


def acceptance_gate(
    store: Any, frequencies: dict[str, dict[str, Any]],
    performance: dict[str, Any], *, cohort: Optional[str] = None,
) -> dict[str, Any]:
    """Research acceptance gate over the supplied (cohort-scoped) performance.

    Trade counts and evidence-hygiene blockers are scoped to the cohort when
    given so legacy rows can never satisfy — or permanently poison — the
    authoritative 300-verified-terminal-trade requirement.  Window conflicts
    stay global: a structural integrity defect anywhere blocks acceptance.
    """
    cohort_join = (
        "JOIN runtime_sessions rs ON rs.session_id=e.session_id "
        if cohort is not None else ""
    )
    cohort_filter = "AND rs.cohort=? " if cohort is not None else ""
    params: tuple[Any, ...] = (str(cohort),) if cohort is not None else ()
    verified = performance["verified_terminal"]
    verified_count = int(verified["count"])
    unresolved = int(_one(
        store,
        f"""SELECT COUNT(*) count FROM entries e {cohort_join}
           WHERE e.status='UNRESOLVED_FINAL' {cohort_filter}""",
        params,
    ).get("count") or 0)
    conflicts = int(_one(
        store,
        """SELECT COUNT(*) count FROM (
           SELECT window_id FROM entries GROUP BY window_id HAVING COUNT(*)>1)""",
    ).get("count") or 0)
    duplicates = conflicts
    incomplete_evidence = int(_one(
        store,
        f"""SELECT COUNT(*) count FROM pnl_records p
           JOIN entries e ON e.entry_id=p.entry_id {cohort_join}
           WHERE (p.verified=0 OR execution_evidence_complete=0
           OR fee_evidence_complete=0 OR resolution_evidence_complete=0)
           {cohort_filter}""",
        params,
    ).get("count") or 0)
    pf = verified.get("profit_factor")
    expectancy = verified.get("expectancy")
    frequency_3h = frequencies["3h"].get("entries_per_hour")
    frequency_12h = frequencies["12h"].get("entries_per_hour")
    asset_results = performance.get("by_asset", {})
    profitable_assets = [name for name, result in asset_results.items()
                         if float(result.get("net_pnl") or 0) > 0]
    no_single_asset_all_profit = len(profitable_assets) >= 2
    blockers: list[str] = []
    if verified_count < FORWARD_TERMINAL_REQUIREMENT:
        blockers.append("fewer_than_300_verified_terminal_trades")
    if conflicts:
        blockers.append("asset_window_conflicts")
    if duplicates:
        blockers.append("duplicate_entries")
    if unresolved:
        blockers.append("unresolved_final")
    if incomplete_evidence:
        blockers.append("incomplete_fee_or_execution_evidence")
    if frequency_3h is None or frequency_12h is None:
        blockers.append("frequency_interval_not_fully_observed")
    elif min(float(frequency_3h), float(frequency_12h)) < TARGET_MIN_ENTRIES_PER_HOUR:
        blockers.append("frequency_below_19_per_hour")
    if pf is None or float(pf) <= 1:
        blockers.append("fee_net_profit_factor_not_above_1")
    if expectancy is None or float(expectancy) <= 0:
        blockers.append("fee_net_expectancy_not_positive")
    if not no_single_asset_all_profit:
        blockers.append("profit_not_diversified_across_assets")
    negative_expectancy = expectancy is not None and float(expectancy) <= 0
    frequency_reached = (
        frequency_3h is not None and frequency_12h is not None
        and min(float(frequency_3h), float(frequency_12h)) >= TARGET_MIN_ENTRIES_PER_HOUR
    )
    if frequency_reached and negative_expectancy:
        verdict = "REJECT_CANDIDATE"
    elif not blockers:
        verdict = "RESEARCH_ACCEPTANCE_GATE_PASSED_SHADOW_ONLY"
    else:
        verdict = "READY_FOR_MORE_SHADOW"
    return {
        "verdict": verdict,
        "cohort": cohort,
        "frequency_target_status": (
            "PROVEN_OVER_REQUIRED_INTERVALS" if frequency_reached
            else "FREQUENCY_TARGET_NOT_YET_PROVEN"
        ),
        "verified_terminal_trades": verified_count,
        "required_verified_terminal_trades": FORWARD_TERMINAL_REQUIREMENT,
        "conflicts": conflicts,
        "duplicates": duplicates,
        "unresolved_final": unresolved,
        "incomplete_evidence_rows": incomplete_evidence,
        "no_single_asset_all_profit": no_single_asset_all_profit,
        "blockers": blockers,
        "live_enablement_authorized": False,
    }


def build_metrics(
    store: Any, now_ms: int, *, session_id: Optional[str] = None,
    starting_equity_usd: float = 130.0, cohort: Optional[str] = None,
) -> dict[str, Any]:
    """Build the metrics payload.

    With ``cohort`` set, ``performance``, ``compound_preview``, and the
    ``acceptance_gate`` (including the 300-verified-terminal-trade count)
    include only post-activation trades of that cohort; the unfiltered
    historical aggregate is returned separately, explicitly labelled
    non-authoritative.
    """
    activation = cohort_activation_ts_ms(store, cohort) if cohort else None
    frequencies = rolling_frequency(
        store, now_ms, session_id=session_id, not_before_ts_ms=activation)
    performance = performance_metrics(store, cohort=cohort)
    acceptance = acceptance_gate(store, frequencies, performance, cohort=cohort)
    legacy: Optional[dict[str, Any]] = None
    if cohort is not None:
        legacy = {
            "label": "NON_AUTHORITATIVE_LEGACY_ALL_HISTORY",
            "performance": performance_metrics(store),
        }
    return {
        "generated_ts_ms": int(now_ms),
        "cohort": cohort,
        "cohort_activation_ts_ms": activation,
        "frequency": frequencies,
        "funnel": {
            key: frequencies["session"][key]
            for key in (
                "raw_events", "unique_source_events", "available_asset_windows",
                "eligible_windows", "unique_positive_edge_opportunities",
                "execution_attempts", "actual_entries", "terminal_trades",
                "missed_opportunities",
            )
        },
        "performance": performance,
        "legacy_non_authoritative": legacy,
        "execution": execution_metrics(store, now_ms),
        "rejects": reject_taxonomy(store, now_ms),
        "compound_preview": compound_preview(
            store, starting_equity_usd=starting_equity_usd, cohort=cohort
        ),
        "acceptance_gate": acceptance,
    }
