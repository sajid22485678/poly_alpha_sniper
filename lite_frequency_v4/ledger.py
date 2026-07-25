"""Authoritative cohort capital-ledger arithmetic for Frequency V4.

One shared, read-only computation of the Phase 1 shadow capital state.  The
same cohort-scoped definitions the store enforces atomically at entry time
(``V4Store.create_entry``) are reproduced here for reporting, recovery
verification, and tests, from any object exposing ``query(sql, params)``.

Shadow-only: this module never places, sizes, or influences an order.  It
reads committed rows and returns arithmetic.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Callable, Iterable, Optional

from .config import ACTIVE_COHORT
from .risk import conservative_exit_fee_buffer


QueryFn = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class CapitalLedgerState:
    cohort: str
    activation_ts_ms: Optional[int]
    activation_commit: Optional[str]
    starting_equity_usd: float
    realized_net_pnl_usd: float
    current_equity_usd: float
    open_position_count: int
    open_position_cost_usd: float
    unresolved_position_count: int
    unresolved_capital_usd: float
    reserved_order_usd: float
    exit_fee_buffer_per_position_usd: float
    exit_fee_buffers_usd: float
    committed_total_usd: float
    available_cash_usd: float
    max_exposure_pct: float
    max_committed_usd: float
    exposure_pct: float
    peak_committed_usd: float
    peak_exposure_pct: float
    invariant_committed_within_equity: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _one(query: QueryFn, sql: str, params: Iterable[Any]) -> dict[str, Any]:
    rows = query(sql, tuple(params))
    if isinstance(rows, dict):
        return rows
    if rows:
        first = rows[0]
        return dict(first) if not isinstance(first, dict) else first
    return {}


def compute_capital_ledger(
    query: QueryFn, *, cohort: str = ACTIVE_COHORT,
    fee_rate: float = 0.07, fee_buffer_usd: float = 0.02,
) -> CapitalLedgerState:
    """Compute the authoritative cohort ledger from committed rows.

    ``query`` must return a list of mapping-like rows (the V4 store and its
    read workers both satisfy this).  All aggregates are scoped to the
    cohort through ``entries.session_id -> runtime_sessions.cohort``; no
    other session's rows can influence the result.
    """
    cohort_row = _one(
        query,
        """SELECT activation_ts_ms,activation_commit,starting_equity_usd,
           max_exposure_pct,peak_committed_usd,peak_exposure_pct
           FROM cohorts WHERE cohort=?""", (str(cohort),),
    )
    starting = float(cohort_row.get("starting_equity_usd") or 130.0)
    max_pct = float(cohort_row.get("max_exposure_pct") or 1.0)
    realized = float(_one(
        query,
        """SELECT COALESCE(SUM(pr.net_pnl),0) v FROM pnl_records pr
           JOIN entries e ON e.entry_id=pr.entry_id
           JOIN runtime_sessions s ON s.session_id=e.session_id
           WHERE s.cohort=?""", (str(cohort),),
    ).get("v") or 0)
    open_row = _one(
        query,
        """SELECT COUNT(*) n,COALESCE(SUM(p.committed_exposure_usd),0) v
           FROM positions p JOIN entries e ON e.entry_id=p.entry_id
           JOIN runtime_sessions s ON s.session_id=e.session_id
           WHERE p.status='OPEN' AND s.cohort=?""", (str(cohort),),
    )
    unresolved_row = _one(
        query,
        """SELECT COUNT(*) n,COALESCE(SUM(p.committed_exposure_usd),0) v
           FROM positions p JOIN entries e ON e.entry_id=p.entry_id
           JOIN runtime_sessions s ON s.session_id=e.session_id
           WHERE p.status='UNRESOLVED_FINAL' AND s.cohort=?""", (str(cohort),),
    )
    reserved = float(_one(
        query,
        """SELECT COALESCE(SUM(l.reserved_commitment_usd),0) v
           FROM window_locks l
           JOIN runtime_sessions s ON s.session_id=l.session_id
           WHERE l.state='RESERVED' AND s.cohort=?""", (str(cohort),),
    ).get("v") or 0)
    open_count = int(open_row.get("n") or 0)
    open_cost = float(open_row.get("v") or 0)
    unresolved_count = int(unresolved_row.get("n") or 0)
    unresolved_cost = float(unresolved_row.get("v") or 0)
    buffer_each = conservative_exit_fee_buffer(fee_rate, fee_buffer_usd)
    buffers = round(open_count * buffer_each, 10)
    equity = round(starting + realized, 10)
    committed = round(open_cost + unresolved_cost + reserved + buffers, 10)
    max_committed = round(equity * max_pct, 10)
    available = round(equity - committed, 10)
    return CapitalLedgerState(
        cohort=str(cohort),
        activation_ts_ms=(
            int(cohort_row["activation_ts_ms"])
            if cohort_row.get("activation_ts_ms") is not None else None),
        activation_commit=(
            str(cohort_row["activation_commit"])
            if cohort_row.get("activation_commit") else None),
        starting_equity_usd=starting,
        realized_net_pnl_usd=round(realized, 10),
        current_equity_usd=equity,
        open_position_count=open_count,
        open_position_cost_usd=round(open_cost, 10),
        unresolved_position_count=unresolved_count,
        unresolved_capital_usd=round(unresolved_cost, 10),
        reserved_order_usd=round(reserved, 10),
        exit_fee_buffer_per_position_usd=buffer_each,
        exit_fee_buffers_usd=buffers,
        committed_total_usd=committed,
        available_cash_usd=available,
        max_exposure_pct=max_pct,
        max_committed_usd=max_committed,
        exposure_pct=round(committed / equity, 10) if equity > 0 else 0.0,
        peak_committed_usd=float(cohort_row.get("peak_committed_usd") or 0),
        peak_exposure_pct=float(cohort_row.get("peak_exposure_pct") or 0),
        invariant_committed_within_equity=committed <= max_committed + 1e-9,
    )
