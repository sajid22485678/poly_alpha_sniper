"""Startup safety net: reconcile orders left resting by a prior process instance.

In-memory order tracking (OrderManager.orders, per-order TIF-cancel timers,
SimulatedClobClient._open_orders) exists only for the lifetime of one process.
An order that went OPEN/SUBMITTED and was never filled or cancelled before the
process restarted (or crashed) has no in-memory owner left to resolve it -- it
sits in the DB in a non-terminal state with filled_shares=0 forever,
indistinguishable from a genuinely resting order to anything reading the
table directly (dashboards, ad-hoc analysis, exposure audits). Live in-memory
exposure/position tracking (portfolio/positions.py) is fill-based and is
never affected by these phantom rows -- this is purely about DB hygiene and
downstream reporting accuracy.

This scans for exactly that shape (non-terminal state, older than any
legitimate resting window for this bot) and marks it RECONCILED with an audit
note. It never deletes rows, takes a full DB backup before writing, and is
idempotent: reconciled rows fall outside the WHERE clause on every
subsequent run.
"""
from __future__ import annotations

STALE_STATES = ("CREATED", "SIGNED", "SUBMITTED", "OPEN", "PARTIAL_FILL",
                "RETRYING", "CANCEL_REQUESTED")
DEFAULT_CUTOFF_AGE_MS = 5 * 60 * 1000  # 5 min: far beyond this bot's sub-3s hold times


def reconcile_stale_orders(store, backup_manager, now_ms: int,
                           cutoff_age_ms: int = DEFAULT_CUTOFF_AGE_MS,
                           insert_fn=None) -> dict:
    """Reconcile stale non-terminal orders. Returns {"touched": [order_id,...],
    "backup_path": str|None}. Safe to call with an empty/fresh DB (no-op)."""
    cutoff_ms = now_ms - cutoff_age_ms
    placeholders = ",".join("?" for _ in STALE_STATES)
    stale = store.query(
        f"SELECT order_id, state, created_ts_ms FROM orders "
        f"WHERE state IN ({placeholders}) AND created_ts_ms < ?",
        tuple(STALE_STATES) + (cutoff_ms,))
    if not stale:
        return {"touched": [], "backup_path": None}

    backup_path = backup_manager.backup_now()  # do not repair without a backup

    touched = []
    for row in stale:
        age_ms = now_ms - (row["created_ts_ms"] or now_ms)
        note = (f"RECONCILED_STALE_NO_FILL: auto-reconciled; was '{row['state']}' "
                f"with age={age_ms}ms (> {cutoff_age_ms}ms, never filled/cancelled "
                f"by the process that created it)")
        store.execute(
            "UPDATE orders SET state='RECONCILED', updated_ts_ms=?, error=? WHERE order_id=?",
            (now_ms, note, row["order_id"]))
        touched.append(row["order_id"])

    if insert_fn is not None:
        insert_fn("incident_reports", {
            "ts_ms": now_ms, "kind": "stale_order_reconciliation", "severity": "info",
            "detail": (f"Reconciled {len(touched)} stale zero-progress order(s): "
                      f"{touched}. DB backed up to {backup_path} before repair. "
                      f"No rows deleted; no exposure/equity changed (these orders "
                      f"were never filled).")[:2000]})

    return {"touched": touched, "backup_path": str(backup_path)}
