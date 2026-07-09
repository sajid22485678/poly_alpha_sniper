"""Terminal reconciliation for a position whose exit path is permanently
stuck -- e.g. the market expired/resolved and Polymarket's CLOB no longer
serves an orderbook for the token, so normal exit logic (which needs a book
to construct a sell request) can never succeed and would otherwise retry
forever.

Conservative by design: this module makes NO live network calls and does
NOT verify the market's actual resolution outcome. It settles the position
as if it lost (payout = $0/share) -- i.e. the cost basis is treated as
fully at risk / unrecovered -- because that is the conservative assumption
(never assume a recovery value you cannot verify). This is NEVER a
substitute for a confirmed resolution: the note field says so explicitly,
and the reason string is a distinct, unmistakable
"RECONCILED_EXIT_NO_BOOK_TERMINAL" so it can never be confused with a
genuine STOP_LOSS/TAKE_PROFIT exit in reports or dashboards.

Places/cancels no order. Pure DB + in-memory-portfolio bookkeeping only.
"""
from __future__ import annotations

from typing import Optional

DEFAULT_STUCK_EXIT_CUTOFF_MS = 5 * 60 * 1000  # 5 min, matches storage/order_reconciliation.py

RECONCILED_EXIT_NO_BOOK_TERMINAL = "RECONCILED_EXIT_NO_BOOK_TERMINAL"


def reconcile_unexitable_position(portfolio, pos, reason: str, now_ms: int,
                                  insert_fn) -> Optional[dict]:
    """Conservatively terminate a position that could not be exited.
    Returns the exit record dict written, or None if the position was
    already gone (race with a normal fill/prior reconciliation -- a no-op,
    not an error)."""
    realized = portfolio.settle_resolution(pos.token_id, won=False, ts_ms=now_ms)
    if realized is None:
        return None

    record = {
        "ts_ms": now_ms,
        "token_id": pos.token_id,
        "market_id": pos.market_id,
        "reason": RECONCILED_EXIT_NO_BOOK_TERMINAL,
        "price": 0.0,
        "shares": pos.shares,
        "pnl_usd": realized,
        "hold_seconds": max(0.0, (now_ms - pos.entry_ts_ms) / 1000.0),
        "detail": (
            f"CONSERVATIVE reconciliation, NOT a confirmed market resolution. {reason}. "
            f"Treated as a total loss of cost basis (${abs(realized):.4f}) because the "
            f"orderbook was unavailable/unusable and the actual outcome could not be "
            f"verified read-only. Requires human review."
        ),
    }
    insert_fn("exits", record)
    insert_fn("incident_reports", {
        "ts_ms": now_ms,
        "kind": "position_reconciled_no_book",
        "severity": "critical",
        "detail": (
            f"Position {pos.token_id} (market {pos.market_id}) could not be exited: "
            f"{reason}. Conservatively reconciled as a full loss of cost basis "
            f"(${abs(realized):.4f}). Panic remains active pending human review. "
            f"This does NOT confirm the market's actual resolution."
        )[:2000],
    })
    return record
