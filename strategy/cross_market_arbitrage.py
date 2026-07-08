"""Complement-arbitrage scanner (REPORT ONLY — feeds dashboard/near-miss,
never auto-executes: this strategy trades lag, not structural arb)."""
from __future__ import annotations

from typing import Callable

from poly_alpha_sniper.core.contracts import MarketInfo
from poly_alpha_sniper.strategy.complement_check import complement_gap


def scan_complement_arbs(markets: list[MarketInfo],
                         book_lookup: Callable) -> list[dict]:
    out: list[dict] = []
    for m in markets:
        gap = complement_gap(book_lookup(m.yes_token_id), book_lookup(m.no_token_id))
        if gap["arb"]:
            out.append({"market_id": m.market_id, "title": m.title[:80], **gap,
                        "note": "REPORT ONLY — not auto-traded"})
    return out
