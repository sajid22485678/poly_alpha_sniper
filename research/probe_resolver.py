"""RESEARCH / SHADOW-ONLY: official Polymarket outcome parser for probe
resolution. Pure; never places orders, never fabricates an outcome.

Evidence standard: the market row must be CLOSED and its outcomePrices must
be degenerate (winner >= 0.99, loser <= 0.01) -- the shape a resolved
Up/Down market settles to. Anything else returns (None, reason) and the
caller retries. resolution_source_url / Chainlink URLs are never consulted.
"""
from __future__ import annotations

import json
from typing import Optional


def outcome_from_market_row(row: Optional[dict]) -> tuple[Optional[str], str]:
    """-> ("YES"|"NO", "resolved") on hard evidence, else (None, why)."""
    if not isinstance(row, dict):
        return None, "no_market_row"
    if row.get("closed") is not True:
        return None, "market_not_closed_yet"
    prices_raw = row.get("outcomePrices")
    try:
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        p_yes, p_no = float(prices[0]), float(prices[1])
    except (TypeError, ValueError, IndexError, KeyError):
        return None, "outcome_prices_unparseable"
    if p_yes >= 0.99 and p_no <= 0.01:
        return "YES", "resolved"
    if p_no >= 0.99 and p_yes <= 0.01:
        return "NO", "resolved"
    return None, "outcome_prices_not_degenerate"
