"""LITE SHADOW ONLY: persist hypothetical five-share entries.

The broker has no exchange client and exposes no place/cancel-order operation.
Its entire effect is one insert into the dedicated Lite store.
"""
from __future__ import annotations

from typing import Optional

from .lite_strategy import LiteDecision


STRATEGY_NAME = "lite_momentum_v1"


class LiteBroker:
    def __init__(self, store):
        self.store = store

    def open_trade(self, market, decision: LiteDecision, now_ms: int, *,
                   cex_source: str = "", cex_entry_price: Optional[float] = None,
                   strategy_name: str = STRATEGY_NAME) -> dict:
        """Insert one simulated OPEN trade and return its persisted row."""
        if not decision.accepted:
            raise ValueError("cannot open a rejected Lite decision")
        if decision.side not in ("BUY_YES", "BUY_NO"):
            raise ValueError("Lite entry side must be BUY_YES or BUY_NO")
        expected_token = (market.yes_token_id if decision.side == "BUY_YES"
                          else market.no_token_id)
        if not expected_token or str(decision.token_id) != str(expected_token):
            raise ValueError("Lite decision token does not match the direct outcome token")
        try:
            entry_price = float(decision.entry_price)
        except (TypeError, ValueError) as exc:
            raise ValueError("Lite decision has no executable entry price") from exc
        if not (0.0 < entry_price < 1.0):
            raise ValueError("Lite entry price must be between zero and one")

        # Never trust a caller-supplied size/cost: the isolated broker itself
        # reasserts the only sizing rule Lite has.
        shares = 5.0
        entry_cost = shares * entry_price
        anchor_available = bool(getattr(market, "anchor_available", False))
        row = {
            "asset": str(market.asset),
            "market_id": str(market.market_id),
            "event_id": str(getattr(market, "event_id", "") or ""),
            "slug": str(market.slug),
            "condition_id": str(getattr(market, "condition_id", "") or ""),
            "yes_token_id": str(market.yes_token_id),
            "no_token_id": str(market.no_token_id),
            "side": decision.side,
            "shares": shares,
            "entry_price": entry_price,
            "entry_cost": entry_cost,
            "entry_ts": int(now_ms),
            "window_close_ts": int(market.window_close_s * 1000),
            "status": "OPEN",
            "exit_price": None,
            "exit_ts": None,
            "pnl": None,
            "resolution_source": "",
            "resolution_reason": "",
            "retry_count": 0,
            "anchor_available": anchor_available,
            "price_to_beat": getattr(market, "price_to_beat", None),
            "no_anchor_trade": not anchor_available,
            "cex_source": str(cex_source or ""),
            "cex_entry_price": (float(cex_entry_price)
                                if cex_entry_price is not None else None),
            "momentum_pct": decision.momentum_pct,
            "strategy_name": str(strategy_name),
        }
        inserted = self.store.insert_trade(row)
        if isinstance(inserted, dict):
            return inserted
        if inserted is not None:
            row["id"] = inserted
        return row

    # Convenient naming for orchestrators; still performs only insert_trade.
    submit = open_trade
