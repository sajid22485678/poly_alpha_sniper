"""Track resolutions/settlement of markets we hold positions in."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import MarketInfo, Outcome, Position


class SettlementTracker:
    def __init__(self, clock):
        self.clock = clock
        self._tracked: dict[str, dict] = {}  # market_id -> {market, positions}

    def track(self, market: MarketInfo, position: Position) -> None:
        entry = self._tracked.setdefault(market.market_id,
                                         {"market": market, "positions": {}})
        entry["positions"][position.token_id] = position

    def untrack_token(self, token_id: str) -> None:
        for entry in self._tracked.values():
            entry["positions"].pop(token_id, None)

    def pending(self) -> list[dict]:
        return [{"market_id": mid, "expiry_ts_ms": e["market"].expiry_ts_ms,
                 "tokens": list(e["positions"])}
                for mid, e in self._tracked.items() if e["positions"]]

    def on_resolution(self, market_id: str, winning_outcome: Outcome) -> list[dict]:
        entry = self._tracked.pop(market_id, None)
        if entry is None:
            return []
        events = []
        market: MarketInfo = entry["market"]
        for token_id, pos in entry["positions"].items():
            won = (token_id == market.token_for(winning_outcome))
            events.append({
                "ts_ms": self.clock.now_ms(), "market_id": market_id,
                "token_id": token_id, "outcome": winning_outcome.value,
                "shares": pos.shares, "payout_usd": pos.shares * (1.0 if won else 0.0),
                "status": "RESOLVED_WIN" if won else "RESOLVED_LOSS"})
        return events
