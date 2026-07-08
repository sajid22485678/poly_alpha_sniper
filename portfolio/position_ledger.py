"""Append-only position event ledger (audit trail, persisted by the app)."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import Position

EVENTS = ("OPEN", "INCREASE", "REDUCE", "CLOSE", "SETTLE")


class PositionLedger:
    def __init__(self):
        self._entries: list[dict] = []

    def add(self, event: str, position: Position, ts_ms: int, note: str = "") -> dict:
        if event not in EVENTS:
            raise ValueError(f"unknown ledger event {event}")
        entry = {
            "ts_ms": ts_ms, "event": event, "token_id": position.token_id,
            "market_id": position.market_id, "outcome": position.outcome.value,
            "shares": position.shares, "avg_entry_price": position.avg_entry_price,
            "realized_pnl": position.realized_pnl, "entry_ts_ms": position.entry_ts_ms,
            "tier": position.tier.value, "entry_signal_id": position.entry_signal_id,
            "exit_plan": position.exit_plan, "note": note,
        }
        self._entries.append(entry)
        return entry

    def entries(self, token_id: str | None = None) -> list[dict]:
        if token_id is None:
            return list(self._entries)
        return [e for e in self._entries if e["token_id"] == token_id]

    def to_records(self) -> list[dict]:
        return list(self._entries)
