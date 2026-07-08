"""In-memory orderbook store per token with staleness tracking."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import OrderbookSnapshot


class OrderbookStore:
    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self._books: dict[str, OrderbookSnapshot] = {}
        self._prev: dict[str, OrderbookSnapshot] = {}

    def update_snapshot(self, snap: OrderbookSnapshot) -> None:
        if snap.token_id in self._books:
            self._prev[snap.token_id] = self._books[snap.token_id]
        if snap.best_bid is not None and snap.best_ask is not None:
            snap.crossed = snap.best_bid > snap.best_ask
        self._books[snap.token_id] = snap

    def get(self, token_id: str) -> Optional[OrderbookSnapshot]:
        return self._books.get(token_id)

    def previous(self, token_id: str) -> Optional[OrderbookSnapshot]:
        return self._prev.get(token_id)

    def is_fresh(self, token_id: str) -> bool:
        book = self._books.get(token_id)
        if book is None:
            return False
        return not book.is_stale(self.clock.now_ms(),
                                 self.cfg.polymarket.max_orderbook_staleness_ms)

    def tracked_tokens(self) -> list[str]:
        return list(self._books.keys())

    def drop(self, token_id: str) -> None:
        self._books.pop(token_id, None)
        self._prev.pop(token_id, None)
