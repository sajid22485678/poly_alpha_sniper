"""LITE SHADOW ONLY: public, direct-token Polymarket order books.

This module knows only the unauthenticated CLOB ``/book`` endpoint.  It has
no order methods and never derives one outcome's price from the other: callers
must request the actual YES/Up or NO/Down token they intend to simulate.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from math import isfinite
from typing import Optional


PUBLIC_CLOB_BASE_URL = "https://clob.polymarket.com"


@dataclass(frozen=True)
class LiteBookQuote:
    token_id: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    bid_depth_usd: float
    ask_depth_usd: float
    ts_ms: int

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    def age_ms(self, now_ms: int) -> int:
        return max(0, int(now_ms) - int(self.ts_ms))

    def is_stale(self, now_ms: int, max_age_ms: int) -> bool:
        return self.age_ms(now_ms) > int(max_age_ms)


def _levels(raw, *, bids: bool) -> list[tuple[float, float]]:
    """Return valid ``(price, shares)`` levels, best price first."""
    out: list[tuple[float, float]] = []
    for level in raw or []:
        try:
            if isinstance(level, dict):
                price = float(level["price"])
                size = float(level["size"])
            else:
                price = float(level[0])
                size = float(level[1])
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        if not (isfinite(price) and isfinite(size)):
            continue
        if not (0.0 <= price <= 1.0) or size <= 0.0:
            continue
        out.append((price, size))
    out.sort(key=lambda item: item[0], reverse=bids)
    return out


def normalize_book(data: dict, token_id: str, ts_ms: int,
                   depth_levels: int = 3) -> LiteBookQuote:
    """Normalize a public CLOB book response without complement pricing."""
    payload = data.get("data") if isinstance(data, dict) and isinstance(data.get("data"), dict) else data
    payload = payload if isinstance(payload, dict) else {}
    bids = _levels(payload.get("bids"), bids=True)
    asks = _levels(payload.get("asks"), bids=False)
    n = max(1, int(depth_levels))
    return LiteBookQuote(
        token_id=str(token_id),
        best_bid=bids[0][0] if bids else None,
        best_ask=asks[0][0] if asks else None,
        bid_depth_usd=sum(price * size for price, size in bids[:n]),
        ask_depth_usd=sum(price * size for price, size in asks[:n]),
        ts_ms=int(ts_ms),
    )


# A descriptive alias for tests/callers that prefer parser terminology.
parse_book = normalize_book


class LiteBookClient:
    """Minimal aiohttp client for the public CLOB book endpoint only."""

    normalize = staticmethod(normalize_book)

    def __init__(self, base_url: str = PUBLIC_CLOB_BASE_URL, session_factory=None,
                 timeout_s: float = 4.0, depth_levels: int = 3):
        self.base_url = str(base_url).rstrip("/")
        self._session_factory = session_factory
        self._session = None
        self.timeout_s = float(timeout_s)
        self.depth_levels = int(depth_levels)

    async def _get_session(self):
        closed = bool(getattr(self._session, "closed", False)) if self._session is not None else True
        if self._session is None or closed:
            if self._session_factory is not None:
                self._session = self._session_factory()
            else:
                import aiohttp
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=self.timeout_s),
                    headers={"Accept": "application/json"},
                )
        return self._session

    async def get_book(self, token_id: str,
                       now_ms: Optional[int] = None) -> Optional[LiteBookQuote]:
        """Fetch one token's direct book; network/parse failures return None."""
        token = str(token_id or "")
        if not token:
            return None
        try:
            session = await self._get_session()
            async with session.get(
                    f"{self.base_url}/book", params={"token_id": token}) as response:
                if int(response.status) != 200:
                    return None
                data = await response.json()
        except Exception:  # noqa: BLE001 -- a failed public read must not stop Lite
            return None
        received_ms = int(now_ms) if now_ms is not None else int(time.time() * 1000)
        return normalize_book(data, token, received_ms, self.depth_levels)

    async def get_books(self, token_ids: list[str],
                        now_ms: Optional[int] = None) -> dict[str, Optional[LiteBookQuote]]:
        """Fetch several direct token books concurrently."""
        tokens = [str(token) for token in token_ids if token]
        quotes = await asyncio.gather(
            *(self.get_book(token, now_ms=now_ms) for token in tokens),
            return_exceptions=True,
        )
        return {
            token: (quote if isinstance(quote, LiteBookQuote) else None)
            for token, quote in zip(tokens, quotes)
        }

    async def close(self) -> None:
        session = self._session
        self._session = None
        if session is not None and not bool(getattr(session, "closed", False)):
            try:
                await session.close()
            except Exception:  # noqa: BLE001 -- shutdown remains best effort
                pass
