"""LITE SHADOW ONLY: public, direct-token Polymarket order books.

This module knows only the unauthenticated CLOB ``/book`` endpoint.  It has
no order methods and never derives one outcome's price from the other: callers
must request the actual YES/Up or NO/Down token they intend to simulate.

The CLOB source timestamp and local receipt timestamp are deliberately kept
separate.  Freshness is based on the source timestamp so an old upstream book
cannot be made fresh merely by downloading it again.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from math import isfinite
from typing import Iterable, Optional


PUBLIC_CLOB_BASE_URL = "https://clob.polymarket.com"
LITE_EXECUTABLE_SHARES = 5.0


BookLevel = tuple[float, float]


@dataclass(frozen=True)
class LiteBookSweep:
    """An all-or-none simulated sweep of executable book levels."""

    side: str
    shares: float
    notional: float
    vwap: float
    worst_price: float
    levels: tuple[BookLevel, ...]


@dataclass(frozen=True)
class LiteBookQuote:
    # The original six fields remain first for compatibility with existing
    # tests and callers that construct small quotes positionally.
    token_id: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    bid_depth_usd: float
    ask_depth_usd: float
    ts_ms: int
    bids: tuple[BookLevel, ...] = ()
    asks: tuple[BookLevel, ...] = ()
    market: str = ""
    source_ts_ms: Optional[int] = None
    book_hash: str = ""
    min_order_size: Optional[float] = None
    hash_reused: bool = False

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def received_ts_ms(self) -> int:
        """Local receipt timestamp (the legacy ``ts_ms`` field)."""
        return int(self.ts_ms)

    @property
    def condition_id(self) -> str:
        """The CLOB response's ``market`` field is the condition id."""
        return self.market

    @property
    def asset_id(self) -> str:
        return self.token_id

    @property
    def total_bid_shares(self) -> float:
        return sum(size for _, size in self.bids)

    @property
    def total_ask_shares(self) -> float:
        return sum(size for _, size in self.asks)

    def effective_ts_ms(self) -> int:
        """Use exchange provenance for age, falling back for legacy quotes."""
        if self.source_ts_ms is not None:
            return int(self.source_ts_ms)
        return int(self.ts_ms)

    def age_ms(self, now_ms: int) -> int:
        return max(0, int(now_ms) - self.effective_ts_ms())

    def receipt_age_ms(self, now_ms: int) -> int:
        return max(0, int(now_ms) - int(self.ts_ms))

    def is_future(self, now_ms: int) -> bool:
        return self.effective_ts_ms() > int(now_ms)

    def is_stale(self, now_ms: int, max_age_ms: int) -> bool:
        return self.is_future(now_ms) or self.age_ms(now_ms) > int(max_age_ms)

    def buy_sweep(self, shares: float = LITE_EXECUTABLE_SHARES) -> Optional[LiteBookSweep]:
        """Sweep direct-token asks; return ``None`` unless all shares fill."""
        return executable_buy_sweep(self, shares)

    def sell_sweep(self, shares: float = LITE_EXECUTABLE_SHARES) -> Optional[LiteBookSweep]:
        """Sweep owned-token bids; return ``None`` unless all shares fill."""
        return executable_sell_sweep(self, shares)

    def buy_vwap(self, shares: float = LITE_EXECUTABLE_SHARES) -> Optional[float]:
        sweep = self.buy_sweep(shares)
        return sweep.vwap if sweep is not None else None

    def sell_vwap(self, shares: float = LITE_EXECUTABLE_SHARES) -> Optional[float]:
        sweep = self.sell_sweep(shares)
        return sweep.vwap if sweep is not None else None


def _levels(raw, *, bids: bool) -> list[BookLevel]:
    """Return every valid ``(price, shares)`` level, best price first."""
    out: list[BookLevel] = []
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


def _source_timestamp_ms(value) -> Optional[int]:
    """Parse the official integer millisecond timestamp, failing closed."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        text = str(value).strip()
        if not text or any(character not in "0123456789" for character in text):
            return None
        parsed = int(text)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def _optional_positive_float(value) -> tuple[Optional[float], bool]:
    """Return ``(parsed, valid)``; absent values are valid and stay ``None``."""
    if value is None or value == "":
        return None, True
    if isinstance(value, bool):
        return None, False
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None, False
    if not isfinite(parsed) or parsed <= 0.0:
        return None, False
    return parsed, True


def normalize_book(data: dict, token_id: str, ts_ms: int,
                   depth_levels: int = 3) -> Optional[LiteBookQuote]:
    """Validate and normalize one official public CLOB ``/book`` response.

    The response is rejected when it cannot prove that it belongs to the
    requested token, or when its source timestamp is missing, malformed, or
    later than the local receipt timestamp.
    """
    payload = data.get("data") if isinstance(data, dict) and isinstance(data.get("data"), dict) else data
    if not isinstance(payload, dict):
        return None

    requested_token = str(token_id or "")
    returned_tokens = [
        str(payload.get(name))
        for name in ("asset_id", "token_id", "token", "asset")
        if payload.get(name) is not None and str(payload.get(name)) != ""
    ]
    if (not requested_token or not returned_tokens
            or any(returned != requested_token for returned in returned_tokens)):
        return None
    returned_token = returned_tokens[0]

    try:
        received_ms = int(ts_ms)
    except (TypeError, ValueError, OverflowError):
        return None
    if received_ms <= 0:
        return None
    source_ms = _source_timestamp_ms(payload.get("timestamp"))
    if source_ms is None or source_ms > received_ms:
        return None

    min_order_size, min_order_valid = _optional_positive_float(
        payload.get("min_order_size"))
    if not min_order_valid or min_order_size is None:
        return None

    bids = _levels(payload.get("bids"), bids=True)
    asks = _levels(payload.get("asks"), bids=False)
    n = max(1, int(depth_levels))
    return LiteBookQuote(
        token_id=returned_token,
        best_bid=bids[0][0] if bids else None,
        best_ask=asks[0][0] if asks else None,
        bid_depth_usd=sum(price * size for price, size in bids[:n]),
        ask_depth_usd=sum(price * size for price, size in asks[:n]),
        ts_ms=received_ms,
        bids=tuple(bids),
        asks=tuple(asks),
        market=str(payload.get("market") or payload.get("condition_id") or ""),
        source_ts_ms=source_ms,
        book_hash=str(payload.get("hash") or ""),
        min_order_size=min_order_size,
    )


# A descriptive alias for tests/callers that prefer parser terminology.
parse_book = normalize_book


def sweep_levels(levels: Iterable[BookLevel], shares: float, *,
                 side: str) -> Optional[LiteBookSweep]:
    """All-or-none VWAP across already price-priority-sorted levels."""
    try:
        requested = float(shares)
    except (TypeError, ValueError, OverflowError):
        return None
    if not isfinite(requested) or requested <= 0.0:
        return None

    remaining = requested
    notional = 0.0
    fills: list[BookLevel] = []
    for raw_price, raw_size in levels:
        try:
            price, size = float(raw_price), float(raw_size)
        except (TypeError, ValueError, OverflowError):
            continue
        if not (isfinite(price) and isfinite(size)) or not (0.0 <= price <= 1.0) or size <= 0.0:
            continue
        filled = min(remaining, size)
        if filled <= 0.0:
            continue
        fills.append((price, filled))
        notional += price * filled
        remaining -= filled
        if remaining <= 1e-12:
            break
    if remaining > 1e-12 or not fills:
        return None
    return LiteBookSweep(
        side=str(side),
        shares=requested,
        notional=notional,
        vwap=notional / requested,
        worst_price=fills[-1][0],
        levels=tuple(fills),
    )


def _meets_minimum(quote: LiteBookQuote, shares: float) -> bool:
    try:
        requested = float(shares)
        minimum = (float(quote.min_order_size)
                   if quote.min_order_size is not None else None)
    except (TypeError, ValueError, OverflowError):
        return False
    if not isfinite(requested) or requested <= 0.0:
        return False
    return minimum is None or requested + 1e-12 >= minimum


def executable_buy_sweep(quote: LiteBookQuote,
                         shares: float = LITE_EXECUTABLE_SHARES) -> Optional[LiteBookSweep]:
    """Return the executable ask sweep for the direct token, never a proxy."""
    if quote is None or not _meets_minimum(quote, shares):
        return None
    return sweep_levels(quote.asks, shares, side="BUY")


def executable_sell_sweep(quote: LiteBookQuote,
                          shares: float = LITE_EXECUTABLE_SHARES) -> Optional[LiteBookSweep]:
    """Return the executable owned-token bid sweep, never midpoint/last."""
    if quote is None or not _meets_minimum(quote, shares):
        return None
    return sweep_levels(quote.bids, shares, side="SELL")


def executable_buy_vwap(quote: LiteBookQuote,
                        shares: float = LITE_EXECUTABLE_SHARES) -> Optional[float]:
    sweep = executable_buy_sweep(quote, shares)
    return sweep.vwap if sweep is not None else None


def executable_sell_vwap(quote: LiteBookQuote,
                         shares: float = LITE_EXECUTABLE_SHARES) -> Optional[float]:
    sweep = executable_sell_sweep(quote, shares)
    return sweep.vwap if sweep is not None else None


def five_share_buy_sweep(quote: LiteBookQuote) -> Optional[LiteBookSweep]:
    return executable_buy_sweep(quote, LITE_EXECUTABLE_SHARES)


def five_share_sell_sweep(quote: LiteBookQuote) -> Optional[LiteBookSweep]:
    return executable_sell_sweep(quote, LITE_EXECUTABLE_SHARES)


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
        self._last_hash_by_token: dict[str, str] = {}

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
        """Fetch one token's direct book; network/validation failures return None."""
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
        quote = normalize_book(data, token, received_ms, self.depth_levels)
        if quote is None:
            return None
        if quote.book_hash:
            previous = self._last_hash_by_token.get(token)
            quote = replace(quote, hash_reused=previous == quote.book_hash)
            self._last_hash_by_token[token] = quote.book_hash
        return quote

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
