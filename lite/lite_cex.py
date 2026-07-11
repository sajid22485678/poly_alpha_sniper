"""LITE SHADOW ONLY: simple polled CEX prices with short momentum memory.

Public REST tickers only (Bybit spot, OKX fallback) -- no WebSockets, no
auth, no keys. A ~2-3s poll is plenty for 10/30/60s momentum under Lite's
8s freshness budget, and polling cannot silently go stale the way an
unmonitored WS can."""
from __future__ import annotations

import time
import asyncio
import math
import statistics
from collections import deque
from typing import Optional

BYBIT_URL = "https://api.bybit.com/v5/market/tickers"
OKX_URL = "https://www.okx.com/api/v5/market/ticker"
MEMORY_S = 90  # enough history for the longest momentum window


class LiteCexFeed:
    def __init__(self, assets: list[str], session_factory=None):
        self.assets = list(assets)
        self._session = None
        self._session_factory = session_factory
        self._samples: dict[str, deque] = {a: deque() for a in self.assets}
        self._source: dict[str, str] = {}

    async def _get_json(self, url: str, params: dict) -> Optional[dict]:
        if self._session is None:
            if self._session_factory is not None:
                self._session = self._session_factory()
            else:
                import aiohttp
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=4))
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception:  # noqa: BLE001 -- one failed poll is not an event
            return None

    async def poll_once(self, now_ms: Optional[int] = None) -> None:
        """Poll assets concurrently and stamp successful local receipt time.

        ``now_ms`` is accepted only to make unit tests deterministic. Runtime
        calls omit it, so a slow/fallback request can never masquerade as a
        fresh sample by inheriting the poll's start time.
        """
        async def fetch_with_receipt(asset: str):
            result = await self._fetch_price(asset)
            received_ms = (int(now_ms) if now_ms is not None
                           else int(time.time() * 1000))
            return result, received_ms

        results = await asyncio.gather(
            *(fetch_with_receipt(asset) for asset in self.assets),
            return_exceptions=True,
        )
        for asset, result in zip(self.assets, results):
            if isinstance(result, BaseException):
                continue
            (price, source), received_ms = result
            if price is not None and price > 0:
                self.record(asset, price, received_ms, source)

    async def _fetch_price(self, asset: str) -> tuple[Optional[float], str]:
        data = await self._get_json(BYBIT_URL, {"category": "spot",
                                                "symbol": f"{asset}USDT"})
        try:
            return float(data["result"]["list"][0]["lastPrice"]), "bybit"
        except (TypeError, KeyError, IndexError, ValueError):
            pass
        data = await self._get_json(OKX_URL, {"instId": f"{asset}-USDT"})
        try:
            return float(data["data"][0]["last"]), "okx"
        except (TypeError, KeyError, IndexError, ValueError):
            return None, ""

    # -- pure state (unit-testable without network) -----------------------
    def record(self, asset: str, price: float, ts_ms: int, source: str = "test") -> None:
        q = self._samples.setdefault(asset, deque())
        normalized_source = str(source or "unknown")
        # Never compare prices across venues.  A fallback/source switch starts
        # a new evidenced momentum series instead of manufacturing a return.
        if q and q[-1][2] != normalized_source:
            q.clear()
        q.append((int(ts_ms), float(price), normalized_source))
        self._source[asset] = source
        cutoff = ts_ms - MEMORY_S * 1000
        while q and q[0][0] < cutoff:
            q.popleft()

    def latest(self, asset: str, now_ms: int) -> tuple[Optional[float], Optional[int], str]:
        """-> (price, age_ms, source); (None, None, "") when no samples."""
        q = self._samples.get(asset)
        if not q:
            return None, None, ""
        ts, price, _source = q[-1]
        # Preserve a negative age so downstream validity checks can reject a
        # future-dated sample instead of silently clamping it to fresh.
        return price, int(now_ms) - int(ts), self._source.get(asset, "")

    def momentum_pct(self, asset: str, window_s: int, now_ms: int) -> Optional[float]:
        """(now - then) / then using the oldest sample at least window_s old
        but inside memory; None when history is too short -- never guessed."""
        q = self._samples.get(asset)
        if not q or len(q) < 2:
            return None
        now_ts, now_price, source = q[-1]
        if now_ts > int(now_ms):
            return None
        target = now_ms - window_s * 1000
        past = None
        past_ts = None
        for ts, price, sample_source in q:     # oldest -> newest
            if sample_source != source:
                continue
            if ts <= target:
                past = price
                past_ts = ts
            else:
                break
        tolerance_ms = max(4_000, int(window_s * 250))
        if (past is None or past <= 0 or past_ts is None
                or past_ts < target - tolerance_ms):
            return None
        return (now_price - past) / past

    def momentum_map(self, asset: str, windows_s: list[int],
                     now_ms: int) -> dict[int, Optional[float]]:
        return {
            int(window): self.momentum_pct(asset, int(window), now_ms)
            for window in windows_s
        }

    def momentum_values(self, asset: str, windows_s: list[int], now_ms: int) -> list[float]:
        """Return only evidenced window returns; insufficient history is absent."""
        values = [self.momentum_pct(asset, int(window), now_ms) for window in windows_s]
        return [value for value in values if value is not None]

    def feature_snapshot(self, asset: str, windows_s: list[int], now_ms: int) -> dict:
        q = self._samples.get(asset) or ()
        returns = self.momentum_map(asset, windows_s, now_ms)
        tick_return = None
        tick_returns: list[float] = []
        recent = [sample for sample in q if sample[0] >= int(now_ms) - 30_000]
        for previous, current in zip(recent, recent[1:]):
            if previous[2] != current[2] or previous[1] <= 0:
                continue
            value = (current[1] - previous[1]) / previous[1]
            if math.isfinite(value):
                tick_returns.append(value)
        if tick_returns:
            tick_return = tick_returns[-1]
        volatility = statistics.pstdev(tick_returns) if len(tick_returns) >= 2 else 0.0
        return {
            "returns": returns,
            "return_10s": returns.get(10),
            "return_30s": returns.get(30),
            "return_60s": returns.get(60),
            "tick_return": tick_return,
            "volatility": volatility,
            "sample_count": len(recent),
            "source": self._source.get(asset, ""),
        }

    def state(self, asset: str, now_ms: int, max_age_ms: int) -> dict:
        price, age_ms, source = self.latest(asset, now_ms)
        stale = not (price is not None and age_ms is not None
                     and 0 <= age_ms <= max_age_ms)
        return {
            "price": price,
            "age_ms": age_ms,
            "source": source,
            "status": "stale" if stale else "ok",
            "stale": stale,
        }

    async def close(self) -> None:
        if self._session is not None and not getattr(self._session, "closed", True):
            await self._session.close()
