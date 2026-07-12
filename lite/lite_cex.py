"""LITE SHADOW ONLY: public CEX prices with point-in-time feature memory.

Provider and local receipt timestamps are retained separately.  Features use
only samples that were both published and received by the evaluation time;
venue changes and out-of-order observations never manufacture returns.
"""
from __future__ import annotations

import asyncio
import math
import statistics
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

BYBIT_URL = "https://api.bybit.com/v5/market/tickers"
OKX_URL = "https://www.okx.com/api/v5/market/ticker"
MEMORY_S = 360  # covers a five-minute market plus polling tolerance
WINDOW_REFERENCE_TOLERANCE_MS = 8_000


@dataclass(frozen=True)
class _CexSample:
    provider_ts_ms: int
    receipt_ts_ms: int
    price: float
    source: str


def _positive_price(value) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0.0 else None


def _provider_timestamp_ms(value) -> Optional[int]:
    if isinstance(value, bool) or value is None:
        return None
    text = str(value).strip()
    if not text or any(character not in "0123456789" for character in text):
        return None
    try:
        parsed = int(text)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


class LiteCexFeed:
    def __init__(self, assets: list[str], session_factory=None):
        self.assets = list(assets)
        self._session = None
        self._session_factory = session_factory
        self._samples: dict[str, deque[_CexSample]] = {
            asset: deque() for asset in self.assets
        }
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
        """Poll concurrently and bind exchange time to the actual receipt.

        ``now_ms`` is test-only.  Runtime calls timestamp each completed
        request, so a slow fallback cannot inherit the poll start time.
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
            fetched, received_ms = result
            try:
                if len(fetched) == 3:
                    price, source, provider_ts_ms = fetched
                else:  # compatibility with a test/custom legacy fetcher
                    price, source = fetched
                    provider_ts_ms = received_ms
            except (TypeError, ValueError):
                continue
            if provider_ts_ms is not None:
                self.record(
                    asset, price, provider_ts_ms, source,
                    receipt_ts_ms=received_ms,
                )

    async def _fetch_price(
            self, asset: str) -> tuple[Optional[float], str, Optional[int]]:
        data = await self._get_json(BYBIT_URL, {
            "category": "spot", "symbol": f"{asset}USDT",
        })
        try:
            price = _positive_price(data["result"]["list"][0]["lastPrice"])
            provider_ts_ms = _provider_timestamp_ms(data["time"])
            if price is not None and provider_ts_ms is not None:
                return price, "bybit", provider_ts_ms
        except (TypeError, KeyError, IndexError):
            pass

        data = await self._get_json(OKX_URL, {"instId": f"{asset}-USDT"})
        try:
            row = data["data"][0]
            price = _positive_price(row["last"])
            provider_ts_ms = _provider_timestamp_ms(row["ts"])
            if price is not None and provider_ts_ms is not None:
                return price, "okx", provider_ts_ms
        except (TypeError, KeyError, IndexError):
            pass
        return None, "", None

    # -- pure state (unit-testable without network) -----------------------
    def record(self, asset: str, price: float, ts_ms: int,
               source: str = "test", receipt_ts_ms: Optional[int] = None) -> bool:
        """Record one observation; legacy ``ts_ms`` means both timestamps."""
        parsed_price = _positive_price(price)
        try:
            provider_ms = int(ts_ms)
            receipt_ms = (provider_ms if receipt_ts_ms is None
                          else int(receipt_ts_ms))
        except (TypeError, ValueError, OverflowError):
            return False
        if parsed_price is None or provider_ms < 0 or receipt_ms < 0:
            return False
        # An exchange observation cannot have arrived before it existed.
        if provider_ms > receipt_ms:
            return False

        q = self._samples.setdefault(asset, deque())
        normalized_source = str(source or "unknown")
        if q and q[-1].source != normalized_source:
            # Never compare prices across venues.  A fallback/source switch
            # starts a new evidenced series.
            q.clear()
        elif q and (provider_ms <= q[-1].provider_ts_ms
                    or receipt_ms < q[-1].receipt_ts_ms):
            # Replayed/late same-venue ticks cannot rewrite the feature path.
            return False

        q.append(_CexSample(
            provider_ts_ms=provider_ms,
            receipt_ts_ms=receipt_ms,
            price=parsed_price,
            source=normalized_source,
        ))
        self._source[asset] = normalized_source
        cutoff = provider_ms - MEMORY_S * 1000
        while q and q[0].provider_ts_ms < cutoff:
            q.popleft()
        return True

    def _point_in_time_samples(self, asset: str, now_ms: int) -> list[_CexSample]:
        q = self._samples.get(asset)
        if not q:
            return []
        now = int(now_ms)
        current_source = q[-1].source
        return [
            sample for sample in q
            if sample.source == current_source
            and sample.provider_ts_ms <= now
            and sample.receipt_ts_ms <= now
        ]

    def latest(self, asset: str,
               now_ms: int) -> tuple[Optional[float], Optional[int], str]:
        """Return legacy ``(price, provider_age_ms, source)`` shape."""
        q = self._samples.get(asset)
        if not q:
            return None, None, ""
        sample = q[-1]
        # Keep negative age observable so state() rejects future data.
        return (sample.price, int(now_ms) - sample.provider_ts_ms,
                sample.source)

    def momentum_pct(self, asset: str, window_s: int,
                     now_ms: int) -> Optional[float]:
        """Return an evidenced point-in-time window return, never a guess."""
        samples = self._point_in_time_samples(asset, now_ms)
        if len(samples) < 2:
            return None
        latest = samples[-1]
        target = int(now_ms) - int(window_s) * 1000
        past = None
        for sample in samples:  # oldest -> newest
            if sample.provider_ts_ms <= target:
                past = sample
            else:
                break
        tolerance_ms = max(4_000, int(window_s) * 250)
        if (past is None or past.price <= 0.0
                or past.provider_ts_ms < target - tolerance_ms):
            return None
        return (latest.price - past.price) / past.price

    def momentum_map(self, asset: str, windows_s: list[int],
                     now_ms: int) -> dict[int, Optional[float]]:
        return {
            int(window): self.momentum_pct(asset, int(window), now_ms)
            for window in windows_s
        }

    def momentum_values(self, asset: str, windows_s: list[int],
                        now_ms: int) -> list[float]:
        """Return only evidenced window returns; missing history is absent."""
        values = [self.momentum_pct(asset, int(window), now_ms)
                  for window in windows_s]
        return [value for value in values if value is not None]

    def feature_snapshot(self, asset: str, windows_s: list[int], now_ms: int,
                         window_start_ms: Optional[int] = None) -> dict:
        samples = self._point_in_time_samples(asset, now_ms)
        returns = self.momentum_map(asset, windows_s, now_ms)
        recent = [sample for sample in samples
                  if sample.provider_ts_ms >= int(now_ms) - 30_000]

        tick_returns: list[float] = []
        latest_move_ts_ms = None
        latest_move_receipt_ts_ms = None
        for previous, current in zip(recent, recent[1:]):
            value = (current.price - previous.price) / previous.price
            if math.isfinite(value):
                tick_returns.append(value)
                if current.price != previous.price:
                    latest_move_ts_ms = current.provider_ts_ms
                    latest_move_receipt_ts_ms = current.receipt_ts_ms

        tick_return = tick_returns[-1] if tick_returns else None
        tick_direction = (None if tick_return is None else
                          (1 if tick_return > 0.0 else
                           (-1 if tick_return < 0.0 else 0)))
        acceleration = (tick_returns[-1] - tick_returns[-2]
                        if len(tick_returns) >= 2 else None)
        deceleration = None
        if len(tick_returns) >= 2:
            previous, current = tick_returns[-2], tick_returns[-1]
            same_direction = (previous == 0.0 or current == 0.0
                              or (previous > 0.0) == (current > 0.0))
            deceleration = (max(0.0, abs(previous) - abs(current))
                            if same_direction else 0.0)
        volatility = (statistics.pstdev(tick_returns)
                      if len(tick_returns) >= 2 else 0.0)

        latest = samples[-1] if samples else None
        reference = None
        if (window_start_ms is not None
                and int(window_start_ms) <= int(now_ms)):
            boundary = int(window_start_ms)
            for sample in samples:
                if sample.provider_ts_ms <= boundary:
                    reference = sample
                else:
                    break
            if (reference is not None
                    and boundary - reference.provider_ts_ms
                    > WINDOW_REFERENCE_TOLERANCE_MS):
                reference = None
        window_return = None
        if latest is not None and reference is not None and reference.price > 0.0:
            window_return = (latest.price - reference.price) / reference.price

        return {
            "returns": returns,
            "return_5s": returns.get(5),
            "return_10s": returns.get(10),
            "return_30s": returns.get(30),
            "return_60s": returns.get(60),
            "tick_return": tick_return,
            "tick_direction": tick_direction,
            "acceleration": acceleration,
            "deceleration": deceleration,
            "volatility": volatility,
            "sample_count": len(recent),
            "source": latest.source if latest is not None else "",
            "latest_price": latest.price if latest is not None else None,
            "provider_ts_ms": (latest.provider_ts_ms
                               if latest is not None else None),
            "receipt_ts_ms": (latest.receipt_ts_ms
                              if latest is not None else None),
            "provider_age_ms": (int(now_ms) - latest.provider_ts_ms
                                if latest is not None else None),
            "receipt_age_ms": (int(now_ms) - latest.receipt_ts_ms
                               if latest is not None else None),
            "latest_move_ts_ms": latest_move_ts_ms,
            "latest_move_receipt_ts_ms": latest_move_receipt_ts_ms,
            "window_reference_price": (reference.price
                                       if reference is not None else None),
            "window_reference_ts_ms": (reference.provider_ts_ms
                                       if reference is not None else None),
            "window_reference_receipt_ts_ms": (
                reference.receipt_ts_ms if reference is not None else None),
            "window_return": window_return,
            "window_return_pct": window_return,
        }

    def state(self, asset: str, now_ms: int, max_age_ms: int) -> dict:
        price, age_ms, source = self.latest(asset, now_ms)
        q = self._samples.get(asset)
        sample = q[-1] if q else None
        receipt_age_ms = (int(now_ms) - sample.receipt_ts_ms
                          if sample is not None else None)
        stale = not (
            price is not None
            and age_ms is not None
            and receipt_age_ms is not None
            and 0 <= age_ms <= int(max_age_ms)
            and 0 <= receipt_age_ms <= int(max_age_ms)
        )
        return {
            "price": price,
            "age_ms": age_ms,
            "provider_age_ms": age_ms,
            "receipt_age_ms": receipt_age_ms,
            "provider_ts_ms": (sample.provider_ts_ms
                               if sample is not None else None),
            "receipt_ts_ms": (sample.receipt_ts_ms
                              if sample is not None else None),
            "source": source,
            "status": "stale" if stale else "ok",
            "stale": stale,
        }

    async def close(self) -> None:
        if self._session is not None and not getattr(self._session, "closed", True):
            await self._session.close()
