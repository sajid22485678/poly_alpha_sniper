"""Unauthenticated GET-only REST hydration and bounded book recovery."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import time
from typing import Any, Awaitable, Callable, Optional

from .books import BookNormalization, NoBookReason, normalize_book
from .config import CLOB_BASE_URL, GAMMA_BASE_URL
from .contracts import BookState, MarketIdentity


@dataclass(frozen=True, slots=True)
class RestResponse:
    ok: bool
    status: int
    data: Any
    receipt_ts_ms: int
    receipt_monotonic_ns: int
    error: str = ""


class PublicJsonClient:
    """Small public JSON reader with no mutation or account surface."""

    def __init__(self, *, timeout_s: float = 5.0, session_factory=None):
        self.timeout_s = float(timeout_s)
        self._session_factory = session_factory
        self._session = None

    async def _session_for_get(self):
        if self._session is None or bool(getattr(self._session, "closed", False)):
            if self._session_factory is not None:
                self._session = self._session_factory()
            else:
                import aiohttp

                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=self.timeout_s),
                    headers={"Accept": "application/json"},
                )
        return self._session

    async def get_json(self, url: str, params: Optional[dict[str, Any]] = None) -> RestResponse:
        receipt_ts_ms = int(time.time() * 1000)
        receipt_monotonic_ns = time.monotonic_ns()
        try:
            session = await self._session_for_get()
            async with session.get(str(url), params=params or {}) as response:
                receipt_ts_ms = int(time.time() * 1000)
                receipt_monotonic_ns = time.monotonic_ns()
                status = int(response.status)
                if status != 200:
                    return RestResponse(
                        False, status, None, receipt_ts_ms,
                        receipt_monotonic_ns, f"http_{status}")
                data = await response.json()
                return RestResponse(
                    True, status, data, receipt_ts_ms, receipt_monotonic_ns)
        except Exception as exc:  # public read failures are fail-closed evidence
            return RestResponse(
                False, 0, None, int(time.time() * 1000), time.monotonic_ns(),
                f"read_failed:{type(exc).__name__}")

    async def close(self) -> None:
        session, self._session = self._session, None
        if session is not None and not bool(getattr(session, "closed", False)):
            try:
                await session.close()
            except Exception:
                pass


class GammaPublicClient(PublicJsonClient):
    def __init__(self, base_url: str = GAMMA_BASE_URL, **kwargs):
        super().__init__(**kwargs)
        self.base_url = str(base_url).rstrip("/")

    async def get_markets(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        response = await self.get_json(f"{self.base_url}/markets", params)
        return response.data if response.ok and isinstance(response.data, list) else []

    async def get_market(self, market_id: str) -> Optional[dict[str, Any]]:
        identity = str(market_id or "")
        if not identity:
            return None
        response = await self.get_json(f"{self.base_url}/markets/{identity}")
        return response.data if response.ok and isinstance(response.data, dict) else None

    async def get_event(self, event_id: str) -> Optional[dict[str, Any]]:
        identity = str(event_id or "")
        if not identity:
            return None
        response = await self.get_json(f"{self.base_url}/events/{identity}")
        return response.data if response.ok and isinstance(response.data, dict) else None


class ClobPublicClient(PublicJsonClient):
    def __init__(self, base_url: str = CLOB_BASE_URL, **kwargs):
        super().__init__(**kwargs)
        self.base_url = str(base_url).rstrip("/")

    async def get_book_response(self, token_id: str) -> RestResponse:
        token = str(token_id or "")
        if not token:
            return RestResponse(
                False, 0, None, int(time.time() * 1000), time.monotonic_ns(),
                "token_missing")
        return await self.get_json(
            f"{self.base_url}/book", {"token_id": token})

    async def get_book(self, token_id: str) -> Optional[dict[str, Any]]:
        response = await self.get_book_response(token_id)
        return response.data if response.ok and isinstance(response.data, dict) else None


@dataclass(frozen=True, slots=True)
class BookRecoveryResult:
    book: Optional[BookState]
    reason: NoBookReason
    attempts: int
    elapsed_ms: int
    errors: tuple[str, ...] = ()

    @property
    def recovered(self) -> bool:
        return self.book is not None and self.reason is NoBookReason.OK


def recovery_delays_ms(
        attempts: int, minimum_ms: int = 250,
        maximum_ms: int = 750) -> tuple[int, ...]:
    total = max(1, int(attempts))
    low, high = int(minimum_ms), int(maximum_ms)
    if low < 250 or high > 750 or low > high:
        raise ValueError("book recovery delays must stay within 250-750ms")
    delays: list[int] = []
    current = low
    for _ in range(max(0, total - 1)):
        delays.append(min(high, current))
        current = min(high, current * 2)
    return tuple(delays)


async def bounded_book_recovery(
        fetch_book: Callable[[str], Awaitable[Any]], *,
        market: MarketIdentity, token_id: str,
        attempts: int = 3, minimum_delay_ms: int = 250,
        maximum_delay_ms: int = 750, max_age_ms: int = 2_000,
        wall_clock_ms: Callable[[], int] = lambda: int(time.time() * 1000),
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> BookRecoveryResult:
    """Recover one exact token only; never substitute a market or window."""
    token = str(token_id or "")
    if token not in (market.yes_token_id, market.no_token_id):
        return BookRecoveryResult(None, NoBookReason.WRONG_TOKEN, 0, 0)
    delays = recovery_delays_ms(attempts, minimum_delay_ms, maximum_delay_ms)
    started_ns = int(monotonic_ns())
    errors: list[str] = []
    final_reason = NoBookReason.HTTP_FAILURE
    completed = 0
    for index in range(max(1, int(attempts))):
        completed += 1
        try:
            fetched = await fetch_book(token)
        except Exception as exc:
            fetched = None
            errors.append(f"fetch_failed:{type(exc).__name__}")
        receipt_ts = int(wall_clock_ms())
        receipt_mono = int(monotonic_ns())
        if isinstance(fetched, RestResponse):
            if fetched.ok:
                payload = fetched.data
                receipt_ts = fetched.receipt_ts_ms
                receipt_mono = fetched.receipt_monotonic_ns
            else:
                payload = None
                errors.append(fetched.error or f"http_{fetched.status}")
        else:
            payload = fetched
        if isinstance(payload, dict):
            normalized: BookNormalization = normalize_book(
                payload,
                expected_token_id=token,
                expected_condition_id=market.condition_id,
                expected_market_id=market.market_id,
                receipt_ts_ms=receipt_ts,
                receipt_monotonic_ns=receipt_mono,
                source="clob_rest_recovery",
                now_ms=receipt_ts,
                max_age_ms=max_age_ms,
                hydrated=True,
            )
            final_reason = normalized.reason
            if normalized.valid:
                elapsed = max(0, (int(monotonic_ns()) - started_ns) // 1_000_000)
                return BookRecoveryResult(
                    normalized.book, NoBookReason.OK, completed,
                    int(elapsed), tuple(errors))
            errors.append(normalized.reason.value)
            # Identity/timestamp safety failures are not transient and must not
            # be retried as though another market could repair them.
            if normalized.reason in {
                    NoBookReason.WRONG_TOKEN, NoBookReason.WRONG_MARKET,
                    NoBookReason.FUTURE_TIMESTAMP,
                    NoBookReason.REGRESSED_TIMESTAMP}:
                break
        else:
            final_reason = NoBookReason.HTTP_FAILURE
        if index < len(delays):
            await sleep(delays[index] / 1000.0)
    elapsed = max(0, (int(monotonic_ns()) - started_ns) // 1_000_000)
    return BookRecoveryResult(
        None, final_reason, completed, int(elapsed), tuple(errors))


async def hydrate_market_books(
        client: ClobPublicClient, market: MarketIdentity, *,
        attempts: int = 3, minimum_delay_ms: int = 250,
        maximum_delay_ms: int = 750, max_age_ms: int = 2_000,
) -> dict[str, BookRecoveryResult]:
    async def fetch(token: str) -> RestResponse:
        return await client.get_book_response(token)

    yes, no = await asyncio.gather(
        bounded_book_recovery(
            fetch, market=market, token_id=market.yes_token_id,
            attempts=attempts, minimum_delay_ms=minimum_delay_ms,
            maximum_delay_ms=maximum_delay_ms, max_age_ms=max_age_ms),
        bounded_book_recovery(
            fetch, market=market, token_id=market.no_token_id,
            attempts=attempts, minimum_delay_ms=minimum_delay_ms,
            maximum_delay_ms=maximum_delay_ms, max_age_ms=max_age_ms),
    )
    return {market.yes_token_id: yes, market.no_token_id: no}


recover_book = bounded_book_recovery
