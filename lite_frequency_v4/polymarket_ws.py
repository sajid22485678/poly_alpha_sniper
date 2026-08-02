"""Public Polymarket CLOB market-channel adapter for Frequency V4 shadow.

Only unauthenticated market data is exposed.  The implementation follows the
documented market-channel wire schema, including nested ``price_changes``,
literal ``PING``/``PONG`` heartbeats, dynamic subscription messages, and
custom lifecycle events.  Polymarket publishes no sequence/previous-sequence
contract on this channel, so hashes remain opaque change identifiers and are
never promoted into fictional sequence guarantees.
"""
from __future__ import annotations

import asyncio
import json
from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Mapping, Optional

import websockets

from .contracts import SourceEvent
from .events import (
    EventDecision,
    EventDisposition,
    EventGate,
    SequencePolicy,
    SourceHealth,
    SystemClock,
    backoff_seconds,
    canonical_json,
    canonical_payload_hash_of,
    canonical_payload_hash,
    invoke_callback,
    monotonic_ns,
    parse_positive_millis,
    stable_event_id,
    wall_ms,
)


POLYMARKET_MARKET_WS_URL = (
    "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)
POLYMARKET_HEARTBEAT = "PING"
POLYMARKET_HEARTBEAT_REPLY = "PONG"
SUPPORTED_EVENT_TYPES = frozenset({
    "book",
    "price_change",
    "tick_size_change",
    "last_trade_price",
    "best_bid_ask",
    "new_market",
    "market_resolved",
})


def initial_subscription(token_ids: Iterable[str]) -> dict[str, Any]:
    return {
        "assets_ids": sorted({str(token) for token in token_ids if str(token)}),
        "type": "market",
        "custom_feature_enabled": True,
    }


def dynamic_subscription(token_ids: Iterable[str], *, subscribe: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "assets_ids": sorted({str(token) for token in token_ids if str(token)}),
        "operation": "subscribe" if subscribe else "unsubscribe",
    }
    if subscribe:
        payload["custom_feature_enabled"] = True
    return payload


def _decimal(value: object, *, positive: bool = False,
             probability: bool = False) -> Optional[Decimal]:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    if positive and parsed <= 0:
        return None
    if probability and not (Decimal("0") <= parsed <= Decimal("1")):
        return None
    return parsed


#: Sentinel distinguishing "not computed yet" from a legitimately absent best
#: price, which is ``None``.
_UNCOMPUTED = object()


def _splice_sorted(
    levels: tuple[tuple[float, float], ...], price: float,
    size: Optional[float], *, descending: bool,
) -> tuple[tuple[float, float], ...]:
    """Return ``levels`` with one price level replaced, inserted or removed.

    ``levels`` is ordered best-price-first; ``size is None`` removes the level.
    The ordering of every other entry is preserved exactly, and unchanged
    entries are carried over by reference rather than rebuilt.
    """

    out: list[tuple[float, float]] = []
    placed = size is None
    for entry in levels:
        existing = entry[0]
        if existing == price:
            continue
        if not placed and (
                (descending and existing < price)
                or (not descending and existing > price)):
            out.append((price, size))
            placed = True
        out.append(entry)
    if not placed:
        out.append((price, size))
    return tuple(out)


@dataclass
class _BookState:
    """One immutable-by-convention version of a token's order book.

    A ``_BookState`` is never edited in place: every accepted snapshot or delta
    installs a *new* instance under :attr:`PolymarketMarketWS._books`.  That
    replacement is what makes the derived views below safe to memoize on the
    instance -- a new book version is a new object with an empty cache, so
    there is no invalidation to get wrong and no way to serve a stale view.

    The memoized values are the ones the old code recomputed on every access:
    ``best_bid``/``best_ask`` were ``max()``/``min()`` scans evaluated four
    times per delta (twice by the crossed-book guard, twice by the reported-BBO
    guard), and the sorted level lists were rebuilt from scratch by every
    ``current_book`` call.  Measured on the ingest benchmark at depth 40, those
    two rebuilds were 28.3 percent of per-message CPU before this change.
    """

    condition_id: str
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    provider_ts_ms: int
    book_hash: str = ""
    #: Monotonic identity assigned when this version is installed.  Consumers
    #: use it to cache anything derived from the levels without needing to
    #: compare book contents.  Zero means "never installed".
    version: int = 0
    _best_bid: Any = field(default=_UNCOMPUTED, repr=False, compare=False)
    _best_ask: Any = field(default=_UNCOMPUTED, repr=False, compare=False)
    _levels: Any = field(default=None, repr=False, compare=False)
    #: Set only when the previous version's sorted view was already built, so
    #: this one can be derived from it by splicing the single changed level
    #: instead of re-sorting.  Never chained: if the parent's view is not
    #: materialized there is nothing to derive from, so no link is kept and no
    #: unbounded ancestry can accumulate.
    _parent_levels: Any = field(default=None, repr=False, compare=False)
    _delta: Any = field(default=None, repr=False, compare=False)

    @property
    def best_bid(self) -> Optional[Decimal]:
        if self._best_bid is _UNCOMPUTED:
            self._best_bid = max(self.bids) if self.bids else None
        return self._best_bid

    @property
    def best_ask(self) -> Optional[Decimal]:
        if self._best_ask is _UNCOMPUTED:
            self._best_ask = min(self.asks) if self.asks else None
        return self._best_ask

    def levels(self) -> tuple[tuple[tuple[float, float], ...],
                              tuple[tuple[float, float], ...]]:
        """Sorted ``(price, size)`` tuples, best price first, computed once.

        Tuples all the way down, so sharing them between callers cannot let one
        consumer disturb another.  ``current_book`` still hands out fresh lists
        built from these, preserving its documented copy-safety contract.
        """

        if self._levels is not None:
            return self._levels
        spliced = self._spliced_levels()
        if spliced is not None:
            self._levels = spliced
        else:
            self._levels = (
                tuple((float(price), float(self.bids[price]))
                      for price in sorted(self.bids, reverse=True)),
                tuple((float(price), float(self.asks[price]))
                      for price in sorted(self.asks)),
            )
        # The derivation inputs are consumed; dropping them releases the
        # previous version's view as soon as this one exists.
        self._parent_levels = None
        self._delta = None
        return self._levels

    def _spliced_levels(self) -> Any:
        """Derive this version's sorted view from the previous one, or None.

        A delta changes exactly one level on one side, so the new sorted view
        differs from the old by one entry.  Splicing it costs a pointer copy per
        level; rebuilding costs a sort plus a ``Decimal``-to-``float``
        conversion for every level on both sides.

        Returns ``None`` -- meaning "rebuild from scratch" -- whenever the
        result cannot be trusted.  The length check is the guard that matters:
        two numerically distinct ``Decimal`` prices could in principle collapse
        onto one ``float``, and if that ever happened the spliced view would
        disagree with the authoritative dictionaries.  Comparing counts catches
        it in constant time, and the fallback is simply the old code path.
        """

        parent = self._parent_levels
        delta = self._delta
        if parent is None or delta is None:
            return None
        buy_side, price, size = delta
        price_f = float(price)
        size_f = None if size is None else float(size)
        if buy_side:
            bids = _splice_sorted(parent[0], price_f, size_f, descending=True)
            asks = parent[1]
        else:
            bids = parent[0]
            asks = _splice_sorted(parent[1], price_f, size_f, descending=False)
        if len(bids) != len(self.bids) or len(asks) != len(self.asks):
            return None
        return (bids, asks)


class PolymarketMarketWS:
    """Reconnectable, hydration-aware public market data adapter."""

    source = "polymarket"

    def __init__(
            self,
            token_conditions: Optional[Mapping[str, str]] = None,
            *,
            url: str = POLYMARKET_MARKET_WS_URL,
            clock: object | None = None,
            event_gate: EventGate | None = None,
            on_event: Optional[Callable[..., Any]] = None,
            on_health: Optional[Callable[..., Any]] = None,
            on_hydration_request: Optional[Callable[..., Any]] = None,
            websocket_factory: Optional[Callable[..., Any]] = None,
            heartbeat_interval_s: float = 10.0,
            pong_timeout_s: float = 10.0,
            max_buffered_deltas_per_token: int = 64,
            hydration_request_cooldown_s: float = 0.25,
            max_event_age_ms: int = 2_000,
            hydration_recovery_interval_s: float = 1.0,
            hydration_recovery_after_s: float = 5.0,
            hydration_recovery_max_attempts: int = 6,
            hydration_recovery_backoff_cap_s: float = 20.0,
    ):
        self.url = str(url)
        self.clock = clock or SystemClock()
        self.gate = event_gate or EventGate()
        self.on_event = on_event
        self.on_health = on_health
        self.on_hydration_request = on_hydration_request
        self.websocket_factory = websocket_factory or websockets.connect
        self.heartbeat_interval_s = float(heartbeat_interval_s)
        self.pong_timeout_s = float(pong_timeout_s)
        self.max_buffered_deltas_per_token = int(max_buffered_deltas_per_token)
        self.hydration_request_cooldown_s = float(hydration_request_cooldown_s)
        self.max_event_age_ms = int(max_event_age_ms)
        self.hydration_recovery_interval_s = float(hydration_recovery_interval_s)
        self.hydration_recovery_after_s = float(hydration_recovery_after_s)
        self.hydration_recovery_max_attempts = int(hydration_recovery_max_attempts)
        self.hydration_recovery_backoff_cap_s = float(
            hydration_recovery_backoff_cap_s)
        if self.heartbeat_interval_s <= 0 or self.pong_timeout_s <= 0:
            raise ValueError("heartbeat intervals must be positive")
        if self.max_buffered_deltas_per_token <= 0:
            raise ValueError("max buffered deltas must be positive")
        if self.hydration_request_cooldown_s <= 0:
            raise ValueError("hydration request cooldown must be positive")
        if self.max_event_age_ms <= 0:
            raise ValueError("maximum event age must be positive")
        if self.hydration_recovery_interval_s <= 0:
            raise ValueError("hydration recovery interval must be positive")
        if self.hydration_recovery_after_s <= 0:
            raise ValueError("hydration recovery delay must be positive")
        if self.hydration_recovery_max_attempts <= 0:
            raise ValueError("hydration recovery attempts must be positive")
        if self.hydration_recovery_backoff_cap_s < self.hydration_recovery_after_s:
            raise ValueError(
                "hydration recovery backoff cap must not be below its delay")

        self._desired: dict[str, str] = {
            str(token): str(condition)
            for token, condition in (token_conditions or {}).items()
            if str(token) and str(condition)
        }
        self._books: dict[str, _BookState] = {}
        #: Monotonic counter stamped onto each installed book version.  Only
        #: ``_install_book`` advances it, so a version number identifies exactly
        #: one set of levels for the life of the adapter.
        self._book_seq = 0
        self._hydrated: set[str] = set()
        self._buffers: dict[str, deque[tuple[SourceEvent, dict[str, Any]]]] = (
            defaultdict(deque))
        self._ws: Any = None
        self._task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._recovery_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._last_ping_mono_ns = 0
        self._last_pong_mono_ns = 0
        self._last_hydration_request_mono_ns: dict[str, int] = {}
        #: Bounded targeted-hydration recovery state, all keyed by token and all
        #: scoped to one transport epoch and one desired-token set.
        self._unhydrated_since_mono_ns: dict[str, int] = {}
        self._targeted_attempts: dict[str, int] = {}
        self._targeted_next_mono_ns: dict[str, int] = {}
        self._targeted_exhausted: set[str] = set()
        #: Tokens this epoch has already sent a subscribe for.  The wire is
        #: idempotent but a second subscribe is still a duplicate request, and
        #: recovery must not manufacture a subscription storm out of one silent
        #: token, so every avoided repeat is counted rather than sent.
        self._subscribed_tokens: set[str] = set()
        self.health_state = SourceHealth(source=self.source)
        self.health_state.desired_subscriptions = len(self._desired)

    @property
    def connection_epoch(self) -> int:
        return self.health_state.connection_epoch

    @property
    def hydrated_tokens(self) -> frozenset[str]:
        return frozenset(self._hydrated)

    def health(self) -> dict[str, Any]:
        self.health_state.desired_subscriptions = len(self._desired)
        self.health_state.hydrated_subscriptions = len(self._hydrated)
        self._refresh_unhydrated_gauges()
        return self.health_state.to_dict()

    def _missing_tokens(self) -> list[str]:
        """Currently desired tokens with no accepted book in this epoch."""

        return sorted(set(self._desired) - self._hydrated)

    def _refresh_unhydrated_gauges(self) -> None:
        missing = self._missing_tokens()
        self.health_state.unhydrated_active_tokens = len(missing)
        now_mono = monotonic_ns(self.clock)
        ages = [
            now_mono - since
            for since in (self._unhydrated_since_mono_ns.get(token)
                          for token in missing)
            if since is not None
        ]
        self.health_state.oldest_unhydrated_age_ms = (
            int(max(ages) // 1_000_000) if ages else 0)

    def _track_unhydrated(self, tokens: Iterable[str]) -> None:
        """Start the unhydrated clock for tokens that do not already have one.

        Idempotent on purpose: a token that has been waiting must keep its
        original timestamp, or a repeated sweep would keep resetting its age to
        zero and the bounded delay would never elapse.
        """

        now_mono = monotonic_ns(self.clock)
        for token in tokens:
            self._unhydrated_since_mono_ns.setdefault(str(token), now_mono)

    def _clear_recovery_state(self, token: str) -> None:
        key = str(token)
        self._unhydrated_since_mono_ns.pop(key, None)
        self._targeted_attempts.pop(key, None)
        self._targeted_next_mono_ns.pop(key, None)
        self._targeted_exhausted.discard(key)

    def _install_book(self, token: str, book: _BookState) -> _BookState:
        """Publish a new book version under ``token``.

        The single place a book becomes visible, and therefore the single place
        a version is stamped.  A proposal that is rejected never reaches here
        and never consumes a version.
        """

        self._book_seq += 1
        book.version = self._book_seq
        self._books[token] = book
        return book

    def book_state(self, token_id: str) -> dict[str, Any]:
        book = self._books.get(str(token_id))
        if book is None:
            return {}
        return {
            "condition_id": book.condition_id,
            "best_bid": float(book.best_bid) if book.best_bid is not None else None,
            "best_ask": float(book.best_ask) if book.best_ask is not None else None,
            "provider_ts_ms": book.provider_ts_ms,
            "hash": book.book_hash,
            "hydrated": str(token_id) in self._hydrated,
        }

    def current_book(self, token_id: str) -> dict[str, Any]:
        """Return a normalized copy of the current full in-memory book.

        Level entries are immutable tuples ordered best-price first and each
        containing only primitive values.  The level lists and mapping are
        new objects, so callers can pass them directly to ``normalize_book``
        or decorate them without mutating the adapter's executable state.
        """

        normalized_token = str(token_id)
        book = self._books.get(normalized_token)
        if book is None:
            return {}
        # The sort and the Decimal->float conversion happen once per book
        # version; the per-call cost is one shallow list copy of already-built
        # immutable tuples.  The copy is deliberate and load-bearing: callers
        # are documented to be able to decorate or mutate the returned lists,
        # and a shared list would let one consumer corrupt another.
        bid_levels, ask_levels = book.levels()
        return {
            "token_id": normalized_token,
            "condition_id": book.condition_id,
            "bids": list(bid_levels),
            "asks": list(ask_levels),
            "provider_ts_ms": book.provider_ts_ms,
            "hash": book.book_hash,
            "hydrated": normalized_token in self._hydrated,
            "connection_epoch": self.connection_epoch,
            # Identity of this exact set of levels.  Consumers that build their
            # own representation (the engine's BookLevel tuples) key their cache
            # on it instead of rebuilding per event.  Adapter-level fields above
            # are deliberately *not* covered by it.
            "book_version": book.version,
        }

    # Explicit descriptive alias for callers that prefer the longer name.
    normalized_book = current_book

    async def _publish_health(self) -> None:
        await invoke_callback(self.on_health, self.health())

    async def _publish(self, event: SourceEvent,
                       decision: EventDecision) -> EventDecision:
        self.health_state.record(decision)
        await invoke_callback(self.on_event, event, decision)
        return decision

    async def _request_hydration(self, token: str, condition: str,
                                 reason: str, *, force: bool = False) -> bool:
        now_mono = monotonic_ns(self.clock)
        last_mono = self._last_hydration_request_mono_ns.get(str(token))
        cooldown_ns = int(self.hydration_request_cooldown_s * 1_000_000_000)
        if (not force and last_mono is not None
                and now_mono - last_mono < cooldown_ns):
            return False
        self._last_hydration_request_mono_ns[str(token)] = now_mono
        self.health_state.hydration_requests += 1
        await invoke_callback(
            self.on_hydration_request,
            str(token), str(condition), str(reason), self.connection_epoch,
        )
        return True

    def _new_connection_epoch(self) -> None:
        previous = self.health_state.connection_epoch
        self.health_state.connection_epoch = previous + 1
        if previous > 0:
            self.health_state.reconnect_count += 1
        # Old state remains historical in downstream storage but is never
        # executable in a new transport epoch.
        self._books.clear()
        self._hydrated.clear()
        self._buffers.clear()
        self._last_hydration_request_mono_ns.clear()
        # Recovery bookkeeping is per epoch for the same reason the books are:
        # attempts spent against a socket that no longer exists must not count
        # against the new one, and an exhausted token must get a fresh budget
        # once its subscription is re-established.
        self._unhydrated_since_mono_ns.clear()
        self._targeted_attempts.clear()
        self._targeted_next_mono_ns.clear()
        self._targeted_exhausted.clear()
        self._subscribed_tokens.clear()
        self.health_state.unhydrated_active_tokens = len(self._desired)
        self.health_state.oldest_unhydrated_age_ms = 0
        self.health_state.hydration_transition_reason = (
            "reconnect" if previous > 0 else "first_connection")
        # Heartbeat accounting is per transport epoch.  A stale unanswered
        # PING inherited from a previous connection otherwise trips the
        # pong-timeout check on the FIRST poll of the new epoch's heartbeat
        # loop - its age already exceeds pong_timeout_s - closing the fresh
        # socket before it ever sends its own first PING.  One genuine pong
        # loss then poisons every later epoch into a perpetual reconnect
        # loop with frozen heartbeat/pong timestamps.
        self._last_ping_mono_ns = 0
        self._last_pong_mono_ns = 0
        self.health_state.hydrated_subscriptions = 0
        self.health_state.state = "SUBSCRIBING"

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="frequency_v4_poly_ws")

    async def stop(self) -> None:
        self._stop.set()
        for task in (self._heartbeat_task, self._recovery_task, self._task):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._heartbeat_task, self._recovery_task, self._task):
            if task is not None:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as exc:  # noqa: BLE001 - see below
                    # A socket that ended without a close handshake is what
                    # stopping *observes*, not a reason stopping failed.  If
                    # this propagated it would abort the caller's shutdown
                    # sequence mid-way -- and in this engine that sequence is
                    # what durably ends the runtime session, so a peer
                    # disconnecting rudely would leave the session row open
                    # forever.  Record it and finish stopping.
                    self.health_state.last_error = (
                        f"stop:{type(exc).__name__}:{exc}")[:240]
        self._heartbeat_task = None
        self._recovery_task = None
        self._task = None
        self._ws = None
        self.health_state.connected = False
        self.health_state.state = "STOPPED"
        await self._publish_health()

    async def set_subscriptions(self, token_conditions: Mapping[str, str]) -> None:
        wanted = {
            str(token): str(condition)
            for token, condition in token_conditions.items()
            if str(token) and str(condition)
        }
        old_tokens, new_tokens = set(self._desired), set(wanted)
        removed = sorted(old_tokens - new_tokens)
        added = sorted(new_tokens - old_tokens)
        changed = sorted(token for token in old_tokens & new_tokens
                         if self._desired[token] != wanted[token])
        removed = sorted(set(removed + changed))
        added = sorted(set(added + changed))
        self._desired = wanted
        # Obsolete tokens leave in one step, taking their book, their buffer and
        # their whole recovery budget with them.  A token dropped here can never
        # come back through a retry scheduled before the rotation, because every
        # retry re-checks the live desired set before it acts.
        for token in removed:
            self._books.pop(token, None)
            self._hydrated.discard(token)
            self._buffers.pop(token, None)
            self._last_hydration_request_mono_ns.pop(token, None)
            self._clear_recovery_state(token)
            self._subscribed_tokens.discard(token)
        if removed:
            self.health_state.obsolete_tokens_removed += len(removed)
        self.health_state.desired_subscriptions = len(wanted)
        if removed or added:
            self.health_state.hydration_transition_reason = "market_rotation"
        if self._ws is not None and self.health_state.connected:
            if removed:
                await self._ws.send(canonical_json(
                    dynamic_subscription(removed, subscribe=False)))
            if added:
                fresh = [token for token in added
                         if token not in self._subscribed_tokens]
                self.health_state.duplicate_subscriptions_prevented += (
                    len(added) - len(fresh))
                if fresh:
                    await self._ws.send(canonical_json(
                        dynamic_subscription(fresh, subscribe=True)))
                    self._subscribed_tokens.update(fresh)
                self._track_unhydrated(added)
                await asyncio.gather(*(
                    self._request_hydration(
                        token, self._desired[token], "dynamic_subscription")
                    for token in added
                ))
        if not wanted:
            self.health_state.state = "WAITING_FOR_SUBSCRIPTIONS"
        elif self.health_state.connected:
            self.health_state.state = (
                "READY" if self._hydrated == set(wanted) else "HYDRATING")
        self.health_state.hydrated_subscriptions = len(self._hydrated)
        await self._publish_health()

    async def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            if not self._desired:
                self.health_state.state = "WAITING_FOR_SUBSCRIPTIONS"
                await asyncio.sleep(0.25)
                continue
            connected_started_ns: Optional[int] = None
            try:
                self.health_state.state = "CONNECTING"
                await self._publish_health()
                async with self.websocket_factory(
                        self.url, ping_interval=None, close_timeout=3) as ws:
                    self._ws = ws
                    self.health_state.connected = True
                    connected_started_ns = monotonic_ns(self.clock)
                    self._new_connection_epoch()
                    await ws.send(canonical_json(initial_subscription(self._desired)))
                    self._subscribed_tokens.update(self._desired)
                    self.health_state.state = "HYDRATING"
                    self.health_state.backoff_seconds = 0.0
                    self._track_unhydrated(sorted(self._desired))
                    await asyncio.gather(*(
                        self._request_hydration(token, condition, "reconnect")
                        for token, condition in sorted(self._desired.items())
                    ))
                    await self._publish_health()
                    self._heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(ws),
                        name="frequency_v4_poly_heartbeat")
                    self._recovery_task = asyncio.create_task(
                        self._hydration_recovery_loop(),
                        name="frequency_v4_poly_hydration_recovery")
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        await self.handle_message(raw)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001 - transport must reconnect
                self.health_state.last_error = repr(exc)[:240]
            finally:
                # Connected is cleared first so both loops observe a dead
                # transport even if the cancellation races their next poll.
                self.health_state.connected = False
                for attribute in ("_heartbeat_task", "_recovery_task"):
                    task = getattr(self, attribute)
                    if task is not None:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        setattr(self, attribute, None)
                self._ws = None
            if self._stop.is_set():
                break
            if (connected_started_ns is not None
                    and monotonic_ns(self.clock) - connected_started_ns
                    >= 30_000_000_000):
                # A genuinely stable 30-second session clears failure history;
                # merely completing a handshake does not.
                attempt = 0
            delay = backoff_seconds(attempt)
            attempt += 1
            self.health_state.state = "BACKOFF"
            self.health_state.backoff_seconds = delay
            await self._publish_health()
            await asyncio.sleep(delay)
        self.health_state.connected = False

    def _targeted_backoff_ns(self, attempts: int) -> int:
        """Capped exponential backoff, deterministic and jitter-free.

        Attempt *n* waits ``after * 2**(n-1)`` seconds, clamped to the cap, so
        the schedule is reproducible in a test with a fake clock and can never
        grow without bound.
        """

        delay = self.hydration_recovery_after_s * (2 ** max(0, attempts - 1))
        return int(min(delay, self.hydration_recovery_backoff_cap_s)
                   * 1_000_000_000)

    async def _recover_unhydrated_once(self) -> list[str]:
        """One bounded sweep over the active desired tokens that are missing.

        This is the whole point of the mechanism.  A desired token that receives
        no frame at all triggers none of the event-driven hydration paths -- the
        delta, missing-book and crossed-book requests all need an inbound
        message for that token -- so before this sweep existed its one-shot
        subscribe-time request was the only attempt it would ever get.  If that
        attempt did not produce an accepted snapshot the token stayed out of
        ``_hydrated`` forever, and because READY is all-or-nothing over the
        active set, the source stayed HYDRATING on a healthy connection with
        fresh frames for every other token.

        Recovery is targeted, never a reconnect: the socket is left alone and
        only the missing token is re-requested.
        """

        if not self.health_state.connected or self._ws is None:
            return []
        now_mono = monotonic_ns(self.clock)
        missing = self._missing_tokens()
        self._track_unhydrated(missing)
        threshold_ns = int(self.hydration_recovery_after_s * 1_000_000_000)
        retried: list[str] = []
        for token in missing:
            # Rotation can land between two awaits in this loop, so the live
            # desired set is re-checked immediately before acting on a token.
            condition = self._desired.get(token)
            if condition is None or token in self._hydrated:
                continue
            since = self._unhydrated_since_mono_ns.get(token)
            if since is None or now_mono - since < threshold_ns:
                continue
            due = self._targeted_next_mono_ns.get(token)
            if due is not None and now_mono < due:
                continue
            attempts = self._targeted_attempts.get(token, 0)
            if attempts >= self.hydration_recovery_max_attempts:
                if token not in self._targeted_exhausted:
                    self._targeted_exhausted.add(token)
                    self.health_state.targeted_retry_exhaustions += 1
                    self.health_state.hydration_transition_reason = (
                        "targeted_retry_exhausted")
                # Deliberately no promotion and no removal: an active token that
                # cannot hydrate holds the source in HYDRATING, which is the
                # fail-closed outcome the contract requires.
                continue
            attempts += 1
            self._targeted_attempts[token] = attempts
            self._targeted_next_mono_ns[token] = (
                now_mono + self._targeted_backoff_ns(attempts))
            self.health_state.targeted_retry_attempts += 1
            self.health_state.hydration_transition_reason = "targeted_retry"
            # Re-assert the subscription for this one token only when the epoch
            # has no live subscribe for it; otherwise the REST re-request alone
            # is the recovery and the avoided resubscribe is counted.
            if token in self._subscribed_tokens:
                self.health_state.duplicate_subscriptions_prevented += 1
            else:
                await self._ws.send(canonical_json(
                    dynamic_subscription([token], subscribe=True)))
                self._subscribed_tokens.add(token)
            await self._request_hydration(
                token, condition, "targeted_hydration_retry", force=True)
            retried.append(token)
        self._refresh_unhydrated_gauges()
        if retried:
            await self._publish_health()
        return retried

    async def _hydration_recovery_loop(self) -> None:
        while not self._stop.is_set() and self.health_state.connected:
            await asyncio.sleep(self.hydration_recovery_interval_s)
            if self._stop.is_set() or not self.health_state.connected:
                return
            try:
                await self._recover_unhydrated_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - recovery is never fatal
                self.health_state.last_error = (
                    f"hydration_recovery:{type(exc).__name__}:{exc}")[:240]

    async def _heartbeat_loop(self, ws: Any) -> None:
        interval_ns = int(self.heartbeat_interval_s * 1_000_000_000)
        timeout_ns = int(self.pong_timeout_s * 1_000_000_000)
        poll_s = min(1.0, self.heartbeat_interval_s, self.pong_timeout_s)
        started_ns = monotonic_ns(self.clock)
        while not self._stop.is_set() and self.health_state.connected:
            await asyncio.sleep(poll_s)
            now_mono = monotonic_ns(self.clock)
            if (self._last_ping_mono_ns > self._last_pong_mono_ns
                    and now_mono - self._last_ping_mono_ns >= timeout_ns):
                self.health_state.last_error = "polymarket_pong_timeout"
                await ws.close()
                return
            last_sent = self._last_ping_mono_ns or started_ns
            if now_mono - last_sent < interval_ns:
                continue
            self._last_ping_mono_ns = now_mono
            self.health_state.last_heartbeat_sent_ts_ms = wall_ms(self.clock)
            await ws.send(POLYMARKET_HEARTBEAT)

    async def send_heartbeat_once(self, ws: Any | None = None) -> None:
        target = ws or self._ws
        if target is None:
            raise RuntimeError("websocket is not connected")
        self._last_ping_mono_ns = monotonic_ns(self.clock)
        self.health_state.last_heartbeat_sent_ts_ms = wall_ms(self.clock)
        await target.send(POLYMARKET_HEARTBEAT)

    async def handle_message(
            self, raw: str | bytes, *, receipt_ts_ms: Optional[int] = None,
            receipt_monotonic_ns: Optional[int] = None,
    ) -> list[EventDecision]:
        received_ms = int(receipt_ts_ms if receipt_ts_ms is not None
                          else wall_ms(self.clock))
        received_mono = int(
            receipt_monotonic_ns if receipt_monotonic_ns is not None
            else monotonic_ns(self.clock))
        self.health_state.last_frame_receipt_ts_ms = received_ms
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                self.health_state.parse_errors += 1
                return []
        if str(raw).strip() == POLYMARKET_HEARTBEAT_REPLY:
            self._last_pong_mono_ns = received_mono
            self.health_state.last_pong_receipt_ts_ms = received_ms
            if self._last_ping_mono_ns:
                self.health_state.heartbeat_rtt_ms = max(
                    0.0, (received_mono - self._last_ping_mono_ns) / 1_000_000)
            await self._publish_health()
            return []
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            self.health_state.parse_errors += 1
            self.health_state.last_error = "invalid_polymarket_json"
            return []
        messages = payload if isinstance(payload, list) else [payload]
        decisions: list[EventDecision] = []
        for message in messages:
            if not isinstance(message, dict):
                self.health_state.parse_errors += 1
                continue
            event_type = str(message.get("event_type") or message.get("type") or "")
            if event_type not in SUPPORTED_EVENT_TYPES:
                continue
            if event_type == "book":
                decision = await self._handle_book(
                    message, received_ms, received_mono, channel="market")
                if decision is not None:
                    decisions.append(decision)
            elif event_type == "price_change":
                decisions.extend(await self._handle_price_change(
                    message, received_ms, received_mono))
            else:
                decision = await self._handle_auxiliary(
                    event_type, message, received_ms, received_mono)
                if decision is not None:
                    decisions.append(decision)
        return decisions

    def _identity_ok(self, token: str, condition: str) -> bool:
        return bool(token in self._desired
                    and condition
                    and self._desired[token] == condition)

    def _source_event(self, *, event_type: str, payload: dict[str, Any],
                      provider_ts_ms: int, receipt_ts_ms: int,
                      receipt_monotonic_ns: int, token_id: str = "",
                      condition_id: str = "", channel: str = "market") -> SourceEvent:
        # One serialization, two consumers.  ``canonical_payload_hash`` is
        # defined as the sha256 of exactly this string, so hashing it directly
        # is byte-identical to calling it -- it just does not serialize the
        # same dictionary a second time.
        payload_json = canonical_json(payload)
        payload_hash = canonical_payload_hash_of(payload_json)
        identity = token_id or condition_id or str(payload.get("market") or "")
        event_key = stable_event_id(
            self.source, channel, event_type, identity, provider_ts_ms,
            payload_hash,
        )
        return SourceEvent(
            source=self.source,
            channel=channel,
            event_type=event_type,
            event_key=event_key,
            payload_hash=payload_hash,
            provider_ts_ms=provider_ts_ms,
            receipt_ts_ms=receipt_ts_ms,
            receipt_monotonic_ns=receipt_monotonic_ns,
            sequence=None,
            asset="",
            market_id=condition_id,
            condition_id=condition_id,
            token_id=token_id,
            window_open_ms=None,
            payload_json=payload_json,
            connection_epoch=self.connection_epoch,
        )

    @staticmethod
    def _parse_levels(raw: object) -> Optional[dict[Decimal, Decimal]]:
        if not isinstance(raw, list):
            return None
        levels: dict[Decimal, Decimal] = {}
        for item in raw:
            if not isinstance(item, dict):
                return None
            price = _decimal(item.get("price"), probability=True)
            size = _decimal(item.get("size"), positive=True)
            if (price is None or size is None
                    or not Decimal("0") < price < Decimal("1")
                    or price in levels):
                return None
            levels[price] = size
        return levels

    async def _handle_book(
            self, payload: dict[str, Any], receipt_ms: int, receipt_mono: int,
            *, channel: str,
    ) -> Optional[EventDecision]:
        token = str(payload.get("asset_id") or "")
        condition = str(payload.get("market") or payload.get("condition_id") or "")
        provider_ms = parse_positive_millis(payload.get("timestamp"))
        bids = self._parse_levels(payload.get("bids"))
        asks = self._parse_levels(payload.get("asks"))
        if (not token or not condition or provider_ms is None
                or bids is None or asks is None):
            self.health_state.parse_errors += 1
            return None
        event = self._source_event(
            event_type="book", payload=payload,
            provider_ts_ms=provider_ms, receipt_ts_ms=receipt_ms,
            receipt_monotonic_ns=receipt_mono, token_id=token,
            condition_id=condition, channel=channel)
        if not self._identity_ok(token, condition):
            return await self._publish(event, EventDecision(
                EventDisposition.REJECT_IDENTITY, event,
                "book_token_or_condition_mismatch"))
        if provider_ms > receipt_ms:
            return await self._publish(event, EventDecision(
                EventDisposition.REJECT_FUTURE, event,
                "book_timestamp_after_receipt"))
        if receipt_ms - provider_ms > self.max_event_age_ms:
            return await self._publish(event, EventDecision(
                EventDisposition.REJECT_STALE, event,
                "book_too_old_at_receipt"))
        if bids and asks and max(bids) > min(asks):
            return await self._publish(event, EventDecision(
                EventDisposition.REJECT_INVALID, event,
                "crossed_book_snapshot"))
        current = self._books.get(token)
        if current is not None and provider_ms < current.provider_ts_ms:
            return await self._publish(event, EventDecision(
                EventDisposition.REJECT_TIMESTAMP_REGRESSION, event,
                "book_snapshot_older_than_local_book"))
        decision = self.gate.evaluate(
            event, stream_key=f"book_snapshot:{token}",
            sequence_policy=SequencePolicy.NONE,
            allow_equal_timestamp_distinct=True,
            max_age_ms=self.max_event_age_ms)
        await self._publish(event, decision)
        # A newly fetched/subscribed snapshot may be byte-identical to the
        # prior transport epoch.  It remains a deduplicated event (and cannot
        # retrigger strategy evidence), but a fresh exact snapshot may safely
        # re-establish local hydration after reconnect.
        rehydrate_duplicate = (
            decision.duplicate and token not in self._hydrated)
        if not decision.accepted and not rehydrate_duplicate:
            return decision
        self._install_book(token, _BookState(
            condition_id=condition, bids=bids, asks=asks,
            provider_ts_ms=provider_ms,
            book_hash=str(payload.get("hash") or "")))
        was_missing = token not in self._hydrated
        if was_missing and self._targeted_attempts.get(token):
            self.health_state.targeted_retry_successes += 1
            self.health_state.hydration_transition_reason = (
                "targeted_retry_recovered")
        self._hydrated.add(token)
        self._clear_recovery_state(token)
        await self._flush_buffer(token)
        self.health_state.hydrated_subscriptions = len(self._hydrated)
        # Hydration progress promotes readiness only while the transport is
        # actually connected.  REST snapshots also flow through here and can
        # land during BACKOFF; a disconnected source must never report READY
        # merely because REST data kept its books fresh.
        if self.health_state.connected:
            self.health_state.state = (
                "READY" if self._desired and self._hydrated == set(self._desired)
                else "HYDRATING")
        await self._publish_health()
        return decision

    async def accept_rest_book(
            self, payload: dict[str, Any], *, receipt_ts_ms: Optional[int] = None,
            receipt_monotonic_ns: Optional[int] = None,
    ) -> Optional[EventDecision]:
        """Admit an exact-token public REST snapshot without regressing WS state."""

        return await self._handle_book(
            payload,
            int(receipt_ts_ms if receipt_ts_ms is not None else wall_ms(self.clock)),
            int(receipt_monotonic_ns if receipt_monotonic_ns is not None
                else monotonic_ns(self.clock)),
            channel="market",
        )

    async def _handle_price_change(
            self, payload: dict[str, Any], receipt_ms: int, receipt_mono: int,
    ) -> list[EventDecision]:
        condition = str(payload.get("market") or "")
        provider_ms = parse_positive_millis(payload.get("timestamp"))
        changes = payload.get("price_changes")
        if not condition or provider_ms is None or not isinstance(changes, list):
            self.health_state.parse_errors += 1
            return []
        decisions: list[EventDecision] = []
        for index, change in enumerate(changes):
            if not isinstance(change, dict):
                self.health_state.parse_errors += 1
                continue
            token = str(change.get("asset_id") or "")
            normalized = {
                "market": condition,
                "timestamp": str(provider_ms),
                "event_type": "price_change",
                "change_index": index,
                "change": change,
            }
            event = self._source_event(
                event_type="price_change", payload=normalized,
                provider_ts_ms=provider_ms, receipt_ts_ms=receipt_ms,
                receipt_monotonic_ns=receipt_mono, token_id=token,
                condition_id=condition)
            if not self._identity_ok(token, condition):
                decisions.append(await self._publish(event, EventDecision(
                    EventDisposition.REJECT_IDENTITY, event,
                    "delta_token_or_condition_mismatch")))
                continue
            if not self._valid_change(change):
                decisions.append(await self._publish(event, EventDecision(
                    EventDisposition.REJECT_INVALID, event,
                    "invalid_price_change")))
                continue
            if provider_ms > receipt_ms:
                decisions.append(await self._publish(event, EventDecision(
                    EventDisposition.REJECT_FUTURE, event,
                    "price_change_timestamp_after_receipt")))
                continue
            if receipt_ms - provider_ms > self.max_event_age_ms:
                decisions.append(await self._publish(event, EventDecision(
                    EventDisposition.REJECT_STALE, event,
                    "price_change_too_old_at_receipt")))
                continue
            if token not in self._hydrated:
                queue = self._buffers[token]
                if len(queue) >= self.max_buffered_deltas_per_token:
                    queue.popleft()
                queue.append((event, change))
                decision = EventDecision(
                    EventDisposition.BUFFER_UNHYDRATED, event,
                    "price_change_before_full_snapshot", request_hydration=True)
                decisions.append(await self._publish(event, decision))
                await self._request_hydration(token, condition, "delta_before_hydration")
                continue
            decisions.append(await self._apply_delta(event, change))
        return decisions

    @staticmethod
    def _valid_change(change: dict[str, Any]) -> bool:
        price = _decimal(change.get("price"), probability=True)
        size = _decimal(change.get("size"), probability=False)
        best_bid = _decimal(change.get("best_bid"), probability=True)
        best_ask = _decimal(change.get("best_ask"), probability=True)
        return bool(
            str(change.get("side") or "").upper() in {"BUY", "SELL"}
            and price is not None and Decimal("0") < price < Decimal("1")
            and size is not None and size >= 0
            and best_bid is not None and best_ask is not None
        )

    @staticmethod
    def _reported_bbo_matches(book: _BookState,
                              change: dict[str, Any]) -> bool:
        reported_bid = _decimal(change.get("best_bid"), probability=True)
        reported_ask = _decimal(change.get("best_ask"), probability=True)
        bid_ok = (reported_bid == book.best_bid
                  or (book.best_bid is None and reported_bid == Decimal("0")))
        ask_ok = (reported_ask == book.best_ask
                  or (book.best_ask is None and reported_ask == Decimal("1")))
        return bool(bid_ok and ask_ok)

    async def _apply_delta(self, event: SourceEvent,
                           change: dict[str, Any]) -> EventDecision:
        token = event.token_id
        current = self._books.get(token)
        if current is None:
            decision = EventDecision(
                EventDisposition.BUFFER_UNHYDRATED, event,
                "missing_local_book", request_hydration=True)
            await self._publish(event, decision)
            await self._request_hydration(token, event.condition_id, "missing_local_book")
            return decision
        price = _decimal(change.get("price"), probability=True)
        size = _decimal(change.get("size"))
        assert price is not None and size is not None
        if event.provider_ts_ms < current.provider_ts_ms:
            decision = EventDecision(
                EventDisposition.REJECT_TIMESTAMP_REGRESSION, event,
                "price_change_older_than_local_book")
            return await self._publish(event, decision)
        # A delta touches exactly one side, so only that side is copied.  The
        # untouched side is shared with the previous version, which is safe
        # because no book dict is ever edited in place after construction --
        # every mutation path copies first, here and in ``_handle_book``.
        # Copying both sides made the cost of a one-level change proportional
        # to total book depth on both sides.
        buy_side = str(change.get("side")).upper() == "BUY"
        if buy_side:
            bids, asks = dict(current.bids), current.asks
            levels = bids
        else:
            bids, asks = current.bids, dict(current.asks)
            levels = asks
        if size == 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        proposed = _BookState(
            condition_id=current.condition_id, bids=bids, asks=asks,
            provider_ts_ms=event.provider_ts_ms,
            book_hash=str(change.get("hash") or ""))
        # The untouched side's best price is already known; carrying it over
        # avoids a scan that provably cannot have changed.
        if buy_side:
            proposed._best_ask = current.best_ask
        else:
            proposed._best_bid = current.best_bid
        # Only worth linking when the previous sorted view actually exists --
        # otherwise there is nothing to derive from and the link would just
        # keep a dead book version alive.
        if current._levels is not None:
            proposed._parent_levels = current._levels
            proposed._delta = (
                buy_side, price, None if size == 0 else size)
        if (proposed.best_bid is not None and proposed.best_ask is not None
                and proposed.best_bid > proposed.best_ask):
            self._books.pop(token, None)
            self._hydrated.discard(token)
            self._track_unhydrated([token])
            decision = EventDecision(
                EventDisposition.REJECT_INVALID, event,
                "crossed_book_after_delta", request_hydration=True)
            await self._publish(event, decision)
            self.health_state.hydrated_subscriptions = len(self._hydrated)
            self.health_state.state = "HYDRATING"
            await self._publish_health()
            await self._request_hydration(
                token, event.condition_id, "crossed_book", force=True)
            return decision
        if not self._reported_bbo_matches(proposed, change):
            self._books.pop(token, None)
            self._hydrated.discard(token)
            self._track_unhydrated([token])
            decision = EventDecision(
                EventDisposition.REJECT_BBO_MISMATCH, event,
                "reported_bbo_disagrees_with_local_delta", request_hydration=True)
            await self._publish(event, decision)
            self.health_state.hydrated_subscriptions = len(self._hydrated)
            self.health_state.state = "HYDRATING"
            await self._publish_health()
            await self._request_hydration(
                token, event.condition_id, "bbo_mismatch", force=True)
            return decision
        decision = self.gate.evaluate(
            event, stream_key=f"book_delta:{token}",
            sequence_policy=SequencePolicy.NONE,
            allow_equal_timestamp_distinct=True,
            max_age_ms=self.max_event_age_ms)
        await self._publish(event, decision)
        if decision.accepted:
            self._install_book(token, proposed)
        return decision

    async def _flush_buffer(self, token: str) -> None:
        queue = self._buffers.pop(token, deque())
        if not queue:
            return
        # Preserve receipt order.  Sorting by provider time would make a late,
        # out-of-order provider event appear valid and would falsify arrival
        # semantics.  Local provider watermarks reject such regressions.
        ordered = sorted(queue, key=lambda item: (
            item[0].receipt_monotonic_ns, item[0].event_key))
        snapshot_ts = self._books[token].provider_ts_ms
        for event, change in ordered:
            if event.provider_ts_ms <= snapshot_ts:
                continue
            if token not in self._hydrated:
                break
            await self._apply_delta(event, change)

    async def _handle_auxiliary(
            self, event_type: str, payload: dict[str, Any],
            receipt_ms: int, receipt_mono: int,
    ) -> Optional[EventDecision]:
        provider_ms = parse_positive_millis(payload.get("timestamp"))
        if provider_ms is None:
            self.health_state.parse_errors += 1
            return None
        condition = str(payload.get("market") or payload.get("condition_id") or "")
        token = str(payload.get("asset_id") or "")
        if event_type == "market_resolved":
            winning = str(payload.get("winning_asset_id") or "")
            known = {tok for tok, cond in self._desired.items() if cond == condition}
            if known and winning not in known:
                token = winning
                event = self._source_event(
                    event_type=event_type, payload=payload,
                    provider_ts_ms=provider_ms, receipt_ts_ms=receipt_ms,
                    receipt_monotonic_ns=receipt_mono, token_id=token,
                    condition_id=condition)
                return await self._publish(event, EventDecision(
                    EventDisposition.REJECT_IDENTITY, event,
                    "resolved_winner_not_in_verified_pair"))
        event = self._source_event(
            event_type=event_type, payload=payload,
            provider_ts_ms=provider_ms, receipt_ts_ms=receipt_ms,
            receipt_monotonic_ns=receipt_mono, token_id=token,
            condition_id=condition)
        if event_type in {"last_trade_price", "tick_size_change", "best_bid_ask"}:
            if not self._identity_ok(token, condition):
                return await self._publish(event, EventDecision(
                    EventDisposition.REJECT_IDENTITY, event,
                    "auxiliary_token_or_condition_mismatch"))
        stream_identity = token or condition or event.event_key
        admitted = self.gate.evaluate(
            event, stream_key=f"{event_type}:{stream_identity}",
            sequence_policy=SequencePolicy.NONE,
            allow_equal_timestamp_distinct=True,
            max_age_ms=self.max_event_age_ms)
        if not admitted.accepted:
            return await self._publish(event, admitted)
        if event_type == "best_bid_ask" and token in self._hydrated:
            book = self._books.get(token)
            if book is not None and not self._reported_bbo_matches(book, payload):
                self._books.pop(token, None)
                self._hydrated.discard(token)
                self._track_unhydrated([token])
                decision = EventDecision(
                    EventDisposition.REJECT_BBO_MISMATCH, event,
                    "best_bid_ask_disagrees_with_local_book",
                    request_hydration=True)
                await self._publish(event, decision)
                self.health_state.hydrated_subscriptions = len(self._hydrated)
                self.health_state.state = "HYDRATING"
                await self._publish_health()
                await self._request_hydration(
                    token, condition, "bbo_event_mismatch", force=True)
                return decision
        return await self._publish(event, admitted)


# Clear, compatibility-friendly name for the v4 runtime wiring.
PolymarketWS = PolymarketMarketWS
