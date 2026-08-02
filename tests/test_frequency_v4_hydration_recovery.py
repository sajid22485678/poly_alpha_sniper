"""Bounded targeted hydration recovery for silent Polymarket tokens.

The defect these cover: a desired active token can receive no websocket frame
at all.  Every pre-existing hydration re-request is triggered *by* an inbound
message for that token -- a delta before the snapshot, a missing local book, a
crossed book, a BBO disagreement -- so a token that says nothing triggers none
of them.  Its one subscribe-time request was the only attempt it would ever
get, and if that attempt did not land the token stayed out of the hydrated set
forever.  Readiness is all-or-nothing over the active desired set, so the whole
source stayed HYDRATING on a live connection with fresh frames for every other
token.

These tests drive the production adapter: the real ``_run`` loop over a scripted
websocket, the real ``_recover_unhydrated_once`` sweep, and the real
``handle_message`` ingest path.  Nothing here sets ``health_state.state`` or
``_hydrated`` by hand to manufacture a verdict.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from lite_frequency_v4.polymarket_ws import PolymarketMarketWS


YES = "token-yes"
NO = "token-no"
THIRD = "token-third"
CONDITION = "condition-1"
OTHER_CONDITION = "condition-2"

#: Fast enough that a test never waits on it, and unrelated to the fake clock
#: that drives every bound under test.
SWEEP_S = 0.005


class Clock:
    """Wall and monotonic time the test moves explicitly."""

    def __init__(self, now_ms: int = 2_000, mono_ns: int = 5_000_000_000):
        self.wall = now_ms
        self.mono = mono_ns

    def now_ms(self) -> int:
        return self.wall

    def monotonic_ns(self) -> int:
        return self.mono

    def advance(self, seconds: float) -> None:
        self.mono += int(seconds * 1_000_000_000)
        self.wall += int(seconds * 1_000)


class ScriptedWs:
    """Websocket whose inbound stream the test feeds frame by frame."""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed = False
        self.inbox: asyncio.Queue = asyncio.Queue()

    async def send(self, value: str) -> None:
        self.sent.append(value)

    async def close(self) -> None:
        self.closed = True
        await self.inbox.put(None)

    async def feed(self, payload: object) -> None:
        await self.inbox.put(json.dumps(payload))

    def __aiter__(self) -> "ScriptedWs":
        return self

    async def __anext__(self) -> str:
        item = await self.inbox.get()
        if item is None:
            raise StopAsyncIteration
        return item

    # --- subscription wire inspection --------------------------------------
    def subscribes(self) -> list[list[str]]:
        out = []
        for raw in self.sent:
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(message, dict):
                continue
            if message.get("operation") == "subscribe":
                out.append(list(message.get("assets_ids") or []))
            elif "assets_ids" in message and message.get("type") == "market":
                out.append(list(message.get("assets_ids") or []))
        return out

    def unsubscribes(self) -> list[list[str]]:
        out = []
        for raw in self.sent:
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(message, dict) and message.get(
                    "operation") == "unsubscribe":
                out.append(list(message.get("assets_ids") or []))
        return out


class Factory:
    """``websockets.connect`` stand-in handing out scripted sockets in order."""

    def __init__(self, *sockets: ScriptedWs) -> None:
        self._queue = list(sockets)
        self.opened: list[ScriptedWs] = []

    def __call__(self, url: str, **_kwargs: object) -> "Factory._Ctx":
        socket = self._queue.pop(0) if self._queue else ScriptedWs()
        self.opened.append(socket)
        return Factory._Ctx(socket)

    class _Ctx:
        def __init__(self, socket: ScriptedWs) -> None:
            self.socket = socket

        async def __aenter__(self) -> ScriptedWs:
            return self.socket

        async def __aexit__(self, *_exc: object) -> bool:
            return False


def book(*, token: str = YES, condition: str = CONDITION, ts: int = 1_500,
         bid: str = "0.55", ask: str = "0.65") -> dict[str, object]:
    return {
        "event_type": "book",
        "asset_id": token,
        "market": condition,
        "timestamp": str(ts),
        "hash": f"book-{token}-{ts}",
        "bids": [{"price": bid, "size": "10"}],
        "asks": [{"price": ask, "size": "10"}],
    }


class Harness:
    """A connected adapter plus the callbacks the engine would provide."""

    def __init__(self, adapter: PolymarketMarketWS, clock: Clock,
                 factory: Factory) -> None:
        self.adapter = adapter
        self.clock = clock
        self.factory = factory
        self.hydration_requests: list[tuple[str, str, str, int]] = []
        self.health: list[dict] = []

    @property
    def ws(self) -> ScriptedWs:
        return self.factory.opened[-1]

    def requests_for(self, token: str) -> list[tuple[str, str, str, int]]:
        return [r for r in self.hydration_requests if r[0] == token]

    def state(self) -> str:
        return str(self.adapter.health().get("state"))

    async def sweep(self) -> list[str]:
        """Run one production recovery sweep."""

        return await self.adapter._recover_unhydrated_once()

    async def settle(self, turns: int = 8) -> None:
        for _ in range(turns):
            await asyncio.sleep(0)


async def _start(tokens: dict[str, str] | None = None,
                 *, sockets: int = 1, **kwargs: object):
    """Bring a real ``_run`` loop up against a scripted socket."""

    clock = Clock()
    factory = Factory(*[ScriptedWs() for _ in range(max(1, sockets))])
    options: dict[str, object] = {
        "clock": clock,
        "websocket_factory": factory,
        "hydration_recovery_interval_s": SWEEP_S,
        "hydration_recovery_after_s": 5.0,
        "hydration_recovery_max_attempts": 3,
        "hydration_recovery_backoff_cap_s": 20.0,
        "heartbeat_interval_s": 3_600.0,
        "pong_timeout_s": 3_600.0,
    }
    options.update(kwargs)
    adapter = PolymarketMarketWS(
        dict(tokens if tokens is not None else {YES: CONDITION, NO: CONDITION}),
        **options)  # type: ignore[arg-type]
    harness = Harness(adapter, clock, factory)

    async def on_hydration_request(token: str, condition: str, reason: str,
                                   epoch: int) -> None:
        harness.hydration_requests.append((token, condition, reason, epoch))

    async def on_health(payload: dict) -> None:
        harness.health.append(dict(payload))

    adapter.on_hydration_request = on_hydration_request
    adapter.on_health = on_health
    await adapter.start()
    # Waiting on `connected` alone is not enough: `_run` sets it, then sends the
    # initial subscribe, stamps the unhydrated clocks and fires the reconnect
    # hydration requests.  The recovery task is created last, so its existence
    # is what says the epoch is fully established.
    await _await_epoch(adapter)
    return harness


async def _await_epoch(adapter: PolymarketMarketWS, *, turns: int = 2_000,
                       epoch_above: int = 0) -> None:
    for _ in range(turns):
        if (adapter.health_state.connected
                and adapter._recovery_task is not None
                and adapter.connection_epoch > epoch_above):
            return
        await asyncio.sleep(0.001)
    raise AssertionError(
        f"adapter never established an epoch above {epoch_above} "
        f"(connected={adapter.health_state.connected}, "
        f"epoch={adapter.connection_epoch})")


# ---------------------------------------------------------------------------
# 1-4: a silent token blocks READY, is re-requested, and recovers.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_silent_active_token_blocks_ready_then_targeted_retry_recovers():
    harness = await _start()
    adapter = harness.adapter
    try:
        # (1)(2) YES speaks, NO never does.  The source must not be READY.
        await adapter.handle_message(json.dumps(book(token=YES)))
        assert adapter.hydrated_tokens == frozenset({YES})
        assert harness.state() == "HYDRATING"
        health = adapter.health()
        assert health["unhydrated_active_tokens"] == 1

        # No re-request before the bounded interval elapses.
        harness.clock.advance(4.0)
        assert await harness.sweep() == []
        assert harness.requests_for(NO) == [(NO, CONDITION, "reconnect", 1)]

        # (3) Past the interval, exactly the missing token is re-requested.
        harness.clock.advance(2.0)
        assert await harness.sweep() == [NO]
        reasons = [r[2] for r in harness.requests_for(NO)]
        assert reasons == ["reconnect", "targeted_hydration_retry"]
        assert adapter.health()["targeted_retry_attempts"] == 1
        assert adapter.health()["oldest_unhydrated_age_ms"] >= 6_000

        # (4) A later valid frame for the silent token completes hydration.
        harness.clock.advance(0.5)
        await adapter.handle_message(
            json.dumps(book(token=NO, ts=harness.clock.wall - 100)))
        assert adapter.hydrated_tokens == frozenset({YES, NO})
        assert harness.state() == "READY"
        health = adapter.health()
        assert health["targeted_retry_successes"] == 1
        assert health["unhydrated_active_tokens"] == 0
        assert health["oldest_unhydrated_age_ms"] == 0
        assert health["hydration_transition_reason"] == "targeted_retry_recovered"
    finally:
        await adapter.stop()


# ---------------------------------------------------------------------------
# 5-7: the retry budget is bounded, backs off deterministically, fails closed.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retry_count_and_backoff_are_bounded_and_exhaustion_fails_closed():
    harness = await _start(hydration_recovery_max_attempts=4,
                           hydration_recovery_after_s=5.0,
                           hydration_recovery_backoff_cap_s=12.0)
    adapter = harness.adapter
    try:
        await adapter.handle_message(json.dumps(book(token=YES)))

        # (6) The first attempt waits out the bounded interval; each later one
        #     waits ``after * 2**(n-1)`` since the previous attempt, clamped to
        #     the cap -- so 5, then 5, then 10, then 12 rather than 20.
        for expected_wait in (5.0, 5.0, 10.0, 12.0):
            harness.clock.advance(expected_wait - 0.5)
            assert await harness.sweep() == [], "retried before its backoff"
            harness.clock.advance(0.5)
            assert await harness.sweep() == [NO]

        # (5) The budget is spent; further sweeps do not retry.
        health = adapter.health()
        assert health["targeted_retry_attempts"] == 4
        for _ in range(4):
            harness.clock.advance(60.0)
            assert await harness.sweep() == []

        # (7) Exhaustion is counted once and stays fail-closed: the token is
        #     still unhydrated and the source is still HYDRATING.
        health = adapter.health()
        assert health["targeted_retry_exhaustions"] == 1
        assert health["targeted_retry_attempts"] == 4
        assert health["hydration_transition_reason"] == "targeted_retry_exhausted"
        assert NO not in adapter.hydrated_tokens
        assert harness.state() == "HYDRATING"
        assert health["unhydrated_active_tokens"] == 1
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_exhausted_token_still_hydrates_from_a_real_frame():
    """Fail-closed must not mean permanently deaf."""

    harness = await _start(hydration_recovery_max_attempts=1)
    adapter = harness.adapter
    try:
        await adapter.handle_message(json.dumps(book(token=YES)))
        harness.clock.advance(6.0)
        assert await harness.sweep() == [NO]
        harness.clock.advance(120.0)
        assert await harness.sweep() == []
        assert adapter.health()["targeted_retry_exhaustions"] == 1

        await adapter.handle_message(
            json.dumps(book(token=NO, ts=harness.clock.wall - 50)))
        assert harness.state() == "READY"
    finally:
        await adapter.stop()


# ---------------------------------------------------------------------------
# 8-9: rotation removes obsolete tokens; active ones can never be dropped.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rotation_drops_the_obsolete_token_and_unblocks_ready():
    harness = await _start()
    adapter = harness.adapter
    try:
        await adapter.handle_message(json.dumps(book(token=YES)))
        harness.clock.advance(6.0)
        await harness.sweep()
        assert harness.state() == "HYDRATING"

        # The silent token's window rolls over and it stops being desired.
        await adapter.set_subscriptions({YES: CONDITION})
        assert harness.state() == "READY"
        health = adapter.health()
        assert health["obsolete_tokens_removed"] == 1
        assert health["unhydrated_active_tokens"] == 0
        assert health["hydration_transition_reason"] == "market_rotation"
        assert harness.ws.unsubscribes() == [[NO]]
        assert NO not in adapter._unhydrated_since_mono_ns
        assert NO not in adapter._targeted_attempts
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_a_still_active_token_is_never_removed_to_manufacture_ready():
    harness = await _start()
    adapter = harness.adapter
    try:
        await adapter.handle_message(json.dumps(book(token=YES)))
        for _ in range(6):
            harness.clock.advance(30.0)
            await harness.sweep()
        # Recovery exhausted its budget and still never dropped the token.
        assert set(adapter._desired) == {YES, NO}
        assert adapter.health()["desired_subscriptions"] == 2
        assert adapter.health()["obsolete_tokens_removed"] == 0
        assert harness.state() == "HYDRATING"
        assert harness.ws.unsubscribes() == []
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_rotation_during_retry_cannot_resurrect_obsolete_state():
    harness = await _start()
    adapter = harness.adapter
    try:
        await adapter.handle_message(json.dumps(book(token=YES)))
        harness.clock.advance(6.0)
        assert await harness.sweep() == [NO]

        # NO rotates out and THIRD rotates in, both under one call.
        await adapter.set_subscriptions({YES: CONDITION, THIRD: CONDITION})
        harness.clock.advance(60.0)
        retried = await harness.sweep()
        assert retried == [THIRD], "a retired token must never be re-requested"
        assert NO not in adapter._targeted_attempts
        assert NO not in adapter._unhydrated_since_mono_ns
        assert NO not in adapter._subscribed_tokens
        # A late frame for the retired token cannot re-enter the hydrated set.
        await adapter.handle_message(
            json.dumps(book(token=NO, ts=harness.clock.wall - 50)))
        assert NO not in adapter.hydrated_tokens
    finally:
        await adapter.stop()


# ---------------------------------------------------------------------------
# 10: reconnect clears and reconstructs hydration state.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reconnect_clears_and_reconstructs_hydration_state():
    harness = await _start(sockets=2)
    adapter = harness.adapter
    try:
        first = harness.ws
        await adapter.handle_message(json.dumps(book(token=YES)))
        harness.clock.advance(6.0)
        await harness.sweep()
        assert adapter.health()["targeted_retry_attempts"] == 1
        epoch_before = adapter.connection_epoch

        await first.close()
        await _await_epoch(adapter, epoch_above=epoch_before)
        assert adapter.health_state.reconnect_count >= 1

        # Every scrap of epoch state is rebuilt, not inherited.
        assert adapter.hydrated_tokens == frozenset()
        assert adapter._targeted_attempts == {}
        assert adapter._targeted_exhausted == set()
        assert set(adapter._subscribed_tokens) == {YES, NO}
        assert adapter.health()["unhydrated_active_tokens"] == 2
        assert harness.ws.subscribes() == [sorted([YES, NO])]
        assert sorted(r[0] for r in harness.hydration_requests
                      if r[3] == adapter.connection_epoch) == sorted([NO, YES])

        # And a fresh budget: the token that had spent one attempt retries again.
        harness.clock.advance(6.0)
        assert await harness.sweep() == sorted([NO, YES])
    finally:
        await adapter.stop()


# ---------------------------------------------------------------------------
# 11-13: no duplicate subscriptions, no reconnect storm, multiple recoveries.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_recovery_emits_no_duplicate_subscription():
    harness = await _start()
    adapter = harness.adapter
    try:
        await adapter.handle_message(json.dumps(book(token=YES)))
        for _ in range(3):
            harness.clock.advance(60.0)
            await harness.sweep()
        # The only subscribe on the wire is the epoch's initial one.
        assert harness.ws.subscribes() == [sorted([YES, NO])]
        health = adapter.health()
        assert health["duplicate_subscriptions_prevented"] == 3
        assert health["targeted_retry_attempts"] == 3
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_a_silent_token_never_causes_a_reconnect_storm():
    harness = await _start()
    adapter = harness.adapter
    try:
        await adapter.handle_message(json.dumps(book(token=YES)))
        for _ in range(10):
            harness.clock.advance(30.0)
            await harness.sweep()
            await harness.settle(2)
        assert len(harness.factory.opened) == 1, "recovery opened a new socket"
        assert adapter.health_state.reconnect_count == 0
        assert adapter.connection_epoch == 1
        assert harness.ws.closed is False
        assert adapter.health_state.connected is True
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_multiple_missing_tokens_recover_deterministically():
    harness = await _start({YES: CONDITION, NO: CONDITION,
                            THIRD: OTHER_CONDITION})
    adapter = harness.adapter
    try:
        await adapter.handle_message(json.dumps(book(token=YES)))
        harness.clock.advance(6.0)
        assert await harness.sweep() == sorted([NO, THIRD])
        assert adapter.health()["targeted_retry_attempts"] == 2
        # Each is re-requested under its own condition id, not a shared one.
        assert harness.requests_for(NO)[-1][1] == CONDITION
        assert harness.requests_for(THIRD)[-1][1] == OTHER_CONDITION

        harness.clock.advance(0.5)
        await adapter.handle_message(
            json.dumps(book(token=NO, ts=harness.clock.wall - 50)))
        assert harness.state() == "HYDRATING"
        assert adapter.health()["unhydrated_active_tokens"] == 1

        await adapter.handle_message(json.dumps(book(
            token=THIRD, condition=OTHER_CONDITION,
            ts=harness.clock.wall - 50)))
        assert harness.state() == "READY"
        assert adapter.health()["targeted_retry_successes"] == 2
    finally:
        await adapter.stop()


# ---------------------------------------------------------------------------
# 15: shutdown drains the retry task.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shutdown_leaves_no_retry_task_running():
    harness = await _start()
    adapter = harness.adapter
    await adapter.handle_message(json.dumps(book(token=YES)))
    harness.clock.advance(6.0)
    await harness.sweep()
    recovery = adapter._recovery_task
    assert recovery is not None and not recovery.done()

    before = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}
    await adapter.stop()
    await harness.settle(10)

    assert adapter._recovery_task is None
    assert adapter._heartbeat_task is None
    assert adapter._task is None
    assert recovery.done()
    assert adapter.health_state.state == "STOPPED"
    leaked = {
        t for t in asyncio.all_tasks()
        if t is not asyncio.current_task() and not t.done()
        and "frequency_v4_poly" in (t.get_name() or "")
    }
    assert leaked == set(), f"leaked adapter tasks: {leaked}"
    assert not [t for t in before if not t.done()
                and "frequency_v4_poly" in (t.get_name() or "")]


@pytest.mark.asyncio
async def test_disconnect_stops_the_recovery_loop_without_leaking():
    harness = await _start(sockets=2)
    adapter = harness.adapter
    try:
        first_recovery = adapter._recovery_task
        assert first_recovery is not None
        epoch_before = adapter.connection_epoch
        await harness.ws.close()
        await _await_epoch(adapter, epoch_above=epoch_before)
        assert first_recovery.done(), "old epoch's recovery task outlived it"
        assert adapter._recovery_task is not first_recovery
        assert adapter._recovery_task is not None
    finally:
        await adapter.stop()
    assert adapter._recovery_task is None


# ---------------------------------------------------------------------------
# 16-17: the frozen five-sample bound, and the same bound without the fix.
# ---------------------------------------------------------------------------

def _hydration_episode_samples(statuses: list[str]) -> int:
    """Longest run of consecutive non-READY samples, the frozen contract's unit."""

    longest = current = 0
    for status in statuses:
        current = current + 1 if status != "READY" else 0
        longest = max(longest, current)
    return longest


async def _sampled_episode(*, recovery_enabled: bool) -> list[str]:
    """Sample the adapter every 30 s while one token stays silent.

    The engine answers a targeted re-request by fetching the book over REST and
    feeding it back through ``accept_rest_book``, which is the production path;
    the fake here answers the same way.  With recovery disabled no re-request is
    ever issued, so nothing feeds it back -- which is the defect.
    """

    harness = await _start(hydration_recovery_after_s=5.0,
                           hydration_recovery_max_attempts=8,
                           hydration_recovery_backoff_cap_s=20.0)
    adapter = harness.adapter
    answered: list[str] = []

    async def on_hydration_request(token: str, condition: str, reason: str,
                                   _epoch: int) -> None:
        harness.hydration_requests.append((token, condition, reason, _epoch))
        if reason != "targeted_hydration_retry":
            return
        answered.append(token)
        await adapter.accept_rest_book(
            book(token=token, condition=condition,
                 ts=harness.clock.wall - 50))

    if recovery_enabled:
        adapter.on_hydration_request = on_hydration_request

    statuses: list[str] = []
    try:
        await adapter.handle_message(json.dumps(book(token=YES)))
        for _ in range(12):  # twelve 30-second samples
            for _ in range(6):  # six 5-second recovery opportunities
                harness.clock.advance(5.0)
                await harness.sweep()
            statuses.append(harness.state())
    finally:
        await adapter.stop()
    return statuses


@pytest.mark.asyncio
async def test_recovery_keeps_hydration_inside_the_frozen_five_sample_bound():
    statuses = await _sampled_episode(recovery_enabled=True)
    assert _hydration_episode_samples(statuses) <= 5
    assert statuses[-1] == "READY"


@pytest.mark.asyncio
async def test_without_targeted_recovery_the_same_run_breaches_the_bound():
    """Reverting the fix reproduces an over-bound HYDRATING episode."""

    statuses = await _sampled_episode(recovery_enabled=False)
    assert _hydration_episode_samples(statuses) > 5
    assert statuses[-1] != "READY"
    assert set(statuses) == {"HYDRATING"}


@pytest.mark.asyncio
async def test_removing_the_sweep_itself_reproduces_the_breach(monkeypatch):
    """The same breach, reverted at the implementation rather than the caller.

    Neutering the sweep is the code-level equivalent of deleting this fix: the
    engine callback is still wired and would still answer, but with nothing
    asking, the silent token never hydrates.
    """

    async def no_sweep(_self) -> list[str]:
        return []

    monkeypatch.setattr(
        PolymarketMarketWS, "_recover_unhydrated_once", no_sweep)
    statuses = await _sampled_episode(recovery_enabled=True)
    assert _hydration_episode_samples(statuses) > 5
    assert statuses[-1] != "READY"


# ---------------------------------------------------------------------------
# 18: nothing else moved.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_recovery_does_not_disturb_ordering_or_stale_rejection():
    harness = await _start()
    adapter = harness.adapter
    try:
        await adapter.handle_message(json.dumps(book(token=YES, ts=1_800)))
        await adapter.handle_message(json.dumps(book(token=NO, ts=1_800)))
        assert harness.state() == "READY"
        baseline = adapter.book_state(YES)["provider_ts_ms"]

        # A stale snapshot is still rejected and still cannot regress the book.
        await adapter.handle_message(
            json.dumps(book(token=YES, ts=harness.clock.wall - 30_000)))
        assert adapter.book_state(YES)["provider_ts_ms"] == baseline
        # An older-than-local snapshot is still a timestamp regression.
        await adapter.handle_message(json.dumps(book(token=YES, ts=1_700)))
        assert adapter.book_state(YES)["provider_ts_ms"] == baseline
        # A future snapshot is still refused, and never inflates the counter.
        await adapter.handle_message(
            json.dumps(book(token=YES, ts=harness.clock.wall + 10_000)))
        assert adapter.book_state(YES)["provider_ts_ms"] == baseline

        assert harness.state() == "READY"
        health = adapter.health()
        assert health["targeted_retry_attempts"] == 0
        assert health["unhydrated_active_tokens"] == 0
        # A healthy source runs the sweep and does nothing at all.
        harness.clock.advance(600.0)
        assert await harness.sweep() == []
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_recovery_never_runs_while_disconnected():
    harness = await _start()
    adapter = harness.adapter
    try:
        await adapter.handle_message(json.dumps(book(token=YES)))
        adapter.health_state.connected = False
        harness.clock.advance(600.0)
        assert await harness.sweep() == []
        assert adapter.health()["targeted_retry_attempts"] == 0
    finally:
        adapter.health_state.connected = True
        await adapter.stop()


def test_recovery_bounds_are_validated() -> None:
    for bad in ({"hydration_recovery_interval_s": 0.0},
                {"hydration_recovery_after_s": -1.0},
                {"hydration_recovery_max_attempts": 0},
                {"hydration_recovery_backoff_cap_s": 0.5}):
        with pytest.raises(ValueError):
            PolymarketMarketWS({YES: CONDITION}, **bad)  # type: ignore[arg-type]


def test_health_publishes_every_bounded_recovery_counter() -> None:
    health = PolymarketMarketWS({YES: CONDITION}).health()
    for field in ("unhydrated_active_tokens", "oldest_unhydrated_age_ms",
                  "targeted_retry_attempts", "targeted_retry_successes",
                  "targeted_retry_exhaustions", "obsolete_tokens_removed",
                  "duplicate_subscriptions_prevented",
                  "frame_rejections_book_preserved",
                  "authoritative_invalidations",
                  "duplicate_hydration_requests_suppressed",
                  "hydration_transition_reason"):
        assert field in health, field


# ===========================================================================
# Frame rejection must not clear hydration.
#
# The controlled gate at 4320fd6 failed the frozen five-sample hydration bound
# with polymarket READY in only 23 of 45 samples.  High-frequency observation
# showed no token was ever unhydrated for more than 328 ms; *different* tokens
# flickered constantly because every BBO disagreement or crossed delta dropped
# a still-valid canonical book and cleared that token's hydration.  Readiness is
# all-or-nothing across fourteen active tokens, so the source was READY only 54%
# of the time and a run of six 30-second polls each caught a momentary flicker.
# ===========================================================================

def price_change(*, token: str = YES, condition: str = CONDITION,
                 ts: int, price: str = "0.50", size: str = "5",
                 side: str = "BUY", best_bid: str, best_ask: str,
                 suffix: str = "1") -> dict[str, object]:
    return {
        "event_type": "price_change",
        "market": condition,
        "timestamp": str(ts),
        "price_changes": [{
            "asset_id": token, "price": price, "size": size, "side": side,
            "hash": f"delta-{token}-{suffix}",
            "best_bid": best_bid, "best_ask": best_ask,
        }],
    }


def best_bid_ask(*, token: str = YES, condition: str = CONDITION, ts: int,
                 best_bid: str, best_ask: str) -> dict[str, object]:
    return {
        "event_type": "best_bid_ask", "asset_id": token, "market": condition,
        "timestamp": str(ts), "best_bid": best_bid, "best_ask": best_ask,
    }


async def _both_hydrated(harness: Harness) -> None:
    adapter = harness.adapter
    for token in (YES, NO):
        await adapter.handle_message(
            json.dumps(book(token=token, ts=harness.clock.wall - 100)))
    assert harness.state() == "READY"


@pytest.mark.asyncio
async def test_bbo_mismatch_frame_preserves_ready_and_the_prior_book():
    harness = await _start()
    adapter = harness.adapter
    try:
        await _both_hydrated(harness)
        before = adapter.current_book(YES)

        harness.clock.advance(0.2)
        decisions = await adapter.handle_message(json.dumps(price_change(
            ts=harness.clock.wall - 50, best_bid="0.59", best_ask="0.65")))
        assert decisions[0].disposition.name == "REJECT_BBO_MISMATCH"

        # (1)(3) frame dropped, book kept, hydration kept, still READY.
        assert harness.state() == "READY"
        assert YES in adapter.hydrated_tokens
        assert adapter.current_book(YES)["bids"] == before["bids"]
        health = adapter.health()
        assert health["frame_rejections_book_preserved"] == 1
        assert health["authoritative_invalidations"] == 0
        assert health["unhydrated_active_tokens"] == 0
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_timestamp_regression_frame_preserves_ready_and_the_prior_book():
    harness = await _start()
    adapter = harness.adapter
    try:
        await _both_hydrated(harness)
        before = adapter.current_book(YES)

        # (2)(3) a delta older than the local book is refused outright and
        #        cannot regress or invalidate anything.  The book was seeded at
        #        wall-100, so wall-500 is strictly older and still a valid
        #        positive provider timestamp.
        decisions = await adapter.handle_message(json.dumps(price_change(
            ts=harness.clock.wall - 500, best_bid="0.55", best_ask="0.65",
            suffix="old")))
        assert decisions[0].disposition.name == "REJECT_TIMESTAMP_REGRESSION"
        assert harness.state() == "READY"
        assert YES in adapter.hydrated_tokens
        assert adapter.current_book(YES)["bids"] == before["bids"]
        assert adapter.health()["authoritative_invalidations"] == 0

        # Nor can an older snapshot overwrite the newer local book.
        await adapter.handle_message(
            json.dumps(book(token=YES, ts=harness.clock.wall - 800,
                            bid="0.10")))
        assert harness.state() == "READY"
        assert YES in adapter.hydrated_tokens
        assert adapter.current_book(YES)["bids"] == before["bids"]
        assert adapter.health()["authoritative_invalidations"] == 0
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_repeated_transient_rejections_never_leave_ready():
    """(11) The exact shape that produced the six-sample episode."""

    harness = await _start()
    adapter = harness.adapter
    try:
        await _both_hydrated(harness)
        states: list[str] = []
        # Two rejections, then a clean delta that resets the run -- repeated far
        # more times than the gate's six samples covered.
        for cycle in range(40):
            for strike in range(adapter.book_desync_strikes - 1):
                harness.clock.advance(0.05)
                await adapter.handle_message(json.dumps(price_change(
                    ts=harness.clock.wall - 20, best_bid="0.59",
                    best_ask="0.65", suffix=f"c{cycle}s{strike}")))
                states.append(harness.state())
            harness.clock.advance(0.05)
            await adapter.handle_message(json.dumps(price_change(
                ts=harness.clock.wall - 20, price="0.55", size="12",
                best_bid="0.55", best_ask="0.65", suffix=f"c{cycle}ok")))
            states.append(harness.state())

        assert set(states) == {"READY"}, "a transient rejection left READY"
        health = adapter.health()
        assert health["authoritative_invalidations"] == 0
        assert health["frame_rejections_book_preserved"] == 40 * (
            adapter.book_desync_strikes - 1)
        # (9) and no request storm: a preserved book asks for nothing.
        assert harness.requests_for(YES) == [(YES, CONDITION, "reconnect", 1)]
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_confirmed_desync_invalidates_only_the_affected_token():
    """(4) and (10): a genuine divergence is still fail-closed."""

    harness = await _start()
    adapter = harness.adapter
    try:
        await _both_hydrated(harness)
        for strike in range(adapter.book_desync_strikes):
            harness.clock.advance(0.05)
            await adapter.handle_message(json.dumps(price_change(
                ts=harness.clock.wall - 20, best_bid="0.59", best_ask="0.65",
                suffix=f"s{strike}")))

        assert harness.state() == "HYDRATING"
        assert YES not in adapter.hydrated_tokens
        assert adapter.current_book(YES) == {}
        # Only the affected token: the other one keeps its book and hydration.
        assert NO in adapter.hydrated_tokens
        assert adapter.current_book(NO) != {}
        health = adapter.health()
        assert health["authoritative_invalidations"] == 1
        assert health["unhydrated_active_tokens"] == 1
        assert health["hydration_transition_reason"].startswith(
            "confirmed_desync:")
        # Exactly one resync request for the invalidated token.
        retries = [r for r in harness.requests_for(YES) if r[2] != "reconnect"]
        assert len(retries) == 1

        # Fail-closed until a valid canonical book is rebuilt.
        harness.clock.advance(0.05)
        await adapter.handle_message(json.dumps(price_change(
            ts=harness.clock.wall - 20, best_bid="0.55", best_ask="0.65",
            suffix="after")))
        assert harness.state() == "HYDRATING"
        await adapter.handle_message(
            json.dumps(book(token=YES, ts=harness.clock.wall - 50)))
        assert harness.state() == "READY"
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_best_bid_ask_disagreement_also_preserves_the_book():
    harness = await _start()
    adapter = harness.adapter
    try:
        await _both_hydrated(harness)
        harness.clock.advance(0.2)
        await adapter.handle_message(json.dumps(best_bid_ask(
            ts=harness.clock.wall - 50, best_bid="0.11", best_ask="0.22")))
        assert harness.state() == "READY"
        assert YES in adapter.hydrated_tokens
        assert adapter.health()["frame_rejections_book_preserved"] == 1
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_one_outstanding_hydration_request_per_token_and_epoch():
    """(8) The 4,968-request storm, closed."""

    harness = await _start({YES: CONDITION})
    adapter = harness.adapter
    try:
        # YES has no book yet, so every delta hits the missing-book path, which
        # used to re-request hydration for each one.
        for index in range(60):
            harness.clock.advance(0.05)
            await adapter.handle_message(json.dumps(price_change(
                ts=harness.clock.wall - 20, best_bid="0.55", best_ask="0.65",
                suffix=f"b{index}")))

        requests = harness.requests_for(YES)
        assert len(requests) == 1, requests            # the epoch's reconnect one
        assert adapter.health()["duplicate_hydration_requests_suppressed"] >= 59
        assert len(adapter._resync_inflight) == 1

        # (7) the buffered deltas are bounded, not accumulated without limit.
        assert (len(adapter._buffers.get(YES, ()))
                <= adapter.max_buffered_deltas_per_token)

        # Hydrating the token releases the guard and replays the buffer.
        await adapter.handle_message(
            json.dumps(book(token=YES, ts=harness.clock.wall - 50)))
        assert harness.state() == "READY"
        assert adapter._resync_inflight == set()
        assert not adapter._buffers.get(YES)
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_reconnect_and_rotation_clear_the_new_state_too():
    """(5)(6) epoch and rotation reset every new per-token structure."""

    harness = await _start(sockets=2)
    adapter = harness.adapter
    try:
        await _both_hydrated(harness)
        harness.clock.advance(0.05)
        await adapter.handle_message(json.dumps(price_change(
            ts=harness.clock.wall - 20, best_bid="0.59", best_ask="0.65")))
        assert adapter._desync_strikes.get(YES) == 1

        # Rotation drops the token's strike record with everything else.
        await adapter.set_subscriptions({NO: CONDITION, THIRD: CONDITION})
        assert YES not in adapter._desync_strikes
        assert YES not in adapter._resync_inflight

        epoch_before = adapter.connection_epoch
        await harness.ws.close()
        await _await_epoch(adapter, epoch_above=epoch_before)
        assert adapter._desync_strikes == {}
        assert adapter.hydrated_tokens == frozenset()
        # The epoch cleared every request, then issued exactly one per desired
        # token -- which is the invariant, not an empty set.
        assert adapter._resync_inflight == set(adapter._desired) == {NO, THIRD}
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_preserving_rejections_leave_no_task_leak_at_shutdown():
    """(12) with the new paths exercised first."""

    harness = await _start()
    adapter = harness.adapter
    await _both_hydrated(harness)
    for strike in range(adapter.book_desync_strikes):
        harness.clock.advance(0.05)
        await adapter.handle_message(json.dumps(price_change(
            ts=harness.clock.wall - 20, best_bid="0.59", best_ask="0.65",
            suffix=f"leak{strike}")))
    await adapter.stop()
    await harness.settle(10)
    assert adapter._recovery_task is None
    assert adapter._heartbeat_task is None
    assert adapter._task is None
    leaked = {t for t in asyncio.all_tasks()
              if t is not asyncio.current_task() and not t.done()
              and "frequency_v4_poly" in (t.get_name() or "")}
    assert leaked == set(), f"leaked adapter tasks: {leaked}"


def test_desync_and_stale_bounds_are_validated() -> None:
    for bad in ({"book_desync_strikes": 0}, {"book_stale_after_ms": 0}):
        with pytest.raises(ValueError):
            PolymarketMarketWS({YES: CONDITION}, **bad)  # type: ignore[arg-type]
