"""Incremental Polymarket book maintenance must equal the full rebuild.

The adapter no longer re-sorts both sides of a book on every delta, and the
engine no longer rebuilds a ``BookLevel`` for every level of every event.  Both
optimizations are only admissible if the observable result is byte-for-byte what
the previous full-rebuild code produced, so the tests here compare against an
independent reference computed from the authoritative ``Decimal`` dictionaries
rather than against the optimized code's own intermediate state.
"""
from __future__ import annotations

import json
import random
from decimal import Decimal

import pytest

from poly_alpha_sniper.lite_frequency_v4.books import (
    NoBookReason,
    compact_book_evidence,
    five_share_buy_sweep,
    five_share_sell_sweep,
    microprice,
    multi_level_imbalance,
    normalize_book,
)
from poly_alpha_sniper.lite_frequency_v4.contracts import (
    BookLevel,
    BookLevelCache,
    BookState,
)
from poly_alpha_sniper.lite_frequency_v4.engine import FrequencyV4Engine
from poly_alpha_sniper.lite_frequency_v4.events import (
    EventDisposition,
    canonical_json,
    canonical_payload_hash,
    canonical_payload_hash_of,
)
from poly_alpha_sniper.lite_frequency_v4.polymarket_ws import (
    PolymarketMarketWS,
    _splice_sorted,
)


TOKEN = "token-yes"
OTHER_TOKEN = "token-no"
CONDITION = "condition-1"


class Clock:
    def __init__(self) -> None:
        self.wall = 1_000_000
        self.mono = 1_000_000_000

    def now_ms(self) -> int:
        return self.wall

    def monotonic_ns(self) -> int:
        return self.mono

    def advance(self, ms: int) -> None:
        self.wall += ms
        self.mono += ms * 1_000_000


def adapter(clock: Clock) -> PolymarketMarketWS:
    stream = PolymarketMarketWS(
        {TOKEN: CONDITION, OTHER_TOKEN: CONDITION}, clock=clock)
    stream._new_connection_epoch()
    return stream


def reference_levels(book) -> tuple[tuple, tuple]:
    """Rebuild the sorted view straight from the authoritative dictionaries.

    This is the pre-optimization code path, kept here as the oracle.  It never
    consults the memoized view, so agreement between the two is real evidence
    rather than a tautology.
    """

    return (
        tuple((float(price), float(book.bids[price]))
              for price in sorted(book.bids, reverse=True)),
        tuple((float(price), float(book.asks[price]))
              for price in sorted(book.asks)),
    )


def assert_matches_reference(stream: PolymarketMarketWS, token: str = TOKEN) -> None:
    state = stream._books[token]
    expected_bids, expected_asks = reference_levels(state)
    current = stream.current_book(token)
    assert tuple(current["bids"]) == expected_bids
    assert tuple(current["asks"]) == expected_asks
    prices = [level[0] for level in current["bids"]]
    assert prices == sorted(prices, reverse=True)
    prices = [level[0] for level in current["asks"]]
    assert prices == sorted(prices)


def snapshot_message(clock: Clock, bids, asks, *, token: str = TOKEN,
                     tag: str = "s0") -> str:
    return json.dumps({
        "event_type": "book", "asset_id": token, "market": CONDITION,
        "timestamp": str(clock.wall), "hash": tag,
        "bids": [{"price": str(p), "size": str(s)} for p, s in bids],
        "asks": [{"price": str(p), "size": str(s)} for p, s in asks],
    })


def delta_message(clock: Clock, price, size, side, best_bid, best_ask, *,
                  token: str = TOKEN, tag: str = "d0") -> str:
    return json.dumps({
        "event_type": "price_change", "market": CONDITION,
        "timestamp": str(clock.wall),
        "price_changes": [{
            "asset_id": token, "price": str(price), "size": str(size),
            "side": side, "hash": tag,
            "best_bid": str(best_bid), "best_ask": str(best_ask),
        }],
    })


async def hydrate(stream: PolymarketMarketWS, clock: Clock, bids, asks,
                  *, token: str = TOKEN) -> None:
    clock.advance(10)
    await stream.handle_message(
        snapshot_message(clock, bids, asks, token=token),
        receipt_ts_ms=clock.wall, receipt_monotonic_ns=clock.mono)


async def apply(stream: PolymarketMarketWS, clock: Clock, price, size, side,
                best_bid, best_ask, *, token: str = TOKEN, tag: str = "d") -> None:
    clock.advance(5)
    await stream.handle_message(
        delta_message(clock, price, size, side, best_bid, best_ask,
                      token=token, tag=tag),
        receipt_ts_ms=clock.wall, receipt_monotonic_ns=clock.mono)


# ---------------------------------------------------------------------------
# 1. Incremental maintenance equals the full rebuild under random traffic.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("seed", [1, 7, 13, 29, 101])
@pytest.mark.asyncio
async def test_random_delta_stream_matches_full_rebuild(seed: int) -> None:
    rng = random.Random(seed)
    clock = Clock()
    stream = adapter(clock)
    depth = 10
    bids = [(Decimal("0.400") - Decimal(i) / 1000, Decimal("10")) for i in range(depth)]
    asks = [(Decimal("0.600") + Decimal(i) / 1000, Decimal("10")) for i in range(depth)]
    await hydrate(stream, clock, bids, asks)
    assert TOKEN in stream.hydrated_tokens

    for step in range(120):
        state = stream._books.get(TOKEN)
        if state is None:
            break
        best_bid = max(state.bids) if state.bids else Decimal("0")
        best_ask = min(state.asks) if state.asks else Decimal("1")
        buy = rng.random() < 0.5
        inner = [p for p in (state.bids if buy else state.asks)
                 if p != (best_bid if buy else best_ask)]
        roll = rng.random()
        if roll < 0.35 and inner:                      # update in place
            price, size = rng.choice(inner), Decimal(rng.randint(1, 99))
        elif roll < 0.60 and inner:                    # delete existing
            price, size = rng.choice(inner), Decimal("0")
        elif roll < 0.85:                              # insert new level
            offset = Decimal(rng.randint(1, 300)) / 1000
            price = best_bid - offset if buy else best_ask + offset
            if not Decimal("0") < price < Decimal("1"):
                continue
            size = Decimal(rng.randint(1, 99))
        else:                                          # delete absent price
            price = (best_bid - Decimal("0.0007") if buy
                     else best_ask + Decimal("0.0007"))
            if not Decimal("0") < price < Decimal("1"):
                continue
            size = Decimal("0")
        # Reading the book materializes the sorted view, which is what arms the
        # splice path for the next delta.  Doing it only sometimes exercises
        # both the spliced and the rebuilt branch in one run.
        if rng.random() < 0.7:
            stream.current_book(TOKEN)
        await apply(stream, clock, price, size, "BUY" if buy else "SELL",
                    best_bid, best_ask, tag=f"d{step}")
        if stream._books.get(TOKEN) is not None:
            assert_matches_reference(stream)


# ---------------------------------------------------------------------------
# 2-4. Insert, update, delete, repeated writes to one price, empty sides.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_insert_update_and_delete_single_levels() -> None:
    clock = Clock()
    stream = adapter(clock)
    await hydrate(stream, clock,
                  [("0.55", "10"), ("0.50", "6")],
                  [("0.65", "10"), ("0.70", "7")])
    assert stream.current_book(TOKEN)["bids"] == [(0.55, 10.0), (0.50, 6.0)]

    await apply(stream, clock, "0.52", "4", "BUY", "0.55", "0.65", tag="ins")
    assert stream.current_book(TOKEN)["bids"] == [
        (0.55, 10.0), (0.52, 4.0), (0.50, 6.0)]
    assert_matches_reference(stream)

    await apply(stream, clock, "0.52", "9", "BUY", "0.55", "0.65", tag="upd")
    assert stream.current_book(TOKEN)["bids"] == [
        (0.55, 10.0), (0.52, 9.0), (0.50, 6.0)]
    assert_matches_reference(stream)

    await apply(stream, clock, "0.52", "0", "BUY", "0.55", "0.65", tag="del")
    assert stream.current_book(TOKEN)["bids"] == [(0.55, 10.0), (0.50, 6.0)]
    assert_matches_reference(stream)

    # Deleting a price that is not present is a no-op, not a corruption.
    await apply(stream, clock, "0.31", "0", "BUY", "0.55", "0.65", tag="noop")
    assert stream.current_book(TOKEN)["bids"] == [(0.55, 10.0), (0.50, 6.0)]
    assert_matches_reference(stream)


@pytest.mark.asyncio
async def test_repeated_writes_to_the_same_price_keep_one_level() -> None:
    clock = Clock()
    stream = adapter(clock)
    await hydrate(stream, clock, [("0.55", "10")], [("0.65", "10")])
    for index, size in enumerate(("3", "8", "1", "44", "12")):
        await apply(stream, clock, "0.50", size, "BUY", "0.55", "0.65",
                    tag=f"r{index}")
        stream.current_book(TOKEN)
    book = stream.current_book(TOKEN)
    assert book["bids"] == [(0.55, 10.0), (0.50, 12.0)]
    assert len(stream._books[TOKEN].bids) == 2
    assert_matches_reference(stream)


@pytest.mark.asyncio
async def test_side_can_empty_and_refill_without_losing_ordering() -> None:
    clock = Clock()
    stream = adapter(clock)
    await hydrate(stream, clock, [("0.55", "10")], [("0.65", "10")])
    stream.current_book(TOKEN)
    # Empty the ask side entirely; the reported best ask becomes the 1.0 float.
    await apply(stream, clock, "0.65", "0", "SELL", "0.55", "1", tag="empty")
    assert stream.current_book(TOKEN)["asks"] == []
    assert stream._books[TOKEN].best_ask is None
    assert_matches_reference(stream)

    await apply(stream, clock, "0.70", "5", "SELL", "0.55", "0.70", tag="refill")
    assert stream.current_book(TOKEN)["asks"] == [(0.70, 5.0)]
    assert_matches_reference(stream)

    await apply(stream, clock, "0.66", "2", "SELL", "0.55", "0.66", tag="front")
    assert stream.current_book(TOKEN)["asks"] == [(0.66, 2.0), (0.70, 5.0)]
    assert_matches_reference(stream)


# ---------------------------------------------------------------------------
# 5-6. Deep books, and a snapshot arriving after deltas.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_deep_book_splices_at_every_position() -> None:
    clock = Clock()
    stream = adapter(clock)
    depth = 60
    bids = [(Decimal("0.400") - Decimal(i) / 1000, Decimal(10 + i))
            for i in range(depth)]
    asks = [(Decimal("0.600") + Decimal(i) / 1000, Decimal(10 + i))
            for i in range(depth)]
    await hydrate(stream, clock, bids, asks)
    stream.current_book(TOKEN)
    for index in range(1, depth):
        await apply(stream, clock, bids[index][0], Decimal(500 + index),
                    "BUY", bids[0][0], asks[0][0], tag=f"deep{index}")
        assert_matches_reference(stream)
    current = stream.current_book(TOKEN)
    assert len(current["bids"]) == depth
    assert len(current["asks"]) == depth


@pytest.mark.asyncio
async def test_snapshot_after_deltas_replaces_the_book_authoritatively() -> None:
    clock = Clock()
    stream = adapter(clock)
    await hydrate(stream, clock,
                  [("0.55", "10"), ("0.50", "6")],
                  [("0.65", "10")])
    await apply(stream, clock, "0.52", "4", "BUY", "0.55", "0.65", tag="pre")
    stream.current_book(TOKEN)
    stale_version = stream.current_book(TOKEN)["book_version"]

    await hydrate(stream, clock, [("0.44", "3")], [("0.71", "8")])
    fresh = stream.current_book(TOKEN)
    assert fresh["bids"] == [(0.44, 3.0)]
    assert fresh["asks"] == [(0.71, 8.0)]
    assert fresh["book_version"] > stale_version
    assert_matches_reference(stream)
    # Nothing from the pre-snapshot book survives into the new version.
    assert set(stream._books[TOKEN].bids) == {Decimal("0.44")}


# ---------------------------------------------------------------------------
# 7-8. Stale updates and the resync paths remain exactly as before.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_stale_delta_is_rejected_and_leaves_the_book_untouched() -> None:
    clock = Clock()
    stream = adapter(clock)
    await hydrate(stream, clock, [("0.55", "10")], [("0.65", "10")])
    before = stream.current_book(TOKEN)

    book_ts = stream._books[TOKEN].provider_ts_ms
    clock.advance(5)

    def older(ts: int, tag: str) -> str:
        return json.dumps({
            "event_type": "price_change", "market": CONDITION,
            "timestamp": str(ts),
            "price_changes": [{
                "asset_id": TOKEN, "price": "0.50", "size": "9", "side": "BUY",
                "hash": tag, "best_bid": "0.55", "best_ask": "0.65",
            }],
        })

    # Older than the local book but inside the age budget: a regression.
    decisions = await stream.handle_message(
        older(book_ts - 1, "regressed"),
        receipt_ts_ms=clock.wall, receipt_monotonic_ns=clock.mono)
    assert [d.disposition for d in decisions] == [
        EventDisposition.REJECT_TIMESTAMP_REGRESSION]

    # Beyond the age budget: rejected earlier, before the book is consulted.
    decisions = await stream.handle_message(
        older(book_ts - 500_000, "aged"),
        receipt_ts_ms=clock.wall, receipt_monotonic_ns=clock.mono)
    assert [d.disposition for d in decisions] == [EventDisposition.REJECT_STALE]

    after = stream.current_book(TOKEN)
    assert after["bids"] == before["bids"]
    assert after["asks"] == before["asks"]
    assert after["book_version"] == before["book_version"]


@pytest.mark.asyncio
async def test_crossed_delta_drops_the_book_and_requests_resync() -> None:
    requests: list[tuple[object, ...]] = []
    clock = Clock()
    stream = PolymarketMarketWS(
        {TOKEN: CONDITION, OTHER_TOKEN: CONDITION}, clock=clock,
        on_hydration_request=lambda *args: requests.append(args))
    stream._new_connection_epoch()
    await hydrate(stream, clock, [("0.55", "10")], [("0.65", "10")])
    stream.current_book(TOKEN)

    # One crossing delta is refused without discarding the valid book it failed
    # to update, and without dropping the subscription's hydration.
    await apply(stream, clock, "0.80", "5", "BUY", "0.80", "0.65", tag="crossed")
    assert stream.current_book(TOKEN)["bids"] == [(0.55, 10.0)]
    assert TOKEN in stream.hydrated_tokens

    # A run of them proves the local book diverged; only then is it invalidated.
    for index in range(2, stream.book_desync_strikes + 1):
        await apply(stream, clock, "0.80", "5", "BUY", "0.80", "0.65",
                    tag=f"crossed{index}")
    assert stream.current_book(TOKEN) == {}
    assert TOKEN not in stream.hydrated_tokens
    assert any("confirmed_desync" in str(entry) for entry in requests)


@pytest.mark.asyncio
async def test_bbo_mismatch_drops_the_book_and_requests_resync() -> None:
    requests: list[tuple[object, ...]] = []
    clock = Clock()
    stream = PolymarketMarketWS(
        {TOKEN: CONDITION, OTHER_TOKEN: CONDITION}, clock=clock,
        on_hydration_request=lambda *args: requests.append(args))
    stream._new_connection_epoch()
    await hydrate(stream, clock, [("0.55", "10")], [("0.65", "10")])
    stream.current_book(TOKEN)

    # Local book will have best bid 0.55, but the provider reports 0.58.
    await apply(stream, clock, "0.50", "5", "BUY", "0.58", "0.65", tag="bbo")
    assert stream.current_book(TOKEN)["bids"] == [(0.55, 10.0)]
    assert TOKEN in stream.hydrated_tokens

    for index in range(2, stream.book_desync_strikes + 1):
        await apply(stream, clock, "0.50", "5", "BUY", "0.58", "0.65",
                    tag=f"bbo{index}")
    assert stream.current_book(TOKEN) == {}
    assert TOKEN not in stream.hydrated_tokens
    assert any("confirmed_desync" in str(entry) for entry in requests)


@pytest.mark.asyncio
async def test_reconnect_clears_books_and_versions_never_repeat() -> None:
    clock = Clock()
    stream = adapter(clock)
    await hydrate(stream, clock, [("0.55", "10")], [("0.65", "10")])
    first_version = stream.current_book(TOKEN)["book_version"]

    stream._new_connection_epoch()
    assert stream.current_book(TOKEN) == {}
    assert TOKEN not in stream.hydrated_tokens

    await hydrate(stream, clock, [("0.55", "10")], [("0.65", "10")])
    second_version = stream.current_book(TOKEN)["book_version"]
    # A recycled version number would let a consumer serve pre-reconnect levels.
    assert second_version > first_version


@pytest.mark.asyncio
async def test_unsubscribe_clears_the_book_and_a_later_book_gets_a_new_version() -> None:
    clock = Clock()
    stream = adapter(clock)
    await hydrate(stream, clock, [("0.55", "10")], [("0.65", "10")])
    version = stream.current_book(TOKEN)["book_version"]
    await stream.set_subscriptions({OTHER_TOKEN: CONDITION})
    assert stream.current_book(TOKEN) == {}

    await stream.set_subscriptions({TOKEN: CONDITION, OTHER_TOKEN: CONDITION})
    await hydrate(stream, clock, [("0.55", "10")], [("0.65", "10")])
    assert stream.current_book(TOKEN)["book_version"] > version


# ---------------------------------------------------------------------------
# 9-11. Reuse is only ever applied to levels that genuinely did not change.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_untouched_side_is_reused_by_identity_and_touched_side_is_not() -> None:
    clock = Clock()
    stream = adapter(clock)
    await hydrate(stream, clock,
                  [("0.55", "10"), ("0.50", "6")],
                  [("0.65", "10"), ("0.70", "7")])
    stream.current_book(TOKEN)
    before = stream._books[TOKEN]
    before_bids, before_asks = before.levels()

    await apply(stream, clock, "0.50", "9", "BUY", "0.55", "0.65", tag="one")
    after = stream._books[TOKEN]
    after_bids, after_asks = after.levels()

    # Untouched side: same tuple object, so no rebuild happened at all.
    assert after_asks is before_asks
    # Touched side: a genuinely different tuple carrying the new size.
    assert after_bids is not before_bids
    assert after_bids == ((0.55, 10.0), (0.50, 9.0))
    # The unchanged entry inside the touched side is carried over by identity.
    assert after_bids[0] is before_bids[0]
    # The changed entry is a fresh tuple, never the stale one.
    assert after_bids[1] is not before_bids[1]
    assert before_bids == ((0.55, 10.0), (0.50, 6.0))


@pytest.mark.asyncio
async def test_previously_published_book_is_never_mutated_later() -> None:
    clock = Clock()
    stream = adapter(clock)
    await hydrate(stream, clock,
                  [("0.55", "10"), ("0.50", "6")],
                  [("0.65", "10"), ("0.70", "7")])
    published = stream.current_book(TOKEN)
    frozen_bids = list(published["bids"])
    frozen_asks = list(published["asks"])
    old_state = stream._books[TOKEN]
    old_bids_snapshot = dict(old_state.bids)
    old_asks_snapshot = dict(old_state.asks)

    for index in range(6):
        side = "BUY" if index % 2 == 0 else "SELL"
        price = "0.50" if side == "BUY" else "0.70"
        await apply(stream, clock, price, str(20 + index), side,
                    "0.55", "0.65", tag=f"m{index}")
        stream.current_book(TOKEN)

    assert published["bids"] == frozen_bids
    assert published["asks"] == frozen_asks
    assert old_state.bids == old_bids_snapshot
    assert old_state.asks == old_asks_snapshot
    assert old_state.levels() == (tuple(frozen_bids), tuple(frozen_asks))
    assert old_state.best_bid == Decimal("0.55")
    assert old_state.best_ask == Decimal("0.65")


@pytest.mark.asyncio
async def test_returned_lists_are_caller_owned() -> None:
    clock = Clock()
    stream = adapter(clock)
    await hydrate(stream, clock, [("0.55", "10")], [("0.65", "10")])
    first = stream.current_book(TOKEN)
    second = stream.current_book(TOKEN)
    assert first["bids"] is not second["bids"]
    first["bids"].append((0.01, 1.0))
    first["asks"].clear()
    third = stream.current_book(TOKEN)
    assert third["bids"] == [(0.55, 10.0)]
    assert third["asks"] == [(0.65, 10.0)]


@pytest.mark.asyncio
async def test_distinct_decimals_collapsing_onto_one_float_fall_back_to_rebuild() -> None:
    """Two prices that differ as ``Decimal`` but not as ``float``.

    The splice keys on the float, so it cannot represent this book; the length
    guard must detect that and fall back to the authoritative rebuild rather
    than silently dropping a level.
    """

    clock = Clock()
    stream = adapter(clock)
    twin = Decimal("0.29999999999999998889776975374843")
    assert twin != Decimal("0.3") and float(twin) == float(Decimal("0.3"))
    await hydrate(stream, clock,
                  [("0.3", "10"), (twin, "20"), ("0.20", "5")],
                  [("0.65", "10")])
    stream.current_book(TOKEN)
    assert len(stream._books[TOKEN].bids) == 3

    await apply(stream, clock, "0.20", "9", "BUY", "0.3", "0.65", tag="twin")
    assert_matches_reference(stream)
    assert len(stream.current_book(TOKEN)["bids"]) == 3


def test_splice_sorted_preserves_ordering_at_every_insertion_point() -> None:
    bids = ((0.6, 1.0), (0.5, 1.0), (0.4, 1.0))
    assert _splice_sorted(bids, 0.7, 2.0, descending=True) == (
        (0.7, 2.0), (0.6, 1.0), (0.5, 1.0), (0.4, 1.0))
    assert _splice_sorted(bids, 0.55, 2.0, descending=True) == (
        (0.6, 1.0), (0.55, 2.0), (0.5, 1.0), (0.4, 1.0))
    assert _splice_sorted(bids, 0.3, 2.0, descending=True) == (
        (0.6, 1.0), (0.5, 1.0), (0.4, 1.0), (0.3, 2.0))
    assert _splice_sorted(bids, 0.5, 9.0, descending=True) == (
        (0.6, 1.0), (0.5, 9.0), (0.4, 1.0))
    assert _splice_sorted(bids, 0.5, None, descending=True) == (
        (0.6, 1.0), (0.4, 1.0))
    assert _splice_sorted(bids, 0.51, None, descending=True) == bids
    assert _splice_sorted((), 0.5, 1.0, descending=True) == ((0.5, 1.0),)

    asks = ((0.6, 1.0), (0.7, 1.0), (0.8, 1.0))
    assert _splice_sorted(asks, 0.55, 2.0, descending=False) == (
        (0.55, 2.0), (0.6, 1.0), (0.7, 1.0), (0.8, 1.0))
    assert _splice_sorted(asks, 0.65, 2.0, descending=False) == (
        (0.6, 1.0), (0.65, 2.0), (0.7, 1.0), (0.8, 1.0))
    assert _splice_sorted(asks, 0.9, 2.0, descending=False) == (
        (0.6, 1.0), (0.7, 1.0), (0.8, 1.0), (0.9, 2.0))
    assert _splice_sorted((), 0.5, None, descending=False) == ()


# ---------------------------------------------------------------------------
# 12. Cache bounds and memory behaviour.
# ---------------------------------------------------------------------------
def test_book_level_cache_reuses_validated_levels_and_stays_bounded() -> None:
    pool = BookLevelCache(max_entries=8)
    first = pool.level(0.55, 10.0)
    again = pool.level(0.55, 10.0)
    assert again is first
    assert pool.hits == 1 and pool.misses == 1

    for index in range(40):
        pool.level(0.1 + index / 1000, 1.0 + index)
    assert len(pool) <= 8
    assert pool.stats()["max_entries"] == 8

    with pytest.raises(ValueError):
        BookLevelCache(0)
    with pytest.raises(ValueError):
        BookLevelCache(True)


def test_book_level_cache_never_hands_out_a_level_the_contract_rejects() -> None:
    """A cache hit must not be able to launder an invalid pair.

    ``True`` hashes and compares equal to ``1``, so a naive ``(price, shares)``
    key lets a bool collide with an already-cached numeric pair.  ``BookLevel``
    rejects bools outright, so the cache must too.
    """

    pool = BookLevelCache()
    assert pool.level(0.5, 1).shares == 1.0
    with pytest.raises(ValueError):
        pool.level(0.5, True)
    with pytest.raises(ValueError):
        pool.level(True, 5.0)

    pool_price = BookLevelCache()
    assert pool_price.level(0.5, 1.0).price == 0.5
    with pytest.raises(ValueError):
        pool_price.level(True, 1.0)

    # Invalid pairs are never cached, so they keep raising.
    for _ in range(3):
        with pytest.raises(ValueError):
            pool.level(1.5, 1.0)
        with pytest.raises(ValueError):
            pool.level(0.5, 0.0)
    assert all(not isinstance(key[0], bool) for key in pool._levels)


def test_book_level_cache_tolerates_unhashable_input() -> None:
    pool = BookLevelCache()
    with pytest.raises(ValueError):
        pool.level(["0.5"], 1.0)
    assert pool.misses >= 1


@pytest.mark.asyncio
async def test_long_delta_stream_does_not_grow_adapter_state() -> None:
    """Book versions accumulate on the wire, not in memory.

    Each accepted delta installs a new ``_BookState`` that replaces the previous
    one.  Nothing may retain the displaced versions, and the derivation links
    used by the splice must be released once consumed.
    """

    clock = Clock()
    stream = adapter(clock)
    depth = 20
    bids = [(Decimal("0.400") - Decimal(i) / 1000, Decimal("10")) for i in range(depth)]
    asks = [(Decimal("0.600") + Decimal(i) / 1000, Decimal("10")) for i in range(depth)]
    await hydrate(stream, clock, bids, asks)

    for step in range(600):
        price = bids[1 + (step % (depth - 1))][0]
        await apply(stream, clock, price, Decimal(1 + step % 90), "BUY",
                    bids[0][0], asks[0][0], tag=f"g{step}")
        stream.current_book(TOKEN)

    assert len(stream._books) == 1
    state = stream._books[TOKEN]
    assert len(state.bids) == depth
    assert len(state.asks) == depth
    # The derivation inputs are dropped once the view exists, so no chain of
    # previous versions can be kept alive through them.
    assert state._parent_levels is None
    assert state._delta is None
    assert_matches_reference(stream)


def test_engine_book_level_cache_is_pruned_on_token_rotation() -> None:
    """Five-minute markets rotate tokens; the level cache must rotate with them."""

    engine = FrequencyV4Engine.__new__(FrequencyV4Engine)
    engine._book_level_cache = {}
    engine._book_level_pool = BookLevelCache()
    engine.markets = {}
    engine._active_subscription_map = {}
    engine._active_subscription_windows = frozenset()

    subscribed: list[dict[str, str]] = []

    class _Ws:
        async def set_subscriptions(self, mapping):
            subscribed.append(dict(mapping))

    engine.poly_ws = _Ws()
    engine._spawn_background = lambda *args, **kwargs: None

    engine._book_level_cache["old-a"] = (1, (), ())
    engine._book_level_cache["old-b"] = (2, (), ())
    engine._book_level_cache["keep"] = (3, (), ())

    import asyncio as _asyncio

    async def rotate():
        engine._active_subscription_map = {"stale": "c"}
        await engine._sync_active_subscriptions(0)

    _asyncio.run(rotate())
    assert subscribed == [{}]
    # Nothing is subscribed any more, so no per-token levels may be retained.
    assert engine._book_level_cache == {}


def test_engine_book_levels_cache_is_keyed_on_book_version() -> None:
    engine = FrequencyV4Engine.__new__(FrequencyV4Engine)
    engine._book_level_cache = {}
    engine._book_level_pool = BookLevelCache()

    first = {"bids": [(0.55, 10.0)], "asks": [(0.65, 10.0)], "book_version": 4}
    bids_a, asks_a = engine._book_levels(TOKEN, first)
    bids_b, asks_b = engine._book_levels(TOKEN, dict(first))
    assert bids_a is bids_b and asks_a is asks_b

    changed = {"bids": [(0.56, 10.0)], "asks": [(0.65, 10.0)], "book_version": 5}
    bids_c, asks_c = engine._book_levels(TOKEN, changed)
    assert bids_c is not bids_a
    assert bids_c[0].price == 0.56
    # The unchanged ask level is pooled, so it is the very same object.
    assert asks_c[0] is asks_a[0]

    # A book with no version (a stub or a patched adapter) must never be cached.
    unversioned = {"bids": [(0.51, 1.0)], "asks": [(0.61, 1.0)]}
    engine._book_levels("other", unversioned)
    assert "other" not in engine._book_level_cache


# ---------------------------------------------------------------------------
# 13-14. Features and strategy inputs are unchanged by pooling.
# ---------------------------------------------------------------------------
def _book_state(bids, asks, *, pooled: bool) -> BookState:
    pool = BookLevelCache()
    build = ((lambda p, s: pool.level(p, s)) if pooled
             else (lambda p, s: BookLevel(p, s)))
    return BookState(
        token_id=TOKEN, condition_id=CONDITION,
        bids=tuple(build(p, s) for p, s in bids),
        asks=tuple(build(p, s) for p, s in asks),
        provider_ts_ms=1_000, receipt_ts_ms=1_010,
        receipt_monotonic_ns=10, source="polymarket_ws",
        event_id="evt-1", payload_hash="hash-1")


def test_pooled_levels_produce_identical_features_and_strategy_inputs() -> None:
    bids = [(0.55, 10.0), (0.54, 6.0), (0.53, 3.0), (0.52, 8.0), (0.51, 2.0)]
    asks = [(0.65, 10.0), (0.66, 6.0), (0.67, 3.0), (0.68, 8.0), (0.69, 2.0)]
    pooled = _book_state(bids, asks, pooled=True)
    direct = _book_state(bids, asks, pooled=False)

    assert pooled == direct
    assert pooled.bids == direct.bids
    assert pooled.best_bid == direct.best_bid
    assert pooled.best_ask == direct.best_ask
    assert microprice(pooled) == microprice(direct)
    assert multi_level_imbalance(pooled) == multi_level_imbalance(direct)
    assert five_share_buy_sweep(pooled) == five_share_buy_sweep(direct)
    assert five_share_sell_sweep(pooled) == five_share_sell_sweep(direct)
    assert compact_book_evidence(pooled) == compact_book_evidence(direct)
    assert canonical_json(compact_book_evidence(pooled)) == \
        canonical_json(compact_book_evidence(direct))


@pytest.mark.asyncio
async def test_current_book_still_normalizes_after_incremental_updates() -> None:
    clock = Clock()
    stream = adapter(clock)
    await hydrate(stream, clock,
                  [("0.55", "10"), ("0.50", "6")],
                  [("0.65", "10"), ("0.70", "7")])
    await apply(stream, clock, "0.52", "4", "BUY", "0.55", "0.65", tag="n1")
    current = stream.current_book(TOKEN)
    normalized = normalize_book(
        current, expected_token_id=TOKEN, expected_condition_id=CONDITION,
        receipt_ts_ms=clock.wall, receipt_monotonic_ns=clock.mono,
        hydrated=current["hydrated"],
        connection_epoch=current["connection_epoch"],
        now_ms=clock.wall, max_age_ms=60_000)
    assert normalized.reason is NoBookReason.OK
    assert normalized.book is not None
    assert [(level.price, level.shares) for level in normalized.book.bids] == [
        (0.55, 10.0), (0.52, 4.0), (0.50, 6.0)]
    assert [(level.price, level.shares) for level in normalized.book.asks] == [
        (0.65, 10.0), (0.70, 7.0)]


# ---------------------------------------------------------------------------
# 15. Event identity is byte-for-byte what it was.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("payload", [
    {"b": 1, "a": 2},
    {"a": 2, "b": 1},
    {"z": {"n": [3, 2, 1], "m": "ünïcødé ✅"}, "a": None},
    {"nested": {"deep": {"deeper": [{"k": 1.5}, {"k": -0.0}]}}},
    {"dup": [1, 1, 1], "same": "same"},
    {"num": 10_000_000_000_000_000_1, "float": 1.0, "bool": True},
    {"empty_list": [], "empty_map": {}, "null": None},
    [1, "two", {"three": 3}],
])
def test_canonical_payload_hash_of_matches_the_original_helper(payload) -> None:
    canonical = canonical_json(payload)
    assert canonical_payload_hash_of(canonical) == canonical_payload_hash(payload)


def test_reordered_keys_hash_identically_and_distinct_payloads_do_not() -> None:
    assert canonical_payload_hash({"a": 1, "b": 2}) == \
        canonical_payload_hash({"b": 2, "a": 1})
    assert canonical_payload_hash({"a": 1}) != canonical_payload_hash({"a": "1"})
    assert canonical_payload_hash({"a": 1}) != canonical_payload_hash({"a": 2})


@pytest.mark.asyncio
async def test_event_identity_is_stable_across_the_optimized_ingest_path() -> None:
    """The adapter now hashes the string it already serialized.

    The event key and payload hash it publishes must be exactly what a fresh
    ``canonical_payload_hash`` of the same payload produces.
    """

    seen: list[object] = []

    async def on_event(event, _decision):
        seen.append(event)

    clock = Clock()
    stream = PolymarketMarketWS(
        {TOKEN: CONDITION, OTHER_TOKEN: CONDITION}, clock=clock,
        on_event=on_event)
    stream._new_connection_epoch()
    await hydrate(stream, clock, [("0.55", "10")], [("0.65", "10")])
    assert seen
    book_event = next(e for e in seen if e.event_type == "book")
    payload = json.loads(book_event.payload_json)
    assert book_event.payload_json == canonical_json(payload)
    assert book_event.payload_hash == canonical_payload_hash(payload)
    assert book_event.payload_hash == canonical_payload_hash_of(
        book_event.payload_json)
