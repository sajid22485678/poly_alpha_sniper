import pytest

from poly_alpha_sniper.lite_frequency_v4.books import (
    NoBookReason,
    classify_book_pair,
    compact_book_evidence,
    five_share_buy_sweep,
    five_share_sell_sweep,
    microprice,
    multi_level_imbalance,
    normalize_book,
)
from poly_alpha_sniper.lite_frequency_v4.contracts import (
    AnchorStatus,
    MarketIdentity,
)
from poly_alpha_sniper.lite_frequency_v4.rest import (
    bounded_book_recovery,
    recovery_delays_ms,
)


NOW_MS = 1_800_000_010_000
OPEN_MS = 1_800_000_000_000


def _market():
    return MarketIdentity(
        asset="BTC", slug=f"btc-updown-5m-{OPEN_MS // 1000}",
        market_id="m", event_id="e", condition_id="c",
        yes_token_id="yes", no_token_id="no",
        window_open_ms=OPEN_MS, window_close_ms=OPEN_MS + 300_000,
        anchor_status=AnchorStatus.FIELD_MISSING,
    )


def _raw(token="yes", ts=NOW_MS, **overrides):
    row = {
        "asset_id": token,
        "market": "c",
        "market_id": "m",
        "timestamp": str(ts),
        "hash": f"hash-{token}-{ts}",
        "min_order_size": "1",
        "tick_size": "0.01",
        "neg_risk": False,
        "bids": [
            {"price": "0.40", "size": "3"},
            {"price": "0.42", "size": "2"},
        ],
        "asks": [
            {"price": "0.48", "size": "3"},
            {"price": "0.45", "size": "2"},
        ],
    }
    row.update(overrides)
    return row


def _normalize(token="yes", **overrides):
    return normalize_book(
        _raw(token, **overrides), expected_token_id=token,
        expected_condition_id="c", expected_market_id="m",
        receipt_ts_ms=NOW_MS, receipt_monotonic_ns=10,
        now_ms=NOW_MS, max_age_ms=2_000,
    )


def test_strict_normalization_preserves_provenance_and_sorts_levels():
    result = _normalize()
    assert result.valid
    book = result.book
    assert [level.price for level in book.bids] == [0.42, 0.40]
    assert [level.price for level in book.asks] == [0.45, 0.48]
    assert book.provider_ts_ms == NOW_MS
    assert book.receipt_ts_ms == NOW_MS
    assert book.receipt_monotonic_ns == 10
    assert book.payload_hash and book.event_id == f"hash-yes-{NOW_MS}"


@pytest.mark.parametrize(("kwargs", "reason"), [
    ({"asset_id": "wrong"}, NoBookReason.WRONG_TOKEN),
    ({"market": "wrong"}, NoBookReason.WRONG_MARKET),
    ({"timestamp": str(NOW_MS + 1)}, NoBookReason.FUTURE_TIMESTAMP),
    ({"bids": [], "asks": []}, NoBookReason.EMPTY_LEVELS),
    ({"bids": [{"price": "nan", "size": "1"}]}, NoBookReason.INVALID_PAYLOAD),
    ({"min_order_size": "6"}, NoBookReason.MINIMUM_ORDER_SIZE),
])
def test_invalid_identity_time_levels_and_minimum_fail_closed(kwargs, reason):
    result = _normalize(**kwargs)
    assert result.book is None and result.reason is reason


def test_regressed_duplicate_and_sequence_gap_are_distinct():
    regressed = normalize_book(
        _raw(ts=NOW_MS - 1), expected_token_id="yes",
        expected_condition_id="c", receipt_ts_ms=NOW_MS,
        previous_provider_ts_ms=NOW_MS)
    assert regressed.reason is NoBookReason.REGRESSED_TIMESTAMP
    first = _normalize()
    duplicate = normalize_book(
        _raw(), expected_token_id="yes", expected_condition_id="c",
        receipt_ts_ms=NOW_MS, previous_provider_ts_ms=NOW_MS,
        previous_payload_hash=first.book.payload_hash)
    assert duplicate.reason is NoBookReason.DUPLICATE_EVENT
    gap = normalize_book(
        _raw(sequence="12"), expected_token_id="yes",
        expected_condition_id="c", receipt_ts_ms=NOW_MS,
        previous_sequence=10)
    assert gap.reason is NoBookReason.SEQUENCE_GAP and gap.sequence_gap


def test_exact_five_share_buy_and_sell_use_full_depth_vwap_and_worst_price():
    book = _normalize().book
    buy = five_share_buy_sweep(book)
    sell = five_share_sell_sweep(book)
    assert buy.shares == sell.shares == 5.0
    assert buy.vwap == pytest.approx((0.45 * 2 + 0.48 * 3) / 5)
    assert buy.worst_price == 0.48
    assert sell.vwap == pytest.approx((0.42 * 2 + 0.40 * 3) / 5)
    assert sell.worst_price == 0.40


def test_insufficient_depth_never_returns_partial_sweep():
    book = _normalize(asks=[{"price": "0.45", "size": "4.999"}]).book
    assert five_share_buy_sweep(book) is None


def test_paired_book_validation_binds_direct_tokens_condition_freshness_and_depth():
    market = _market()
    yes = _normalize("yes").book
    no = _normalize("no").book
    valid = classify_book_pair(
        market, yes, no, now_ms=NOW_MS,
        max_age_ms=2_000, max_pair_skew_ms=1_500)
    assert valid.valid and valid.reason is NoBookReason.OK
    missing = classify_book_pair(
        market, None, no, now_ms=NOW_MS,
        max_age_ms=2_000, max_pair_skew_ms=1_500)
    assert missing.reason is NoBookReason.YES_BOOK_MISSING


def test_compact_evidence_keeps_consumed_levels_and_microstructure():
    book = _normalize().book
    sweep = five_share_buy_sweep(book)
    evidence = compact_book_evidence(book, sweep, max_levels=1)
    assert len(evidence["asks"]) == 1
    assert len(evidence["sweep"]["levels"]) == 2
    assert multi_level_imbalance(book, 2) == pytest.approx(0.0)
    assert 0.40 < microprice(book) < 0.48


def test_recovery_delay_is_bounded_and_identity_failure_does_not_retry():
    assert recovery_delays_ms(4) == (250, 500, 750)


@pytest.mark.asyncio
async def test_bounded_rest_recovery_retries_transient_gap_and_recovers_exact_book():
    market = _market()
    calls = 0
    sleeps = []

    async def fetch(token):
        nonlocal calls
        calls += 1
        return None if calls == 1 else _raw(token)

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    result = await bounded_book_recovery(
        fetch, market=market, token_id="yes", attempts=3,
        wall_clock_ms=lambda: NOW_MS,
        monotonic_ns=lambda: 10_000_000,
        sleep=fake_sleep,
    )
    assert result.recovered and result.attempts == 2
    assert sleeps == [0.25]
    assert result.book.token_id == market.yes_token_id


@pytest.mark.asyncio
async def test_bounded_recovery_refuses_token_from_another_window_without_fetching():
    called = False

    async def fetch(_token):
        nonlocal called
        called = True
        return _raw()

    result = await bounded_book_recovery(
        fetch, market=_market(), token_id="another-window-token")
    assert result.reason is NoBookReason.WRONG_TOKEN
    assert result.attempts == 0 and called is False
