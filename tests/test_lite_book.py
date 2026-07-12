import pytest

from poly_alpha_sniper.lite.lite_book import (
    LiteBookClient,
    LiteBookQuote,
    executable_buy_vwap,
    five_share_buy_sweep,
    five_share_sell_sweep,
    normalize_book,
)


def _payload(**overrides):
    payload = {
        "market": "condition-1",
        "asset_id": "token-yes",
        "timestamp": "9000",
        "hash": "book-hash-1",
        "min_order_size": "5",
        "bids": [
            {"price": "0.48", "size": "2"},
            {"price": "0.47", "size": "4"},
        ],
        "asks": [
            {"price": "0.51", "size": "2"},
            {"price": "0.52", "size": "4"},
        ],
    }
    payload.update(overrides)
    return payload


def test_normalize_preserves_provenance_receipt_and_all_valid_levels():
    quote = normalize_book(
        _payload(
            bids=[
                {"price": "0.47", "size": "4"},
                {"price": "0.49", "size": "1"},
                {"price": "nan", "size": "9"},
                {"price": "0.48", "size": "2"},
                {"price": "1.1", "size": "1"},
            ],
            asks=[
                ["0.53", "4"],
                ["0.51", "2"],
                ["0.52", "3"],
                ["bad", "1"],
            ],
        ),
        "token-yes",
        ts_ms=10_000,
        depth_levels=1,
    )

    assert quote is not None
    assert quote.token_id == quote.asset_id == "token-yes"
    assert quote.market == quote.condition_id == "condition-1"
    assert quote.book_hash == "book-hash-1"
    assert quote.min_order_size == 5.0
    assert quote.source_ts_ms == 9_000
    assert quote.ts_ms == quote.received_ts_ms == 10_000
    assert quote.bids == ((0.49, 1.0), (0.48, 2.0), (0.47, 4.0))
    assert quote.asks == ((0.51, 2.0), (0.52, 3.0), (0.53, 4.0))
    assert quote.best_bid == 0.49
    assert quote.best_ask == 0.51
    # Compatibility depth fields retain their configured top-N meaning while
    # full levels remain available for exact executable sweeps.
    assert quote.bid_depth_usd == pytest.approx(0.49)
    assert quote.ask_depth_usd == pytest.approx(1.02)
    assert quote.total_bid_shares == 7.0
    assert quote.total_ask_shares == 9.0


def test_normalize_preserves_optional_tick_and_negative_risk_metadata():
    quote = normalize_book(
        _payload(tick_size="0.01", neg_risk=False),
        "token-yes", ts_ms=10_000)
    assert quote is not None
    assert quote.tick_size == pytest.approx(0.01)
    assert quote.neg_risk is False
    assert normalize_book(
        _payload(tick_size="nan"), "token-yes", ts_ms=10_000) is None


@pytest.mark.parametrize("returned", ["wrong-token", "", None])
def test_normalize_rejects_mismatched_or_missing_returned_asset(returned):
    assert normalize_book(
        _payload(asset_id=returned), "token-yes", ts_ms=10_000
    ) is None


def test_normalize_rejects_any_conflicting_returned_token_identifier():
    assert normalize_book(
        _payload(asset_id="token-yes", token_id="wrong-token"),
        "token-yes",
        ts_ms=10_000,
    ) is None


@pytest.mark.parametrize("timestamp", [None, "", "not-a-time", "9000.0", 0, -1])
def test_normalize_rejects_missing_or_malformed_source_timestamp(timestamp):
    assert normalize_book(
        _payload(timestamp=timestamp), "token-yes", ts_ms=10_000
    ) is None


def test_normalize_rejects_future_source_timestamp():
    assert normalize_book(
        _payload(timestamp="10001"), "token-yes", ts_ms=10_000
    ) is None


def test_normalize_requires_official_minimum_order_size():
    assert normalize_book(
        _payload(min_order_size=None), "token-yes", ts_ms=10_000
    ) is None


def test_freshness_uses_source_time_not_local_receipt_time():
    quote = normalize_book(
        _payload(timestamp="1000"), "token-yes", ts_ms=10_000
    )

    assert quote is not None
    assert quote.age_ms(10_100) == 9_100
    assert quote.receipt_age_ms(10_100) == 100
    assert quote.is_stale(10_100, max_age_ms=1_000)


def test_manually_constructed_future_quote_fails_closed():
    quote = LiteBookQuote(
        "token-yes", 0.48, 0.51, 10.0, 10.0, 10_000,
        source_ts_ms=10_001,
    )

    assert quote.is_future(10_000)
    assert quote.is_stale(10_000, max_age_ms=5_000)


def test_exact_five_share_buy_uses_ask_sweep_vwap_not_top_or_midpoint():
    quote = normalize_book(_payload(), "token-yes", ts_ms=10_000)

    assert quote is not None
    sweep = five_share_buy_sweep(quote)
    assert sweep is not None
    assert sweep.side == "BUY"
    assert sweep.shares == 5.0
    assert sweep.levels == ((0.51, 2.0), (0.52, 3.0))
    assert sweep.notional == pytest.approx(2.58)
    assert sweep.vwap == pytest.approx(0.516)
    assert sweep.worst_price == 0.52
    assert executable_buy_vwap(quote) == pytest.approx(0.516)
    assert quote.buy_vwap() == pytest.approx(0.516)
    assert quote.buy_vwap() != quote.best_ask
    assert quote.buy_vwap() != pytest.approx((quote.best_bid + quote.best_ask) / 2)


def test_exact_five_share_sell_uses_owned_token_bid_sweep():
    quote = normalize_book(_payload(), "token-yes", ts_ms=10_000)

    assert quote is not None
    sweep = five_share_sell_sweep(quote)
    assert sweep is not None
    assert sweep.side == "SELL"
    assert sweep.levels == ((0.48, 2.0), (0.47, 3.0))
    assert sweep.notional == pytest.approx(2.37)
    assert sweep.vwap == pytest.approx(0.474)
    assert sweep.worst_price == 0.47
    assert quote.sell_vwap() == pytest.approx(0.474)


def test_exact_five_share_sweeps_reject_insufficient_share_depth():
    quote = normalize_book(
        _payload(
            bids=[{"price": "0.48", "size": "4.999"}],
            asks=[{"price": "0.51", "size": "4.999"}],
        ),
        "token-yes",
        ts_ms=10_000,
    )

    assert quote is not None
    assert five_share_buy_sweep(quote) is None
    assert five_share_sell_sweep(quote) is None


def test_exact_five_share_sweep_honors_official_minimum_order_size():
    quote = normalize_book(
        _payload(
            min_order_size="6",
            asks=[{"price": "0.51", "size": "10"}],
        ),
        "token-yes",
        ts_ms=10_000,
    )

    assert quote is not None
    assert five_share_buy_sweep(quote) is None


class _Response:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self):
        return self.payload


class _Session:
    closed = False

    def __init__(self, payloads):
        self.payloads = list(payloads)

    def get(self, _url, *, params):
        assert params == {"token_id": "token-yes"}
        return _Response(self.payloads.pop(0))

    async def close(self):
        self.closed = True


async def test_client_marks_reused_book_hash_observable_without_rejecting_it():
    session = _Session([
        _payload(timestamp="9000", hash="same-hash"),
        _payload(timestamp="9500", hash="same-hash"),
    ])
    client = LiteBookClient(session_factory=lambda: session)

    first = await client.get_book("token-yes", now_ms=10_000)
    second = await client.get_book("token-yes", now_ms=10_000)

    assert first is not None and first.hash_reused is False
    assert second is not None and second.hash_reused is True
    assert second.source_ts_ms == 9_500
    await client.close()
