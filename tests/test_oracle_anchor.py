"""WS3/WS5B: oracle anchor resolution + validation (price-to-beat gate)."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import RejectReason
from poly_alpha_sniper.strategy.oracle_anchor import resolve_oracle_anchor, validate_oracle_anchor
from poly_alpha_sniper.strategy.price_to_beat_validator import validate_price_to_beat
from poly_alpha_sniper.tests.helpers import NOW_MS, market

MAX_AGE_MS = 300_000
MAX_BASIS = 0.02


def _market_with_anchor(price_to_beat=100_000.0, source="polymarket_event_metadata",
                        resolution_url="https://data.chain.link/streams/btc-usd"):
    m = market()
    m.price_to_beat = price_to_beat
    m.price_to_beat_source = source
    m.resolution_source_url = resolution_url
    m.raw["event_start_ts_ms"] = NOW_MS - 60_000
    return m


def test_missing_price_to_beat_rejects():
    m = market()  # price_to_beat is None by default
    anchor = resolve_oracle_anchor(m, cex_price=100_100.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)
    assert anchor.available is False
    ok, reason = validate_oracle_anchor(anchor, m, NOW_MS, MAX_AGE_MS, MAX_BASIS)
    assert not ok
    assert reason == RejectReason.MISSING_ORACLE_ANCHOR


def test_valid_fresh_anchor_passes():
    m = _market_with_anchor()
    anchor = resolve_oracle_anchor(m, cex_price=100_050.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)
    assert anchor.available is True
    assert anchor.oracle_anchor_quality == "good"
    ok, reason = validate_oracle_anchor(anchor, m, NOW_MS, MAX_AGE_MS, MAX_BASIS)
    assert ok
    assert reason == ""


def test_stale_anchor_rejects_when_window_already_closed():
    m = _market_with_anchor()
    anchor = resolve_oracle_anchor(m, cex_price=100_050.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)
    # evaluate long after the market's own expiry -> stale
    ok, reason = validate_oracle_anchor(anchor, m, m.expiry_ts_ms + 1000, MAX_AGE_MS, MAX_BASIS)
    assert not ok
    assert reason == RejectReason.ORACLE_ANCHOR_STALE


def test_stale_anchor_rejects_when_older_than_max_age():
    m = _market_with_anchor()
    m.raw["event_start_ts_ms"] = NOW_MS - (MAX_AGE_MS + 1000)
    anchor = resolve_oracle_anchor(m, cex_price=100_050.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)
    ok, reason = validate_oracle_anchor(anchor, m, NOW_MS, MAX_AGE_MS, MAX_BASIS)
    assert not ok
    assert reason == RejectReason.ORACLE_ANCHOR_STALE


def test_basis_instability_rejects():
    m = _market_with_anchor(price_to_beat=100_000.0)
    # cex 5% away from anchor -- way beyond the 2% default threshold
    anchor = resolve_oracle_anchor(m, cex_price=105_000.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)
    ok, reason = validate_oracle_anchor(anchor, m, NOW_MS, MAX_AGE_MS, MAX_BASIS)
    assert not ok
    assert reason == RejectReason.ORACLE_CEX_BASIS_UNSTABLE


def test_small_basis_does_not_reject():
    m = _market_with_anchor(price_to_beat=100_000.0)
    anchor = resolve_oracle_anchor(m, cex_price=100_100.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)  # 0.1%
    ok, reason = validate_oracle_anchor(anchor, m, NOW_MS, MAX_AGE_MS, MAX_BASIS)
    assert ok


def test_metadata_mismatch_rejects():
    m = _market_with_anchor()
    anchor = resolve_oracle_anchor(m, cex_price=100_050.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)
    other_market = market(market_id="different-market", asset="ETH")
    other_market.price_to_beat = m.price_to_beat
    ok, reason = validate_oracle_anchor(anchor, other_market, NOW_MS, MAX_AGE_MS, MAX_BASIS)
    assert not ok
    assert reason == RejectReason.PRICE_TO_BEAT_MISMATCH


def test_unknown_source_rejects():
    m = _market_with_anchor(source="")
    anchor = resolve_oracle_anchor(m, cex_price=100_050.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)
    ok, reason = validate_oracle_anchor(anchor, m, NOW_MS, MAX_AGE_MS, MAX_BASIS)
    assert not ok
    assert reason == RejectReason.ORACLE_SOURCE_UNKNOWN


def test_price_to_beat_validator_wrapper_matches_oracle_anchor_validator():
    """WS5B is a thin named wrapper -- must behave identically."""
    m = _market_with_anchor()
    anchor = resolve_oracle_anchor(m, cex_price=100_050.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)
    a = validate_oracle_anchor(anchor, m, NOW_MS, MAX_AGE_MS, MAX_BASIS)
    b = validate_price_to_beat(anchor, m, NOW_MS, MAX_AGE_MS, MAX_BASIS)
    assert a == b


def test_never_pretends_fallback_is_measured_chainlink():
    """oracle_source must be the real string recorded, never silently
    upgraded to imply a direct Chainlink subscription that doesn't exist."""
    m = _market_with_anchor(source="polymarket_event_metadata")
    anchor = resolve_oracle_anchor(m, cex_price=100_050.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)
    assert anchor.oracle_source == "polymarket_event_metadata"
    assert anchor.latest_oracle_price is None  # no live Chainlink feed exists -- never fabricated
