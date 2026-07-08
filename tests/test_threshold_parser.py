from poly_alpha_sniper.core.contracts import MarketType
from poly_alpha_sniper.discovery.threshold_parser import parse_market_text


def test_btc_up_down_parses():
    p = parse_market_text("Bitcoin Up or Down - July 8, 3:40 PM ET")
    assert p.ok
    assert p.asset == "BTC"
    assert p.market_type == MarketType.UP_DOWN
    assert p.confidence >= 95


def test_eth_threshold_with_comma():
    p = parse_market_text("Will ETH be above $3,450 at 3:45 PM ET?")
    assert p.ok
    assert p.asset == "ETH"
    assert p.market_type == MarketType.THRESHOLD
    assert p.threshold == 3450.0


def test_k_suffix_threshold():
    p = parse_market_text("Will Bitcoin be above $108.5k at 4:00 PM ET?")
    assert p.ok
    assert p.threshold == 108500.0


def test_sol_higher_lower():
    p = parse_market_text("Solana Higher or Lower - July 8, 2:05 PM ET")
    assert p.ok
    assert p.asset == "SOL"
    assert p.market_type == MarketType.UP_DOWN


def test_multi_asset_rejected():
    p = parse_market_text("Will BTC or ETH be higher today?")
    assert not p.ok
    assert "multiple assets" in " ".join(p.notes)


def test_no_asset_rejected():
    p = parse_market_text("Will the S&P 500 close above 6000?")
    assert not p.ok


def test_subjective_rejected():
    p = parse_market_text("Will Elon tweet about Bitcoin today?")
    assert not p.ok


def test_threshold_market_without_price_rejected():
    p = parse_market_text("Will Bitcoin be above the key level at 3 PM?")
    assert not p.ok


def test_unknown_phrasing_rejected():
    p = parse_market_text("Bitcoin volatility index settlement")
    assert not p.ok


def test_implausible_threshold_penalized():
    p = parse_market_text("Will Bitcoin be above $5 at 3:45 PM ET?")
    assert p.confidence < 95
