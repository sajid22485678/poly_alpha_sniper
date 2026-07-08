"""Shared test fixtures: books, markets, stats, signals."""
from __future__ import annotations

from poly_alpha_sniper.core.config_loader import Config, load_config
from poly_alpha_sniper.core.contracts import (
    BookLevel, CexWindowStats, Direction, EdgeResult, FairProbability, MarketInfo,
    MarketQualityResult, MarketType, MultiCexView, OrderbookSnapshot, OrderSide,
    Position, Outcome, PortfolioSnapshot, Shock, Signal, Tier)

NOW_MS = 1_752_000_000_000  # fixed reference epoch ms


def cfg() -> Config:
    return load_config()


def book(token_id: str = "tok_yes", bid: float = 0.58, ask: float = 0.60,
         depth: float = 500.0, ts_ms: int = NOW_MS) -> OrderbookSnapshot:
    return OrderbookSnapshot(
        token_id=token_id,
        bids=[BookLevel(bid, depth), BookLevel(round(bid - 0.01, 2), depth),
              BookLevel(round(bid - 0.02, 2), depth)],
        asks=[BookLevel(ask, depth), BookLevel(round(ask + 0.01, 2), depth),
              BookLevel(round(ask + 0.02, 2), depth)],
        ts_ms=ts_ms, source="test")


def market(market_id: str = "m1", asset: str = "BTC",
           market_type: MarketType = MarketType.UP_DOWN,
           threshold: float | None = None, up_means_yes: bool = True,
           expiry_ms: int = NOW_MS + 240_000) -> MarketInfo:
    return MarketInfo(
        market_id=market_id, condition_id="c1", title=f"{asset} Up or Down test",
        asset=asset, market_type=market_type, threshold=threshold,
        direction_up_means_yes=up_means_yes,
        yes_token_id="tok_yes", no_token_id="tok_no",
        expiry_ts_ms=expiry_ms, tick_size=0.01, min_order_size_usd=1.0,
        active=True, closed=False, liquidity_usd=1000.0, volume_24h_usd=5000.0,
        mapping_confidence=100.0)


def stats(asset: str = "BTC", price: float = 100_000.0, ret2: float = 0.002,
          zscore: float = 3.0, momentum: float = 0.7, vol: float = 0.0002,
          fresh: bool = True, ts_ms: int = NOW_MS) -> CexWindowStats:
    return CexWindowStats(
        asset=asset, exchange="binance", price=price, ts_ms=ts_ms,
        returns={1: ret2 / 2, 2: ret2, 3: ret2 * 1.1, 5: ret2 * 1.2,
                 10: ret2 * 1.3, 15: ret2 * 1.3, 30: ret2 * 1.4},
        volatility=vol, zscore=zscore, momentum=momentum, impulse=0.6, fresh=fresh)


def view(asset: str = "BTC", **kw) -> MultiCexView:
    st = stats(asset=asset, **kw)
    return MultiCexView(asset=asset, primary=st, per_exchange={"binance": st},
                        confirming_exchanges=2, direction_agreement=True)


def shock(asset: str = "BTC", direction: Direction = Direction.UP,
          ts_ms: int = NOW_MS, fakeout: float = 0.1) -> Shock:
    return Shock(asset=asset, direction=direction, ts_ms=ts_ms,
                 returns={1: 0.001, 2: 0.002, 5: 0.0022, 10: 0.0025, 30: 0.003},
                 zscore=3.0, impulse=0.6, momentum=0.7, volatility=0.0002,
                 confirming_exchanges=2, fakeout_risk=fakeout, reason="test")


def edge(side: OrderSide = OrderSide.BUY_YES, fair: float = 0.70,
         price: float = 0.60, after_slip: float | None = None) -> EdgeResult:
    raw = fair - price
    slip = after_slip if after_slip is not None else raw - 0.02
    return EdgeResult(side=side, fair_probability=fair, market_price=price,
                      raw_edge=raw, edge_after_spread=raw - 0.01,
                      edge_after_slippage=slip,
                      confidence_adjusted_edge=slip * 0.8, suggested_size_usd=1.0)


def signal(tier: Tier = Tier.A, edge_after_slip: float = 0.09,
           confidence: float = 80.0, quality: float = 70.0,
           fakeout: float = 0.1, ts_ms: int = NOW_MS) -> Signal:
    m = market()
    return Signal(
        signal_id="sig-test-1", ts_ms=ts_ms, asset="BTC", market=m,
        side=OrderSide.BUY_YES, direction=Direction.UP,
        shock=shock(fakeout=fakeout, ts_ms=ts_ms - 200),
        fair=FairProbability(p_up=0.70, p_down=0.30, confidence=confidence),
        edge=edge(after_slip=edge_after_slip),
        market_quality=MarketQualityResult(score=quality),
        trade_quality=70.0, alpha_score=75.0, tier=tier,
        exit_plan="TP 12% / SL 9%", seconds_to_expiry=240.0)


def portfolio_snapshot(equity: float = 10.0, cash: float = 10.0, **kw) -> PortfolioSnapshot:
    defaults = dict(equity_usd=equity, available_cash_usd=cash, equity_ath_usd=equity)
    defaults.update(kw)
    return PortfolioSnapshot(**defaults)


def position(token_id: str = "tok_yes", market_id: str = "m1",
             outcome: Outcome = Outcome.YES, shares: float = 1.6,
             entry: float = 0.60, entry_ts_ms: int = NOW_MS - 30_000,
             tier: Tier = Tier.A) -> Position:
    return Position(token_id=token_id, market_id=market_id, outcome=outcome,
                    shares=shares, avg_entry_price=entry, entry_ts_ms=entry_ts_ms,
                    tier=tier)
