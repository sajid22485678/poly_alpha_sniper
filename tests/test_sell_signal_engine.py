from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import ExitReason, FairProbability, Outcome
from poly_alpha_sniper.strategy.exit_engine import ExitEngine
from poly_alpha_sniper.strategy.exit_priority_engine import order_exits
from poly_alpha_sniper.strategy.sell_signal_engine import SellSignalEngine
from poly_alpha_sniper.tests.helpers import (
    NOW_MS, book, cfg, market, portfolio_snapshot, position, view)


def _sse():
    return SellSignalEngine(ExitEngine(cfg(), SimClock(NOW_MS)))


def test_multiple_positions_ordered_by_priority():
    m1 = market("m1")
    m2 = market("m2", expiry_ms=NOW_MS + 10_000)  # expiry risk -> priority 2
    p1 = position(token_id="tok_a", market_id="m1", entry=0.60)   # take profit
    p2 = position(token_id="tok_b", market_id="m2", entry=0.60)
    books = {"tok_a": book("tok_a", bid=0.70, ask=0.72),
             "tok_b": book("tok_b", bid=0.61, ask=0.63)}
    markets = {"m1": m1, "m2": m2}
    firing = _sse().evaluate_all(
        [p1, p2], markets.get, books.get,
        lambda pos: FairProbability(p_up=0.80, p_down=0.20, confidence=80),
        lambda asset: view(momentum=0.6), portfolio_snapshot())
    assert len(firing) == 2
    assert firing[0][1].reason == ExitReason.EXPIRY_RISK  # priority 2 first
    assert firing[1][1].reason in (ExitReason.TAKE_PROFIT, ExitReason.PARTIAL_TAKE_PROFIT)


def test_healthy_positions_yield_nothing():
    p = position(entry=0.60)
    firing = _sse().evaluate_all(
        [p], lambda mid: market(), lambda tid: book(bid=0.63, ask=0.65),
        lambda pos: FairProbability(p_up=0.75, p_down=0.25, confidence=80),
        lambda asset: view(momentum=0.6), portfolio_snapshot())
    assert firing == []


def test_order_exits_dedupes_per_token():
    p = position()
    d1 = __import__("poly_alpha_sniper.core.contracts", fromlist=["ExitDecision"])
    from poly_alpha_sniper.core.contracts import ExitDecision
    low = ExitDecision.full(ExitReason.TAKE_PROFIT)
    high = ExitDecision.full(ExitReason.STOP_LOSS)
    ordered = order_exits([(p, low), (p, high)])
    assert len(ordered) == 1
    assert ordered[0][1].reason == ExitReason.STOP_LOSS
