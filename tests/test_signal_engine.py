from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import Direction, OrderSide
from poly_alpha_sniper.strategy.market_quality_score import MarketQualityScorer
from poly_alpha_sniper.strategy.probability_model import ProbabilityModel
from poly_alpha_sniper.strategy.signal_engine import SignalEngine
from poly_alpha_sniper.tests.helpers import NOW_MS, book, cfg, market, shock, view


def _engine(c=None):
    c = c or cfg()
    return SignalEngine(c, SimClock(NOW_MS), ProbabilityModel(c), MarketQualityScorer(c))


def test_up_shock_yields_buy_yes():
    # cheap YES ask (0.50) with strong up momentum -> positive edge
    sig = _engine().build_signal(
        shock(direction=Direction.UP), market(), view(momentum=0.9, ret2=0.004),
        book(bid=0.48, ask=0.50), book("tok_no", bid=0.48, ask=0.50), NOW_MS)
    assert sig is not None
    assert sig.side == OrderSide.BUY_YES
    assert sig.edge.raw_edge > 0
    assert sig.signal_id
    assert sig.exit_plan
    assert sig.seconds_to_expiry > 0


def test_down_shock_yields_buy_no():
    sig = _engine().build_signal(
        shock(direction=Direction.DOWN), market(),
        view(momentum=-0.9, ret2=-0.004),
        book(bid=0.48, ask=0.50), book("tok_no", bid=0.48, ask=0.50), NOW_MS)
    assert sig is not None
    assert sig.side == OrderSide.BUY_NO


def test_flipped_mapping_up_shock_buys_no_token():
    m = market(up_means_yes=False)  # YES token = DOWN condition
    sig = _engine().build_signal(
        shock(direction=Direction.UP), m, view(momentum=0.9, ret2=0.004),
        book(bid=0.48, ask=0.50), book("tok_no", bid=0.48, ask=0.50), NOW_MS)
    assert sig is not None
    assert sig.side == OrderSide.BUY_NO


def test_no_book_returns_none():
    sig = _engine().build_signal(
        shock(direction=Direction.UP), market(), view(momentum=0.9),
        None, book("tok_no"), NOW_MS)
    assert sig is None


def test_no_positive_edge_returns_none():
    # YES already priced at 0.97: no edge on an up move
    sig = _engine().build_signal(
        shock(direction=Direction.UP), market(), view(momentum=0.9, ret2=0.004),
        book(bid=0.96, ask=0.97), book("tok_no", bid=0.02, ask=0.03), NOW_MS)
    assert sig is None


def test_deterministic_signal_id():
    a = _engine().build_signal(shock(), market(), view(momentum=0.9, ret2=0.004),
                               book(bid=0.48, ask=0.50), book("tok_no"), NOW_MS)
    b = _engine().build_signal(shock(), market(), view(momentum=0.9, ret2=0.004),
                               book(bid=0.48, ask=0.50), book("tok_no"), NOW_MS)
    assert a.signal_id == b.signal_id
