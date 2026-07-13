from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

import pytest

from lite_frequency_v4.config import FrequencyV4Config
from lite_frequency_v4.contracts import (
    AnchorStatus,
    BookLevel,
    BookState,
    EntrySide,
    MarketIdentity,
)
from lite_frequency_v4.economics import (
    calculate_economics,
    exact_sweep,
    positive_ev,
    sweep_fee,
)
from lite_frequency_v4.edge_models import EnsembleResult


OPEN_MS = 1_800_000_000_000
NOW_MS = OPEN_MS + 120_000


def identity() -> MarketIdentity:
    return MarketIdentity(
        asset="BTC",
        slug=f"btc-updown-5m-{OPEN_MS // 1000}",
        market_id="market-1",
        event_id="event-1",
        condition_id="condition-1",
        yes_token_id="yes-token",
        no_token_id="no-token",
        window_open_ms=OPEN_MS,
        window_close_ms=OPEN_MS + 300_000,
        anchor_status=AnchorStatus.UNANCHORED,
    )


def book(
    token: str,
    *,
    bids=((0.39, 10.0),),
    asks=((0.40, 2.0), (0.42, 3.0)),
    provider_ts_ms: int = NOW_MS - 100,
    receipt_ts_ms: int = NOW_MS - 50,
    epoch: int = 1,
    condition: str = "condition-1",
    hydrated: bool = True,
    minimum: float = 5.0,
) -> BookState:
    return BookState(
        token_id=token,
        condition_id=condition,
        bids=tuple(BookLevel(price, shares) for price, shares in bids),
        asks=tuple(BookLevel(price, shares) for price, shares in asks),
        provider_ts_ms=provider_ts_ms,
        receipt_ts_ms=receipt_ts_ms,
        receipt_monotonic_ns=receipt_ts_ms * 1_000_000,
        source="polymarket_ws",
        event_id=f"book-{token}-{provider_ts_ms}",
        payload_hash=f"hash-{token}-{provider_ts_ms}",
        connection_epoch=epoch,
        min_order_size=minimum,
        tick_size=0.01,
        hydrated=hydrated,
    )


def ensemble(probability_yes: float, *, reliability: float = 0.8,
             uncalibrated: bool = False) -> EnsembleResult:
    return EnsembleResult(
        regime="NORMAL",
        fair_probability_yes=probability_yes,
        reliability=reliability,
        outputs=(),
        model_uncalibrated=uncalibrated,
    )


def test_exact_five_share_sweep_uses_all_consumed_levels_and_worst_price() -> None:
    state = book("yes-token")
    sweep = exact_sweep(state, buy=True)
    assert sweep is not None
    assert sweep.shares == 5.0
    assert sweep.notional == pytest.approx(2 * 0.40 + 3 * 0.42)
    assert sweep.vwap == pytest.approx(0.412)
    assert sweep.worst_price == 0.42
    assert [(level.price, level.shares) for level in sweep.levels] == [
        (0.40, 2.0), (0.42, 3.0)
    ]

    sell = exact_sweep(book(
        "yes-token",
        bids=((0.41, 2.0), (0.40, 3.0)),
        asks=((0.42, 5.0),),
    ), buy=False)
    assert sell is not None
    assert sell.vwap == pytest.approx(0.404)
    assert sell.worst_price == 0.40


def test_sweep_refuses_non_five_size_unhydrated_or_insufficient_depth() -> None:
    state = book("yes-token", asks=((0.40, 4.999),))
    assert exact_sweep(state, buy=True) is None
    assert exact_sweep(book("yes-token"), buy=True, shares=4.0) is None
    assert exact_sweep(book("yes-token", hydrated=False), buy=True) is None


def test_fee_is_level_accurate_and_rounded_once() -> None:
    sweep = exact_sweep(book("yes-token"), buy=True)
    assert sweep is not None
    expected = sum(
        Decimal(str(level.shares)) * Decimal("0.07")
        * Decimal(str(level.price)) * (Decimal("1") - Decimal(str(level.price)))
        for level in sweep.levels
    ).quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
    assert sweep_fee(sweep, 0.07) == float(expected)
    with pytest.raises(ValueError, match="invalid fee rate"):
        sweep_fee(sweep, -0.01)


def test_fee_net_edge_formula_includes_every_cost_and_selects_best_side() -> None:
    cfg = FrequencyV4Config()
    yes = book("yes-token")
    no = book("no-token", bids=((0.58, 10.0),), asks=((0.60, 5.0),))
    calc = calculate_economics(
        identity=identity(),
        ensemble=ensemble(0.47),
        yes_book=yes,
        no_book=no,
        now_ms=NOW_MS,
        cfg=cfg,
    )
    side = calc.result.yes
    assert calc.yes_sweep is not None and calc.yes_fee_total is not None
    expected_execution = (
        cfg.execution_buffer_base
        + 0.10 * float(yes.spread)
        + 0.50 * (calc.yes_sweep.vwap - float(yes.best_ask))
    )
    expected_latency = cfg.latency_buffer_base + 0.1 * 0.00025
    expected_uncertainty = cfg.uncertainty_buffer_base + (1.0 - 0.8) * 0.015
    expected_edge = (
        0.47
        - calc.yes_sweep.vwap
        - calc.yes_fee_total / 5.0
        - expected_execution
        - expected_latency
        - expected_uncertainty
    )
    assert side.estimated_fee == pytest.approx(calc.yes_fee_total / 5.0)
    assert side.execution_buffer == pytest.approx(expected_execution)
    assert side.latency_buffer == pytest.approx(expected_latency)
    assert side.uncertainty_buffer == pytest.approx(expected_uncertainty)
    assert side.net_edge == pytest.approx(expected_edge)
    assert calc.result.selected_side is EntrySide.BUY_YES
    assert calc.result.selected_net_edge == pytest.approx(expected_edge)
    assert positive_ev(calc.result) is (expected_edge > 0.0)


def test_probabilities_are_strictly_bounded_and_exact_complements() -> None:
    cfg = FrequencyV4Config()
    calc = calculate_economics(
        identity=identity(),
        ensemble=ensemble(1.0),
        yes_book=book("yes-token"),
        no_book=book("no-token", bids=((0.58, 10.0),), asks=((0.60, 5.0),)),
        now_ms=NOW_MS,
        cfg=cfg,
    )
    assert calc.result.fair_probability_yes == cfg.fair_probability_ceiling
    assert calc.result.fair_probability_no == pytest.approx(1.0 - cfg.fair_probability_ceiling)
    assert calc.result.fair_probability_yes + calc.result.fair_probability_no == 1.0
    assert 0.0 < calc.result.fair_probability_no < 1.0
    assert calc.result.coherent is True


def test_insufficient_depth_invalidates_only_that_side_and_never_fabricates_sweep() -> None:
    calc = calculate_economics(
        identity=identity(),
        ensemble=ensemble(0.60),
        yes_book=book("yes-token", asks=((0.40, 4.0),)),
        no_book=book("no-token", bids=((0.58, 10.0),), asks=((0.60, 5.0),)),
        now_ms=NOW_MS,
        cfg=FrequencyV4Config(),
    )
    assert calc.yes_sweep is None
    assert calc.result.yes.valid is False
    assert calc.result.yes.invalidation_reason == "insufficient_five_share_depth"
    assert calc.result.yes.executable_vwap is None


@pytest.mark.parametrize(
    ("yes_kwargs", "no_kwargs", "reason"),
    [
        ({"provider_ts_ms": NOW_MS - 3_000, "receipt_ts_ms": NOW_MS - 2_900}, {}, "stale_snapshot"),
        ({"provider_ts_ms": NOW_MS + 1, "receipt_ts_ms": NOW_MS + 1}, {}, "future_book"),
        ({"condition": "wrong-condition"}, {}, "wrong_market"),
        ({"epoch": 1}, {"epoch": 2}, "paired_book_connection_epoch_mismatch"),
        ({"provider_ts_ms": NOW_MS - 10, "receipt_ts_ms": NOW_MS - 10},
         {"provider_ts_ms": NOW_MS - 1_600, "receipt_ts_ms": NOW_MS - 1_600},
         "paired_book_timestamp_skew"),
    ],
)
def test_unsafe_book_evidence_fails_closed(yes_kwargs, no_kwargs, reason) -> None:
    calc = calculate_economics(
        identity=identity(),
        ensemble=ensemble(0.70),
        yes_book=book("yes-token", **yes_kwargs),
        no_book=book("no-token", bids=((0.58, 10.0),), asks=((0.60, 5.0),), **no_kwargs),
        now_ms=NOW_MS,
        cfg=FrequencyV4Config(),
    )
    assert reason in calc.invalid_reasons
    assert not positive_ev(calc.result)
    assert calc.result.yes.valid is False or calc.result.no.valid is False


def test_negative_fee_net_ev_never_passes_even_if_frequency_objective_is_unmet() -> None:
    calc = calculate_economics(
        identity=identity(),
        ensemble=ensemble(0.40, reliability=0.0, uncalibrated=True),
        yes_book=book("yes-token", bids=((0.44, 10.0),), asks=((0.46, 5.0),)),
        no_book=book("no-token", bids=((0.58, 10.0),), asks=((0.60, 5.0),)),
        now_ms=NOW_MS,
        cfg=FrequencyV4Config(),
    )
    assert calc.result.selected_net_edge is not None
    assert calc.result.selected_net_edge < 0.0
    assert positive_ev(calc.result) is False


def test_economics_replay_is_byte_for_byte_deterministic() -> None:
    kwargs = dict(
        identity=identity(),
        ensemble=ensemble(0.47),
        yes_book=book("yes-token"),
        no_book=book("no-token", bids=((0.58, 10.0),), asks=((0.60, 5.0),)),
        now_ms=NOW_MS,
        cfg=FrequencyV4Config(),
    )
    first = calculate_economics(**kwargs)
    second = calculate_economics(**kwargs)
    assert first == second
    assert first.result.to_dict() == second.result.to_dict()
