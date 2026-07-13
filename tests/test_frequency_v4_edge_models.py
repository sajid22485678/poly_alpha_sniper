from __future__ import annotations

from dataclasses import replace

from lite_frequency_v4.contracts import (
    AnchorStatus,
    BookLevel,
    BookState,
    CexFeatures,
    Direction,
    MarketIdentity,
)
from lite_frequency_v4.edge_models import (
    BookFeaturePoint,
    DeterministicEdgeEnsemble,
    ModelContext,
)


OPEN_MS = 1_800_000_000_000


def market(*, now_ms: int = OPEN_MS + 120_000, remaining_ms: int | None = None) -> MarketIdentity:
    if remaining_ms is None:
        open_ms, close_ms = OPEN_MS, OPEN_MS + 300_000
    else:
        close_ms = now_ms + remaining_ms
        open_ms = close_ms - 300_000
    return MarketIdentity(
        asset="BTC",
        slug=f"btc-updown-5m-{open_ms // 1000}",
        market_id="market-1",
        event_id="event-1",
        condition_id="condition-1",
        yes_token_id="yes-token",
        no_token_id="no-token",
        window_open_ms=open_ms,
        window_close_ms=close_ms,
        anchor_status=AnchorStatus.FIELD_MISSING,
    )


def book(
    token: str,
    *,
    now_ms: int = OPEN_MS + 120_000,
    bid: float = 0.49,
    ask: float = 0.51,
    bid_depth: float = 25.0,
    ask_depth: float = 25.0,
) -> BookState:
    return BookState(
        token_id=token,
        condition_id="condition-1",
        bids=(BookLevel(bid, bid_depth),),
        asks=(BookLevel(ask, ask_depth),),
        provider_ts_ms=now_ms - 100,
        receipt_ts_ms=now_ms - 50,
        receipt_monotonic_ns=(now_ms - 50) * 1_000_000,
        source="polymarket_ws",
        event_id=f"{token}-{now_ms}",
        payload_hash=f"hash-{token}-{now_ms}",
        connection_epoch=1,
        min_order_size=5.0,
        tick_size=0.01,
    )


def cex(*, now_ms: int = OPEN_MS + 120_000, **overrides) -> CexFeatures:
    values = dict(
        asset="BTC",
        provider="okx",
        provider_ts_ms=now_ms - 100,
        receipt_ts_ms=now_ms - 50,
        latest_move_ts_ms=now_ms - 100,
        evidence_age_ms=100,
        returns={1: 0.0010, 3: 0.0015, 5: 0.0020, 10: 0.0025, 15: 0.0030, 30: 0.0040},
        tick_return=0.0003,
        acceleration=0.0002,
        volatility=0.0004,
        window_open_price=50_000.0,
        window_return=0.003,
        sample_count=30,
        classification="NEW_TICK",
        valid=True,
        invalidation_reason="",
        evidence_event_ids=("cex-1", "cex-0"),
    )
    values.update(overrides)
    return CexFeatures(**values)


def context(*, now_ms: int = OPEN_MS + 120_000, identity: MarketIdentity | None = None,
            cex_features: CexFeatures | None = None, yes: BookState | None = None,
            no: BookState | None = None, history=(), response_ms: int | None = 1_000,
            yes_flow: float = 0.0, no_flow: float = 0.0) -> ModelContext:
    identity = identity or market(now_ms=now_ms)
    return ModelContext(
        market=identity,
        now_ms=now_ms,
        cex=cex_features or cex(now_ms=now_ms),
        yes_book=yes or book("yes-token", now_ms=now_ms),
        no_book=no or book("no-token", now_ms=now_ms),
        book_history=tuple(history),
        yes_trade_flow=yes_flow,
        no_trade_flow=no_flow,
        polymarket_response_ms=response_ms,
    )


def test_cex_lead_lag_impulse_is_bounded_and_auditable() -> None:
    output = DeterministicEdgeEnsemble().lead_lag_impulse(context())
    assert output.family == "lead_lag"
    assert output.direction is Direction.YES
    assert 0.0 < output.raw_score <= 1.0
    assert 0.0 < output.estimated_probability_yes < 1.0
    assert output.invalidation_reason == ""
    assert output.evidence_event_ids == ("cex-1", "cex-0")


def test_trend_requires_persistent_multi_horizon_evidence_not_one_tick_noise() -> None:
    ensemble = DeterministicEdgeEnsemble()
    valid = ensemble.trend_continuation(context())
    noisy = ensemble.trend_continuation(context(cex_features=cex(
        returns={1: 0.003, 3: -0.003, 5: 0.002, 10: -0.002, 15: None, 30: None}
    )))
    assert valid.family == "trend_continuation"
    assert valid.direction is Direction.YES
    assert valid.invalidation_reason == ""
    assert noisy.direction is Direction.NONE
    assert noisy.invalidation_reason in {
        "insufficient_multi_horizon_history", "one_tick_or_inconsistent_trend"
    }


def test_liquidity_sweep_reversal_requires_displacement_recovery_and_confirmation() -> None:
    now = OPEN_MS + 120_000
    history = (
        BookFeaturePoint(now - 3_000, now - 3_000, 0.50, 0.50, 20, 20, 20, 20),
        BookFeaturePoint(now - 2_000, now - 2_000, 0.54, 0.46, 20, 10, 20, 30),
        BookFeaturePoint(now - 1_000, now - 1_000, 0.52, 0.48, 20, 16, 20, 24),
    )
    ctx = context(
        now_ms=now,
        history=history,
        cex_features=cex(now_ms=now, tick_return=-0.0001),
    )
    output = DeterministicEdgeEnsemble().liquidity_sweep_reversal(ctx)
    assert output.family == "sweep_reversal"
    assert output.direction is Direction.NO
    assert output.invalidation_reason == ""
    assert output.reliability > 0.0


def test_liquidity_reversal_does_not_look_ahead_to_future_recovery() -> None:
    now = OPEN_MS + 120_000
    history = (
        BookFeaturePoint(now - 2_000, now - 2_000, 0.50, 0.50, 20, 20, 20, 20),
        BookFeaturePoint(now - 1_000, now - 1_000, 0.54, 0.46, 20, 10, 20, 30),
        BookFeaturePoint(now + 1, now + 1, 0.52, 0.48, 20, 16, 20, 24),
    )
    output = DeterministicEdgeEnsemble().liquidity_sweep_reversal(
        context(now_ms=now, history=history)
    )
    assert output.direction is Direction.NONE
    assert output.invalidation_reason == "insufficient_book_recovery_history"


def test_window_open_displacement_is_elapsed_time_aware() -> None:
    ensemble = DeterministicEdgeEnsemble()
    valid = ensemble.window_open_displacement(context())
    before_evidence = ensemble.window_open_displacement(context(
        now_ms=OPEN_MS + 500,
        identity=market(now_ms=OPEN_MS + 500),
        cex_features=cex(now_ms=OPEN_MS + 500),
        yes=book("yes-token", now_ms=OPEN_MS + 500),
        no=book("no-token", now_ms=OPEN_MS + 500),
    ))
    assert valid.family == "window_open"
    assert valid.direction is Direction.YES
    assert valid.invalidation_reason == ""
    assert before_evidence.direction is Direction.NONE
    assert before_evidence.invalidation_reason == "elapsed_time_outside_window"


def test_order_book_microstructure_uses_depth_microprice_and_flow() -> None:
    now = OPEN_MS + 120_000
    yes = book("yes-token", now_ms=now, bid_depth=100.0, ask_depth=10.0)
    no = book("no-token", now_ms=now, bid_depth=10.0, ask_depth=100.0)
    output = DeterministicEdgeEnsemble().order_book_microstructure(
        context(now_ms=now, yes=yes, no=no, yes_flow=20.0, no_flow=1.0)
    )
    assert output.family == "microstructure"
    assert output.direction is Direction.YES
    assert output.invalidation_reason == ""
    assert output.confidence > 0.0


def test_paired_book_parity_identifies_asymmetric_underpricing() -> None:
    now = OPEN_MS + 120_000
    # YES is offered materially below the complement of NO.  A parity family
    # must produce a directional signal instead of silently returning NONE.
    yes = book("yes-token", now_ms=now, bid=0.35, ask=0.37)
    no = book("no-token", now_ms=now, bid=0.57, ask=0.59)
    output = DeterministicEdgeEnsemble().paired_book_parity(
        context(now_ms=now, yes=yes, no=no)
    )
    assert output.family == "paired_parity"
    assert output.invalidation_reason == ""
    assert output.direction is Direction.YES
    assert output.raw_score > 0.0


def test_late_window_dominance_requires_fresh_strong_evidence() -> None:
    now = OPEN_MS + 270_000
    identity = market(now_ms=now, remaining_ms=30_000)
    valid_ctx = context(
        now_ms=now,
        identity=identity,
        cex_features=cex(now_ms=now, window_return=-0.006, volatility=0.001),
        yes=book("yes-token", now_ms=now),
        no=book("no-token", now_ms=now),
    )
    stale_ctx = replace(valid_ctx, cex=cex(
        now_ms=now, window_return=-0.006, volatility=0.001,
        evidence_age_ms=751,
    ))
    valid = DeterministicEdgeEnsemble().late_window_dominance(valid_ctx)
    stale = DeterministicEdgeEnsemble().late_window_dominance(stale_ctx)
    assert valid.family == "late_dominance"
    assert valid.direction is Direction.NO
    assert valid.invalidation_reason == ""
    assert stale.direction is Direction.NONE
    assert stale.invalidation_reason == "late_window_evidence_unsafe"


def test_correlated_cex_models_share_one_group_cap_and_probability_is_coherent() -> None:
    ensemble = DeterministicEdgeEnsemble(max_adjustment=0.15)
    ctx = context()
    result = ensemble.evaluate(ctx)
    baseline = ctx.baseline_probability_yes
    cex_contribution = sum(
        output.contribution for output in result.outputs
        if output.correlation_group == "cex_directional"
    )
    assert cex_contribution <= 0.12 + 1e-12
    assert abs(result.fair_probability_yes - baseline) <= 0.15 + 1e-12
    assert 0.0 < result.fair_probability_yes < 1.0
    assert result.model_uncalibrated is True
    assert len({output.family for output in result.outputs}) == 7


def test_model_replay_is_deterministic_and_never_emits_fake_certainty() -> None:
    ensemble = DeterministicEdgeEnsemble()
    ctx = context()
    first = ensemble.evaluate(ctx)
    second = ensemble.evaluate(ctx)
    assert first == second
    assert [output.to_dict() for output in first.outputs] == [
        output.to_dict() for output in second.outputs
    ]
    assert all(0.0 < output.estimated_probability_yes < 1.0 for output in first.outputs)
    assert all(-1.0 <= output.raw_score <= 1.0 for output in first.outputs)
