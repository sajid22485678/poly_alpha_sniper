from __future__ import annotations

import pytest

from lite_frequency_v4.config import FrequencyV4Config
from lite_frequency_v4.contracts import (
    AnchorStatus,
    BookLevel,
    EntrySide,
    FairValueResult,
    FairValueSide,
    MarketIdentity,
    Sweep,
)
from lite_frequency_v4.economics import EconomicCalculation
from lite_frequency_v4.execution import (
    ExecutionRouter,
    ExecutionState,
    RouterAction,
    RouterDecision,
    ShadowEntryBuilder,
    candidate_contract,
    candidate_key,
    tier_for_edge,
)


OPEN_MS = 1_800_000_000_000
NOW_MS = OPEN_MS + 120_000
MONO_NS = 500_000_000_000


def identity(*, asset: str = "BTC", market_id: str = "market-1") -> MarketIdentity:
    return MarketIdentity(
        asset=asset,
        slug=f"{asset.lower()}-updown-5m-{OPEN_MS // 1000}",
        market_id=market_id,
        event_id=f"event-{asset.lower()}",
        condition_id=f"condition-{asset.lower()}",
        yes_token_id=f"yes-{asset.lower()}",
        no_token_id=f"no-{asset.lower()}",
        window_open_ms=OPEN_MS,
        window_close_ms=OPEN_MS + 300_000,
        anchor_status=AnchorStatus.FIELD_MISSING,
    )


def calc(
    edge: float,
    *,
    selected: EntrySide = EntrySide.BUY_YES,
    price: float = 0.40,
    invalid_reasons: tuple[str, ...] = (),
    phase: str = "INITIAL",
) -> EconomicCalculation:
    probability_yes = 0.65 if selected is EntrySide.BUY_YES else 0.35
    yes_edge = edge if selected is EntrySide.BUY_YES else -0.05
    no_edge = edge if selected is EntrySide.BUY_NO else -0.05
    yes_price = price if selected is EntrySide.BUY_YES else 0.60
    no_price = price if selected is EntrySide.BUY_NO else 0.60
    yes = FairValueSide(
        side=EntrySide.BUY_YES,
        fair_probability=probability_yes,
        executable_vwap=yes_price,
        worst_consumed_price=yes_price,
        spread=0.02,
        depth_shares=10.0,
        estimated_fee=0.01,
        execution_buffer=0.002,
        latency_buffer=0.001,
        uncertainty_buffer=0.01,
        net_edge=yes_edge,
        evidence_age_ms=100,
        valid=True,
    )
    no = FairValueSide(
        side=EntrySide.BUY_NO,
        fair_probability=1.0 - probability_yes,
        executable_vwap=no_price,
        worst_consumed_price=no_price,
        spread=0.02,
        depth_shares=10.0,
        estimated_fee=0.01,
        execution_buffer=0.002,
        latency_buffer=0.001,
        uncertainty_buffer=0.01,
        net_edge=no_edge,
        evidence_age_ms=100,
        valid=True,
    )
    result = FairValueResult(
        calculated_ts_ms=NOW_MS,
        phase=phase,
        fair_probability_yes=probability_yes,
        fair_probability_no=1.0 - probability_yes,
        yes=yes,
        no=no,
        selected_side=selected,
        selected_net_edge=edge,
        coherent=True,
        model_uncalibrated=True,
        evidence_event_ids=("book-yes", "book-no", "cex-1"),
    )
    selected_level = BookLevel(price, 5.0)
    selected_sweep = Sweep(
        side="BUY",
        shares=5.0,
        notional=price * 5.0,
        vwap=price,
        worst_price=price,
        levels=(selected_level,),
    )
    return EconomicCalculation(
        result=result,
        yes_sweep=selected_sweep if selected is EntrySide.BUY_YES else None,
        no_sweep=selected_sweep if selected is EntrySide.BUY_NO else None,
        yes_fee_total=0.05 if selected is EntrySide.BUY_YES else None,
        no_fee_total=0.05 if selected is EntrySide.BUY_NO else None,
        invalid_reasons=invalid_reasons,
    )


@pytest.mark.parametrize(
    ("edge", "tier"),
    [
        (0.020, "STRONG_CROSS"),
        (0.019999, "MEDIUM_MAKER"),
        (0.010, "MEDIUM_MAKER"),
        (0.009999, "WEAK_CONFIRM"),
        (0.005, "WEAK_CONFIRM"),
        (0.004999, "BELOW_ENTRY_CLASSIFICATION"),
        (0.0, "NO_POSITIVE_EDGE"),
        (-0.1, "NO_POSITIVE_EDGE"),
    ],
)
def test_transparent_execution_tiers(edge: float, tier: str) -> None:
    assert tier_for_edge(edge, FrequencyV4Config()) == tier


def test_strong_edge_crosses_immediately_with_no_maker_fill_claim() -> None:
    router = ExecutionRouter(FrequencyV4Config())
    decision = router.on_candidate(
        identity=identity(), calculation=calc(0.020),
        now_ms=NOW_MS, monotonic_ns=MONO_NS,
    )
    assert decision.action is RouterAction.CROSS_SPREAD
    assert decision.reason == "strong_edge_immediate_cross"
    assert decision.side is EntrySide.BUY_YES
    assert decision.state is None
    assert decision.maker_fill_assumed is False
    assert router.active(identity()) is None


def test_medium_edge_observes_and_crosses_on_fresh_subsecond_recompute() -> None:
    cfg = FrequencyV4Config()
    router = ExecutionRouter(cfg)
    started = router.on_candidate(
        identity=identity(), calculation=calc(0.015),
        now_ms=NOW_MS, monotonic_ns=MONO_NS,
    )
    assert started.action is RouterAction.START_OBSERVATION
    assert started.state is not None
    assert started.state.state is ExecutionState.MAKER_OBSERVE
    assert started.state.maker_deadline_ts_ms - started.state.maker_start_ts_ms == 1_000
    assert started.state.maker_fill_assumed is False

    early = router.on_candidate(
        identity=identity(), calculation=calc(0.030),
        now_ms=NOW_MS + 499, monotonic_ns=MONO_NS + 499_000_000,
    )
    assert early.action is RouterAction.CONTINUE_OBSERVING
    crossed = router.on_candidate(
        identity=identity(), calculation=calc(0.030),
        now_ms=NOW_MS + 500, monotonic_ns=MONO_NS + 500_000_000,
    )
    assert crossed.action is RouterAction.CROSS_SPREAD
    assert crossed.actual_observation_ms == 500
    assert crossed.reason == "fresh_recompute_clears_cross"
    assert crossed.maker_fill_assumed is False
    assert crossed.state is not None and crossed.state.price_touched is False


def test_weak_edge_is_confirmation_only_until_fresh_cross_threshold() -> None:
    router = ExecutionRouter(FrequencyV4Config())
    started = router.on_candidate(
        identity=identity(), calculation=calc(0.007),
        now_ms=NOW_MS, monotonic_ns=MONO_NS,
    )
    assert started.action is RouterAction.START_OBSERVATION
    assert started.state is not None
    assert started.state.state is ExecutionState.CONFIRM_ONLY
    crossed = router.on_candidate(
        identity=identity(), calculation=calc(0.021),
        now_ms=NOW_MS + 600, monotonic_ns=MONO_NS + 600_000_000,
    )
    assert crossed.action is RouterAction.CROSS_SPREAD
    assert crossed.reason == "fresh_recompute_clears_cross"


def test_transient_edge_dip_does_not_prematurely_finalize_window() -> None:
    router = ExecutionRouter(FrequencyV4Config())
    router.on_candidate(
        identity=identity(), calculation=calc(0.015),
        now_ms=NOW_MS, monotonic_ns=MONO_NS,
    )
    dip = router.on_candidate(
        identity=identity(), calculation=calc(-0.010),
        now_ms=NOW_MS + 700, monotonic_ns=MONO_NS + 700_000_000,
    )
    assert dip.action is RouterAction.CONTINUE_OBSERVING
    assert router.active(identity()) is not None
    expired = router.on_candidate(
        identity=identity(), calculation=calc(-0.010),
        now_ms=NOW_MS + 1_000, monotonic_ns=MONO_NS + 1_000_000_000,
    )
    assert expired.action is RouterAction.SKIP
    assert expired.reason == "edge_expired"
    assert expired.actual_observation_ms == 1_000
    assert router.active(identity()) is None


def test_chase_worsening_beyond_cap_is_rejected_after_recompute() -> None:
    router = ExecutionRouter(FrequencyV4Config())
    router.on_candidate(
        identity=identity(), calculation=calc(0.015, price=0.40),
        now_ms=NOW_MS, monotonic_ns=MONO_NS,
    )
    decision = router.on_candidate(
        identity=identity(), calculation=calc(0.030, price=0.421),
        now_ms=NOW_MS + 500, monotonic_ns=MONO_NS + 500_000_000,
    )
    assert decision.action is RouterAction.SKIP
    assert decision.reason == "chase_rejected"
    assert decision.chase_worsening == pytest.approx(0.021)
    assert decision.state is not None
    assert decision.state.state is ExecutionState.SKIPPED


def test_genuine_evidence_safety_failure_terminates_observation_immediately() -> None:
    router = ExecutionRouter(FrequencyV4Config())
    router.on_candidate(
        identity=identity(), calculation=calc(0.015),
        now_ms=NOW_MS, monotonic_ns=MONO_NS,
    )
    failure = router.on_candidate(
        identity=identity(), calculation=calc(0.025, invalid_reasons=("stale_snapshot",)),
        now_ms=NOW_MS + 100, monotonic_ns=MONO_NS + 100_000_000,
    )
    assert failure.action is RouterAction.SAFETY_FAIL
    assert failure.reason == "stale_snapshot"
    assert failure.actual_observation_ms == 100
    assert failure.state is not None
    assert failure.state.state is ExecutionState.SAFETY_FAILED
    assert router.active(identity()) is None


def test_one_window_controller_never_switches_to_opposite_side() -> None:
    router = ExecutionRouter(FrequencyV4Config())
    original = identity()
    router.on_candidate(
        identity=original, calculation=calc(0.015, selected=EntrySide.BUY_YES),
        now_ms=NOW_MS, monotonic_ns=MONO_NS,
    )
    changed = router.on_candidate(
        identity=identity(market_id="duplicate-market"),
        calculation=calc(0.030, selected=EntrySide.BUY_NO),
        now_ms=NOW_MS + 600,
        monotonic_ns=MONO_NS + 600_000_000,
    )
    assert changed.action is RouterAction.CONTINUE_OBSERVING
    assert changed.side is EntrySide.BUY_YES
    terminal = router.on_candidate(
        identity=original,
        calculation=calc(0.030, selected=EntrySide.BUY_NO),
        now_ms=NOW_MS + 1_000,
        monotonic_ns=MONO_NS + 1_000_000_000,
    )
    assert terminal.action is RouterAction.SKIP
    assert terminal.reason == "selected_side_changed"


def test_below_threshold_positive_and_negative_edges_never_enter() -> None:
    router = ExecutionRouter(FrequencyV4Config())
    weak = router.on_candidate(
        identity=identity(), calculation=calc(0.004),
        now_ms=NOW_MS, monotonic_ns=MONO_NS,
    )
    negative = router.on_candidate(
        identity=identity(asset="ETH"), calculation=calc(-0.2),
        now_ms=NOW_MS, monotonic_ns=MONO_NS,
    )
    assert weak.action is RouterAction.NO_ACTION
    assert weak.reason == "positive_edge_below_weak_threshold"
    assert negative.action is RouterAction.NO_ACTION
    assert negative.reason == "no_positive_fee_net_edge"


def test_shadow_entry_builder_is_exactly_five_shares_and_never_assumes_maker_fill() -> None:
    cfg = FrequencyV4Config()
    calculation = calc(0.025, price=0.40)
    decision = ExecutionRouter(cfg).on_candidate(
        identity=identity(), calculation=calculation,
        now_ms=NOW_MS, monotonic_ns=MONO_NS,
    )
    row = ShadowEntryBuilder(cfg).build(
        identity=identity(),
        decision=decision,
        candidate_id=1,
        window_lock_id=2,
        runtime_session_id="session-1",
        current_commit="abc123",
        now_ms=NOW_MS,
    )
    assert row["strategy_id"] == "lite_frequency_v4"
    assert row["mode"] == "lite_frequency_v4_shadow"
    assert row["shares"] == 5.0
    assert row["entry_mode"] == "CROSS_SPREAD"
    assert row["maker_fill_assumed"] == 0
    assert row["entry_cost"] == pytest.approx(row["entry_notional"] + row["entry_fee"])
    with pytest.raises(ValueError, match="only an evidenced cross"):
        ShadowEntryBuilder(cfg).build(
            identity=identity(),
            decision=RouterDecision(
                RouterAction.NO_ACTION, "test", "NO_POSITIVE_EDGE",
                EntrySide.BUY_YES, calculation, None,
            ),
            candidate_id=1,
            window_lock_id=2,
            runtime_session_id="session-1",
            current_commit="abc123",
            now_ms=NOW_MS,
        )


def test_candidate_contract_and_router_replay_are_deterministic() -> None:
    market = identity()
    calculation = calc(0.015)
    decisions = []
    for _ in range(2):
        router = ExecutionRouter(FrequencyV4Config())
        decisions.append(router.on_candidate(
            identity=market,
            calculation=calculation,
            now_ms=NOW_MS,
            monotonic_ns=MONO_NS,
        ))
    assert candidate_key(market, calculation, NOW_MS) == candidate_key(
        market, calculation, NOW_MS
    )
    assert decisions[0].action == decisions[1].action
    assert decisions[0].state is not None and decisions[1].state is not None
    assert decisions[0].state.candidate_id == decisions[1].state.candidate_id
    candidate = candidate_contract(
        identity=market,
        models=(),
        regime="NORMAL",
        calculation=calculation,
        decision=decisions[0],
        now_ms=NOW_MS,
        monotonic_ns=MONO_NS,
    )
    assert candidate.maker_fill_assumed is False
    assert candidate.initial_fair_value == calculation.result
    assert candidate.candidate_id == decisions[0].state.candidate_id
