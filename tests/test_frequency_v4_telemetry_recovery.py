"""Recovery semantics under sustained, safe policy sampling.

The 34.6-minute controlled gate at ``1f30bd5`` produced a lane that was safe by
every measure the system has and still could not certify:

- ``telemetry_data_safety`` HEALTHY in 68/70 samples
- critical evidence lost **0**, incomplete **0**
- unexpected noncritical loss **0**, reconciliation mismatch **0**
- queues bounded in 70/70
- ``telemetry_capacity_state`` ``POLICY_SAMPLING_ACTIVE`` in **70/70**

...and ``operational_ready`` true in only 29/70, with exactly one degraded
reason across all 70 samples: ``telemetry_recovery_window``.  The recovery
blockers were ``uncontrolled_overload`` (28), ``queue_above_low_water`` (23),
``queue_accumulating`` (12) and ``service_imbalance`` (4).

The root cause is that two of those blockers were gated on the queue draining
back below the **low-water mark** -- ~1,024 rows in a 20,000-row queue.  Low
water is the threshold at which the shedding policy *engages*; requiring the
lane to fall back under it made sustained-but-safe sampling permanently
uncertifiable.  ``controlled_overload`` carried the same test, so both fired
together and the healthy-window counter reset before it could ever reach ten.

The corrected contract: recovery is gated on the queue being **bounded** and
non-accumulating, on conservation closing, and on the policy being internally
consistent -- never on the pressure threshold that starts the policy.
POLICY_SAMPLING_ACTIVE stays fully visible; it is reported, not penalised.
"""
from __future__ import annotations

import math

import pytest

from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    _RECOVERY_HEALTHY_WINDOWS,
    _RECOVERY_SETTLE_S,
    TelemetryCapacityState,
    _AdaptiveTelemetryController,
    _capacity_state,
)


#: Offered rows per control tick that genuinely drives the lane into
#: ``POLICY_SAMPLING_ACTIVE``.  Below this the synthetic load stays
#: ``WITHIN_CAPACITY`` and the tests would pass without exercising the state
#: the whole contract is about.
_SAMPLING_OFFERED = 2_000


def _controller(*, maximum: int = 32, capacity: int = 20_000):
    return _AdaptiveTelemetryController(
        physical_max_chunk=maximum,
        queue_capacity=capacity,
        flush_interval_s=0.25,
        budgeted_sink=True,
        started_monotonic=0.0,
    )


#: Wall time one synthetic dispatch occupies the lane.  The controller derives
#: sustainable dispatch rate from *service* time, so this is what decides
#: whether a scenario is genuinely at capacity: 20 ms per dispatch means 50
#: dispatches/s, and 50 x the 32-row physical chunk is 1,600 rows/s.  The
#: sampling scenarios below offer 2,000 rows/tick, so they overload the lane by
#: a real shortfall rather than by an artefact of how often it happens to be
#: asked to run.
_DISPATCH_MS = 20.0


def _drive(controller, depths, *, admitted=20, committed=20, tick0=1,
           overload_handled=0, dispatch_ms=_DISPATCH_MS, **deltas):
    """Drive a depth series with the fixture's own accounting closed exactly.

    Rows admitted but neither committed nor still queued are attributed to the
    shedding policy, exactly as the writer attributes them.  Without that the
    depth series describes inventory moving for no reason, which the
    conservation identity correctly refuses to call balanced.
    """

    decision = None
    # Phases chain, so the lane does not start empty; read back what the
    # counters say is still held rather than assuming zero.
    held = controller.window.conservation_now(
        0.0, queued_logical=0, inflight_logical=0).gap
    for offset, depth in enumerate(depths):
        tick = float(tick0 + offset)
        resolved = admitted - committed - (depth - held)
        extra, resolved = (0, resolved) if resolved >= 0 else (-resolved, 0)
        controller.add(tick, queue_depth=depth, offered=admitted + extra,
                       admitted=admitted + extra,
                       overload_handled=overload_handled + resolved,
                       policy_resolved=resolved,
                       **deltas)
        controller.observe_commit(
            now=tick, rows=committed, logical_rows=committed,
            transaction_ms=dispatch_ms, total_ms=dispatch_ms,
            queue_depth=depth)
        held = depth
        decision = controller.decide(
            now=tick, queue_depth=depth, transaction_budget_ms=250.0,
            queued_logical=depth)
    return decision


class _Timeline:
    """Drive one controller across contiguous ticks.

    Continuity matters: jumping the clock forward drops the rolling window and
    would make "the streak survived" untestable for the wrong reason.  Every
    phase here continues from the previous tick.
    """

    def __init__(self, controller):
        self.controller = controller
        self.tick = 1

    def run(self, depths, **kwargs):
        decision = _drive(self.controller, depths, tick0=self.tick, **kwargs)
        self.tick += len(depths)
        return decision

    def sample(self, depths, **kwargs):
        """Run under load heavy enough to be genuinely POLICY_SAMPLING_ACTIVE."""

        kwargs.setdefault("admitted", _SAMPLING_OFFERED)
        kwargs.setdefault("committed", _SAMPLING_OFFERED)
        kwargs.setdefault("overload_handled", 4)
        return self.run(depths, **kwargs)


def _sustained_sampling_depths(controller, samples: int = 120):
    """A queue parked above low water, bounded, oscillating, never growing.

    This is the measured production shape: sustained pressure well inside the
    hard bound, with the shedding policy holding it there.
    """

    base = controller.low_water + 40
    return [base + (offset % 7) - 3 for offset in range(samples)]


# ---------------------------------------------------------------------------
# 1-6. Safe policy sampling accumulates and certifies.
# ---------------------------------------------------------------------------


def test_policy_sampling_active_accumulates_healthy_windows():
    controller = _controller()
    timeline = _Timeline(controller)
    decision = timeline.sample(_sustained_sampling_depths(controller))
    assert decision.overload_active is True          # genuinely shedding
    assert decision.sampling_keep_ratio < 1.0
    assert decision.recovery_healthy_windows > 0
    # The two blockers that pinned the streak at zero for all 70 gate samples.
    assert "queue_above_low_water" not in decision.recovery_blockers
    assert "uncontrolled_overload" not in decision.recovery_blockers


def test_ten_safe_policy_sampling_windows_recover_readiness():
    controller = _controller()
    timeline = _Timeline(controller)
    decision = timeline.sample(_sustained_sampling_depths(controller, 200))
    assert decision.overload_active is True
    assert decision.controlled_overload is True
    assert decision.recovery_healthy_windows >= _RECOVERY_HEALTHY_WINDOWS
    assert decision.current_operational_healthy is True
    assert decision.recovery_blockers == ()


def test_capacity_state_stays_honestly_visible_while_recovering():
    """Recovery must not be bought by relabelling the capacity state."""

    controller = _controller()
    timeline = _Timeline(controller)
    decision = timeline.sample(_sustained_sampling_depths(controller, 200))
    state = _capacity_state(
        decision, queue_depth=controller.low_water + 40,
        capacity=controller.queue_capacity)
    assert decision.current_operational_healthy is True
    # Certified *and* still honestly reported as shedding.
    assert state == TelemetryCapacityState.POLICY_SAMPLING_ACTIVE.value
    assert state != TelemetryCapacityState.WITHIN_CAPACITY.value


def test_keep_ratio_changes_do_not_reset_healthy_windows():
    controller = _controller()
    timeline = _Timeline(controller)
    depths = _sustained_sampling_depths(controller, 200)
    timeline.sample(depths)
    before = controller._healthy_streak
    assert before >= _RECOVERY_HEALTHY_WINDOWS
    # A keep-ratio move driven by offered load, with nothing lost.
    decision = timeline.sample(depths[:40], admitted=_SAMPLING_OFFERED * 3, committed=_SAMPLING_OFFERED * 3)
    assert decision.sampling_keep_ratio <= 1.0
    assert decision.recovery_healthy_windows > before   # kept counting up
    assert "settling" not in decision.recovery_blockers


def test_safe_chunk_changes_do_not_reset_healthy_windows():
    controller = _controller()
    timeline = _Timeline(controller)
    timeline.sample(_sustained_sampling_depths(controller, 200))
    before = controller._healthy_streak
    selected_before = controller.selected_chunk
    # Move the throughput floor -- and so the selected chunk -- with no loss.
    decision = timeline.sample(_sustained_sampling_depths(controller, 60), admitted=_SAMPLING_OFFERED * 4, committed=_SAMPLING_OFFERED * 4)
    assert controller.throughput_required_chunk != selected_before
    assert decision.recovery_healthy_windows > before
    assert "settling" not in decision.recovery_blockers


def test_capacity_transitions_do_not_reset_healthy_windows():
    """WITHIN_CAPACITY <-> POLICY_SAMPLING_ACTIVE is not a setback.

    Crossing back and forth may legitimately cost the *queue-trend* conjunct
    for a few windows while the rolling view catches up, but it must never cost
    a settle window -- that is the 20 s penalty which, applied per transition,
    made the requirement unreachable under fluctuating load.
    """

    controller = _controller()
    timeline = _Timeline(controller)
    high = _sustained_sampling_depths(controller, 120)
    busy = timeline.sample(high)
    assert _capacity_state(
        busy, queue_depth=controller.low_water + 40,
        capacity=controller.queue_capacity
    ) == TelemetryCapacityState.POLICY_SAMPLING_ACTIVE.value

    # Load falls away and the policy deactivates on its own.
    quiet = timeline.run([0] * 120)
    assert _capacity_state(
        quiet, queue_depth=0, capacity=controller.queue_capacity
    ) == TelemetryCapacityState.WITHIN_CAPACITY.value

    back = timeline.sample(high)
    assert _capacity_state(
        back, queue_depth=controller.low_water + 40,
        capacity=controller.queue_capacity
    ) == TelemetryCapacityState.POLICY_SAMPLING_ACTIVE.value

    # No settle penalty was charged for either transition, and readiness
    # re-certifies on the far side of both.
    for decision in (busy, quiet, back):
        assert "settling" not in decision.recovery_blockers
    assert quiet.recovery_healthy_windows >= _RECOVERY_HEALTHY_WINDOWS
    assert back.recovery_healthy_windows >= _RECOVERY_HEALTHY_WINDOWS
    assert back.current_operational_healthy is True


def test_policy_coalescing_with_exact_accounting_remains_recoverable():
    controller = _controller()
    decision = _drive(
        controller, _sustained_sampling_depths(controller, 200),
        overload_handled=4, coalesced=6)
    assert decision.recovery_healthy_windows >= _RECOVERY_HEALTHY_WINDOWS
    assert decision.recovery_blockers == ()


# ---------------------------------------------------------------------------
# 7-13. Genuine safety failures still reset or block.
# ---------------------------------------------------------------------------


def test_unexpected_loss_resets_recovery_immediately():
    controller = _controller()
    timeline = _Timeline(controller)
    timeline.sample(_sustained_sampling_depths(controller, 200))
    assert controller._healthy_streak >= _RECOVERY_HEALTHY_WINDOWS
    decision = timeline.sample(_sustained_sampling_depths(controller, 6), lost=3)
    assert "unexpected_loss" in decision.recovery_blockers
    assert decision.recovery_healthy_windows == 0
    assert decision.current_operational_healthy is False


def test_admission_overflow_resets_recovery_immediately():
    controller = _controller()
    timeline = _Timeline(controller)
    timeline.sample(_sustained_sampling_depths(controller, 200))
    decision = timeline.sample(_sustained_sampling_depths(controller, 6), admission_overflow=2)
    assert "admission_overflow" in decision.recovery_blockers
    assert decision.recovery_healthy_windows == 0


def test_queue_hard_cap_breach_blocks_readiness():
    controller = _controller(capacity=256)
    timeline = _Timeline(controller)
    timeline.run([40, 42, 41, 43, 40] * 40, overload_handled=4)
    decision = timeline.run([256, 256, 256], overload_handled=4)
    assert "queue_hard_cap_breached" in decision.recovery_blockers
    assert decision.recovery_healthy_windows == 0
    assert decision.controlled_overload is False


def test_unbounded_queue_growth_blocks_readiness():
    controller = _controller(capacity=1_000)
    decision = _drive(controller, list(range(10, 700, 10)), overload_handled=4)
    assert "queue_accumulating" in decision.recovery_blockers
    assert decision.recovery_healthy_windows == 0


def test_queue_recovery_resumes_healthy_accumulation():
    controller = _controller(capacity=1_000)
    timeline = _Timeline(controller)
    timeline.run(list(range(10, 700, 10)), overload_handled=4)
    assert controller._healthy_streak == 0
    decision = timeline.run([12, 9, 14, 8, 11, 10] * 40, overload_handled=4)
    assert "queue_accumulating" not in decision.recovery_blockers
    assert decision.recovery_healthy_windows >= _RECOVERY_HEALTHY_WINDOWS


def test_hard_overload_is_reported_when_control_is_actually_lost():
    controller = _controller()
    decision = _drive(controller, _sustained_sampling_depths(controller, 40),
                      overload_handled=4, lost=5)
    state = _capacity_state(
        decision, queue_depth=controller.low_water + 40,
        capacity=controller.queue_capacity)
    assert state == TelemetryCapacityState.HARD_OVERLOAD.value
    assert decision.current_operational_healthy is False


def test_policy_inconsistency_blocks_recovery(monkeypatch):
    """An incoherent overload policy is not a safe one, however quiet the lane."""

    controller = _controller()
    timeline = _Timeline(controller)
    timeline.sample(_sustained_sampling_depths(controller, 200))
    assert controller._healthy_streak >= _RECOVERY_HEALTHY_WINDOWS
    monkeypatch.setattr(
        type(controller), "_policy_consistent",
        lambda _self, *, keep_ratio=None: False)
    decision = timeline.sample(_sustained_sampling_depths(controller, 4))
    assert "policy_inconsistent" in decision.recovery_blockers
    assert decision.controlled_overload is False
    assert decision.recovery_healthy_windows == 0


def test_an_unnamed_active_overload_is_inconsistent():
    """The predicate itself: overload active with no reason is incoherent."""

    controller = _controller()
    assert controller._policy_consistent() is True
    controller._overload_active = True
    controller._overload_reason = None
    assert controller._policy_consistent() is False
    controller._overload_reason = "queue_high_water"
    assert controller._policy_consistent() is True
    # ...and the mirror case: a reason with no active overload.
    controller._overload_active = False
    assert controller._policy_consistent() is False


def test_policy_consistency_rejects_an_impossible_keep_ratio():
    controller = _controller()
    assert controller._policy_consistent(keep_ratio=0.4) is True
    assert controller._policy_consistent(keep_ratio=1.0) is True
    assert controller._policy_consistent(keep_ratio=-0.1) is False
    assert controller._policy_consistent(keep_ratio=1.5) is False
    assert controller._policy_consistent(keep_ratio=float("nan")) is False
    assert controller._policy_consistent(keep_ratio=True) is False


# ---------------------------------------------------------------------------
# 14-20. The export contract: honest, fail-closed, and gated on every check.
# ---------------------------------------------------------------------------


def _telemetry_payload(**overrides):
    base = {
        "state": "HEALTHY",
        "telemetry_data_safety": "HEALTHY",
        "telemetry_capacity_state": "POLICY_SAMPLING_ACTIVE",
        "current_operational_healthy": True,
        "recovery_sample_age_ms": 100,
        "recovery_healthy_windows": _RECOVERY_HEALTHY_WINDOWS,
        "recovery_required_windows": _RECOVERY_HEALTHY_WINDOWS,
        "recovery_blockers": [],
        "queue_bounded": True,
        "queue_oldest_age_s": 0.4,
        "window_unexpected_loss_rows": 0,
        "accounting_reconciliation_mismatch_rows": 0,
        "true_lost_critical_rows": 0,
        "sampling_keep_ratio": 0.6,
        "policy_sampling_active": True,
    }
    base.update(overrides)
    return base


def _readiness(telemetry):
    """Recompute the export's telemetry readiness verdict from a payload.

    Mirrors ``build_frequency_v4_dashboard``'s telemetry gating so the contract
    can be asserted without standing up a whole store.
    """

    data_safety = str(telemetry.get("telemetry_data_safety") or "UNKNOWN")
    capacity = str(telemetry.get("telemetry_capacity_state") or "UNKNOWN")
    healthy_windows = int(telemetry.get("recovery_healthy_windows") or 0)
    required = int(telemetry.get("recovery_required_windows") or 0)
    age = telemetry.get("recovery_sample_age_ms")
    sample_fresh = (
        isinstance(age, (int, float)) and not isinstance(age, bool)
        and math.isfinite(float(age)) and 0 <= float(age) <= 5_000
    )
    recovery_healthy = (
        telemetry.get("current_operational_healthy") is True
        and sample_fresh and required > 0 and healthy_windows >= required
    )
    policy_sampling = capacity in {
        "POLICY_SAMPLING_ACTIVE", "POLICY_COALESCING_ACTIVE",
        "POLICY_DEFER_ACTIVE", "RECOVERING",
    }
    reasons = [
        *(["telemetry_writer_unhealthy"]
          if str(telemetry.get("state") or "") != "HEALTHY" else []),
        *([f"telemetry_data_safety_{data_safety.lower()}"]
          if data_safety in {"UNSAFE", "UNKNOWN"} else []),
        *(["telemetry_unexpected_noncritical_loss"]
          if int(telemetry.get("window_unexpected_loss_rows") or 0) > 0 else []),
        *(["telemetry_accounting_mismatch"] if int(
            telemetry.get("accounting_reconciliation_mismatch_rows") or 0)
          else []),
        *(["telemetry_queue_unbounded"]
          if not bool(telemetry.get("queue_bounded", False)) else []),
        *([f"telemetry_capacity_{capacity.lower()}"]
          if capacity in {"HARD_OVERLOAD", "UNKNOWN"} else []),
        *(["telemetry_recovery_window"] if not recovery_healthy else []),
        *(["telemetry_recovery_sample_stale"] if not sample_fresh else []),
    ]
    combined = (
        "HEALTHY_WITH_POLICY_SAMPLING"
        if data_safety == "HEALTHY" and policy_sampling
        else data_safety if data_safety != "HEALTHY"
        else "HEALTHY" if capacity == "WITHIN_CAPACITY" else capacity
    )
    return {"ready": not reasons, "reasons": reasons, "combined": combined}


def test_healthy_with_policy_sampling_reaches_ready():
    verdict = _readiness(_telemetry_payload())
    assert verdict["ready"] is True
    assert verdict["reasons"] == []
    assert verdict["combined"] == "HEALTHY_WITH_POLICY_SAMPLING"


def test_historical_lifetime_loss_does_not_block_current_health():
    verdict = _readiness(_telemetry_payload(
        rows_dropped=50_000, raw_telemetry_loss_count=50_000,
        window_unexpected_loss_rows=0))
    assert verdict["ready"] is True


def test_current_unexpected_loss_blocks():
    verdict = _readiness(_telemetry_payload(window_unexpected_loss_rows=1))
    assert verdict["ready"] is False
    assert "telemetry_unexpected_noncritical_loss" in verdict["reasons"]


def test_reconciliation_mismatch_blocks():
    verdict = _readiness(_telemetry_payload(
        accounting_reconciliation_mismatch_rows=-2))
    assert verdict["ready"] is False
    assert "telemetry_accounting_mismatch" in verdict["reasons"]


def test_hard_overload_and_unknown_block_readiness():
    for capacity in ("HARD_OVERLOAD", "UNKNOWN"):
        verdict = _readiness(_telemetry_payload(
            telemetry_capacity_state=capacity))
        assert verdict["ready"] is False
        assert f"telemetry_capacity_{capacity.lower()}" in verdict["reasons"]


def test_missing_fields_fail_closed():
    verdict = _readiness({})
    assert verdict["ready"] is False
    assert "telemetry_data_safety_unknown" in verdict["reasons"]
    assert "telemetry_capacity_unknown" in verdict["reasons"]
    assert "telemetry_recovery_window" in verdict["reasons"]
    assert "telemetry_recovery_sample_stale" in verdict["reasons"]
    assert "telemetry_queue_unbounded" in verdict["reasons"]


def test_readiness_requires_every_gate_not_just_recovery():
    """Recovery certifying is necessary, never sufficient."""

    for override in (
        {"queue_bounded": False},
        {"telemetry_data_safety": "UNSAFE"},
        {"state": "DEGRADED"},
        {"recovery_sample_age_ms": 60_000},
        {"recovery_healthy_windows": _RECOVERY_HEALTHY_WINDOWS - 1},
        {"current_operational_healthy": False},
    ):
        verdict = _readiness(_telemetry_payload(**override))
        assert verdict["ready"] is False, override


def test_settle_window_is_preserved_as_a_real_gate():
    """Recovery still costs a settle window after a genuine setback."""

    controller = _controller()
    timeline = _Timeline(controller)
    timeline.sample(_sustained_sampling_depths(controller, 200))
    assert controller._healthy_streak >= _RECOVERY_HEALTHY_WINDOWS
    controller._reset_settle(float(timeline.tick))
    decision = timeline.sample(_sustained_sampling_depths(controller, 2))
    assert "settling" in decision.recovery_blockers
    assert decision.recovery_healthy_windows == 0
    assert _RECOVERY_SETTLE_S >= 15.0


def test_required_window_count_is_still_ten():
    """The bar is not lowered to buy readiness."""

    assert _RECOVERY_HEALTHY_WINDOWS == 10
