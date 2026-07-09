"""Mission D/J: safety of the aggressive shadow opportunity engine.

The whole point of these tests: prove the mode is structurally incapable of
enabling live, placing/cancelling orders, or accepting a candidate the
standard pipeline would reject. Every safety claim is asserted, not assumed.
"""
import itertools
from pathlib import Path

import pytest

from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.core.contracts import TradingMode
from poly_alpha_sniper.strategy.opportunity_engine import (
    OpportunityGates, RESEARCH_ONLY, ROUTINE, STANDARD_QUALIFIED, WATCHLIST_ONLY,
    classify_opportunity, config_is_safe, is_active_for_mode,
    qualified_is_impossible_without_gates)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _all_pass():
    return OpportunityGates(True, True, True, True, True, True, standard_pipeline_accepted=True)


def test_default_config_is_safe():
    assert config_is_safe(load_config()) is True


def test_apply_to_live_flip_makes_config_unsafe():
    cfg = load_config()
    cfg.shadow_aggressive_opportunity_mode.apply_to_live = True
    assert config_is_safe(cfg) is False


def test_force_trade_count_flip_makes_config_unsafe():
    cfg = load_config()
    cfg.shadow_aggressive_opportunity_mode.force_trade_count = True
    assert config_is_safe(cfg) is False


@pytest.mark.parametrize("flag", [
    "require_positive_ev", "require_oracle_anchor", "require_executable_book",
    "require_spread_ok", "require_depth_ok", "require_risk_ok"])
def test_dropping_any_required_invariant_makes_config_unsafe(flag):
    cfg = load_config()
    setattr(cfg.shadow_aggressive_opportunity_mode, flag, False)
    assert config_is_safe(cfg) is False


def test_mode_never_active_in_live_modes():
    cfg = load_config()
    for mode in (TradingMode.LIVE_MICRO, TradingMode.LIVE_FULL):
        assert is_active_for_mode(cfg, mode) is False


def test_mode_active_in_shadow_and_simulation():
    cfg = load_config()
    assert is_active_for_mode(cfg, TradingMode.SHADOW_LIVE) is True
    assert is_active_for_mode(cfg, TradingMode.SIMULATION) is True


def test_mode_inactive_when_config_unsafe_even_in_shadow():
    cfg = load_config()
    cfg.shadow_aggressive_opportunity_mode.apply_to_live = True
    assert is_active_for_mode(cfg, TradingMode.SHADOW_LIVE) is False


def test_qualified_requires_all_gates_and_pipeline_acceptance():
    assert classify_opportunity(_all_pass()) == STANDARD_QUALIFIED


def test_missing_oracle_can_never_be_qualified():
    g = OpportunityGates(False, True, True, True, True, True, True)
    for tier in (None, "HOT_NEAR_MISS", "NEAR_MISS", "WATCHLIST", "ROUTINE_NO_SHOCK"):
        assert classify_opportunity(g, near_miss_tier=tier) != STANDARD_QUALIFIED


def test_missing_book_can_never_be_qualified():
    g = OpportunityGates(True, False, True, True, True, True, True)
    assert classify_opportunity(g) != STANDARD_QUALIFIED


def test_bad_spread_can_never_be_qualified():
    g = OpportunityGates(True, True, False, True, True, True, True)
    assert classify_opportunity(g) != STANDARD_QUALIFIED


def test_negative_ev_can_never_be_qualified():
    g = OpportunityGates(True, True, True, True, True, False, True)
    assert classify_opportunity(g) != STANDARD_QUALIFIED


def test_not_accepted_by_pipeline_is_never_qualified_even_with_all_gates():
    g = OpportunityGates(True, True, True, True, True, True, standard_pipeline_accepted=False)
    assert classify_opportunity(g) != STANDARD_QUALIFIED


def test_near_miss_is_at_most_watchlist():
    g = OpportunityGates(True, False, True, True, True, True, False)
    assert classify_opportunity(g, near_miss_tier="HOT_NEAR_MISS") == WATCHLIST_ONLY
    assert classify_opportunity(g, near_miss_tier="ROUTINE_NO_SHOCK") == ROUTINE
    assert classify_opportunity(g, near_miss_tier=None) == RESEARCH_ONLY


def test_exhaustive_no_gate_combo_with_a_failing_gate_yields_qualified():
    """Brute-force safety proof: across every boolean combination of gates
    where at least one hard gate fails, classify_opportunity must NEVER
    return STANDARD_QUALIFIED, for any tier / acceptance flag."""
    for combo in itertools.product([True, False], repeat=6):
        gates_pass = all(combo)
        if gates_pass:
            continue  # only care about combinations with a failing gate
        for accepted in (True, False):
            g = OpportunityGates(*combo, standard_pipeline_accepted=accepted)
            for tier in (None, "HOT_NEAR_MISS", "NEAR_MISS", "WATCHLIST", "ROUTINE_NO_SHOCK", "FIRED"):
                assert classify_opportunity(g, near_miss_tier=tier) != STANDARD_QUALIFIED
            assert qualified_is_impossible_without_gates(g) is True


def test_opportunity_engine_module_never_imports_execution():
    """It must be impossible for this module to place/cancel an order."""
    src = (PROJECT_ROOT / "strategy" / "opportunity_engine.py").read_text(encoding="utf-8")
    for forbidden in ("place_order", "cancel_order", "SellExecutor", "OrderManager",
                      "live_executor", "clob.polymarket.com", ".env", "LIVE_TRADING_ENABLED"):
        assert forbidden not in src, f"opportunity_engine references {forbidden!r}"
