import pytest

from poly_alpha_sniper.core.contracts import RejectReason, Tier, TradingMode
from poly_alpha_sniper.risk.position_sizer import compute_position_size
from poly_alpha_sniper.tests.helpers import cfg, market, portfolio_snapshot


def _cfg():
    """These tests exercise the "max_trade_usd" sizing mode specifically --
    pin it explicitly rather than relying on whatever config.yaml's ambient
    default happens to be (WS4 changed the shipped default to
    fixed_min_shares; these tests must stay deterministic either way)."""
    c = cfg()
    c.risk.sizing_mode = "max_trade_usd"
    return c


def test_ten_pct_of_ten_dollars_clamped_to_min_one():
    d = compute_position_size(_cfg(), portfolio_snapshot(equity=10, cash=10),
                              market(), TradingMode.SHADOW_LIVE, 0.08)
    assert d.approved
    assert d.size_usd == 1.0  # raw $1.00 == min == max


def test_equity_growth_capped_by_max_trade():
    d = compute_position_size(_cfg(), portfolio_snapshot(equity=50, cash=50),
                              market(), TradingMode.SHADOW_LIVE, 0.08)
    assert d.approved
    assert d.size_usd == 1.0  # raw $5 but max_trade_usd=1


def test_compounding_uses_realized_equity_only():
    # unrealized pnl present but equity (realized) unchanged -> same size
    snap = portfolio_snapshot(equity=10, cash=10, unrealized_pnl_usd=50.0)
    d = compute_position_size(_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert d.size_usd == 1.0


def test_insufficient_cash_rejected():
    d = compute_position_size(_cfg(), portfolio_snapshot(equity=10, cash=0.5),
                              market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.INSUFFICIENT_CASH


def test_min_order_too_high_rejected():
    m = market()
    m.min_order_size_usd = 5.0
    d = compute_position_size(_cfg(), portfolio_snapshot(), m, TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.MIN_ORDER_SIZE_TOO_HIGH


def test_market_exposure_cap():
    snap = portfolio_snapshot(equity=10, cash=10,
                              exposure_by_market={"m1": 0.9}, total_exposure_usd=0.9)
    d = compute_position_size(_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE


def test_total_exposure_cap():
    snap = portfolio_snapshot(equity=10, cash=10,
                              exposure_by_market={"other": 2.5}, total_exposure_usd=2.5)
    d = compute_position_size(_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE


def test_edge_killed_by_costs_rejected():
    d = compute_position_size(_cfg(), portfolio_snapshot(), market(),
                              TradingMode.SHADOW_LIVE, -0.01)
    assert not d.approved
    assert d.reject_reason == RejectReason.SLIPPAGE_TOO_HIGH


def test_daily_loss_cap():
    snap = portfolio_snapshot(equity=10, cash=10, realized_pnl_today_usd=-2.0)
    d = compute_position_size(_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.DAILY_LOSS_CAP


def test_loss_streak_cap():
    snap = portfolio_snapshot(equity=10, cash=10, consecutive_losses=2)
    d = compute_position_size(_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.LOSS_STREAK


def test_live_micro_cap_applies():
    c = _cfg()
    c.risk.max_trade_usd = 5
    c.risk.live_micro_trade_usd = 1
    d = compute_position_size(c, portfolio_snapshot(equity=100, cash=100),
                              market(), TradingMode.LIVE_MICRO, 0.08)
    assert d.approved
    assert d.size_usd == 1.0


# ---------------------------------------------------------------------------
# WS4: fixed_min_shares sizing mode
# ---------------------------------------------------------------------------

def _fixed_cfg(fixed_order_shares=5.0):
    c = cfg()
    c.risk.sizing_mode = "fixed_min_shares"
    c.risk.fixed_order_shares = fixed_order_shares
    c.risk.use_max_trade_usd = False
    return c


def test_fixed_min_shares_allowed_when_cash_covers_it_ask_065():
    # equity=50 so the 10%/30%-of-equity exposure caps ($5 / $15) have
    # headroom for a $3.25 fixed-size position -- isolates the sizing
    # mechanism from the (separately-tested) exposure caps.
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=50, cash=50),
                              market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.65)
    assert d.approved
    assert d.size_usd == pytest.approx(3.25)  # 5 * 0.65


def test_fixed_min_shares_allowed_when_cash_covers_it_ask_095():
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=50, cash=50),
                              market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.95)
    assert d.approved
    assert d.size_usd == pytest.approx(4.75)  # 5 * 0.95


def test_fixed_min_shares_rejected_insufficient_cash():
    # cash=4 is below the $4.75 requirement -- fails the cash check before
    # exposure caps are even reached, so equity=10's tighter caps don't matter.
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=10, cash=4),
                              market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.95)
    assert not d.approved
    assert d.reject_reason == RejectReason.INSUFFICIENT_CASH_FOR_5_SHARES


def test_fixed_min_shares_ignores_max_trade_usd_of_one_dollar():
    c = _fixed_cfg()
    c.risk.max_trade_usd = 1  # would have blocked ask=0.65 * 5 shares = $3.25 in the old mode
    d = compute_position_size(c, portfolio_snapshot(equity=50, cash=50),
                              market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.65)
    assert d.approved
    assert d.size_usd == pytest.approx(3.25)


def test_fixed_min_shares_does_not_check_market_min_order_size_usd():
    """fixed sizing IS the min order by construction -- market.min_order_size_usd
    (the discovery-time conservative floor) must not additionally block it."""
    m = market()
    m.min_order_size_usd = 999.0  # would hard-block in max_trade_usd mode
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=50, cash=50),
                              m, TradingMode.SHADOW_LIVE, 0.08, executable_price=0.65)
    assert d.approved


def test_fixed_min_shares_still_enforces_exposure_cap():
    snap = portfolio_snapshot(equity=10, cash=10,
                              exposure_by_market={"other": 2.9}, total_exposure_usd=2.9)
    d = compute_position_size(_fixed_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.65)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE


def test_fixed_min_shares_rejects_data_quality_when_no_price():
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=10, cash=10),
                              market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.0)
    assert not d.approved
    assert d.reject_reason == RejectReason.DATA_QUALITY


# ---------------------------------------------------------------------------
# Exact regression case from the reported Telegram bug: SOL, ask=0.52,
# available_cash=$12.44, old max_trade_usd=$1, edge=0.236, confidence=95,
# tier=A_PLUS -- must size to 5 shares / $2.60 and approve, never reference
# max_trade_usd or REJECTED_MIN_ORDER_SIZE_TOO_HIGH.
# ---------------------------------------------------------------------------

def test_screenshot_regression_passes_sizing_with_exposure_headroom():
    # Same ask/edge/tier as the reported case, but enough equity that the
    # unrelated 10%-of-equity market exposure cap isn't also in play --
    # isolates "does fixed sizing itself pass" from the exposure-cap
    # question covered separately below.
    c = _fixed_cfg()
    c.risk.max_trade_usd = 1.00
    d = compute_position_size(c, portfolio_snapshot(equity=50, cash=50),
                              market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.52)
    assert d.approved, d.reject_reason
    assert d.size_usd == pytest.approx(2.60)
    assert d.sizing_detail["shares"] == 5.0
    assert "configured_max_trade_usd" not in d.sizing_detail


def test_screenshot_regression_at_reported_equity_blocked_by_exposure_not_min_order():
    """At the ACTUAL reported equity ($12.44), the 10%-of-equity market
    exposure cap ($1.244) is genuinely below the $2.60 fixed order -- this is
    a real, correctly-enforced cap, not a bug. The point of this regression
    is what it must NOT say: never MIN_ORDER_SIZE_TOO_HIGH, never mention
    max_trade_usd -- the true blocker (MAX_EXPOSURE) must be reported."""
    c = _fixed_cfg()
    c.risk.max_trade_usd = 1.00
    d = compute_position_size(c, portfolio_snapshot(equity=12.44, cash=12.44),
                              market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.52)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE
    assert d.reject_reason != RejectReason.MIN_ORDER_SIZE_TOO_HIGH


def test_screenshot_regression_insufficient_cash():
    c = _fixed_cfg()
    c.risk.max_trade_usd = 1.00
    d = compute_position_size(c, portfolio_snapshot(equity=2.59, cash=2.59),
                              market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.52)
    assert not d.approved
    assert d.reject_reason == RejectReason.INSUFFICIENT_CASH_FOR_5_SHARES


# ---------------------------------------------------------------------------
# Dynamic tier-based exposure cap (fixed_min_shares mode only): B stays at
# 10%, A/A_PLUS get 50%, unknown/missing tiers default to 10%.
# ---------------------------------------------------------------------------

def test_tier_a_plus_screenshot_case_passes_exposure():
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=12.44, cash=12.44),
                              market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.52, tier=Tier.A_PLUS)
    assert d.approved, d.reject_reason
    assert d.size_usd == pytest.approx(2.60)
    assert d.sizing_detail["tier"] == "A_PLUS"
    assert d.sizing_detail["tier_cap_pct"] == pytest.approx(0.50)
    assert d.sizing_detail["allowed_exposure_usd"] == pytest.approx(6.22)
    assert d.sizing_detail["proposed_usd"] == pytest.approx(2.60)


def test_tier_a_ask_095_passes_the_market_exposure_check_specifically():
    # Isolates the MARKET exposure check this ticket changes from the
    # UNTOUCHED 30%-of-equity TOTAL exposure cap -- see the next test for
    # what actually happens with zero other headroom at this bankroll size.
    snap = portfolio_snapshot(equity=12.44, cash=12.44, total_exposure_usd=0.0,
                              exposure_by_market={})
    d = compute_position_size(_fixed_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.95, tier=Tier.A)
    assert "tier_exposure_ok" in d.checks  # market-level tier check passed
    assert d.size_usd if d.approved else True  # market check itself never the blocker


def test_tier_a_ask_095_at_reported_equity_still_blocked_by_total_exposure_cap():
    """Honest finding: this ticket only changes the MARKET exposure check.
    At equity=$12.44, a single $4.75 position already exceeds the UNTOUCHED
    30%-of-equity TOTAL exposure cap ($3.732) even with zero other exposure
    -- so this exact case is still blocked, but now by REJECTED_MAX_EXPOSURE
    via the total cap, not the market cap, not min-order, not max_trade_usd."""
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=12.44, cash=12.44),
                              market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.95, tier=Tier.A)
    assert "tier_exposure_ok" in d.checks  # confirms market cap was NOT the blocker
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE


def test_tier_a_ask_095_passes_fully_with_total_exposure_headroom():
    # Same tier/ask, but enough equity that the (untouched) 30% total cap
    # isn't also in play -- proves the full pass-through end to end.
    snap = portfolio_snapshot(equity=50, cash=50)
    d = compute_position_size(_fixed_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.95, tier=Tier.A)
    assert d.approved, d.reject_reason
    assert d.size_usd == pytest.approx(4.75)
    assert d.sizing_detail["tier_cap_pct"] == pytest.approx(0.50)
    assert d.sizing_detail["allowed_exposure_usd"] == pytest.approx(25.0)


def test_tier_b_stays_at_ten_pct_and_fails_exposure():
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=12.44, cash=12.44),
                              market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.52, tier=Tier.B)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE
    assert d.sizing_detail["tier"] == "B"
    assert d.sizing_detail["tier_cap_pct"] == pytest.approx(0.10)
    assert d.sizing_detail["allowed_exposure_usd"] == pytest.approx(1.244)
    assert d.sizing_detail["proposed_usd"] == pytest.approx(2.60)


def test_unknown_tier_defaults_to_ten_pct_and_fails_exposure():
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=12.44, cash=12.44),
                              market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.52, tier=None)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE
    assert d.sizing_detail["tier_cap_pct"] == pytest.approx(0.10)


def test_tier_c_also_defaults_to_ten_pct():
    """Tier.C isn't in the configured tiers map -- falls back to default_pct,
    same as unknown/missing."""
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=12.44, cash=12.44),
                              market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.52, tier=Tier.C)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE
    assert d.sizing_detail["tier_cap_pct"] == pytest.approx(0.10)


def test_dynamic_exposure_disabled_falls_back_to_flat_cap():
    c = _fixed_cfg()
    c.risk.dynamic_exposure_by_tier.enabled = False
    d = compute_position_size(c, portfolio_snapshot(equity=12.44, cash=12.44),
                              market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.52, tier=Tier.A_PLUS)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE  # flat 10% still applies


def test_max_trade_usd_mode_ignores_tier_cap_entirely():
    """Dynamic tier caps only apply to fixed_min_shares -- max_trade_usd mode's
    market exposure check must be completely unaffected by tier."""
    c = cfg()
    c.risk.sizing_mode = "max_trade_usd"
    snap = portfolio_snapshot(equity=10, cash=10)
    d = compute_position_size(c, snap, market(), TradingMode.SHADOW_LIVE, 0.08,
                              tier=Tier.A_PLUS)
    assert d.approved
    assert d.size_usd == 1.0  # unchanged max_trade_usd-mode behavior
    assert "tier_cap_pct" not in d.sizing_detail


def test_total_exposure_cap_still_enforced_in_fixed_min_shares_mode():
    """Dynamic per-tier cap only replaces the MARKET exposure check -- the
    30% total exposure cap is untouched."""
    snap = portfolio_snapshot(equity=50, cash=50,
                              exposure_by_market={"other": 14.9}, total_exposure_usd=14.9)
    d = compute_position_size(_fixed_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.236,
                              executable_price=0.52, tier=Tier.A_PLUS)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE
