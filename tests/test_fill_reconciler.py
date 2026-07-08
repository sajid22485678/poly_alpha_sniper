from poly_alpha_sniper.core.contracts import OrderRecord, OrderState, Outcome, Position
from poly_alpha_sniper.execution.fill_reconciler import FillReconciler
from poly_alpha_sniper.execution.settlement_safety import SettlementSafety
from poly_alpha_sniper.risk.kill_switch import KillSwitch
from poly_alpha_sniper.risk.panic_mode import PanicMode


def _pos(token="tok_yes", shares=2.0):
    return Position(token_id=token, market_id="m1", outcome=Outcome.YES,
                    shares=shares, avg_entry_price=0.6)


def _order(oid="o1", state=OrderState.OPEN):
    return OrderRecord(order_id=oid, exchange_order_id=f"x-{oid}", state=state)


def test_clean_match_ok():
    r = FillReconciler().reconcile(
        local_orders=[_order()], exchange_orders=[_order()],
        local_positions=[_pos()], exchange_positions=[_pos()],
        balance_local=10.0, balance_exchange=10.0)
    assert r.ok
    assert r.mismatches == []


def test_unknown_exchange_order_flags():
    r = FillReconciler().reconcile(
        local_orders=[], exchange_orders=[_order("ghost")],
        local_positions=[], exchange_positions=[],
        balance_local=10.0, balance_exchange=10.0)
    assert not r.ok
    assert any("unknown exchange order" in m for m in r.mismatches)


def test_missing_local_open_order_flags():
    r = FillReconciler().reconcile(
        local_orders=[_order("lonely")], exchange_orders=[],
        local_positions=[], exchange_positions=[],
        balance_local=10.0, balance_exchange=10.0)
    assert not r.ok
    assert any("missing on exchange" in m for m in r.mismatches)


def test_share_mismatch_flags():
    r = FillReconciler().reconcile(
        local_orders=[], exchange_orders=[],
        local_positions=[_pos(shares=2.0)], exchange_positions=[_pos(shares=1.0)],
        balance_local=10.0, balance_exchange=10.0)
    assert not r.ok
    assert any("position mismatch" in m for m in r.mismatches)


def test_balance_mismatch_flags():
    r = FillReconciler().reconcile(
        local_orders=[], exchange_orders=[],
        local_positions=[], exchange_positions=[],
        balance_local=10.0, balance_exchange=9.0)
    assert not r.ok
    assert any("balance mismatch" in m for m in r.mismatches)


def test_settlement_mismatch_freezes_live():
    panic, kill = PanicMode(), KillSwitch()
    safety = SettlementSafety(panic, kill)
    r = FillReconciler().reconcile(
        local_orders=[], exchange_orders=[],
        local_positions=[_pos()], exchange_positions=[],
        balance_local=10.0, balance_exchange=10.0)
    incident = safety.check(r)
    assert incident is not None
    assert panic.is_active
    assert kill.is_active
    assert incident["severity"] == "CRITICAL"


def test_settlement_ok_no_freeze():
    panic, kill = PanicMode(), KillSwitch()
    safety = SettlementSafety(panic, kill)
    r = FillReconciler().reconcile([], [], [], [], 10.0, 10.0)
    assert safety.check(r) is None
    assert not panic.is_active
