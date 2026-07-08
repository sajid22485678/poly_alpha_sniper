import pytest

from poly_alpha_sniper.core.contracts import OrderRecord, OrderState
from poly_alpha_sniper.execution.order_lifecycle import (
    IllegalTransition, LEGAL_TRANSITIONS, OrderLifecycle)

S = OrderState


def _order():
    return OrderRecord(order_id="o1", state=S.CREATED)


def test_full_legal_chain():
    lc = OrderLifecycle()
    o = _order()
    for state in (S.SIGNED, S.SUBMITTED, S.OPEN, S.PARTIAL_FILL, S.MATCHED,
                  S.MINED, S.CONFIRMED, S.RECONCILED):
        lc.transition(o, state, 1000, "step")
    assert o.state == S.RECONCILED
    assert len(lc.history_for("o1")) == 8


def test_illegal_jump_raises():
    lc = OrderLifecycle()
    o = _order()
    with pytest.raises(IllegalTransition):
        lc.transition(o, S.CONFIRMED, 1000)


def test_terminal_states_frozen():
    lc = OrderLifecycle()
    o = _order()
    lc.transition(o, S.SIGNED, 1)
    lc.transition(o, S.SUBMITTED, 2)
    lc.transition(o, S.FAILED, 3)
    with pytest.raises(IllegalTransition):
        lc.transition(o, S.SUBMITTED, 4)


def test_cancel_path():
    lc = OrderLifecycle()
    o = _order()
    for state in (S.SIGNED, S.SUBMITTED, S.OPEN, S.CANCEL_REQUESTED, S.CANCELLED):
        lc.transition(o, state, 1000)
    assert o.state == S.CANCELLED


def test_cancel_race_fill_allowed():
    # cancel requested but order matched in flight
    lc = OrderLifecycle()
    o = _order()
    for state in (S.SIGNED, S.SUBMITTED, S.OPEN, S.CANCEL_REQUESTED, S.MATCHED):
        lc.transition(o, state, 1000)
    assert o.state == S.MATCHED


def test_retry_path():
    lc = OrderLifecycle()
    o = _order()
    for state in (S.SIGNED, S.SUBMITTED, S.RETRYING, S.SUBMITTED, S.OPEN):
        lc.transition(o, state, 1000)
    assert o.state == S.OPEN


def test_every_state_has_transition_entry():
    for state in OrderState:
        assert state in LEGAL_TRANSITIONS


def test_history_records_notes():
    lc = OrderLifecycle()
    o = _order()
    lc.transition(o, S.SIGNED, 123, "signing now")
    assert lc.history[0] == ("o1", "CREATED", "SIGNED", 123, "signing now")
