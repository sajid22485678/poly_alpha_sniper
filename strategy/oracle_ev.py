"""Oracle-aware expected-value formula.

    EV = probability_of_payout - executable_price - fees - slippage - adverse_selection_buffer

This is deliberately a thin, explicit function layered on top of the
existing fair-probability estimate (strategy/probability_model.py) rather
than a replacement for it: rewriting the probability model's internal
weights would need backtested calibration data this bot doesn't have.
Oracle-awareness here is expressed as (a) the fail-closed anchor gate in
oracle_anchor.py, which this module assumes has already passed, and (b) this
explicit EV computation with configurable cost/buffer terms, so an entry
must clear a real, inspectable bar instead of "any positive edge."
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class OracleEvResult:
    ev: float
    probability_of_payout: float
    executable_price: float
    fee_rate: float
    slippage_buffer: float
    adverse_selection_buffer: float


def compute_oracle_ev(probability_of_payout: float, executable_price: float,
                      fee_rate: float, slippage_buffer: float,
                      adverse_selection_buffer: float) -> OracleEvResult:
    """Pure. probability_of_payout is P(this side wins) -- i.e. p_up for a
    BUY_YES/UP side, or p_down (=1-p_up) for a BUY_NO/DOWN side; callers
    orient this the same way strategy/edge_engine.py orients its fair
    probability against side."""
    ev = (probability_of_payout - executable_price - fee_rate
         - slippage_buffer - adverse_selection_buffer)
    return OracleEvResult(
        ev=ev, probability_of_payout=probability_of_payout,
        executable_price=executable_price, fee_rate=fee_rate,
        slippage_buffer=slippage_buffer, adverse_selection_buffer=adverse_selection_buffer)
