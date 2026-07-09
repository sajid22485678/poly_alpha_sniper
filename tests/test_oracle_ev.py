"""WS3: oracle-aware EV formula."""
from __future__ import annotations

import pytest

from poly_alpha_sniper.strategy.oracle_ev import compute_oracle_ev


def test_ev_formula_matches_spec():
    # EV = probability_of_payout - executable_price - fees - slippage - adverse_selection_buffer
    result = compute_oracle_ev(probability_of_payout=0.70, executable_price=0.60,
                               fee_rate=0.0, slippage_buffer=0.01, adverse_selection_buffer=0.01)
    assert result.ev == pytest.approx(0.70 - 0.60 - 0.0 - 0.01 - 0.01)


def test_ev_positive_when_probability_comfortably_above_price():
    result = compute_oracle_ev(0.80, 0.50, fee_rate=0.0, slippage_buffer=0.01, adverse_selection_buffer=0.01)
    assert result.ev > 0


def test_ev_negative_when_price_exceeds_probability():
    result = compute_oracle_ev(0.50, 0.80, fee_rate=0.0, slippage_buffer=0.01, adverse_selection_buffer=0.01)
    assert result.ev < 0


def test_ev_costs_reduce_even_a_positive_raw_edge():
    cheap = compute_oracle_ev(0.70, 0.60, fee_rate=0.0, slippage_buffer=0.0, adverse_selection_buffer=0.0)
    costly = compute_oracle_ev(0.70, 0.60, fee_rate=0.02, slippage_buffer=0.02, adverse_selection_buffer=0.02)
    assert costly.ev < cheap.ev
