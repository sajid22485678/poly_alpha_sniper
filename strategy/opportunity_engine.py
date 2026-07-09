"""Shadow-only aggressive opportunity DISCOVERY engine.

This module amplifies opportunity *visibility* -- near-miss surfacing, tier
classification, and candidate coverage diagnostics -- WITHOUT ever touching
the accept/reject decision path, the execution path, or live behavior.

Safety architecture (why this is provably harmless):
  1. It never imports or calls anything that places/cancels an order.
  2. It never returns STANDARD_QUALIFIED unless EVERY hard gate already holds
     (oracle anchor + executable book + spread + depth + risk + positive EV).
     So it can never label a missing-oracle / missing-book / bad-spread /
     negative-EV candidate as a would-be trade.
  3. is_active_for_mode() returns False for live_micro/live_full -- the mode
     is inert in live, and apply_to_live is a hard-pinned invariant.
  4. Anything short of STANDARD_QUALIFIED is WATCHLIST_ONLY / RESEARCH_ONLY /
     ROUTINE -- diagnostic labels, never a trade.

The bot's REAL accept/reject logic is unchanged and lives in
BalancedAlphaGate / RiskManager / order_validator / oracle_ev. This module is
a read-only lens over that pipeline's outcomes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from poly_alpha_sniper.core.contracts import TradingMode

# Opportunity classifications (ordered strongest -> weakest).
STANDARD_QUALIFIED = "STANDARD_QUALIFIED"   # all hard gates pass; the standard pipeline accepts it
WATCHLIST_ONLY = "WATCHLIST_ONLY"           # near-miss / close to firing; diagnostics only, never a trade
RESEARCH_ONLY = "RESEARCH_ONLY"             # surfaced by a relaxed research path; never a trade
ROUTINE = "ROUTINE"                         # ordinary reject, far from qualifying

# The config flags that are safety invariants -- must hold these exact values
# or the mode is considered mis-configured and treated as disabled.
_REQUIRED_TRUE = ("require_positive_ev", "require_oracle_anchor",
                  "require_executable_book", "require_spread_ok",
                  "require_depth_ok", "require_risk_ok")
_REQUIRED_FALSE = ("force_trade_count", "apply_to_live")


@dataclass(frozen=True)
class OpportunityGates:
    """The hard-gate outcomes for one candidate, as already decided by the
    standard pipeline. All must be True for STANDARD_QUALIFIED."""
    has_oracle_anchor: bool
    has_executable_book: bool
    spread_ok: bool
    depth_ok: bool
    risk_ok: bool
    positive_ev: bool
    standard_pipeline_accepted: bool  # gate.decision in (APPROVE, SHADOW_ONLY)

    @property
    def all_hard_gates_pass(self) -> bool:
        return (self.has_oracle_anchor and self.has_executable_book
                and self.spread_ok and self.depth_ok and self.risk_ok
                and self.positive_ev)


def config_is_safe(cfg) -> bool:
    """True only if the aggressive-mode config still holds every safety
    invariant. If anyone edits it to force trades or apply to live, this
    returns False and the engine treats the mode as disabled."""
    m = getattr(cfg, "shadow_aggressive_opportunity_mode", None)
    if m is None:
        return False
    if not all(getattr(m, name, False) is True for name in _REQUIRED_TRUE):
        return False
    if any(getattr(m, name, True) is not False for name in _REQUIRED_FALSE):
        return False
    return True


def is_active_for_mode(cfg, mode) -> bool:
    """Active only in shadow_live / simulation, and only when enabled AND the
    safety invariants hold. NEVER active in live_micro / live_full -- this is
    the structural guarantee that the mode can't change live behavior."""
    try:
        tm = mode if isinstance(mode, TradingMode) else TradingMode(mode)
    except (ValueError, TypeError):
        return False
    if tm.is_live:
        return False
    m = getattr(cfg, "shadow_aggressive_opportunity_mode", None)
    if m is None or not getattr(m, "enabled", False):
        return False
    return config_is_safe(cfg)


def classify_opportunity(gates: OpportunityGates, *, near_miss_tier: Optional[str] = None) -> str:
    """Classify a candidate. STANDARD_QUALIFIED requires the standard pipeline
    to have accepted it AND every hard gate to pass -- never otherwise. A
    near-miss (by shock score) that did NOT pass the standard gates is at most
    WATCHLIST_ONLY. Everything else is RESEARCH_ONLY / ROUTINE. This function
    NEVER promotes a candidate past what the standard pipeline decided."""
    if gates.standard_pipeline_accepted and gates.all_hard_gates_pass:
        return STANDARD_QUALIFIED
    # Not accepted by the standard pipeline -> it is never a trade here.
    if near_miss_tier in ("HOT_NEAR_MISS", "NEAR_MISS", "WATCHLIST"):
        return WATCHLIST_ONLY
    if near_miss_tier == "ROUTINE_NO_SHOCK":
        return ROUTINE
    return RESEARCH_ONLY


def qualified_is_impossible_without_gates(gates: OpportunityGates) -> bool:
    """Explicit, test-facing statement of the core safety property: if any
    hard gate fails, classify_opportunity can never return STANDARD_QUALIFIED,
    regardless of near_miss_tier or standard_pipeline_accepted."""
    if gates.all_hard_gates_pass:
        return False  # gates all pass; qualified is legitimately possible
    for accepted in (True, False):
        for tier in (None, "HOT_NEAR_MISS", "NEAR_MISS", "WATCHLIST", "ROUTINE_NO_SHOCK"):
            g = OpportunityGates(
                gates.has_oracle_anchor, gates.has_executable_book, gates.spread_ok,
                gates.depth_ok, gates.risk_ok, gates.positive_ev, accepted)
            if classify_opportunity(g, near_miss_tier=tier) == STANDARD_QUALIFIED:
                return False  # found a way to be qualified with a failing gate -> unsafe
    return True
