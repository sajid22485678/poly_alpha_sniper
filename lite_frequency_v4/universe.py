"""Canonical Dynamic Universe Eligibility Policy for Frequency V4.

One deterministic, explainable policy decides which discovered markets may
ever become execution-eligible.  Every relevant path consults this module:

- discovery persistence (``engine._persist_market``) marks non-eligible
  markets ``OBSERVED_ONLY`` and persists the exact rejection reasons;
- the evaluation loop refuses to create candidates for observed-only
  markets;
- the store's atomic entry boundary (``V4Store.create_entry``) re-derives
  the decision from previously committed rows, so even a directly injected
  or corrupted candidate fails closed.

Identity-level conditions live here.  Runtime conditions (evidence
freshness, depth, fee-net edge, persistence health, capital, concurrency)
remain enforced at evaluation/entry time by the engine and store; their
canonical names are declared in ``RUNTIME_CONDITIONS`` so reporting uses
one taxonomy.  This module is pure: no I/O, no engine or store imports.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping, Optional

from .contracts import AnchorStatus, FIVE_MINUTES_MS, MarketIdentity


UNIVERSE_POLICY_VERSION = "dynamic_universe_phase1_v1"

# The only contract family Frequency V4 understands: exact five-minute
# crypto Up/Down binaries settled by corroborated Gamma terminal prices.
SUPPORTED_CONTRACT_FAMILY = "crypto_updown_5m_binary"
EXACT_UPDOWN_5M_SLUG = re.compile(
    r"^(?P<asset>[a-z0-9]+)-updown-5m-(?P<start>[1-9][0-9]{8,})$",
    re.IGNORECASE,
)
# An asset symbol must map to a spot CEX instrument (``<ASSET>-USDT``) so a
# settlement-adjacent oracle/evidence source exists for the contract.
ASSET_SYMBOL = re.compile(r"^[A-Z0-9]{2,16}$")

# Canonical names for the runtime conditions enforced downstream of this
# policy.  They are declared here (not re-implemented) so the dashboard and
# reject taxonomy report one vocabulary for the full 15-condition policy.
RUNTIME_CONDITIONS = (
    "cex_evidence_fresh",
    "polymarket_book_evidence_fresh",
    "market_data_not_regressed_or_future_dated",
    "executable_five_share_depth",
    "positive_conservative_fee_net_edge",
    "persistence_and_reconciliation_healthy",
    "capital_ledger_affordable",
    "concurrency_and_exposure_within_limits",
)

IDENTITY_CONDITIONS = (
    "crypto_updown_contract_classified",
    "asset_symbol_supported",
    "duration_supported",
    "event_condition_market_associations_valid",
    "token_identity_unambiguous",
    "market_state_executable",
    "settlement_mechanism_understood",
    "anchor_evidence_coherent",
)


@dataclass(frozen=True, slots=True)
class EligibilityDecision:
    """Deterministic identity-level universe decision with explicit reasons."""

    eligible: bool
    reasons: tuple[str, ...]
    checked: tuple[str, ...] = IDENTITY_CONDITIONS
    policy_version: str = UNIVERSE_POLICY_VERSION

    @property
    def reject_reason(self) -> Optional[str]:
        return ";".join(self.reasons) if self.reasons else None


def evaluate_market_identity(identity: Any) -> EligibilityDecision:
    """Evaluate the identity-level dynamic-universe conditions.

    Accepts a ``MarketIdentity`` (or any object with the same attributes so
    the store boundary can pass a defensive reconstruction).  Any missing or
    malformed attribute is a rejection, never an exception: the policy fails
    closed on corrupt inputs.
    """
    reasons: list[str] = []

    def _text(name: str) -> str:
        try:
            return str(getattr(identity, name) or "")
        except Exception:
            return ""

    slug = _text("slug")
    asset = _text("asset").upper()
    match = EXACT_UPDOWN_5M_SLUG.fullmatch(slug)
    if match is None:
        reasons.append("contract_not_understood")
    elif match.group("asset").upper() != asset:
        reasons.append("asset_slug_mismatch")
    if not ASSET_SYMBOL.fullmatch(asset):
        reasons.append("asset_symbol_unsupported")

    try:
        open_ms = int(getattr(identity, "window_open_ms"))
        close_ms = int(getattr(identity, "window_close_ms"))
    except Exception:
        open_ms = close_ms = -1
    # Five-minute boundary alignment is enforced upstream at discovery parse
    # (parse_market_row); the policy re-checks contract family and duration,
    # which fully determine the settlement semantics.
    if close_ms - open_ms != FIVE_MINUTES_MS or open_ms <= 0:
        reasons.append("duration_unsupported")

    if not all(_text(name) for name in ("market_id", "event_id", "condition_id")):
        reasons.append("association_invalid")

    yes_token = _text("yes_token_id")
    no_token = _text("no_token_id")
    if not yes_token or not no_token or yes_token == no_token:
        reasons.append("token_identity_ambiguous")

    try:
        executable = (
            getattr(identity, "active") is True
            and getattr(identity, "accepting_orders") is True
            and getattr(identity, "closed") is False
            and getattr(identity, "archived") is False
        )
    except Exception:
        executable = False
    if not executable:
        reasons.append("market_not_executable")

    # Settlement for this family is a corroborated Gamma terminal-price
    # resolution; any other family already failed contract classification.
    # Anchor/price-to-beat is NOT required for this contract (directional
    # models use CEX window-open evidence; settlement uses terminal prices),
    # but corrupt anchor metadata is a data-integrity rejection.
    try:
        raw_status = getattr(identity, "anchor_status", AnchorStatus.FIELD_MISSING)
        status = (raw_status if isinstance(raw_status, AnchorStatus)
                  else AnchorStatus(str(raw_status)))
    except Exception:
        status = None
    if status is None or status is AnchorStatus.PARSE_FAILED:
        reasons.append("anchor_evidence_corrupt")
    elif status is AnchorStatus.ANCHORED:
        try:
            anchor_price = float(getattr(identity, "price_to_beat"))
        except Exception:
            anchor_price = float("nan")
        if not anchor_price > 0.0:
            reasons.append("anchor_price_invalid")

    deduped = tuple(dict.fromkeys(reasons))
    return EligibilityDecision(eligible=not deduped, reasons=deduped)


_DB_ANCHOR_STATUS = {
    "ANCHORED": AnchorStatus.ANCHORED,
    "UNANCHORED": AnchorStatus.UNANCHORED,
    "ANCHOR_FIELD_MISSING": AnchorStatus.FIELD_MISSING,
    "ANCHOR_PARSE_FAILED": AnchorStatus.PARSE_FAILED,
    "ANCHOR_NOT_YET_PUBLISHED": AnchorStatus.NOT_YET_PUBLISHED,
}


def reconstruct_market_identity(
    market_row: Mapping[str, Any],
    identity_row: Mapping[str, Any],
    anchor_row: Optional[Mapping[str, Any]] = None,
) -> Optional[MarketIdentity]:
    """Rebuild a ``MarketIdentity`` from committed DB rows for the boundary check.

    Returns ``None`` when the rows cannot form a coherent identity — the
    caller must treat that as not eligible (fail closed).
    """
    try:
        status = AnchorStatus.FIELD_MISSING
        price_to_beat = None
        if anchor_row is not None:
            status = _DB_ANCHOR_STATUS.get(
                str(anchor_row.get("status") or ""), AnchorStatus.PARSE_FAILED)
            if status is AnchorStatus.ANCHORED:
                price_to_beat = float(anchor_row["price_to_beat"])
        return MarketIdentity(
            asset=str(market_row["asset"]),
            slug=str(market_row["slug"]),
            market_id=str(market_row["polymarket_market_id"]),
            event_id=str(identity_row["event_id"]),
            condition_id=str(identity_row["condition_id"]),
            yes_token_id=str(identity_row["yes_token_id"]),
            no_token_id=str(identity_row["no_token_id"]),
            window_open_ms=int(market_row["open_ts_ms"]),
            window_close_ms=int(market_row["close_ts_ms"]),
            anchor_status=status,
            price_to_beat=price_to_beat,
            active=str(market_row.get("status") or "") == "ACTIVE",
            accepting_orders=bool(market_row.get("accepting_orders")),
            closed=False,
            archived=False,
        )
    except Exception:
        return None


def evaluate_persisted_market(
    market_row: Optional[Mapping[str, Any]],
    identity_row: Optional[Mapping[str, Any]],
    anchor_row: Optional[Mapping[str, Any]] = None,
) -> EligibilityDecision:
    """Defensive execution-boundary form of the canonical policy.

    Used by the store inside the entry transaction so a candidate whose
    market rows are missing, corrupted, or policy-rejected can never open a
    position, regardless of what the submitting caller claims.
    """
    if market_row is None or identity_row is None:
        return EligibilityDecision(False, ("market_identity_rows_missing",))
    if not bool(identity_row.get("association_valid")):
        return EligibilityDecision(False, ("association_invalid",))
    if not bool(identity_row.get("token_pair_valid")):
        return EligibilityDecision(False, ("token_identity_ambiguous",))
    if bool(identity_row.get("ambiguous")):
        return EligibilityDecision(False, ("token_identity_ambiguous",))
    identity = reconstruct_market_identity(market_row, identity_row, anchor_row)
    if identity is None:
        return EligibilityDecision(False, ("identity_reconstruction_failed",))
    return evaluate_market_identity(identity)
