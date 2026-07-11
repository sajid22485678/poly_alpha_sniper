"""Small, pure risk/accounting helpers for Poly Alpha Lite.

This module deliberately has no exchange client, authentication, balance API,
or order submission surface.  It models the constraints that a future
live-small adapter must satisfy while the runtime remains shadow-only.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
from math import isfinite
from typing import Optional

from .lite_config import FIXED_SHARES


CRYPTO_TAKER_FEE_RATE = 0.07
FEE_QUANTUM = Decimal("0.00001")


def taker_fee(shares: float, price: float,
              fee_rate: float = CRYPTO_TAKER_FEE_RATE) -> float:
    """Return the current Polymarket taker fee, rounded to five decimals.

    Official formula: ``shares * fee_rate * price * (1 - price)``.
    Lite crypto entries and executable book exits are modeled as taker fills.
    """
    size = Decimal(str(shares))
    p = Decimal(str(price))
    rate = Decimal(str(fee_rate))
    if (not size.is_finite() or not p.is_finite() or not rate.is_finite()
            or size < 0 or not Decimal("0") <= p <= Decimal("1") or rate < 0):
        raise ValueError("invalid taker fee inputs")
    value = size * rate * p * (Decimal("1") - p)
    return float(value.quantize(FEE_QUANTUM, rounding=ROUND_HALF_UP))


def fill_levels_taker_fee(levels, fee_rate: float = CRYPTO_TAKER_FEE_RATE) -> float:
    """Fee for a multi-price sweep, summed from its consumed fill levels."""
    rate = Decimal(str(fee_rate))
    if not rate.is_finite() or rate < 0:
        raise ValueError("invalid fill fee rate")
    total = Decimal("0")
    consumed = False
    for raw_price, raw_shares in levels:
        price = Decimal(str(raw_price))
        shares = Decimal(str(raw_shares))
        if (not price.is_finite() or not shares.is_finite()
                or not Decimal("0") <= price <= Decimal("1") or shares <= 0):
            raise ValueError("invalid fill level")
        total += shares * rate * price * (Decimal("1") - price)
        consumed = True
    if not consumed:
        raise ValueError("fill levels are required")
    return float(total.quantize(FEE_QUANTUM, rounding=ROUND_HALF_UP))


def sweep_taker_fee(sweep, fee_rate: float = CRYPTO_TAKER_FEE_RATE) -> float:
    if sweep is None:
        raise ValueError("sweep is required")
    return fill_levels_taker_fee(sweep.levels, fee_rate)


def entry_commitment(entry_price: float, *, shares: float = FIXED_SHARES,
                     fee_rate: float = CRYPTO_TAKER_FEE_RATE,
                     fee_buffer_usd: float = 0.0) -> float:
    if not isfinite(float(shares)) or float(shares) != FIXED_SHARES:
        raise ValueError("Lite commitment must use exactly five shares")
    if (not isfinite(float(entry_price)) or not 0 < float(entry_price) < 1
            or not isfinite(float(fee_rate)) or float(fee_rate) < 0
            or not isfinite(float(fee_buffer_usd)) or fee_buffer_usd < 0):
        raise ValueError("invalid commitment inputs")
    if fee_buffer_usd < 0:
        raise ValueError("fee buffer cannot be negative")
    return round(
        FIXED_SHARES * float(entry_price)
        + taker_fee(FIXED_SHARES, float(entry_price), fee_rate)
        + float(fee_buffer_usd),
        10,
    )


def idempotent_intent_key(*, asset: str, slug: str, market_id: str,
                          event_id: str, condition_id: str,
                          window_open_ts: int, window_close_ts: int,
                          side: str) -> str:
    identity = "|".join((
        str(asset).upper(), str(slug), str(market_id), str(event_id),
        str(condition_id), str(int(window_open_ts)),
        str(int(window_close_ts)), str(side), "5",
    ))
    return sha256(identity.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ExposureDecision:
    allowed: bool
    reason: str
    equity_usd: float
    exposure_cap_usd: float
    committed_exposure_usd: float
    proposed_commitment_usd: float
    projected_exposure_usd: float
    available_balance_usd: float


def assess_live_small_exposure(
        *, entry_price: float, committed_exposure_usd: float,
        equity_usd: float, available_balance_usd: Optional[float] = None,
        exposure_cap_pct: float = 0.75,
        fee_rate: float = CRYPTO_TAKER_FEE_RATE,
        fee_buffer_usd: float = 0.0,
        kill_switch: bool = False,
        today_realized_pnl: float = 0.0,
        max_daily_realized_loss_usd: Optional[float] = None,
        consecutive_losses: int = 0,
        max_consecutive_losses: Optional[int] = None) -> ExposureDecision:
    """Pure preview of the future live-small pre-entry capital guard."""
    try:
        equity = float(equity_usd)
        raw_committed = float(committed_exposure_usd)
        cap_pct = float(exposure_cap_pct)
        available = (equity - raw_committed if available_balance_usd is None
                     else float(available_balance_usd))
        risk_numbers = [float(today_realized_pnl)]
        if max_daily_realized_loss_usd is not None:
            risk_numbers.append(float(max_daily_realized_loss_usd))
        numeric_valid = all(isfinite(value) for value in (
            equity, raw_committed, cap_pct, available, float(entry_price),
            float(fee_rate), float(fee_buffer_usd), *risk_numbers))
        counts_valid = (not isinstance(consecutive_losses, bool)
                        and int(consecutive_losses) == consecutive_losses
                        and int(consecutive_losses) >= 0)
        if max_consecutive_losses is not None:
            counts_valid = (counts_valid
                            and not isinstance(max_consecutive_losses, bool)
                            and int(max_consecutive_losses) == max_consecutive_losses
                            and int(max_consecutive_losses) >= 1)
        numeric_valid = numeric_valid and counts_valid
    except (TypeError, ValueError, OverflowError):
        equity = raw_committed = cap_pct = available = 0.0
        numeric_valid = False
    committed = max(0.0, raw_committed) if numeric_valid else 0.0
    if (not numeric_valid or equity <= 0 or raw_committed < 0
            or available < 0 or not 0 < cap_pct <= 1):
        proposed = 0.0
        cap = max(0.0, equity * cap_pct) if numeric_valid else 0.0
        return ExposureDecision(False, "invalid_equity", equity, cap, committed,
                                proposed, committed, available)
    try:
        proposed = entry_commitment(
            entry_price, fee_rate=fee_rate, fee_buffer_usd=fee_buffer_usd)
    except ValueError:
        return ExposureDecision(False, "invalid_commitment", equity,
                                equity * cap_pct, committed, 0.0,
                                committed, available)
    cap = round(equity * cap_pct, 10)
    projected = round(committed + proposed, 10)
    reason = "allowed"
    if kill_switch:
        reason = "live_kill_switch"
    elif (max_daily_realized_loss_usd is not None
          and float(today_realized_pnl) <= -abs(float(max_daily_realized_loss_usd))):
        reason = "max_daily_realized_loss"
    elif (max_consecutive_losses is not None
          and int(consecutive_losses) >= int(max_consecutive_losses)):
        reason = "max_consecutive_losses"
    elif projected > cap + 1e-9:
        reason = "equity_exposure_cap"
    elif proposed > available + 1e-9:
        reason = "insufficient_available_balance"
    return ExposureDecision(
        allowed=reason == "allowed", reason=reason, equity_usd=equity,
        exposure_cap_usd=cap, committed_exposure_usd=committed,
        proposed_commitment_usd=proposed, projected_exposure_usd=projected,
        available_balance_usd=available,
    )
