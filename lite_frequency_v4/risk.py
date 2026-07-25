"""Pure five-share fee, idempotency, and shadow-exposure controls."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
import math
from typing import Any, Iterable, Optional

from .config import FIXED_SHARES, STRATEGY_ID
from .contracts import BookLevel, EntrySide, MarketIdentity, Sweep


CRYPTO_TAKER_FEE_RATE = 0.07
FEE_QUANTUM = Decimal("0.00001")


def _decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"invalid {name}")
    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"invalid {name}") from exc
    if not parsed.is_finite():
        raise ValueError(f"invalid {name}")
    return parsed


def taker_fee(shares: float, price: float,
              fee_rate: float = CRYPTO_TAKER_FEE_RATE) -> float:
    """Official crypto taker curve, rounded to the five-decimal fee quantum."""
    size = _decimal(shares, "shares")
    probability = _decimal(price, "price")
    rate = _decimal(fee_rate, "fee_rate")
    if size < 0 or not Decimal("0") <= probability <= Decimal("1") or rate < 0:
        raise ValueError("invalid taker fee inputs")
    fee = size * rate * probability * (Decimal("1") - probability)
    return float(fee.quantize(FEE_QUANTUM, rounding=ROUND_HALF_UP))


polymarket_taker_fee = taker_fee


def _level_values(level: BookLevel | tuple[float, float] | list[float]) -> tuple[float, float]:
    if isinstance(level, BookLevel):
        return level.price, level.shares
    if not isinstance(level, (tuple, list)) or len(level) != 2:
        raise ValueError("invalid fill level")
    return float(level[0]), float(level[1])


def fill_levels_taker_fee(
        levels: Iterable[BookLevel | tuple[float, float] | list[float]],
        fee_rate: float = CRYPTO_TAKER_FEE_RATE) -> float:
    rate = _decimal(fee_rate, "fee_rate")
    if rate < 0:
        raise ValueError("invalid fee rate")
    total = Decimal("0")
    consumed = False
    for level in levels:
        raw_price, raw_shares = _level_values(level)
        price = _decimal(raw_price, "fill price")
        shares = _decimal(raw_shares, "fill shares")
        if not Decimal("0") <= price <= Decimal("1") or shares <= 0:
            raise ValueError("invalid fill level")
        total += shares * rate * price * (Decimal("1") - price)
        consumed = True
    if not consumed:
        raise ValueError("fill levels are required")
    return float(total.quantize(FEE_QUANTUM, rounding=ROUND_HALF_UP))


def sweep_taker_fee(
        sweep: Sweep, fee_rate: float = CRYPTO_TAKER_FEE_RATE) -> float:
    if not isinstance(sweep, Sweep):
        raise ValueError("sweep evidence is required")
    if abs(float(sweep.shares) - FIXED_SHARES) > 1e-9:
        raise ValueError("Frequency V4 entries use exactly five shares")
    return fill_levels_taker_fee(sweep.levels, fee_rate)


def fee_per_share(sweep: Sweep,
                  fee_rate: float = CRYPTO_TAKER_FEE_RATE) -> float:
    """Probability-edge calculations subtract fee per share, not total fee."""
    return sweep_taker_fee(sweep, fee_rate) / FIXED_SHARES


def conservative_exit_fee_buffer(
        fee_rate: float = CRYPTO_TAKER_FEE_RATE,
        fee_buffer_usd: float = 0.0) -> float:
    """Worst-case exit-fee reservation per open five-share position.

    The taker fee curve peaks at probability 0.5, so committing
    ``taker_fee(5, 0.5)`` plus the configured flat buffer guarantees the
    ledger always holds enough cash to pay any possible exit fee.
    """
    buffer = float(fee_buffer_usd)
    if not math.isfinite(buffer) or buffer < 0.0:
        raise ValueError("invalid exit fee buffer")
    return round(taker_fee(FIXED_SHARES, 0.5, fee_rate) + buffer, 10)


def entry_commitment(
        entry_price: Optional[float] = None, *, sweep: Optional[Sweep] = None,
        shares: float = FIXED_SHARES,
        fee_rate: float = CRYPTO_TAKER_FEE_RATE,
        fee_buffer_usd: float = 0.0) -> float:
    try:
        parsed_shares = float(shares)
        buffer = float(fee_buffer_usd)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("invalid entry commitment") from exc
    if (not math.isfinite(parsed_shares) or parsed_shares != FIXED_SHARES
            or not math.isfinite(buffer) or buffer < 0.0):
        raise ValueError("Frequency V4 commitment must use exactly five shares")
    if sweep is not None:
        if abs(sweep.shares - FIXED_SHARES) > 1e-9:
            raise ValueError("Frequency V4 sweep must use exactly five shares")
        notional = float(sweep.notional)
        fee = sweep_taker_fee(sweep, fee_rate)
    else:
        try:
            price = float(entry_price)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("entry price is required") from exc
        if not math.isfinite(price) or not 0.0 < price < 1.0:
            raise ValueError("entry price must be in (0,1)")
        notional = FIXED_SHARES * price
        fee = taker_fee(FIXED_SHARES, price, fee_rate)
    return round(notional + fee + buffer, 10)


def idempotent_intent_key(
        *, asset: str, slug: str, market_id: str, event_id: str,
        condition_id: str, window_open_ms: int, window_close_ms: int,
        side: EntrySide | str, shares: float = FIXED_SHARES,
        strategy_id: str = STRATEGY_ID) -> str:
    parsed_side = side if isinstance(side, EntrySide) else EntrySide(str(side))
    if float(shares) != FIXED_SHARES:
        raise ValueError("Frequency V4 idempotency requires exactly five shares")
    if int(window_close_ms) - int(window_open_ms) != 300_000:
        raise ValueError("Frequency V4 idempotency requires exact five-minute window")
    identity = "|".join((
        str(strategy_id), str(asset).upper(), str(slug), str(market_id),
        str(event_id), str(condition_id), str(int(window_open_ms)),
        str(int(window_close_ms)), parsed_side.value, "5",
    ))
    return sha256(identity.encode("utf-8")).hexdigest()


def entry_idempotency_key(
        market: MarketIdentity, side: EntrySide | str,
        *, strategy_id: str = STRATEGY_ID) -> str:
    return idempotent_intent_key(
        asset=market.asset,
        slug=market.slug,
        market_id=market.market_id,
        event_id=market.event_id,
        condition_id=market.condition_id,
        window_open_ms=market.window_open_ms,
        window_close_ms=market.window_close_ms,
        side=side,
        strategy_id=strategy_id,
    )


@dataclass(frozen=True, slots=True)
class ExposureDecision:
    allowed: bool
    reason: str
    equity_usd: float
    exposure_cap_usd: float
    committed_exposure_usd: float
    proposed_commitment_usd: float
    projected_exposure_usd: float
    available_balance_usd: float
    open_positions: int
    open_for_asset: int
    live_order_allowed: bool = False


def assess_shadow_exposure(
        *, committed_exposure_usd: float, open_positions: int,
        open_for_asset: int, entry_price: Optional[float] = None,
        sweep: Optional[Sweep] = None, equity_usd: float = 130.0,
        exposure_cap_pct: float = 1.0,
        available_balance_usd: Optional[float] = None,
        max_open_positions: int = 6, max_open_per_asset: int = 1,
        fee_rate: float = CRYPTO_TAKER_FEE_RATE,
        fee_buffer_usd: float = 0.0) -> ExposureDecision:
    """Atomic-store preflight inputs for the fixed-share shadow experiment.

    The live kill switch remains engaged independently; therefore
    ``live_order_allowed`` is permanently false and does not suppress safe
    simulated entries that satisfy these experimental exposure constraints.
    """
    try:
        equity = float(equity_usd)
        cap_pct = float(exposure_cap_pct)
        committed = float(committed_exposure_usd)
        positions = int(open_positions)
        asset_positions = int(open_for_asset)
        max_positions = int(max_open_positions)
        max_asset = int(max_open_per_asset)
        available = (equity - committed if available_balance_usd is None
                     else float(available_balance_usd))
        numeric = (equity, cap_pct, committed, available)
        if (not all(math.isfinite(value) for value in numeric)
                or equity <= 0.0 or committed < 0.0 or available < 0.0
                or not 0.0 < cap_pct <= 1.0
                or positions < 0 or asset_positions < 0
                or max_positions < 1 or max_asset != 1):
            raise ValueError("invalid exposure state")
        proposed = entry_commitment(
            entry_price, sweep=sweep, fee_rate=fee_rate,
            fee_buffer_usd=fee_buffer_usd)
    except (TypeError, ValueError, OverflowError):
        return ExposureDecision(
            False, "invalid_exposure_state", 0.0, 0.0, 0.0, 0.0,
            0.0, 0.0, 0, 0, False)
    cap = round(equity * cap_pct, 10)
    projected = round(committed + proposed, 10)
    reason = "allowed"
    if positions >= max_positions:
        reason = "max_open_positions"
    elif asset_positions >= max_asset:
        reason = "max_open_per_asset"
    elif projected > cap + 1e-9:
        reason = "global_exposure_cap"
    elif proposed > available + 1e-9:
        reason = "insufficient_available_balance"
    return ExposureDecision(
        allowed=reason == "allowed",
        reason=reason,
        equity_usd=equity,
        exposure_cap_usd=cap,
        committed_exposure_usd=committed,
        proposed_commitment_usd=proposed,
        projected_exposure_usd=projected,
        available_balance_usd=available,
        open_positions=positions,
        open_for_asset=asset_positions,
        live_order_allowed=False,
    )


assess_exposure = assess_shadow_exposure
