"""Exact-market official resolution and conservative pre-close exits."""
from __future__ import annotations

import asyncio
import inspect
import json
import math
from typing import Any, Optional

from .lite_book import LiteBookQuote, LiteBookSweep
from .lite_config import FIXED_SHARES
from .lite_risk import CRYPTO_TAKER_FEE_RATE, fill_levels_taker_fee, taker_fee


_POSITIVE_LABELS = {"yes", "up", "above", "higher"}
_NEGATIVE_LABELS = {"no", "down", "below", "lower"}


def _value(obj, name: str, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _as_list(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _first(row: dict, *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value is not None and str(value) != "":
            return str(value)
    return ""


def _slug_window(slug: str) -> tuple[Optional[int], Optional[int]]:
    try:
        start_s = int(str(slug).rsplit("-", 1)[1])
    except (IndexError, TypeError, ValueError):
        return None, None
    if start_s <= 0 or start_s % 300:
        return None, None
    return start_s * 1000, (start_s + 300) * 1000


def validate_market_identity(
        row: Optional[dict], *, market_id: str, slug: str,
        condition_id: str, window_open_ts: Optional[int] = None,
        window_close_ts: Optional[int] = None,
        yes_token_id: str = "", no_token_id: str = "") -> tuple[bool, str]:
    if not isinstance(row, dict):
        return False, "no_market_row"
    checks = (
        ("market_id", str(market_id or ""), _first(row, "id", "market_id")),
        ("slug", str(slug or ""), _first(row, "slug", "market_slug")),
        ("condition_id", str(condition_id or ""),
         _first(row, "conditionId", "condition_id")),
    )
    for label, expected, actual in checks:
        if not expected or actual != expected:
            return False, f"{label}_mismatch"
    actual_open, actual_close = _slug_window(_first(row, "slug", "market_slug"))
    if window_open_ts is not None and actual_open != int(window_open_ts):
        return False, "window_open_mismatch"
    if window_close_ts is not None and actual_close != int(window_close_ts):
        return False, "window_close_mismatch"
    if yes_token_id or no_token_id:
        labels = [str(value).strip().lower() for value in _as_list(row.get("outcomes"))]
        tokens = [str(value) for value in _as_list(row.get("clobTokenIds"))]
        if len(labels) != 2 or len(tokens) != 2:
            return False, "token_mapping_unparseable"
        positive = [i for i, label in enumerate(labels) if label in _POSITIVE_LABELS]
        negative = [i for i, label in enumerate(labels) if label in _NEGATIVE_LABELS]
        if len(positive) != 1 or len(negative) != 1:
            return False, "token_mapping_unparseable"
        if (yes_token_id and tokens[positive[0]] != str(yes_token_id)):
            return False, "yes_token_mismatch"
        if (no_token_id and tokens[negative[0]] != str(no_token_id)):
            return False, "no_token_mismatch"
    return True, "identity_match"


def event_market_row(event: Optional[dict], *, event_id: str,
                     market_id: str, slug: str, condition_id: str,
                     window_open_ts: Optional[int] = None,
                     window_close_ts: Optional[int] = None,
                     yes_token_id: str = "", no_token_id: str = "") -> tuple[Optional[dict], str]:
    if not isinstance(event, dict):
        return None, "no_event_row"
    if _first(event, "id", "eventId", "event_id") != str(event_id):
        return None, "event_id_mismatch"
    if _first(event, "slug", "eventSlug") != str(slug):
        return None, "event_slug_mismatch"
    markets = event.get("markets")
    if not isinstance(markets, list):
        return None, "event_markets_unparseable"
    last_reason = "event_market_missing"
    for row in markets:
        ok, reason = validate_market_identity(
            row if isinstance(row, dict) else None, market_id=market_id,
            slug=slug, condition_id=condition_id,
            window_open_ts=window_open_ts, window_close_ts=window_close_ts,
            yes_token_id=yes_token_id, no_token_id=no_token_id)
        if ok:
            return row, "identity_match"
        last_reason = reason
    return None, last_reason


def official_outcome_from_market_row(
        row: Optional[dict], *, market_id: str = "", event_id: str = "",
        slug: str = "", condition_id: str = "",
        window_open_ts: Optional[int] = None,
        window_close_ts: Optional[int] = None,
        yes_token_id: str = "", no_token_id: str = "") -> tuple[Optional[str], str]:
    del event_id  # event association is independently validated by event_market_row
    ok, reason = validate_market_identity(
        row, market_id=market_id, slug=slug, condition_id=condition_id,
        window_open_ts=window_open_ts, window_close_ts=window_close_ts,
        yes_token_id=yes_token_id, no_token_id=no_token_id)
    if not ok:
        return None, reason
    assert isinstance(row, dict)
    if row.get("closed") is not True:
        return None, "market_not_closed_yet"
    prices = _as_list(row.get("outcomePrices"))
    outcomes = [str(item).strip().lower() for item in _as_list(row.get("outcomes"))]
    if len(prices) != 2:
        return None, "outcome_prices_unparseable"
    if len(outcomes) != 2:
        return None, "outcome_labels_unparseable"
    try:
        parsed = [float(prices[0]), float(prices[1])]
    except (TypeError, ValueError):
        return None, "outcome_prices_unparseable"
    if not all(math.isfinite(value) and 0 <= value <= 1 for value in parsed):
        return None, "outcome_prices_unparseable"
    positive = [i for i, label in enumerate(outcomes) if label in _POSITIVE_LABELS]
    negative = [i for i, label in enumerate(outcomes) if label in _NEGATIVE_LABELS]
    if len(positive) != 1 or len(negative) != 1 or positive[0] == negative[0]:
        return None, "outcome_labels_unparseable"
    p_yes, p_no = parsed[positive[0]], parsed[negative[0]]
    if p_yes >= 0.99 and p_no <= 0.01:
        return "YES", "resolved"
    if p_no >= 0.99 and p_yes <= 0.01:
        return "NO", "resolved"
    return None, "outcome_prices_not_degenerate"


outcome_from_market_row = official_outcome_from_market_row


def market_fee_rate(row: Optional[dict], default: float = CRYPTO_TAKER_FEE_RATE) -> tuple[float, str]:
    if not isinstance(row, dict):
        return float(default), "fee_default_missing_market"
    enabled = row.get("feesEnabled")
    if enabled is False:
        return 0.0, "fees_disabled"
    if enabled is not True:
        return float(default), "fee_default_unparseable_enabled"
    schedule = row.get("feeSchedule")
    try:
        rate = float(schedule.get("rate")) if isinstance(schedule, dict) else float("nan")
        exponent = float(schedule.get("exponent")) if isinstance(schedule, dict) else float("nan")
    except (TypeError, ValueError):
        rate = exponent = float("nan")
    if (math.isfinite(rate) and rate >= 0 and exponent == 1.0
            and schedule.get("takerOnly") is True):
        return rate, "exact_fee_schedule"
    return float(default), "fee_default_unsupported_schedule"


def entry_fee_from_trade(trade, fee_rate: float) -> float:
    """Reprice the entry from exact stored fill levels when forward proof exists."""
    levels = _as_list(_value(trade, "entry_fill_levels", None))
    if levels:
        return fill_levels_taker_fee(levels, fee_rate)
    return taker_fee(
        FIXED_SHARES, float(_value(trade, "entry_price")), fee_rate)


def book_exit_pnl(entry_price: float, shares: float, exit_price: float) -> float:
    return float(shares) * (float(exit_price) - float(entry_price))


calculate_book_exit_pnl = book_exit_pnl


def official_resolution_pnl(side: str, entry_price: float, shares: float,
                            winning_outcome: str,
                            entry_fee: float = 0.0) -> tuple[float, float, bool]:
    won = ((str(side) == "BUY_YES" and str(winning_outcome) == "YES")
           or (str(side) == "BUY_NO" and str(winning_outcome) == "NO"))
    exit_price = 1.0 if won else 0.0
    pnl = float(shares) * (exit_price - float(entry_price)) - float(entry_fee)
    return pnl, exit_price, won


calculate_official_pnl = official_resolution_pnl


def direct_book_exit(
        trade, quote: Optional[LiteBookQuote], now_ms: int, max_age_ms: int,
        max_spread: float) -> tuple[Optional[LiteBookSweep], str]:
    side = str(_value(trade, "side", ""))
    token_id = (_value(trade, "yes_token_id", "") if side == "BUY_YES"
                else _value(trade, "no_token_id", "") if side == "BUY_NO" else "")
    if not token_id:
        return None, "invalid_token"
    if quote is None:
        return None, "no_book"
    if str(quote.token_id) != str(token_id):
        return None, "invalid_token"
    condition_id = str(_value(trade, "condition_id", "") or "")
    if (not quote.condition_id or quote.condition_id != condition_id
            or not quote.book_hash or quote.source_ts_ms is None):
        return None, "invalid_market_book"
    close_ts = int(_value(trade, "window_close_ts", 0) or 0)
    if close_ts <= 0 or int(now_ms) >= close_ts or quote.effective_ts_ms() >= close_ts:
        return None, "post_close_book_forbidden"
    if quote.is_future(now_ms):
        return None, "future_book"
    if quote.is_stale(now_ms, max_age_ms):
        return None, "stale_book"
    if quote.hash_reused:
        return None, "reused_book_snapshot"
    if quote.min_order_size is None:
        return None, "minimum_order_size_missing"
    if FIXED_SHARES < float(quote.min_order_size):
        return None, "minimum_order_size"
    if quote.spread is None or quote.spread < 0:
        return None, "no_book"
    if quote.spread > float(max_spread):
        return None, "spread_too_wide"
    sweep = quote.sell_sweep(FIXED_SHARES)
    if sweep is None:
        return None, "insufficient_five_share_depth"
    return sweep, "direct_token_bid_sweep"


def direct_book_exit_price(trade, quote: Optional[LiteBookQuote], now_ms: int,
                           max_age_ms: int, max_spread: float,
                           min_depth_usd: float = 0.0) -> tuple[Optional[float], str]:
    del min_depth_usd
    sweep, reason = direct_book_exit(trade, quote, now_ms, max_age_ms, max_spread)
    return (sweep.vwap if sweep is not None else None), reason


async def _maybe_await(value):
    return await value if inspect.isawaitable(value) else value


class LiteResolver:
    def __init__(self, store, fetch_market, fetch_event, cfg):
        self.store = store
        self.fetch_market = fetch_market
        self.fetch_event = fetch_event
        self.cfg = cfg

    async def _fetch_exact_row(self, trade) -> tuple[Optional[dict], str]:
        market_id = str(_value(trade, "market_id", "") or "")
        event_id = str(_value(trade, "event_id", "") or "")
        if not market_id or not event_id:
            return None, "stored_identity_incomplete"
        async def invoke(function, identity):
            return await _maybe_await(function(identity))
        try:
            market, event = await asyncio.gather(
                invoke(self.fetch_market, market_id),
                invoke(self.fetch_event, event_id),
            )
        except Exception as exc:
            return None, f"fetch_failed:{type(exc).__name__}"
        identity = {
            "market_id": market_id,
            "slug": str(_value(trade, "slug", "") or ""),
            "condition_id": str(_value(trade, "condition_id", "") or ""),
            "window_open_ts": int(_value(trade, "window_open_ts",
                                          int(_value(trade, "window_close_ts", 0))-300_000)),
            "window_close_ts": int(_value(trade, "window_close_ts", 0)),
            "yes_token_id": str(_value(trade, "yes_token_id", "") or ""),
            "no_token_id": str(_value(trade, "no_token_id", "") or ""),
        }
        ok, reason = validate_market_identity(market, **identity)
        if not ok:
            return None, f"direct_market_{reason}"
        associated, reason = event_market_row(event, event_id=event_id, **identity)
        if associated is None:
            return None, reason
        direct_outcome, direct_reason = official_outcome_from_market_row(
            market, **identity)
        event_outcome, event_reason = official_outcome_from_market_row(
            associated, **identity)
        if (direct_outcome, direct_reason) != (event_outcome, event_reason):
            return None, (
                f"direct_event_outcome_mismatch:{direct_reason}:{event_reason}")
        direct_fee = market_fee_rate(
            market, float(getattr(self.cfg, "crypto_taker_fee_rate", CRYPTO_TAKER_FEE_RATE)))
        event_fee = market_fee_rate(
            associated, float(getattr(self.cfg, "crypto_taker_fee_rate", CRYPTO_TAKER_FEE_RATE)))
        if direct_fee != event_fee:
            return None, "direct_event_fee_schedule_mismatch"
        # Event-associated market is the row used for official outcome and fee
        # evidence; the direct market corroboration above is independently exact.
        return associated, "exact_market_event_identity"

    async def _resolve_one(self, trade, now_ms: int) -> dict:
        trade_id = int(_value(trade, "id", 0) or 0)
        if trade_id <= 0:
            return {"trade_id": None, "action": "error", "reason": "missing_trade_id"}
        close_ts = int(_value(trade, "window_close_ts", 0) or 0)
        if now_ms < close_ts:
            return {"trade_id": trade_id, "action": "waiting", "reason": "market_not_closed_yet"}
        await _maybe_await(self.store.begin_resolution_attempt(trade_id, now_ms))
        row, reason = await self._fetch_exact_row(trade)
        outcome = None
        if row is not None and bool(getattr(self.cfg, "allow_official_resolution", True)):
            outcome, reason = official_outcome_from_market_row(
                row, market_id=str(_value(trade, "market_id", "")),
                slug=str(_value(trade, "slug", "")),
                condition_id=str(_value(trade, "condition_id", "")),
                window_open_ts=int(_value(trade, "window_open_ts", close_ts-300_000)),
                window_close_ts=close_ts,
                yes_token_id=str(_value(trade, "yes_token_id", "")),
                no_token_id=str(_value(trade, "no_token_id", "")))
        elif row is not None:
            reason = "official_resolution_disabled"
        if outcome is not None:
            fee_rate, fee_source = market_fee_rate(
                row, float(getattr(self.cfg, "crypto_taker_fee_rate", CRYPTO_TAKER_FEE_RATE)))
            entry_fee = entry_fee_from_trade(trade, fee_rate)
            net_pnl, exit_price, won = official_resolution_pnl(
                str(_value(trade, "side", "")), float(_value(trade, "entry_price")),
                FIXED_SHARES, outcome, entry_fee)
            gross = net_pnl + entry_fee
            await _maybe_await(self.store.complete_trade(
                trade_id=trade_id, status="CLOSED_WIN" if won else "CLOSED_LOSS",
                exit_price=exit_price, exit_ts=now_ms, pnl=net_pnl, gross_pnl=gross,
                exit_fee=0.0, resolution_source="official_outcome",
                resolution_reason=f"exact_gamma:{outcome}:{fee_source}",
                resolution_verified=True, entry_fee=entry_fee,
                fee_rate=fee_rate))
            return {"trade_id": trade_id, "action": "official_outcome",
                    "outcome": outcome, "won": won, "pnl": net_pnl}
        retry = await _maybe_await(self.store.note_resolution_retry(
            trade_id, now_ms, reason,
            float(self.cfg.resolver_retry_seconds),
            float(self.cfg.resolver_retry_cap_seconds),
            int(self.cfg.resolver_max_retries),
            float(self.cfg.resolver_final_recheck_seconds)))
        action = "unresolved_final" if retry >= int(self.cfg.resolver_max_retries) else "retry"
        return {"trade_id": trade_id, "action": action,
                "retry_count": retry, "reason": reason}

    async def resolve_due(self, now_ms: int) -> list[dict]:
        rows = await _maybe_await(self.store.resolution_trades_due(
            int(now_ms), int(self.cfg.resolver_batch_size)))

        async def safely(trade):
            try:
                return await self._resolve_one(trade, int(now_ms))
            except Exception as exc:
                trade_id = int(_value(trade, "id", 0) or 0)
                reason = f"resolver_exception:{type(exc).__name__}"
                if trade_id:
                    await _maybe_await(self.store.note_resolution_retry(
                        trade_id, int(now_ms), reason,
                        float(self.cfg.resolver_retry_seconds),
                        float(self.cfg.resolver_retry_cap_seconds),
                        int(self.cfg.resolver_max_retries),
                        float(self.cfg.resolver_final_recheck_seconds)))
                return {"trade_id": trade_id or None, "action": "error", "reason": reason}

        return list(await asyncio.gather(*(safely(trade) for trade in rows or [])))

    run_once = resolve_due
    resolve_pending = resolve_due
