"""LITE SHADOW ONLY: honest direct-book exits and official resolution.

Only an exact market identity plus a closed, degenerate public Gamma outcome
can settle a trade officially.  Resolution URLs are intentionally ignored.
"""
from __future__ import annotations

import inspect
import json
import math
from typing import Optional

from .lite_book import LiteBookQuote


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


def _event_id(row: dict) -> str:
    direct = _first(row, "eventId", "event_id", "eventID")
    if direct:
        return direct
    events = row.get("events")
    if isinstance(events, list):
        for event in events:
            if isinstance(event, dict):
                value = _first(event, "id", "eventId", "event_id")
                if value:
                    return value
    return ""


def validate_market_identity(row: Optional[dict], *, market_id: str,
                             event_id: str = "", slug: str = "",
                             condition_id: str = "") -> tuple[bool, str]:
    """Fail closed when any stored, non-empty identity does not match."""
    if not isinstance(row, dict):
        return False, "no_market_row"
    checks = (
        ("market_id", str(market_id or ""), _first(row, "id", "market_id")),
        ("event_id", str(event_id or ""), _event_id(row)),
        ("slug", str(slug or ""), _first(row, "slug", "market_slug")),
        ("condition_id", str(condition_id or ""),
         _first(row, "conditionId", "condition_id")),
    )
    for label, expected, actual in checks:
        if expected and actual != expected:
            return False, f"{label}_mismatch"
    return True, "identity_match"


def official_outcome_from_market_row(
        row: Optional[dict], *, market_id: str = "", event_id: str = "",
        slug: str = "", condition_id: str = "") -> tuple[Optional[str], str]:
    """Return ``YES``/``NO`` only from exact, hard official evidence."""
    ok, reason = validate_market_identity(
        row, market_id=market_id, event_id=event_id,
        slug=slug, condition_id=condition_id)
    if not ok:
        return None, reason
    assert isinstance(row, dict)
    if row.get("closed") is not True:
        return None, "market_not_closed_yet"
    prices = _as_list(row.get("outcomePrices"))
    if len(prices) != 2:
        return None, "outcome_prices_unparseable"
    try:
        parsed_prices = [float(prices[0]), float(prices[1])]
    except (TypeError, ValueError):
        return None, "outcome_prices_unparseable"
    if not all(math.isfinite(price) for price in parsed_prices):
        return None, "outcome_prices_unparseable"

    outcomes = [str(item).strip().lower() for item in _as_list(row.get("outcomes"))]
    if outcomes:
        if len(outcomes) != 2:
            return None, "outcome_labels_unparseable"
        positive = [i for i, label in enumerate(outcomes) if label in _POSITIVE_LABELS]
        negative = [i for i, label in enumerate(outcomes) if label in _NEGATIVE_LABELS]
        if len(positive) != 1 or len(negative) != 1 or positive[0] == negative[0]:
            return None, "outcome_labels_unparseable"
        p_yes, p_no = parsed_prices[positive[0]], parsed_prices[negative[0]]
    else:
        # Gamma's normal Up/Down row order is positive then negative.  Rows
        # that publish labels are mapped above, including reversed ordering.
        p_yes, p_no = parsed_prices
    if p_yes >= 0.99 and p_no <= 0.01:
        return "YES", "resolved"
    if p_no >= 0.99 and p_yes <= 0.01:
        return "NO", "resolved"
    return None, "outcome_prices_not_degenerate"


# Familiar name matching the repository's pure research resolver.
outcome_from_market_row = official_outcome_from_market_row


def book_exit_pnl(entry_price: float, shares: float, exit_price: float) -> float:
    return float(shares) * (float(exit_price) - float(entry_price))


calculate_book_exit_pnl = book_exit_pnl


def official_resolution_pnl(side: str, entry_price: float, shares: float,
                            winning_outcome: str) -> tuple[float, float, bool]:
    won = ((str(side) == "BUY_YES" and str(winning_outcome) == "YES")
           or (str(side) == "BUY_NO" and str(winning_outcome) == "NO"))
    exit_price = 1.0 if won else 0.0
    pnl = float(shares) * exit_price - float(shares) * float(entry_price)
    return pnl, exit_price, won


calculate_official_pnl = official_resolution_pnl


def direct_book_exit_price(trade, quote: Optional[LiteBookQuote], now_ms: int,
                           max_age_ms: int, max_spread: float,
                           min_depth_usd: float) -> tuple[Optional[float], str]:
    side = str(_value(trade, "side", ""))
    token_id = (_value(trade, "yes_token_id", "") if side == "BUY_YES"
                else _value(trade, "no_token_id", "") if side == "BUY_NO" else "")
    if not token_id:
        return None, "invalid_token"
    if quote is None:
        return None, "no_book"
    if str(quote.token_id) != str(token_id):
        return None, "invalid_token"
    if quote.is_stale(now_ms, max_age_ms):
        return None, "stale_book"
    bid = quote.best_bid
    if bid is None or not (0.0 < float(bid) <= 1.0):
        return None, "no_book"
    if float(quote.bid_depth_usd) < float(min_depth_usd):
        return None, "depth_too_low"
    if quote.best_ask is not None:
        spread = float(quote.best_ask) - float(bid)
        if spread < 0:
            return None, "no_book"
        if spread > float(max_spread):
            return None, "spread_too_wide"
    return float(bid), "direct_token_bid"


async def _maybe_await(value):
    return await value if inspect.isawaitable(value) else value


class LiteResolver:
    def __init__(self, store, fetch_markets, book_client, cfg):
        self.store = store
        self.fetch_markets = fetch_markets
        self.book_client = book_client
        self.cfg = cfg

    async def _fetch_exact_row(self, trade) -> tuple[Optional[dict], str]:
        market_id = str(_value(trade, "market_id", "") or "")
        slug = str(_value(trade, "slug", "") or "")
        event_id = str(_value(trade, "event_id", "") or "")
        condition_id = str(_value(trade, "condition_id", "") or "")
        queries = []
        if market_id:
            queries.append({"id": market_id})
        if slug:
            queries.append({"slug": slug})
        last_reason = "no_market_row"
        for params in queries:
            try:
                rows = await _maybe_await(self.fetch_markets(params))
            except Exception as exc:  # noqa: BLE001 -- retry records the public read failure
                last_reason = f"fetch_failed:{type(exc).__name__}"
                continue
            for row in rows if isinstance(rows, list) else []:
                ok, reason = validate_market_identity(
                    row, market_id=market_id, event_id=event_id,
                    slug=slug, condition_id=condition_id)
                if ok:
                    return row, "identity_match"
                last_reason = reason
        return None, last_reason

    async def _resolve_one(self, trade, now_ms: int) -> dict:
        trade_id = _value(trade, "id", _value(trade, "trade_id", None))
        if trade_id is None:
            return {"trade_id": None, "action": "error", "reason": "missing_trade_id"}
        try:
            close_ts = int(_value(trade, "window_close_ts", 0) or 0)
        except (TypeError, ValueError):
            close_ts = 0
        book_due = close_ts <= 0 or now_ms >= close_ts - int(
            float(getattr(self.cfg, "exit_before_close_s", 15.0)) * 1000)

        if bool(getattr(self.cfg, "allow_book_exit", True)) and book_due:
            side = str(_value(trade, "side", ""))
            token_id = (_value(trade, "yes_token_id", "") if side == "BUY_YES"
                        else _value(trade, "no_token_id", "") if side == "BUY_NO" else "")
            try:
                quote = await _maybe_await(self.book_client.get_book(str(token_id))) if token_id else None
            except Exception:  # noqa: BLE001
                quote = None
            exit_price, book_reason = direct_book_exit_price(
                trade, quote, now_ms,
                int(getattr(self.cfg, "book_max_age_ms", self.cfg.cex_max_age_ms)),
                float(self.cfg.max_spread), float(self.cfg.min_depth_usd))
            if exit_price is not None:
                pnl = book_exit_pnl(
                    float(_value(trade, "entry_price", 0.0)),
                    float(_value(trade, "shares", 5.0)), exit_price)
                await _maybe_await(self.store.complete_trade(
                    trade_id=trade_id, status="CLOSED_BOOK_EXIT",
                    exit_price=exit_price, exit_ts=now_ms, pnl=pnl,
                    resolution_source="book_exit",
                    resolution_reason="direct_token_book"))
                return {"trade_id": trade_id, "action": "book_exit", "pnl": pnl}
        else:
            book_reason = "book_exit_not_due"

        # Before the exact market closes, a missing exit book is not an
        # official-resolution failure and must not consume the retry budget.
        if close_ts > 0 and now_ms < close_ts:
            return {"trade_id": trade_id, "action": "waiting", "reason": book_reason}

        if not bool(getattr(self.cfg, "allow_official_resolution", True)):
            official_reason = "official_resolution_disabled"
            outcome = None
        else:
            row, fetch_reason = await self._fetch_exact_row(trade)
            if row is None:
                outcome, official_reason = None, fetch_reason
            else:
                outcome, official_reason = official_outcome_from_market_row(
                    row,
                    market_id=str(_value(trade, "market_id", "") or ""),
                    event_id=str(_value(trade, "event_id", "") or ""),
                    slug=str(_value(trade, "slug", "") or ""),
                    condition_id=str(_value(trade, "condition_id", "") or ""))

        if outcome is not None:
            pnl, exit_price, won = official_resolution_pnl(
                str(_value(trade, "side", "")),
                float(_value(trade, "entry_price", 0.0)),
                float(_value(trade, "shares", 5.0)), outcome)
            await _maybe_await(self.store.complete_trade(
                trade_id=trade_id,
                status="CLOSED_WIN" if won else "CLOSED_LOSS",
                exit_price=exit_price, exit_ts=now_ms, pnl=pnl,
                resolution_source="official_outcome",
                resolution_reason="official_outcome"))
            return {"trade_id": trade_id, "action": "official_outcome",
                    "outcome": outcome, "won": won, "pnl": pnl}

        retry_count = await _maybe_await(
            self.store.note_resolution_retry(trade_id, now_ms, official_reason))
        if retry_count is None:
            retry_count = int(_value(trade, "retry_count", 0) or 0) + 1
        if int(retry_count) >= int(self.cfg.resolver_max_retries):
            final_reason = (f"no_official_outcome_after_{retry_count}_retries "
                            f"({official_reason})")
            await _maybe_await(self.store.mark_unresolved(trade_id, now_ms, final_reason))
            return {"trade_id": trade_id, "action": "unresolved_final",
                    "reason": final_reason}
        return {"trade_id": trade_id, "action": "retry",
                "retry_count": int(retry_count), "reason": official_reason}

    async def resolve_due(self, now_ms: int) -> list[dict]:
        """Process due OPEN/PENDING trades without fabricating outcomes."""
        rows = await _maybe_await(self.store.pending_trades_due(
            int(now_ms), float(self.cfg.resolver_retry_seconds)))
        results: list[dict] = []
        for trade in rows or []:
            try:
                results.append(await self._resolve_one(trade, int(now_ms)))
            except Exception as exc:  # noqa: BLE001 -- one row cannot stop the resolver
                results.append({"trade_id": _value(trade, "id", None),
                                "action": "error", "reason": type(exc).__name__})
        return results

    # Alias reads naturally from bot orchestration code.
    run_once = resolve_due
    resolve_pending = resolve_due
