"""LITE SHADOW ONLY: small deterministic momentum entry policy.

There is deliberately no shock, EV, oracle, bankroll, USD sizing, or live
order logic here.  A valid decision always represents exactly five simulated
shares at the ask of the selected outcome's *direct* token book.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Optional

from .lite_book import LiteBookQuote


LITE_FIXED_SHARES = 5.0
_CLOSED_STATUSES = {
    "CLOSED", "CLOSED_BOOK_EXIT", "CLOSED_WIN", "CLOSED_LOSS",
    "UNRESOLVED", "UNRESOLVED_FINAL",
}


@dataclass(frozen=True)
class LiteDecision:
    accepted: bool
    reject_reason: str
    side: Optional[str] = None
    token_id: str = ""
    shares: float = LITE_FIXED_SHARES
    entry_price: Optional[float] = None
    entry_cost: Optional[float] = None
    momentum_pct: Optional[float] = None


def _value(obj, name: str, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _active_positions(open_positions) -> list:
    if open_positions is None:
        return []
    rows = list(open_positions.values()) if isinstance(open_positions, dict) else list(open_positions)
    active = []
    for row in rows:
        status = str(_value(row, "status", "OPEN") or "OPEN").upper()
        if status not in _CLOSED_STATUSES and not status.startswith("CLOSED_"):
            active.append(row)
    return active


def _momentum_samples(momentum_values, configured_windows: Iterable[int]) -> list[float]:
    if momentum_values is None:
        return []
    if isinstance(momentum_values, dict):
        raw = []
        for window in configured_windows:
            value = momentum_values.get(window, momentum_values.get(str(window)))
            if value is not None:
                raw.append(value)
        if not raw:
            raw = list(momentum_values.values())
    elif isinstance(momentum_values, (int, float)):
        raw = [momentum_values]
    else:
        raw = list(momentum_values)
    out: list[float] = []
    for value in raw:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            out.append(number)
    return out


class LiteStrategy:
    def __init__(self, cfg):
        self.cfg = cfg

    @staticmethod
    def _reject(reason: str, momentum: Optional[float] = None,
                side: Optional[str] = None, token_id: str = "") -> LiteDecision:
        return LiteDecision(
            accepted=False, reject_reason=reason, side=side,
            token_id=token_id, shares=5.0, momentum_pct=momentum,
        )

    @staticmethod
    def _same_window_position(position, market, side: str) -> bool:
        if str(_value(position, "asset", "")) != str(market.asset):
            return False
        if str(_value(position, "side", "")) != side:
            return False
        market_id = str(_value(position, "market_id", "") or "")
        slug = str(_value(position, "slug", "") or "")
        if market_id and market_id == str(market.market_id):
            return True
        if slug and slug == str(market.slug):
            return True
        close_ts = _value(position, "window_close_ts", None)
        if close_ts is None:
            close_ts = _value(position, "window_close_ts_ms", None)
        try:
            return int(close_ts) == int(market.window_close_s * 1000)
        except (TypeError, ValueError):
            return False

    def evaluate(self, market, yes_book: Optional[LiteBookQuote],
                 no_book: Optional[LiteBookQuote], cex_price: Optional[float],
                 cex_age_ms: Optional[int], cex_source: str,
                 momentum_values, now_ms: int, open_positions) -> LiteDecision:
        """Evaluate one market snapshot and return a pure simulated decision."""
        del cex_source  # recorded by the broker, never a strategy preference/gate

        if market is None:
            return self._reject("no_market")
        try:
            seconds_to_close = float(market.seconds_to_close(now_ms))
        except Exception:  # noqa: BLE001 -- malformed injected market is invalid
            return self._reject("no_market")
        if seconds_to_close <= 0:
            return self._reject("expired_market")
        if not (float(self.cfg.time_to_close_min_s) <= seconds_to_close
                <= float(self.cfg.time_to_close_max_s)):
            return self._reject("price_window")

        yes_token = str(getattr(market, "yes_token_id", "") or "")
        no_token = str(getattr(market, "no_token_id", "") or "")
        if not yes_token or not no_token or yes_token == no_token:
            return self._reject("invalid_token")

        try:
            cex = float(cex_price)
            age = int(cex_age_ms)
        except (TypeError, ValueError):
            return self._reject("cex_stale")
        if not math.isfinite(cex) or cex <= 0 or age < 0 \
                or age > int(self.cfg.cex_max_age_ms):
            return self._reject("cex_stale")

        samples = _momentum_samples(
            momentum_values, getattr(self.cfg, "momentum_windows_s", (10, 30, 60)))
        if not samples:
            return self._reject("no_momentum")
        momentum = sum(samples) / len(samples)
        threshold = abs(float(self.cfg.momentum_min_pct))
        if abs(momentum) < threshold or momentum == 0.0:
            return self._reject("no_momentum", momentum)

        if momentum > 0:
            side, token_id, book = "BUY_YES", yes_token, yes_book
        else:
            side, token_id, book = "BUY_NO", no_token, no_book

        all_positions = (list(open_positions.values()) if isinstance(open_positions, dict)
                         else list(open_positions or []))
        if bool(getattr(self.cfg, "max_one_per_asset_window_side", True)) and any(
                self._same_window_position(position, market, side)
                for position in all_positions):
            return self._reject("duplicate_position", momentum, side, token_id)
        positions = _active_positions(all_positions)
        if len(positions) >= int(self.cfg.max_open_positions):
            return self._reject("max_open_positions", momentum, side, token_id)
        per_asset = sum(
            1 for position in positions
            if str(_value(position, "asset", "")) == str(market.asset)
        )
        if per_asset >= int(self.cfg.max_open_per_asset):
            return self._reject("max_open_positions", momentum, side, token_id)

        if book is None:
            return self._reject("no_book", momentum, side, token_id)
        if str(book.token_id) != token_id:
            return self._reject("invalid_token", momentum, side, token_id)
        book_max_age = int(getattr(self.cfg, "book_max_age_ms", self.cfg.cex_max_age_ms))
        if book.is_stale(now_ms, book_max_age):
            return self._reject("stale_book", momentum, side, token_id)
        bid, ask = book.best_bid, book.best_ask
        if bid is None or ask is None or not (0.0 <= bid <= 1.0) \
                or not (0.0 < ask < 1.0) or bid > ask:
            return self._reject("no_book", momentum, side, token_id)
        spread = ask - bid
        if spread > float(self.cfg.max_spread):
            return self._reject("spread_too_wide", momentum, side, token_id)
        if float(book.ask_depth_usd) < float(self.cfg.min_depth_usd):
            return self._reject("depth_too_low", momentum, side, token_id)

        # This literal is intentional: Lite has no USD sizing path and ignores
        # every configurable/max-trade USD value by construction.
        shares = 5.0
        entry_price = float(ask)
        return LiteDecision(
            accepted=True, reject_reason="opened", side=side,
            token_id=token_id, shares=shares, entry_price=entry_price,
            entry_cost=shares * entry_price, momentum_pct=momentum,
        )
