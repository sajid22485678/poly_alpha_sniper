"""LITE SHADOW ONLY: persist hypothetical five-share entries.

The broker has no exchange client and exposes no place/cancel-order operation.
Its entire effect is one insert into the dedicated Lite store.
"""
from __future__ import annotations

import json
import math
from typing import Optional

from .lite_config import FIXED_SHARES
from .lite_risk import CRYPTO_TAKER_FEE_RATE, fill_levels_taker_fee
from .lite_strategy import LiteDecision


STRATEGY_NAME = "lite_direction_sniper_v2"


class LiteBroker:
    def __init__(self, store, fee_rate: float = CRYPTO_TAKER_FEE_RATE,
                 fee_buffer_usd: float = 0.0, book_max_age_ms: int = 8_000,
                 max_spread: float = 0.20):
        self.store = store
        self.fee_rate = float(fee_rate)
        self.fee_buffer_usd = max(0.0, float(fee_buffer_usd))
        self.book_max_age_ms = int(book_max_age_ms)
        self.max_spread = float(max_spread)

    def _validated_evidence(self, evidence: dict, *, market, token_id: str,
                            entry_price: float, entry_fee: float,
                            now_ms: int) -> tuple[tuple[tuple[float, float], ...], float]:
        """Bind persisted proof to this exact token, market, time, price, and fee."""
        try:
            if str(evidence.get("token_id") or "") != str(token_id):
                raise ValueError("token")
            if str(evidence.get("condition_id") or "") != str(market.condition_id):
                raise ValueError("condition")
            source_ts = int(evidence["book_ts"])
            received_ts = int(evidence["received_ts"])
            age_ms = int(evidence["age_ms"])
            if not (0 < source_ts <= received_ts <= int(now_ms)):
                raise ValueError("timestamps")
            if age_ms != int(now_ms) - source_ts or not 0 <= age_ms <= self.book_max_age_ms:
                raise ValueError("age")
            if not str(evidence.get("book_hash") or "") or evidence.get("hash_reused") is not False:
                raise ValueError("hash")
            minimum = float(evidence["min_order_size"])
            if not math.isfinite(minimum) or minimum <= 0 or minimum > FIXED_SHARES:
                raise ValueError("minimum")
            best_bid = float(evidence["best_bid"])
            best_ask = float(evidence["best_ask"])
            spread = float(evidence["spread"])
            if (not all(math.isfinite(value) for value in (best_bid, best_ask, spread))
                    or not 0 <= best_bid <= best_ask <= 1
                    or abs((best_ask-best_bid)-spread) > 1e-9
                    or spread > self.max_spread):
                raise ValueError("spread")
            raw_levels = evidence.get("fill_levels")
            if not isinstance(raw_levels, (list, tuple)) or not raw_levels:
                raise ValueError("levels")
            levels: list[tuple[float, float]] = []
            for raw_level in raw_levels:
                if not isinstance(raw_level, (list, tuple)) or len(raw_level) != 2:
                    raise ValueError("level")
                price, shares = float(raw_level[0]), float(raw_level[1])
                if (not math.isfinite(price) or not math.isfinite(shares)
                        or not 0 < price < 1 or shares <= 0):
                    raise ValueError("level")
                levels.append((price, shares))
            if any(current[0] < previous[0] for previous, current in zip(levels, levels[1:])):
                raise ValueError("price priority")
            total_shares = sum(size for _, size in levels)
            notional = sum(price*size for price, size in levels)
            vwap = notional / total_shares
            if (abs(total_shares-FIXED_SHARES) > 1e-9
                    or abs(float(evidence["fill_shares"])-FIXED_SHARES) > 1e-9
                    or float(evidence["ask_depth_shares"]) + 1e-9 < FIXED_SHARES
                    or abs(float(evidence["fill_vwap"])-vwap) > 1e-9
                    or abs(float(evidence["worst_price"])-levels[-1][0]) > 1e-9
                    or abs(best_ask-levels[0][0]) > 1e-9
                    or abs(entry_price-vwap) > 1e-9):
                raise ValueError("fill binding")
            exact_fee = fill_levels_taker_fee(levels, self.fee_rate)
            if abs(float(entry_fee)-exact_fee) > 1e-9:
                raise ValueError("fee binding")
        except (KeyError, TypeError, ValueError, OverflowError, ZeroDivisionError) as exc:
            raise ValueError("Lite entry lacks bound five-share book evidence") from exc
        return tuple(levels), levels[-1][0]

    def open_trade(self, market, decision: LiteDecision, now_ms: int, *,
                   cex_source: str = "", cex_entry_price: Optional[float] = None,
                   strategy_name: str = STRATEGY_NAME) -> dict:
        """Insert one simulated OPEN trade and return its persisted row."""
        if not decision.accepted:
            raise ValueError("cannot open a rejected Lite decision")
        if decision.side not in ("BUY_YES", "BUY_NO"):
            raise ValueError("Lite entry side must be BUY_YES or BUY_NO")
        expected_token = (market.yes_token_id if decision.side == "BUY_YES"
                          else market.no_token_id)
        if not expected_token or str(decision.token_id) != str(expected_token):
            raise ValueError("Lite decision token does not match the direct outcome token")
        try:
            entry_price = float(decision.entry_price)
        except (TypeError, ValueError) as exc:
            raise ValueError("Lite decision has no executable entry price") from exc
        if not (0.0 < entry_price < 1.0):
            raise ValueError("Lite entry price must be between zero and one")

        # Never trust a caller-supplied size/cost: the isolated broker itself
        # reasserts the only sizing rule Lite has.
        shares = FIXED_SHARES
        entry_cost = shares * entry_price
        evidence = dict(decision.book_evidence or {})
        levels, worst_price = self._validated_evidence(
            evidence, market=market, token_id=str(expected_token),
            entry_price=entry_price, entry_fee=float(decision.entry_fee),
            now_ms=int(now_ms))
        anchor_available = bool(getattr(market, "anchor_available", False))
        row = {
            "asset": str(market.asset),
            "market_id": str(market.market_id),
            "event_id": str(getattr(market, "event_id", "") or ""),
            "slug": str(market.slug),
            "condition_id": str(getattr(market, "condition_id", "") or ""),
            "yes_token_id": str(market.yes_token_id),
            "no_token_id": str(market.no_token_id),
            "side": decision.side,
            "shares": shares,
            "entry_price": entry_price,
            "entry_cost": entry_cost,
            "entry_fee": float(decision.entry_fee),
            "fee_buffer": self.fee_buffer_usd,
            "fee_rate": self.fee_rate,
            "entry_ts": int(now_ms),
            "window_open_ts": int(market.window_start_s * 1000),
            "window_close_ts": int(market.window_close_s * 1000),
            "status": "OPEN",
            "exit_price": None,
            "exit_ts": None,
            "pnl": None,
            "resolution_source": "",
            "resolution_reason": "",
            "retry_count": 0,
            "last_attempt_at": None,
            "last_error": None,
            "next_attempt_at": None,
            "resolution_verified": False,
            "anchor_available": anchor_available,
            "price_to_beat": getattr(market, "price_to_beat", None),
            "no_anchor_trade": not anchor_available,
            "cex_source": str(cex_source or ""),
            "cex_entry_price": (float(cex_entry_price)
                                if cex_entry_price is not None else None),
            "momentum_pct": decision.momentum_pct,
            "strategy_name": str(strategy_name),
            "entry_mode": ("WAIT_FOR_PULLBACK" if decision.wait_duration_ms > 0
                           else decision.entry_action or "ENTER_NOW"),
            "direction_decision_ts": int(now_ms - max(0, decision.wait_duration_ms)),
            "direction_score": decision.direction_score,
            "yes_score": decision.yes_score,
            "no_score": decision.no_score,
            "confidence": decision.confidence,
            "direction_reason": decision.direction_reason,
            "expected_improvement": decision.expected_improvement,
            "actual_improvement": decision.actual_improvement,
            "wait_duration_ms": decision.wait_duration_ms,
            "missed_opportunity": decision.missed_opportunity,
            "chase_prevented": decision.chase_prevented,
            "final_entry_reason": decision.reject_reason,
            "entry_book_ts": evidence.get("book_ts"),
            "entry_book_received_ts": evidence.get("received_ts"),
            "entry_book_age_ms": evidence.get("age_ms"),
            "entry_book_hash": evidence.get("book_hash"),
            "entry_best_bid": evidence.get("best_bid"),
            "entry_best_ask": evidence.get("best_ask"),
            "entry_fill_shares": evidence.get("fill_shares"),
            "entry_ask_depth_shares": evidence.get("ask_depth_shares"),
            "entry_spread": evidence.get("spread"),
            "entry_fill_levels": json.dumps(levels, separators=(",", ":")),
            "entry_worst_price": worst_price,
            "execution_verified": True,
            "accounting_version": 2,
        }
        inserted = self.store.insert_trade(row)
        if isinstance(inserted, dict):
            return inserted
        if inserted is not None:
            row["id"] = inserted
        return row

    # Convenient naming for orchestrators; still performs only insert_trade.
    submit = open_trade
