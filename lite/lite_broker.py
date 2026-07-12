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
from .lite_strategy import LiteDecision, MODEL_VERSION


STRATEGY_NAME = "lite_fair_value_edge_v3"


class LiteBroker:
    def __init__(self, store, fee_rate: float = CRYPTO_TAKER_FEE_RATE,
                 fee_buffer_usd: float = 0.0, book_max_age_ms: int = 8_000,
                 max_spread: float = 0.20):
        self.store = store
        self.fee_rate = float(fee_rate)
        self.fee_buffer_usd = max(0.0, float(fee_buffer_usd))
        self.book_max_age_ms = int(book_max_age_ms)
        self.max_spread = float(max_spread)

    @staticmethod
    def _validated_strategy_evidence(decision: LiteDecision) -> None:
        """Reject incoherent edge telemetry or any claimed maker execution."""
        try:
            if str(decision.model_version) != MODEL_VERSION:
                raise ValueError("model")
            fair_yes = float(decision.fair_probability_yes)
            fair_no = float(decision.fair_probability_no)
            market_yes = float(decision.market_probability_yes)
            yes_price = float(decision.executable_yes_price)
            no_price = float(decision.executable_no_price)
            yes_fee = float(decision.estimated_yes_fee)
            no_fee = float(decision.estimated_no_fee)
            yes_edge = float(decision.net_edge_yes)
            no_edge = float(decision.net_edge_no)
            selected_edge = float(decision.selected_net_edge)
            values = (
                fair_yes, fair_no, market_yes, yes_price, no_price,
                yes_fee, no_fee, yes_edge, no_edge, selected_edge,
                float(decision.execution_buffer_yes),
                float(decision.execution_buffer_no),
                float(decision.uncertainty_buffer),
            )
            if not all(math.isfinite(value) for value in values):
                raise ValueError("finite")
            if (not 0 <= fair_yes <= 1 or not 0 <= fair_no <= 1
                    or abs((fair_yes+fair_no)-1.0) > 1e-9
                    or not 0 <= market_yes <= 1
                    or not 0 < yes_price < 1 or not 0 < no_price < 1
                    or yes_fee < 0 or no_fee < 0
                    or decision.execution_buffer_yes < 0
                    or decision.execution_buffer_no < 0
                    or decision.uncertainty_buffer < 0):
                raise ValueError("bounds")
            expected_edge = yes_edge if decision.side == "BUY_YES" else no_edge
            if (decision.side not in ("BUY_YES", "BUY_NO")
                    or expected_edge <= 0 or abs(selected_edge-expected_edge) > 1e-9
                    or selected_edge + 1e-9 < max(yes_edge, no_edge)):
                raise ValueError("selected edge")
            if str(decision.execution_state) != "CROSS_SPREAD":
                raise ValueError("execution state")
            if bool(decision.maker_fill_assumed):
                raise ValueError("maker fill")
            if str(decision.maker_fill_model) != "observational_no_fill_claim":
                raise ValueError("maker model")
            if int(decision.maker_wait_ms) < 0:
                raise ValueError("maker wait")
            if decision.pullback_start_ts is not None or str(
                    decision.pullback_condition or ""):
                raise ValueError("false pullback")
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("Lite entry lacks coherent fair-value edge evidence") from exc

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
                   strategy_name: str = STRATEGY_NAME,
                   current_commit: str = "UNKNOWN") -> dict:
        """Insert one simulated OPEN trade and return its persisted row."""
        if not decision.accepted:
            raise ValueError("cannot open a rejected Lite decision")
        if decision.side not in ("BUY_YES", "BUY_NO"):
            raise ValueError("Lite entry side must be BUY_YES or BUY_NO")
        self._validated_strategy_evidence(decision)
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
        selected_price = (float(decision.executable_yes_price)
                          if decision.side == "BUY_YES"
                          else float(decision.executable_no_price))
        selected_fee = (float(decision.estimated_yes_fee)
                        if decision.side == "BUY_YES"
                        else float(decision.estimated_no_fee))
        if (abs(selected_price-entry_price) > 1e-9
                or abs(selected_fee-float(decision.entry_fee)) > 1e-9):
            raise ValueError("Lite selected edge is not bound to the entry sweep")
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
            "entry_mode": ("CROSS_SPREAD_AFTER_MAKER_WAIT"
                           if decision.maker_wait_ms > 0 else "CROSS_SPREAD"),
            "direction_decision_ts": int(decision.maker_start_ts or now_ms),
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
            "runtime_commit": str(current_commit or "UNKNOWN"),
            "model_version": decision.model_version,
            "return_5s": decision.return_5s,
            "acceleration": decision.acceleration,
            "window_return": decision.window_return,
            "reliability": decision.reliability,
            "cex_adjustment": decision.cex_adjustment,
            "lead_lag_adjustment": decision.lead_lag_adjustment,
            "market_probability_yes": decision.market_probability_yes,
            "fair_probability_yes": decision.fair_probability_yes,
            "fair_probability_no": decision.fair_probability_no,
            "calibration_bucket": decision.calibration_bucket,
            "executable_yes_price": decision.executable_yes_price,
            "executable_no_price": decision.executable_no_price,
            "estimated_yes_fee": decision.estimated_yes_fee,
            "estimated_no_fee": decision.estimated_no_fee,
            "execution_buffer_yes": decision.execution_buffer_yes,
            "execution_buffer_no": decision.execution_buffer_no,
            "uncertainty_buffer": decision.uncertainty_buffer,
            "net_edge_yes": decision.net_edge_yes,
            "net_edge_no": decision.net_edge_no,
            "selected_net_edge": decision.selected_net_edge,
            "edge_bucket": decision.edge_bucket,
            "lead_lag_status": decision.lead_lag_status,
            "cex_move_ts": decision.cex_move_ts,
            "poly_book_ts": decision.poly_book_ts,
            "lead_lag_ms": decision.lead_lag_ms,
            "poly_response": decision.poly_response,
            "execution_state": decision.execution_state,
            "maker_price": decision.maker_price,
            "maker_start_ts": decision.maker_start_ts,
            "maker_deadline_ts": decision.maker_deadline_ts,
            "maker_wait_ms": decision.maker_wait_ms,
            "maker_fill_assumed": False,
            "maker_fill_model": decision.maker_fill_model,
            "lock_ts": int(decision.maker_start_ts or now_ms),
            "pullback_start_ts": decision.pullback_start_ts,
            "pullback_condition": decision.pullback_condition or None,
            "wait_deadline_ts": decision.maker_deadline_ts,
        }
        inserted = self.store.insert_trade(row)
        if isinstance(inserted, dict):
            return inserted
        if inserted is not None:
            row["id"] = inserted
        return row

    # Convenient naming for orchestrators; still performs only insert_trade.
    submit = open_trade
