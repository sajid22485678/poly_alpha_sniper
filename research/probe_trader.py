"""RESEARCH / SHADOW-ONLY: EXPERIMENTAL_PROBE_TRADING lane. Never places
orders, never affects the baseline decision, baseline bankroll, or live
readiness. Simulated probe positions only.

Probes open MORE OFTEN than baseline because they do NOT require the strict
shock / FIRED / imbalance triggers -- direction comes from CEX price vs the
price_to_beat anchor. What they can NEVER skip (hard gates, shared with the
challenger engine): exact-window price_to_beat, CEX <= 8s, valid market and
time-to-close band, token mapping, executable book, catastrophic spread/
depth, simulated cash. Anchor problems are recorded with their PRECISE reason
(missing_anchor / anchor_upstream_not_published / anchor_hydration_failed /
anchor_schema_unknown / anchor_wrong_window) -- never collapsed into a
generic trigger reject.

Exits are honest: a probe exits at the REAL recorded book (YES exits at bid;
NO at the 1-ask complement proxy, documented) shortly before window close.
If the market rolls over before an exit book is seen, the probe is marked
UNRESOLVED with pnl=None -- outcomes are never fabricated.

Sizing: fixed 5 shares from a SEPARATE simulated probe bankroll. No
compounding, no martingale, no averaging down.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable, Optional

from poly_alpha_sniper.research.challenger_engine import hard_safety_gates

PROBE_VERSION = "probe_v1"
FIXED_SHARES = 5.0
EXIT_BEFORE_CLOSE_S = 25.0        # exit at the last reliably-booked moment
STRATEGIES = ("cex_direction_probe", "anchor_distance_probe", "combined_probe")

# anchor-precise reject reasons (never generic)
_ANCHOR_REASON_MAP = {
    "UPSTREAM_NOT_PUBLISHED": "anchor_upstream_not_published",
    "HYDRATION_FAILED": "anchor_hydration_failed",
    "SCHEMA_UNKNOWN": "anchor_schema_unknown",
    "EVENT_NOT_FOUND": "missing_anchor",
    "": "missing_anchor",
}


@dataclass
class ProbePosition:
    probe_id: str
    strategy: str
    asset: str
    market_id: str
    slug: str
    side: str                    # "BUY_YES" | "BUY_NO"
    shares: float
    entry_ts_ms: int
    entry_price: float
    anchor_ptb: float
    cex_price_at_entry: float
    window_close_ts_ms: Optional[int]


class ProbeTrader:
    """Stateful simulated probe trader. cfg is ResearchProbeTradingConfig."""

    def __init__(self, cfg, starting_bankroll_usd: float = 10.0):
        self.cfg = cfg
        self.bankroll = starting_bankroll_usd
        self.open_positions: dict[str, ProbePosition] = {}   # probe_id -> pos
        self._entered_keys: set[str] = set()                  # market|side dedup
        self.last_reject: dict = {}

    # ------------------------------------------------------------------
    def _reject(self, strategy: str, reason: str, anchor_reason: str = "") -> dict:
        row = {"strategy": strategy, "reject_reason": reason,
               "anchor_missing_reason": anchor_reason}
        self.last_reject = row
        return row

    def _direction(self, f: dict) -> tuple[Optional[str], float]:
        """Side from CEX fair price vs price_to_beat. Returns (side, distance_pct)."""
        cex, ptb = f.get("cex_price"), f.get("price_to_beat")
        if not cex or not ptb or ptb <= 0:
            return None, 0.0
        dist = (cex - ptb) / ptb
        if dist > 0:
            return "BUY_YES", abs(dist)
        if dist < 0:
            return "BUY_NO", abs(dist)
        return None, 0.0

    def _entry_price(self, f: dict, side: str) -> Optional[float]:
        """YES buys at the recorded ask; NO buys at the 1-bid complement of the
        YES book (documented executable proxy -- we track the YES book)."""
        bid, ask = f.get("book_bid"), f.get("book_ask")
        if bid is None or ask is None:
            return None
        price = ask if side == "BUY_YES" else round(1.0 - bid, 4)
        return price if 0 < price < 1 else None

    # ------------------------------------------------------------------
    def on_scan(self, f: dict, now_ms: int,
                insert: Callable[[str, dict], None]) -> None:
        """Called once per throttled scan with the baseline feature row.
        Manages exits first (positions age out), then entry evaluation."""
        self._manage_exits(f, now_ms, insert)
        if not self.cfg.enabled:
            return
        self._evaluate_entries(f, now_ms, insert)

    # ------------------------------------------------------------------
    def _evaluate_entries(self, f: dict, now_ms: int,
                          insert: Callable[[str, dict], None]) -> None:
        # anchor first -- precise reject reasons, never generic
        if f.get("price_to_beat") is None:
            reason = _ANCHOR_REASON_MAP.get(
                str(f.get("anchor_missing_reason") or ""), "missing_anchor")
            self._reject("all", reason, str(f.get("anchor_missing_reason") or ""))
            return
        ttc = f.get("time_to_close_s")
        if ttc is None or not (self.cfg.time_to_close_min_s <= ttc <= self.cfg.time_to_close_max_s):
            self._reject("all", "anchor_wrong_window" if ttc is None else "outside_time_window")
            return
        gates_ok, gate_failure, _g = hard_safety_gates(f, self.bankroll)
        if not gates_ok:
            self._reject("all", gate_failure)
            return

        side, dist = self._direction(f)
        if side is None:
            self._reject("all", "no_clear_cex_direction")
            return

        bands = self.cfg.min_anchor_distance_pct
        candidates = []
        if self.cfg.strategies.get("cex_direction_probe") and dist >= bands["very_loose"]:
            candidates.append("cex_direction_probe")
        if self.cfg.strategies.get("anchor_distance_probe") and dist >= bands["normal"]:
            candidates.append("anchor_distance_probe")
        if self.cfg.strategies.get("combined_probe") and dist >= bands["loose"]:
            candidates.append("combined_probe")
        if not candidates:
            self._reject("all", "anchor_distance_below_band")
            return

        for strategy in candidates:
            if len(self.open_positions) >= self.cfg.max_open_positions:
                self._reject(strategy, "max_open_positions")
                return
            dedup = f"{f.get('market_id')}|{side}"
            if dedup in self._entered_keys:
                self._reject(strategy, "duplicate_probe_position")
                continue
            price = self._entry_price(f, side)
            if price is None:
                self._reject(strategy, "no_executable_probe_price")
                continue
            cost = FIXED_SHARES * price
            if cost > self.bankroll:
                self._reject(strategy, "insufficient_probe_cash")
                return
            probe_id = f"probe-{f.get('market_id')}-{strategy}-{now_ms}"
            close_ts = (now_ms + int(ttc * 1000)) if ttc is not None else None
            pos = ProbePosition(
                probe_id=probe_id, strategy=strategy, asset=str(f.get("asset")),
                market_id=str(f.get("market_id")), slug=str(f.get("market_slug") or ""),
                side=side, shares=FIXED_SHARES, entry_ts_ms=now_ms,
                entry_price=price, anchor_ptb=float(f["price_to_beat"]),
                cex_price_at_entry=float(f.get("cex_price") or 0.0),
                window_close_ts_ms=close_ts)
            self.open_positions[probe_id] = pos
            self._entered_keys.add(dedup)
            self.bankroll -= cost
            insert("experimental_probe_trades", {
                "ts_ms": now_ms, "probe_id": probe_id, "event": "ENTRY",
                "strategy": strategy, "asset": pos.asset, "market_id": pos.market_id,
                "slug": pos.slug, "side": side, "shares": FIXED_SHARES,
                "price": price, "status": "OPEN", "pnl_usd": None, "hold_s": None,
                "reason": f"cex vs anchor distance {dist:.5f}",
                "anchor_ptb": pos.anchor_ptb, "cex_price": pos.cex_price_at_entry,
                "probe_version": PROBE_VERSION,
                "extra": json.dumps({"bankroll_after": round(self.bankroll, 4)})})
            if self.cfg.max_one_per_asset_window:
                return  # at most one new probe per asset per scan batch

    # ------------------------------------------------------------------
    def _manage_exits(self, f: dict, now_ms: int,
                      insert: Callable[[str, dict], None]) -> None:
        for probe_id, pos in list(self.open_positions.items()):
            same_market = str(f.get("market_id")) == pos.market_id
            ttc = f.get("time_to_close_s")
            bid, ask = f.get("book_bid"), f.get("book_ask")
            if same_market and ttc is not None and ttc <= EXIT_BEFORE_CLOSE_S \
                    and bid is not None and ask is not None:
                # honest book exit: YES sells at bid; NO exits at 1-ask proxy
                exit_price = bid if pos.side == "BUY_YES" else round(1.0 - ask, 4)
                pnl = round((exit_price - pos.entry_price) * pos.shares, 4)
                self.bankroll += pos.shares * exit_price
                self._close(pos, now_ms, insert, "PRE_CLOSE_BOOK_EXIT",
                            "CLOSED", exit_price, pnl)
            elif pos.window_close_ts_ms is not None and now_ms > pos.window_close_ts_ms + 60_000:
                # market rolled over without an exit book: never fabricate.
                # Bankroll releases the entry cost back as an accounting no-op?
                # NO -- honest: the stake stays spent until a real outcome is
                # known; UNRESOLVED positions carry pnl=None.
                self._close(pos, now_ms, insert, "NO_FINAL_BOOK_OBSERVED",
                            "UNRESOLVED", None, None)

    def _close(self, pos: ProbePosition, now_ms: int,
               insert: Callable[[str, dict], None], reason: str, status: str,
               exit_price: Optional[float], pnl: Optional[float]) -> None:
        self.open_positions.pop(pos.probe_id, None)
        insert("experimental_probe_trades", {
            "ts_ms": now_ms, "probe_id": pos.probe_id, "event": "EXIT",
            "strategy": pos.strategy, "asset": pos.asset, "market_id": pos.market_id,
            "slug": pos.slug, "side": pos.side, "shares": pos.shares,
            "price": exit_price, "status": status, "pnl_usd": pnl,
            "hold_s": round((now_ms - pos.entry_ts_ms) / 1000, 1),
            "reason": reason, "anchor_ptb": pos.anchor_ptb,
            "cex_price": pos.cex_price_at_entry, "probe_version": PROBE_VERSION,
            "extra": json.dumps({"bankroll_after": round(self.bankroll, 4)})})
