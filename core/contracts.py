"""Shared contracts for poly_alpha_sniper.

Every mode (backtest, simulation, shadow_live, live_micro, live_full) and every
package imports its shared enums, dataclasses and protocols from this module.
This is the anti-drift layer required by the architecture rule: one set of
types, one set of decision vocabularies, no duplicate logic.

ASSUMPTIONS:
- All timestamps are epoch milliseconds (int) unless suffixed otherwise.
- All prices are floats in [0, 1] for Polymarket outcome tokens and raw USD
  floats for CEX prices.
- All sizes are USD notional unless the field name says "shares".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class TradingMode(str, Enum):
    SIMULATION = "simulation"
    SHADOW_LIVE = "shadow_live"
    LIVE_MICRO = "live_micro"
    LIVE_FULL = "live_full"

    @property
    def is_live(self) -> bool:
        return self in (TradingMode.LIVE_MICRO, TradingMode.LIVE_FULL)


class AggressionMode(str, Enum):
    DEFENSIVE = "DEFENSIVE"
    NORMAL = "NORMAL"
    AGGRESSIVE = "AGGRESSIVE"


class Tier(str, Enum):
    A_PLUS = "A_PLUS"
    A = "A"
    B = "B"
    C = "C"


class Decision(str, Enum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    WAIT = "WAIT"
    SHADOW_ONLY = "SHADOW_ONLY"


class OrderSide(str, Enum):
    BUY_YES = "BUY_YES"
    BUY_NO = "BUY_NO"
    SELL_YES = "SELL_YES"
    SELL_NO = "SELL_NO"

    @property
    def is_buy(self) -> bool:
        return self in (OrderSide.BUY_YES, OrderSide.BUY_NO)

    @property
    def is_sell(self) -> bool:
        return not self.is_buy

    @property
    def outcome(self) -> "Outcome":
        return Outcome.YES if self in (OrderSide.BUY_YES, OrderSide.SELL_YES) else Outcome.NO

    @property
    def closing_side(self) -> "OrderSide":
        """The SELL side that closes a position opened by this BUY side."""
        if self == OrderSide.BUY_YES:
            return OrderSide.SELL_YES
        if self == OrderSide.BUY_NO:
            return OrderSide.SELL_NO
        raise ValueError(f"{self} is not an opening side")


class Outcome(str, Enum):
    YES = "YES"
    NO = "NO"


class Direction(str, Enum):
    UP = "UP"
    DOWN = "DOWN"

    @property
    def opposite(self) -> "Direction":
        return Direction.DOWN if self == Direction.UP else Direction.UP


class MarketType(str, Enum):
    UP_DOWN = "UP_DOWN"          # "BTC up or down in next 5 minutes"
    THRESHOLD = "THRESHOLD"      # "BTC above 97,500 at 14:05 ET"
    UNKNOWN = "UNKNOWN"


class OrderState(str, Enum):
    CREATED = "CREATED"
    SIGNED = "SIGNED"
    SUBMITTED = "SUBMITTED"
    OPEN = "OPEN"
    PARTIAL_FILL = "PARTIAL_FILL"
    MATCHED = "MATCHED"
    MINED = "MINED"
    CONFIRMED = "CONFIRMED"
    RETRYING = "RETRYING"
    FAILED = "FAILED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    RECONCILED = "RECONCILED"

    @property
    def is_terminal(self) -> bool:
        return self in (OrderState.RECONCILED, OrderState.FAILED, OrderState.CANCELLED)


class ExitReason(str, Enum):
    EMERGENCY = "EMERGENCY"
    EXPIRY_RISK = "EXPIRY_RISK"
    STOP_LOSS = "STOP_LOSS"
    KILL_SWITCH = "KILL_SWITCH"
    EDGE_DECAY = "EDGE_DECAY"
    OPPOSITE_SIGNAL = "OPPOSITE_SIGNAL"
    PROFIT_LOCK = "PROFIT_LOCK"
    REBALANCE = "REBALANCE"
    TAKE_PROFIT = "TAKE_PROFIT"
    PARTIAL_TAKE_PROFIT = "PARTIAL_TAKE_PROFIT"
    MAX_HOLD = "MAX_HOLD"
    MOMENTUM_FADE = "MOMENTUM_FADE"
    ORDERBOOK_FLIP = "ORDERBOOK_FLIP"
    FAKEOUT_RISK = "FAKEOUT_RISK"
    DAILY_LOSS_RISK = "DAILY_LOSS_RISK"
    PANIC = "PANIC"
    MANUAL = "MANUAL"
    RESOLUTION = "RESOLUTION"


# Exit priority: lower number = executed first. Shared by live + backtest.
EXIT_PRIORITY: dict[ExitReason, int] = {
    ExitReason.EMERGENCY: 1,
    ExitReason.PANIC: 1,
    ExitReason.EXPIRY_RISK: 2,
    ExitReason.STOP_LOSS: 3,
    ExitReason.KILL_SWITCH: 4,
    ExitReason.DAILY_LOSS_RISK: 4,
    ExitReason.EDGE_DECAY: 5,
    ExitReason.OPPOSITE_SIGNAL: 6,
    ExitReason.ORDERBOOK_FLIP: 6,
    ExitReason.MOMENTUM_FADE: 6,
    ExitReason.FAKEOUT_RISK: 6,
    ExitReason.MAX_HOLD: 6,
    ExitReason.PROFIT_LOCK: 7,
    ExitReason.REBALANCE: 8,
    ExitReason.TAKE_PROFIT: 9,
    ExitReason.PARTIAL_TAKE_PROFIT: 9,
    ExitReason.RESOLUTION: 9,
    ExitReason.MANUAL: 2,
}


class RequestPriority(int, Enum):
    """Rate-limit governor priorities. Lower = more important."""
    EMERGENCY_EXIT = 1
    CANCEL = 2
    RECONCILE = 3
    NEW_ENTRY = 4
    DISCOVERY = 5
    ANALYTICS = 6


class RejectReason:
    """Canonical reject reason strings (used in DB rows, Telegram, dashboard)."""
    INVALID_TICK_SIZE = "REJECTED_INVALID_TICK_SIZE"
    INVALID_PRICE_PRECISION = "REJECTED_INVALID_PRICE_PRECISION"
    MIN_ORDER_SIZE_TOO_HIGH = "REJECTED_MIN_ORDER_SIZE_TOO_HIGH"
    SELL_SIZE_TOO_SMALL = "REJECTED_SELL_SIZE_TOO_SMALL"
    INSUFFICIENT_CASH = "REJECTED_INSUFFICIENT_CASH"
    INSUFFICIENT_SHARES = "REJECTED_INSUFFICIENT_SHARES"
    STALE_ORDERBOOK = "REJECTED_STALE_ORDERBOOK"
    STALE_CEX = "REJECTED_STALE_CEX"
    SPREAD_TOO_WIDE = "REJECTED_SPREAD_TOO_WIDE"
    SLIPPAGE_TOO_HIGH = "REJECTED_SLIPPAGE_TOO_HIGH"
    DAILY_LOSS_CAP = "REJECTED_DAILY_LOSS_CAP"
    MAX_EXPOSURE = "REJECTED_MAX_EXPOSURE"
    MAX_OPEN_POSITIONS = "REJECTED_MAX_OPEN_POSITIONS"
    LOSS_STREAK = "REJECTED_LOSS_STREAK"
    LOW_MARKET_QUALITY = "REJECTED_LOW_MARKET_QUALITY"
    LOW_TRADE_QUALITY = "REJECTED_LOW_TRADE_QUALITY"
    FAKEOUT_RISK = "REJECTED_FAKEOUT_RISK"
    PANIC_MODE = "REJECTED_PANIC_MODE"
    KILL_SWITCH = "REJECTED_KILL_SWITCH"
    TOKEN_MAPPING_UNCLEAR = "REJECTED_TOKEN_MAPPING_UNCLEAR"
    INCOMPLETE_TRADE_PACKET = "REJECTED_INCOMPLETE_TRADE_PACKET"
    AMBIGUOUS_MARKET = "REJECTED_AMBIGUOUS_MARKET"
    NO_BEST_BID_ASK = "REJECTED_NO_BEST_BID_ASK"
    LIQUIDITY_TOO_THIN = "REJECTED_LIQUIDITY_TOO_THIN"
    EXPIRY_WINDOW = "REJECTED_EXPIRY_WINDOW"
    EDGE_TOO_SMALL = "REJECTED_EDGE_TOO_SMALL"
    CONFIDENCE_TOO_LOW = "REJECTED_CONFIDENCE_TOO_LOW"
    COOLDOWN = "REJECTED_COOLDOWN"
    TRADE_FREQUENCY = "REJECTED_TRADE_FREQUENCY"
    EXCHANGE_DISAGREEMENT = "REJECTED_EXCHANGE_DISAGREEMENT"
    LIVE_GATES_NOT_PASSED = "REJECTED_LIVE_GATES_NOT_PASSED"
    ONE_POSITION_PER_MARKET = "REJECTED_ONE_POSITION_PER_MARKET"
    RECONCILIATION_MISMATCH = "REJECTED_RECONCILIATION_MISMATCH"


# ---------------------------------------------------------------------------
# CEX data
# ---------------------------------------------------------------------------

@dataclass
class CexTick:
    asset: str                      # "BTC" | "ETH" | "SOL"
    exchange: str                   # "binance" | "bybit" | "okx" | "replay"
    price: float
    ts_ms: int                      # exchange event time (epoch ms)
    recv_ts_ms: int = 0             # local receive time (epoch ms)
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[float] = None


@dataclass
class CexWindowStats:
    """Rolling window statistics for one asset on one (or blended) exchange."""
    asset: str
    exchange: str
    price: float
    ts_ms: int
    returns: dict[int, float] = field(default_factory=dict)   # window_sec -> pct return
    volatility: float = 0.0          # rolling realized vol (per-second stdev of returns)
    zscore: float = 0.0              # z-score of latest short-window return
    momentum: float = 0.0            # signed momentum score [-1, 1]
    impulse: float = 0.0             # impulse strength [0, 1]
    volume_burst: float = 0.0        # [0, 1] if volume data available
    fresh: bool = True
    staleness_ms: int = 0
    reconnect_recent: bool = False   # true shortly after a WS reconnect


@dataclass
class MultiCexView:
    """Blended view across exchanges for one asset."""
    asset: str
    primary: Optional[CexWindowStats] = None
    per_exchange: dict[str, CexWindowStats] = field(default_factory=dict)
    confirming_exchanges: int = 0
    direction_agreement: bool = True
    max_deviation_pct: float = 0.0
    any_stale: bool = False


# ---------------------------------------------------------------------------
# Polymarket orderbook
# ---------------------------------------------------------------------------

@dataclass
class BookLevel:
    price: float
    size: float                     # shares


@dataclass
class OrderbookSnapshot:
    token_id: str
    bids: list[BookLevel] = field(default_factory=list)   # sorted best (highest) first
    asks: list[BookLevel] = field(default_factory=list)   # sorted best (lowest) first
    ts_ms: int = 0
    source: str = "ws"              # "ws" | "rest" | "synthetic" | "replay"
    crossed: bool = False
    gap_suspected: bool = False

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    def depth_usd_at_ask(self, levels: int = 3) -> float:
        return sum(l.price * l.size for l in self.asks[:levels])

    def depth_usd_at_bid(self, levels: int = 3) -> float:
        return sum(l.price * l.size for l in self.bids[:levels])

    def is_stale(self, now_ms: int, max_staleness_ms: int) -> bool:
        return (now_ms - self.ts_ms) > max_staleness_ms


# ---------------------------------------------------------------------------
# Markets
# ---------------------------------------------------------------------------

@dataclass
class MarketInfo:
    market_id: str                  # Polymarket market id / slug
    condition_id: str = ""
    title: str = ""
    asset: str = ""                 # "BTC" | "ETH" | "SOL" | "" if unknown
    market_type: MarketType = MarketType.UNKNOWN
    threshold: Optional[float] = None
    direction_up_means_yes: bool = True   # YES token = "up"/"above" condition
    yes_token_id: str = ""
    no_token_id: str = ""
    expiry_ts_ms: int = 0
    tick_size: float = 0.01
    min_order_size_usd: float = 1.0
    neg_risk: bool = False
    active: bool = True
    closed: bool = False
    liquidity_usd: float = 0.0
    volume_24h_usd: float = 0.0
    mapping_confidence: float = 0.0   # 0-100 from token mapping validator
    parse_reject_reason: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    def seconds_to_expiry(self, now_ms: int) -> float:
        return (self.expiry_ts_ms - now_ms) / 1000.0

    def token_for(self, outcome: Outcome) -> str:
        return self.yes_token_id if outcome == Outcome.YES else self.no_token_id

    def side_for_direction(self, direction: Direction) -> OrderSide:
        """Deterministic direction -> opening order side mapping."""
        if direction == Direction.UP:
            return OrderSide.BUY_YES if self.direction_up_means_yes else OrderSide.BUY_NO
        return OrderSide.BUY_NO if self.direction_up_means_yes else OrderSide.BUY_YES


@dataclass
class ParsedMarket:
    """Deterministic parse result of a market title/description."""
    ok: bool
    asset: str = ""
    market_type: MarketType = MarketType.UNKNOWN
    direction_word: str = ""          # "up"/"down"/"above"/"below"/"higher"/"lower"
    threshold: Optional[float] = None
    expiry_hint: str = ""
    reject_reason: str = ""
    confidence: float = 0.0           # 0-100
    notes: list[str] = field(default_factory=list)


@dataclass
class TokenMappingResult:
    ok: bool
    confidence: float = 0.0           # 0-100
    direction_up_means_yes: bool = True
    reject_reason: str = ""
    checks: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Signals / model outputs
# ---------------------------------------------------------------------------

@dataclass
class Shock:
    asset: str
    direction: Direction
    ts_ms: int
    returns: dict[int, float] = field(default_factory=dict)
    zscore: float = 0.0
    impulse: float = 0.0
    momentum: float = 0.0
    volatility: float = 0.0
    confirming_exchanges: int = 1
    fakeout_risk: float = 0.0         # [0,1]
    reason: str = ""


@dataclass
class FairProbability:
    p_up: float
    p_down: float
    confidence: float                 # 0-100
    explanation: str = ""
    components: dict[str, float] = field(default_factory=dict)


@dataclass
class EdgeResult:
    side: OrderSide
    fair_probability: float
    market_price: float               # executable ask (buy) or bid (sell)
    raw_edge: float
    edge_after_spread: float
    edge_after_slippage: float
    confidence_adjusted_edge: float
    suggested_size_usd: float = 0.0


@dataclass
class MarketQualityResult:
    score: float                      # 0-100
    components: dict[str, float] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)


@dataclass
class GateResult:
    """Balanced alpha gate output — canonical decision object."""
    allow_trade: bool
    tier: Tier
    aggression_mode: AggressionMode
    decision: Decision
    score: float                      # 0-100
    hard_reject: bool = False
    soft_penalties: list[str] = field(default_factory=list)
    failed_checks: list[str] = field(default_factory=list)
    reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "allow_trade": self.allow_trade,
            "tier": self.tier.value,
            "aggression_mode": self.aggression_mode.value,
            "decision": self.decision.value,
            "score": self.score,
            "hard_reject": self.hard_reject,
            "soft_penalties": list(self.soft_penalties),
            "failed_checks": list(self.failed_checks),
            "reason": self.reason,
        }


@dataclass
class Signal:
    """A fully-formed trade opportunity, pre-risk."""
    signal_id: str
    ts_ms: int
    asset: str
    market: MarketInfo
    side: OrderSide
    direction: Direction
    shock: Optional[Shock]
    fair: FairProbability
    edge: EdgeResult
    market_quality: MarketQualityResult
    gate: Optional[GateResult] = None
    trade_quality: float = 0.0
    alpha_score: float = 0.0
    tier: Tier = Tier.C
    exit_plan: str = ""
    seconds_to_expiry: float = 0.0


@dataclass
class RiskDecision:
    approved: bool
    size_usd: float = 0.0
    reject_reason: str = ""
    checks: list[str] = field(default_factory=list)
    # Populated for REJECTED_MIN_ORDER_SIZE_TOO_HIGH so the true blocker (Polymarket's
    # share minimum vs. small-bankroll sizing) can be reported instead of implying
    # edge/confidence needs to improve. Keys: min_shares, ask_price, min_required_usd,
    # configured_max_trade_usd, proposed_usd, available_cash_usd, shortfall_usd.
    sizing_detail: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Orders / positions
# ---------------------------------------------------------------------------

@dataclass
class OrderRequest:
    order_id: str                     # local id (uuid); exchange id filled later
    token_id: str
    market_id: str
    side: OrderSide
    price: float
    size_shares: float
    size_usd: float
    tif_ms: int = 800                 # cancel-if-not-filled window
    priority: RequestPriority = RequestPriority.NEW_ENTRY
    reason: str = ""
    tier: Tier = Tier.C
    signal_id: str = ""
    exit_reason: Optional[ExitReason] = None


@dataclass
class OrderRecord:
    order_id: str
    exchange_order_id: str = ""
    token_id: str = ""
    market_id: str = ""
    side: OrderSide = OrderSide.BUY_YES
    price: float = 0.0
    size_shares: float = 0.0
    size_usd: float = 0.0
    state: OrderState = OrderState.CREATED
    filled_shares: float = 0.0
    avg_fill_price: float = 0.0
    created_ts_ms: int = 0
    updated_ts_ms: int = 0
    error: str = ""
    mode: str = ""                    # TradingMode value at submit time
    tier: str = ""
    signal_id: str = ""
    exit_reason: str = ""


@dataclass
class FillRecord:
    order_id: str
    token_id: str
    market_id: str
    side: OrderSide
    price: float
    size_shares: float
    ts_ms: int
    fee_usd: float = 0.0
    liquidity: str = "taker"


@dataclass
class Position:
    token_id: str
    market_id: str
    outcome: Outcome
    shares: float = 0.0
    avg_entry_price: float = 0.0
    realized_pnl: float = 0.0
    entry_ts_ms: int = 0
    last_update_ms: int = 0
    tier: Tier = Tier.C
    entry_signal_id: str = ""
    exit_plan: str = ""
    current_bid: Optional[float] = None
    current_ask: Optional[float] = None

    @property
    def cost_usd(self) -> float:
        return self.shares * self.avg_entry_price

    @property
    def current_mid(self) -> Optional[float]:
        if self.current_bid is None or self.current_ask is None:
            return None
        return (self.current_bid + self.current_ask) / 2.0

    def unrealized_pnl(self, mark: Optional[float] = None) -> float:
        m = mark if mark is not None else (self.current_bid or 0.0)
        return self.shares * (m - self.avg_entry_price)


@dataclass
class ExitDecision:
    should_exit: bool
    reason: Optional[ExitReason] = None
    priority: int = 99
    size_fraction: float = 1.0        # 1.0 = full close
    detail: str = ""

    @classmethod
    def none(cls) -> "ExitDecision":
        return cls(should_exit=False)

    @classmethod
    def full(cls, reason: ExitReason, detail: str = "") -> "ExitDecision":
        return cls(True, reason, EXIT_PRIORITY.get(reason, 99), 1.0, detail)

    @classmethod
    def partial(cls, reason: ExitReason, fraction: float, detail: str = "") -> "ExitDecision":
        return cls(True, reason, EXIT_PRIORITY.get(reason, 99), fraction, detail)


@dataclass
class PortfolioSnapshot:
    """Point-in-time portfolio state passed to risk/sizing/validation.

    Same shape in backtest, simulation, shadow and live — live fills it from
    reconciled account state, backtest fills it from the simulated ledger.
    """
    equity_usd: float                 # starting_bankroll + realized_pnl (realized only)
    available_cash_usd: float
    realized_pnl_usd: float = 0.0
    unrealized_pnl_usd: float = 0.0
    realized_pnl_today_usd: float = 0.0
    open_positions: int = 0
    total_exposure_usd: float = 0.0
    exposure_by_market: dict[str, float] = field(default_factory=dict)
    consecutive_losses: int = 0
    positions_by_market: dict[str, str] = field(default_factory=dict)  # market_id -> outcome held
    equity_ath_usd: float = 0.0
    trades_today: int = 0


# ---------------------------------------------------------------------------
# Records (DB row shapes used across modes)
# ---------------------------------------------------------------------------

@dataclass
class PredictionRecord:
    ts_ms: int
    asset: str
    market_id: str
    market_title: str
    direction: str
    cex_price: float
    return_1s: float = 0.0
    return_2s: float = 0.0
    return_3s: float = 0.0
    return_5s: float = 0.0
    return_10s: float = 0.0
    return_15s: float = 0.0
    return_30s: float = 0.0
    volatility: float = 0.0
    zscore: float = 0.0
    polymarket_price: float = 0.0
    fair_probability: float = 0.0
    edge: float = 0.0
    edge_after_spread: float = 0.0
    edge_after_slippage: float = 0.0
    confidence: float = 0.0
    market_quality: float = 0.0
    trade_quality: float = 0.0
    alpha_score: float = 0.0
    tier: str = ""
    aggression_mode: str = ""
    decision: str = ""
    reject_reason: str = ""
    mode: str = ""
    resolved_outcome: str = ""        # filled later: "WIN"|"LOSS"|"" for calibration


# ---------------------------------------------------------------------------
# Protocols (structural interfaces)
# ---------------------------------------------------------------------------

@runtime_checkable
class ClobTradingClient(Protocol):
    """Interface every order-capable client (live or simulated) implements.

    The live implementation wraps the official Polymarket CLOB client and is
    only constructed after all live gates pass. The simulator implements the
    same protocol so entry/exit logic is mode-agnostic.
    """

    async def place_order(self, req: OrderRequest) -> OrderRecord: ...
    async def cancel_order(self, order_id: str) -> bool: ...
    async def cancel_all(self) -> int: ...
    async def get_open_orders(self) -> list[OrderRecord]: ...
    async def get_balance_usd(self) -> float: ...
    async def get_positions(self) -> list[Position]: ...


@runtime_checkable
class Store(Protocol):
    """Minimal persistence interface. Implemented by sqlite_store/postgres_store."""

    def insert(self, table: str, row: dict[str, Any]) -> None: ...
    def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]: ...
    def execute(self, sql: str, params: tuple = ()) -> None: ...


# ---------------------------------------------------------------------------
# Small shared helpers (kept here so all modes use identical math)
# ---------------------------------------------------------------------------

def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def round_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return price
    ticks = round(price / tick_size)
    return round(ticks * tick_size, 6)


def is_valid_tick(price: float, tick_size: float, eps: float = 1e-9) -> bool:
    if tick_size <= 0:
        return False
    ratio = price / tick_size
    return abs(ratio - round(ratio)) < 1e-6 and 0.0 < price < 1.0
