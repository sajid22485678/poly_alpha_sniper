"""Emergency fix: bounded retry + conservative shadow-only reconciliation for
positions whose exit path is permanently stuck (no book / every exit attempt
fails), e.g. because the market expired/resolved and Polymarket's CLOB no
longer serves an orderbook for the token.

Covers:
- portfolio/position_reconciliation.py (pure reconciliation function)
- data/orderbook_mirror.py::untrack_token (new method)
- core/app.py::_manage_exits / _reconcile_stuck_exit / _execute_exit
  (bounded retry wiring, panic re-activation guard)
"""
from __future__ import annotations

import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import Secrets, load_config
from poly_alpha_sniper.core.contracts import (
    ExitDecision, ExitReason, FillRecord, Outcome, OrderSide,
)
from poly_alpha_sniper.data.orderbook_mirror import OrderbookMirror
from poly_alpha_sniper.data.orderbook_state import OrderbookStore
from poly_alpha_sniper.portfolio.position_reconciliation import (
    DEFAULT_STUCK_EXIT_CUTOFF_MS, RECONCILED_EXIT_NO_BOOK_TERMINAL,
    reconcile_unexitable_position,
)
from poly_alpha_sniper.portfolio.positions import Portfolio
from poly_alpha_sniper.storage.migrations import run_migrations
from poly_alpha_sniper.storage.sqlite_store import SqliteStore
from poly_alpha_sniper.tests.helpers import NOW_MS, book, cfg

TOKEN = "stuck-token"
MARKET_ID = "m-stuck"


def _store(tmp_path):
    s = SqliteStore(str(tmp_path / "test.db"))
    run_migrations(s)
    return s


def _portfolio_with_open_position(shares=100.0, price=0.01, ts=NOW_MS):
    pf = Portfolio(cfg(), SimClock(NOW_MS))
    pf.apply_fill(FillRecord(order_id="o1", token_id=TOKEN, market_id=MARKET_ID,
                             side=OrderSide.BUY_NO, price=price, size_shares=shares,
                             ts_ms=ts))
    return pf


# ---------------------------------------------------------------------------
# portfolio/position_reconciliation.py -- pure function
# ---------------------------------------------------------------------------

def test_reconciles_open_position_as_conservative_total_loss(tmp_path):
    s = _store(tmp_path)
    pf = _portfolio_with_open_position(shares=100.0, price=0.01)
    assert pf.get(TOKEN) is not None

    record = reconcile_unexitable_position(pf, pf.get(TOKEN), "book permanently unavailable",
                                           NOW_MS + 1000, s.insert)

    assert record is not None
    assert record["reason"] == RECONCILED_EXIT_NO_BOOK_TERMINAL
    assert record["pnl_usd"] == pytest.approx(-1.0)  # 100 shares * $0.01 cost, fully lost
    assert pf.get(TOKEN) is None  # position removed
    s.close()


def test_never_reports_a_profit():
    """Conservative reconciliation must never show pnl_usd > 0 -- that would
    be exactly the fabricated-profit outcome the emergency fix must avoid."""
    for price in (0.01, 0.30, 0.99):
        pf = _portfolio_with_open_position(shares=10.0, price=price)
        record = reconcile_unexitable_position(
            pf, pf.get(TOKEN), "test", NOW_MS + 1000, lambda *a, **kw: None)
        assert record["pnl_usd"] <= 0


def test_detail_states_not_a_confirmed_resolution(tmp_path):
    s = _store(tmp_path)
    pf = _portfolio_with_open_position()
    record = reconcile_unexitable_position(pf, pf.get(TOKEN), "book gone", NOW_MS, s.insert)
    assert "NOT a confirmed market resolution" in record["detail"]
    assert "CONSERVATIVE" in record["detail"]
    s.close()


def test_writes_exits_row_with_exact_schema_fields(tmp_path):
    s = _store(tmp_path)
    pf = _portfolio_with_open_position()
    reconcile_unexitable_position(pf, pf.get(TOKEN), "book gone", NOW_MS, s.insert)
    rows = s.query("SELECT * FROM exits WHERE token_id=?", (TOKEN,))
    assert len(rows) == 1
    row = rows[0]
    assert row["reason"] == RECONCILED_EXIT_NO_BOOK_TERMINAL
    assert row["market_id"] == MARKET_ID
    assert row["price"] == 0.0
    s.close()


def test_writes_incident_report_for_human_review(tmp_path):
    s = _store(tmp_path)
    pf = _portfolio_with_open_position()
    reconcile_unexitable_position(pf, pf.get(TOKEN), "book gone", NOW_MS, s.insert)
    reports = s.query("SELECT * FROM incident_reports WHERE kind='position_reconciled_no_book'")
    assert len(reports) == 1
    assert "human review" in reports[0]["detail"].lower()
    assert reports[0]["severity"] == "critical"
    s.close()


def test_returns_none_and_writes_nothing_when_position_already_gone(tmp_path):
    s = _store(tmp_path)
    pf = _portfolio_with_open_position()
    reconcile_unexitable_position(pf, pf.get(TOKEN), "first", NOW_MS, s.insert)  # closes it
    fake_pos = type("P", (), {"token_id": TOKEN, "market_id": MARKET_ID,
                              "shares": 100.0, "entry_ts_ms": NOW_MS})()

    record = reconcile_unexitable_position(pf, fake_pos, "second", NOW_MS + 1, s.insert)

    assert record is None
    assert len(s.query("SELECT * FROM exits WHERE token_id=?", (TOKEN,))) == 1  # not duplicated
    s.close()


def test_never_deletes_rows_only_inserts(tmp_path):
    s = _store(tmp_path)
    pf = _portfolio_with_open_position()
    before = len(s.query("SELECT * FROM exits"))
    reconcile_unexitable_position(pf, pf.get(TOKEN), "book gone", NOW_MS, s.insert)
    after = len(s.query("SELECT * FROM exits"))
    assert after == before + 1
    s.close()


# ---------------------------------------------------------------------------
# data/orderbook_mirror.py::untrack_token
# ---------------------------------------------------------------------------

async def test_untrack_token_stops_a_single_token_without_full_market():
    clock = SimClock(NOW_MS)
    store = OrderbookStore(cfg(), clock)
    mirror = OrderbookMirror(cfg(), clock, ws=None, rest=None, store=store)
    mirror._tracked.update({"tok_yes", "tok_no"})

    mirror.untrack_token("tok_yes")

    assert "tok_yes" not in mirror._tracked
    assert "tok_no" in mirror._tracked  # only the targeted token is removed


async def test_untrack_token_is_idempotent():
    clock = SimClock(NOW_MS)
    store = OrderbookStore(cfg(), clock)
    mirror = OrderbookMirror(cfg(), clock, ws=None, rest=None, store=store)
    mirror.untrack_token("never-tracked")  # must not raise
    mirror.untrack_token("never-tracked")


# ---------------------------------------------------------------------------
# core/app.py -- bounded retry wiring
# ---------------------------------------------------------------------------

@pytest.fixture()
def app(tmp_path):
    from poly_alpha_sniper.core.app import App
    c = load_config()
    c.telegram.enabled = False
    c.dashboard.auth_enabled = False
    db = (tmp_path / "reconcile.db").as_posix()
    a = App(c, Secrets(env={"DASHBOARD_AUTH_ENABLED": "false",
                            "DATABASE_URL": f"sqlite:///{db}"}))
    a.build()
    yield a
    a.store.close()


def _open_position(app_obj, token=TOKEN, mkt=MARKET_ID, shares=100.0, price=0.01):
    app_obj.portfolio.apply_fill(FillRecord(
        order_id="o1", token_id=token, market_id=mkt, side=OrderSide.BUY_NO,
        price=price, size_shares=shares, ts_ms=app_obj.clock.now_ms()))
    return app_obj.portfolio.get(token)


def _force_exit_decision(app_obj, pos):
    app_obj.sell_signal_engine.evaluate_all = (
        lambda *a, **kw: [(pos, ExitDecision.full(ExitReason.PANIC, "test"))])


async def test_stuck_position_not_reconciled_before_cutoff(app):
    pos = _open_position(app)
    _force_exit_decision(app, pos)
    now = app.clock.now_ms()
    app._exit_stuck_since_ms[TOKEN] = now - 1000  # well under the 5-min cutoff

    await app._manage_exits()

    assert app.portfolio.get(TOKEN) is not None  # still open, not reconciled
    assert not app.store.query(
        "SELECT * FROM exits WHERE reason=?", (RECONCILED_EXIT_NO_BOOK_TERMINAL,))


async def test_stuck_position_reconciled_after_cutoff(app):
    pos = _open_position(app)
    _force_exit_decision(app, pos)
    now = app.clock.now_ms()
    app._exit_stuck_since_ms[TOKEN] = now - DEFAULT_STUCK_EXIT_CUTOFF_MS - 1000

    await app._manage_exits()

    assert app.portfolio.get(TOKEN) is None  # closed
    rows = app.store.query("SELECT * FROM exits WHERE reason=?",
                           (RECONCILED_EXIT_NO_BOOK_TERMINAL,))
    assert len(rows) == 1
    assert TOKEN not in app._exit_stuck_since_ms  # tracking cleared


async def test_reconciliation_never_clears_panic(app):
    pos = _open_position(app)
    _force_exit_decision(app, pos)
    app.panic.activate("emergency_exit_failed:pre-existing")
    assert app.panic.is_active
    now = app.clock.now_ms()
    app._exit_stuck_since_ms[TOKEN] = now - DEFAULT_STUCK_EXIT_CUTOFF_MS - 1000

    await app._manage_exits()

    assert app.panic.is_active  # still active -- reconciliation must never clear it


async def test_position_closed_normally_clears_stuck_tracking(app):
    """A position that closes via a real fill (not reconciliation) must stop
    being tracked as 'stuck' -- covers the case where the book comes back."""
    pos = _open_position(app)
    now = app.clock.now_ms()
    app._exit_stuck_since_ms[TOKEN] = now - 1000
    # simulate the position closing by any means (e.g. a real fill)
    app.portfolio._positions.pop(TOKEN, None)

    await app._manage_exits()

    assert TOKEN not in app._exit_stuck_since_ms


async def test_repeated_exit_no_book_does_not_loop_forever_or_reconcile_early(app):
    """Calling _execute_exit many times with no book must not itself trigger
    reconciliation (that's _manage_exits' job, gated on elapsed time) and
    must not error -- it should just keep logging until the cutoff."""
    pos = _open_position(app)
    decision = ExitDecision.full(ExitReason.PANIC, "test")
    for _ in range(50):
        await app._execute_exit(pos, decision)
    assert app.portfolio.get(TOKEN) is not None  # never force-closed by _execute_exit itself


async def test_panic_not_reactivated_every_cycle_for_same_token(app):
    """Repeated exit failures for the SAME token must only activate panic
    once, not spam panic.activate()/triggers on every single cycle (this is
    what produced ~1 panic_activated log line per 100ms in the incident)."""
    pos = _open_position(app)
    app.book_store.update_snapshot(book(TOKEN))  # book must exist to reach sell_executor

    class RaisingSellExecutor:
        async def execute_exit(self, *a, **kw):
            raise ValueError("cannot build sell request (no bid or zero size)")

    app.sell_executor = RaisingSellExecutor()
    decision = ExitDecision.full(ExitReason.PANIC, "test")
    triggers_before = len(app.panic.triggers)

    for _ in range(20):
        await app._execute_exit(pos, decision)

    triggers_after = len(app.panic.triggers)
    assert triggers_after == triggers_before + 1  # exactly one activation, not 20


async def test_reconciliation_untracks_the_book(app):
    """app.mirror is only constructed once networking starts (see run()),
    so it's None under the build()-only test fixture -- use a lightweight
    fake with the same untrack_token() contract to isolate this behavior."""
    class FakeMirror:
        def __init__(self):
            self.untracked = []

        def untrack_token(self, token_id):
            self.untracked.append(token_id)

    pos = _open_position(app)
    _force_exit_decision(app, pos)
    app.mirror = FakeMirror()
    now = app.clock.now_ms()
    app._exit_stuck_since_ms[TOKEN] = now - DEFAULT_STUCK_EXIT_CUTOFF_MS - 1000

    await app._manage_exits()

    assert app.mirror.untracked == [TOKEN]


async def test_reconciliation_sends_a_telegram_alert_not_silent(app):
    sent = []

    class FakeTelegram:
        async def send(self, text, critical=False):
            sent.append((text, critical))

    app.telegram = FakeTelegram()
    pos = _open_position(app)
    _force_exit_decision(app, pos)
    now = app.clock.now_ms()
    app._exit_stuck_since_ms[TOKEN] = now - DEFAULT_STUCK_EXIT_CUTOFF_MS - 1000

    await app._manage_exits()

    assert len(sent) == 1
    text, critical = sent[0]
    assert critical is True
    assert "RECONCILED" in text
    assert "clear_panic" in text
    assert "NOT a confirmed" in text


# ---------------------------------------------------------------------------
# Safety: mode/dry_run/live invariants, no real orders, no secrets
# ---------------------------------------------------------------------------

def test_mode_is_still_shadow_live_or_simulation():
    from poly_alpha_sniper.core.config_loader import TradingMode
    c = load_config()
    assert c.mode.trading_mode in ("shadow_live", "simulation")
    assert TradingMode(c.mode.trading_mode).is_live is False


def test_dry_run_is_still_true():
    assert load_config().mode.dry_run is True


def test_live_trading_enabled_is_still_false():
    from poly_alpha_sniper.core.config_loader import load_secrets
    assert load_secrets().live_trading_enabled is False


def test_reconciliation_module_never_places_or_cancels_orders():
    import inspect
    from poly_alpha_sniper.portfolio import position_reconciliation
    src = inspect.getsource(position_reconciliation)
    for forbidden in ("place_order", "cancel_order", ".env", "process.env",
                      "PRIVATE_KEY", "API_SECRET", "BOT_TOKEN"):
        assert forbidden not in src


def test_reconciliation_module_makes_no_network_calls():
    import inspect
    from poly_alpha_sniper.portfolio import position_reconciliation
    src = inspect.getsource(position_reconciliation)
    for forbidden in ("aiohttp", "requests."):
        assert forbidden not in src.lower()
    assert "async def" not in src  # pure sync function -- cannot await a network call
