"""EXPERIMENTAL_PROBE_TRADING: simulated probe positions, anchor-precise
rejects, honest exits, lane separation. Shadow-only; never real orders."""
from __future__ import annotations

import pytest

from poly_alpha_sniper.core.config_loader import TradingMode, load_config
from poly_alpha_sniper.research.probe_trader import (
    EXIT_BEFORE_CLOSE_S, FIXED_SHARES, ProbeTrader)
from poly_alpha_sniper.tests.helpers import NOW_MS


def _cfg():
    return load_config().research_probe_trading


def _row(**overrides) -> dict:
    """A feature row where every probe gate passes: anchor present, CEX fresh,
    executable book, sane spread/depth, in the time window, and CEX price
    meaningfully above the anchor (clear BUY_YES direction, dist=0.1%)."""
    row = {
        "ts_ms": NOW_MS, "asset": "BTC", "market_id": "m1",
        "market_slug": "btc-updown-5m-1", "yes_token_id": "ty", "no_token_id": "tn",
        "time_to_close_s": 200.0, "price_to_beat": 100_000.0,
        "anchor_status": "available", "anchor_missing_reason": "",
        "cex_age_ms": 500, "cex_freshness": "fresh", "cex_price": 100_100.0,
        "ret_1s": 0.0, "ret_2s": 0.0, "ret_5s": 0.0, "volatility": 0.0001,
        "zscore": 0.1, "shock_score": 0.1, "ret_score": 0.1, "zscore_score": 0.1,
        "near_miss_tier": "ROUTINE_NO_SHOCK",
        "book_bid": 0.48, "book_ask": 0.50, "spread": 0.02, "depth_usd": 500.0,
        "blocker": "rejected_by_no_shock",
    }
    row.update(overrides)
    return row


class _Sink:
    def __init__(self):
        self.rows: list[tuple[str, dict]] = []

    def __call__(self, table: str, row: dict) -> None:
        self.rows.append((table, row))

    def events(self, kind: str) -> list[dict]:
        return [r for t, r in self.rows if t == "experimental_probe_trades"
                and r.get("event") == kind]


def test_probe_opens_without_shock_or_imbalance():
    """The core requirement: baseline says no_shock, tier is ROUTINE, no
    imbalance data at all -- the probe still opens on CEX-vs-anchor direction
    because all HARD gates pass."""
    trader, sink = ProbeTrader(_cfg()), _Sink()
    trader.on_scan(_row(), NOW_MS, sink)
    entries = sink.events("ENTRY")
    assert len(entries) == 1
    e = entries[0]
    assert e["side"] == "BUY_YES"           # cex 100100 > anchor 100000
    assert e["price"] == 0.50               # YES bought at recorded ask
    assert e["shares"] == FIXED_SHARES
    assert e["status"] == "OPEN"
    assert trader.bankroll == pytest.approx(10.0 - 5 * 0.50)


def test_probe_direction_buy_no_when_cex_below_anchor():
    trader, sink = ProbeTrader(_cfg()), _Sink()
    trader.on_scan(_row(cex_price=99_900.0), NOW_MS, sink)
    e = sink.events("ENTRY")[0]
    assert e["side"] == "BUY_NO"
    assert e["price"] == pytest.approx(1.0 - 0.48)  # NO at 1-bid proxy


@pytest.mark.parametrize("override,expected_reason", [
    ({"price_to_beat": None, "anchor_missing_reason": "UPSTREAM_NOT_PUBLISHED"},
     "anchor_upstream_not_published"),
    ({"price_to_beat": None, "anchor_missing_reason": "HYDRATION_FAILED"},
     "anchor_hydration_failed"),
    ({"price_to_beat": None, "anchor_missing_reason": "SCHEMA_UNKNOWN"},
     "anchor_schema_unknown"),
    ({"price_to_beat": None, "anchor_missing_reason": ""}, "missing_anchor"),
])
def test_missing_anchor_rejects_with_precise_reason(override, expected_reason):
    trader, sink = ProbeTrader(_cfg()), _Sink()
    trader.on_scan(_row(**override), NOW_MS, sink)
    assert sink.events("ENTRY") == []
    assert trader.last_reject["reject_reason"] == expected_reason


@pytest.mark.parametrize("override,expected", [
    ({"cex_age_ms": 9000, "cex_freshness": "fail_closed"}, "cex_not_fail_closed"),
    ({"book_bid": None, "book_ask": None}, "executable_book"),
    ({"spread": 0.5}, "spread_ok"),
    ({"depth_usd": 1.0}, "depth_ok"),
    ({"yes_token_id": ""}, "token_mapping"),
    ({"time_to_close_s": 10.0}, "outside_time_window"),   # too close to close
    ({"time_to_close_s": 300.0}, "outside_time_window"),  # window not open yet
])
def test_hard_gates_reject_probe(override, expected):
    trader, sink = ProbeTrader(_cfg()), _Sink()
    trader.on_scan(_row(**override), NOW_MS, sink)
    assert sink.events("ENTRY") == []
    assert trader.last_reject["reject_reason"] == expected


def test_duplicate_same_market_side_rejected():
    trader, sink = ProbeTrader(_cfg()), _Sink()
    trader.on_scan(_row(), NOW_MS, sink)
    trader.on_scan(_row(), NOW_MS + 20_000, sink)
    assert len(sink.events("ENTRY")) == 1
    assert trader.last_reject["reject_reason"] == "duplicate_probe_position"


def test_insufficient_probe_cash_rejects():
    trader, sink = ProbeTrader(_cfg(), starting_bankroll_usd=1.0), _Sink()
    trader.on_scan(_row(), NOW_MS, sink)   # needs 5*0.50 = $2.50 > $1
    assert sink.events("ENTRY") == []
    assert trader.last_reject["reject_reason"] == "cash_ok"


def test_anchor_distance_below_band_rejects():
    trader, sink = ProbeTrader(_cfg()), _Sink()
    trader.on_scan(_row(cex_price=100_010.0), NOW_MS, sink)  # 0.01% < 0.03% band
    assert sink.events("ENTRY") == []
    assert trader.last_reject["reject_reason"] == "anchor_distance_below_band"


def test_pre_close_book_exit_records_honest_pnl():
    trader, sink = ProbeTrader(_cfg()), _Sink()
    trader.on_scan(_row(), NOW_MS, sink)   # entry YES @ 0.50
    # later scan on the SAME market inside the exit window with a real book
    exit_row = _row(time_to_close_s=EXIT_BEFORE_CLOSE_S - 5,
                    book_bid=0.60, book_ask=0.62)
    trader.on_scan(exit_row, NOW_MS + 175_000, sink)
    exits = sink.events("EXIT")
    assert len(exits) == 1
    x = exits[0]
    assert x["status"] == "CLOSED_EXIT_PRICE"
    assert x["price"] == 0.60                       # YES exits at recorded bid
    assert x["pnl_usd"] == pytest.approx((0.60 - 0.50) * FIXED_SHARES)
    assert x["hold_s"] == pytest.approx(175.0)
    assert trader.open_positions == {}


def test_rollover_without_exit_book_goes_pending_resolution_with_no_pnl():
    trader, sink = ProbeTrader(_cfg()), _Sink()
    trader.on_scan(_row(), NOW_MS, sink)
    # scans move to a DIFFERENT market; original window closed >60s ago
    later = NOW_MS + 200_000 + 61_000
    trader.on_scan(_row(market_id="m2", market_slug="btc-updown-5m-2"), later, sink)
    exits = [x for x in sink.events("EXIT") if x["probe_id"].startswith("probe-m1")]
    assert len(exits) == 1
    assert exits[0]["status"] == "PENDING_RESOLUTION"
    assert exits[0]["pnl_usd"] is None              # outcome never fabricated
    assert exits[0]["price"] is None
    assert len(trader.pending_resolution) == 1      # awaiting official outcome


def _resolved_market_row(yes_price: str, no_price: str, closed=True) -> list[dict]:
    return [{"id": "m1", "closed": closed, "outcomePrices": f'["{yes_price}", "{no_price}"]'}]


async def test_official_outcome_resolves_win_and_loss():
    """PENDING probes resolve against the OFFICIAL closed-market outcome:
    BUY_YES wins when YES settles at 1, loses when NO settles at 1. Payout is
    1 or 0 per share -- never an invented price."""
    for yes, no, expect_status, expect_pnl in (
            ("1", "0", "CLOSED_WIN", 5 * (1.0 - 0.50)),
            ("0", "1", "CLOSED_LOSS", 5 * (0.0 - 0.50))):
        trader, sink = ProbeTrader(_cfg()), _Sink()
        trader.on_scan(_row(), NOW_MS, sink)                       # entry YES @0.50
        trader.on_scan(_row(market_id="m2"), NOW_MS + 261_000, sink)  # -> PENDING

        async def fetch(_params, _rows=_resolved_market_row(yes, no)):
            return _rows

        await trader.resolve_pending(fetch, NOW_MS + 300_000, sink)
        res = sink.events("RESOLUTION")
        assert len(res) == 1
        assert res[0]["status"] == expect_status
        assert res[0]["pnl_usd"] == pytest.approx(expect_pnl)
        assert res[0]["reason"] == "official_outcome"
        assert trader.pending_resolution == {}


async def test_unresolved_market_retries_then_goes_final():
    trader, sink = ProbeTrader(_cfg()), _Sink()
    trader.on_scan(_row(), NOW_MS, sink)
    trader.on_scan(_row(market_id="m2"), NOW_MS + 261_000, sink)

    async def fetch_not_closed(_params):
        return _resolved_market_row("0.6", "0.4", closed=False)  # no evidence

    from poly_alpha_sniper.research.probe_trader import MAX_RESOLUTION_RETRIES
    for i in range(MAX_RESOLUTION_RETRIES - 1):
        await trader.resolve_pending(fetch_not_closed, NOW_MS + 300_000 + i, sink)
        assert trader.pending_resolution                  # still retrying
        assert sink.events("RESOLUTION") == []            # nothing fabricated
    await trader.resolve_pending(fetch_not_closed, NOW_MS + 400_000, sink)
    res = sink.events("RESOLUTION")
    assert len(res) == 1
    assert res[0]["status"] == "UNRESOLVED_FINAL"
    assert res[0]["pnl_usd"] is None                      # never counted as win/loss
    assert trader.pending_resolution == {}


def test_disabled_config_opens_nothing():
    cfg = _cfg()
    cfg.enabled = False
    trader, sink = ProbeTrader(cfg), _Sink()
    trader.on_scan(_row(), NOW_MS, sink)
    assert sink.rows == []


def test_probe_export_separates_and_labels(tmp_path):
    """Exporter reconstructs stats from ENTRY/EXIT events; UNRESOLVED excluded
    from winrate; warning label always present; live readiness untouched."""
    from poly_alpha_sniper.core.app import App
    from poly_alpha_sniper.core.config_loader import Secrets
    from poly_alpha_sniper.dashboard.db_reader import DashboardData
    from poly_alpha_sniper.reporting.agent_export import (
        build_live_readiness, build_probe_trading_export)
    c = load_config()
    c.telegram.enabled = False
    c.dashboard.auth_enabled = False
    db = (tmp_path / "probe.db").as_posix()
    a = App(c, Secrets(env={"DASHBOARD_AUTH_ENABLED": "false",
                            "DATABASE_URL": f"sqlite:///{db}"}))
    a.build()
    try:
        data = DashboardData(db)  # takes the DB PATH, not the store object
        state = {"diagnostics": a.diag, "mode": "shadow_live"}
        before = build_live_readiness(data, state, c, NOW_MS)
        trader, rows = ProbeTrader(c.research_probe_trading), []
        trader.on_scan(_row(), NOW_MS, lambda t, r: a._insert(t, r))
        trader.on_scan(_row(time_to_close_s=20.0, book_bid=0.55, book_ask=0.57),
                       NOW_MS + 100_000, lambda t, r: a._insert(t, r))
        out = build_probe_trading_export(data, c, NOW_MS)
        assert out["enabled"] is True
        assert "NOT BASELINE" in out["warning"]
        assert out["completed_trades"] == 1
        assert out["pnl_usd"] == pytest.approx((0.55 - 0.50) * FIXED_SHARES)
        assert out["by_strategy"]
        after = build_live_readiness(data, state, c, NOW_MS)
        assert before == after                       # probes never touch readiness
    finally:
        a.store.close()


def test_live_remains_disabled_and_no_order_path():
    c = load_config()
    assert c.mode.trading_mode == "shadow_live"
    assert c.mode.dry_run is True
    assert TradingMode(c.mode.trading_mode).is_live is False
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "research" / "probe_trader.py"
           ).read_text(encoding="utf-8")
    for forbidden in ("place_order", "cancel_order", "submit", "load_secrets", "dotenv"):
        assert forbidden not in src
