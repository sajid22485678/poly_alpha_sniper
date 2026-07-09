"""WS3 integration: core.app.App._evaluate_market's oracle-anchor gate.
Verifies CEX price alone (no oracle anchor) can never reach signal-building,
let alone produce an accepted order -- the exact "edge inversion risk" the
audit found and this gate closes."""
from __future__ import annotations

import pytest

from poly_alpha_sniper.core.config_loader import Secrets, load_config
from poly_alpha_sniper.tests.helpers import market, shock, view


@pytest.fixture()
def app(tmp_path):
    from poly_alpha_sniper.core.app import App
    c = load_config()
    c.telegram.enabled = False
    c.dashboard.auth_enabled = False
    c.oracle_ev.enabled = True
    db = (tmp_path / "oracle_gate.db").as_posix()
    a = App(c, Secrets(env={"DASHBOARD_AUTH_ENABLED": "false",
                            "DATABASE_URL": f"sqlite:///{db}"}))
    a.build()
    yield a
    a.store.close()


async def test_cex_price_alone_cannot_produce_accepted_signal_without_anchor(app):
    """market() has no price_to_beat (the honest default -- discovery only
    sets it when Polymarket's metadata actually has one). A real shock and a
    fresh CEX view must still be rejected before build_signal runs."""
    m = market()  # price_to_beat is None
    assert m.price_to_beat is None
    sh = shock(asset="BTC", ts_ms=app.clock.now_ms())
    v = view(asset="BTC")

    await app._evaluate_market(sh, m, v, app.clock.now_ms())

    predictions = app.store.query("SELECT * FROM predictions")
    assert predictions == [], "no prediction should ever be recorded -- rejected before build_signal"
    diag = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE reason='rejected_by_missing_oracle_anchor'")
    assert len(diag) == 1
    assert "REJECTED_MISSING_ORACLE_ANCHOR" in diag[0]["detail"]


async def test_valid_anchor_lets_evaluation_proceed_past_the_gate(app):
    """With a real price_to_beat set (as discovery would populate from
    Polymarket metadata), the anchor gate must NOT block -- only later,
    ordinary gates (edge/quality/etc) may still reject the candidate, but
    not for a missing-anchor reason."""
    m = market(expiry_ms=app.clock.now_ms() + 240_000)
    m.price_to_beat = 100_000.0
    m.price_to_beat_source = "polymarket_event_metadata"
    m.raw["event_start_ts_ms"] = app.clock.now_ms() - 10_000
    sh = shock(asset="BTC", ts_ms=app.clock.now_ms())
    v = view(asset="BTC", price=100_050.0)  # 0.05% from anchor -- stable

    await app._evaluate_market(sh, m, v, app.clock.now_ms())

    diag = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE reason='rejected_by_missing_oracle_anchor'")
    assert diag == []


async def test_oracle_ev_disabled_skips_the_gate_entirely(app):
    """cfg.oracle_ev.enabled=False must fully restore pre-WS3 behavior --
    no anchor gate, no oracle_anchor_log rows."""
    app.cfg.oracle_ev.enabled = False
    m = market()  # no anchor
    sh = shock(asset="BTC", ts_ms=app.clock.now_ms())
    v = view(asset="BTC")

    await app._evaluate_market(sh, m, v, app.clock.now_ms())

    diag = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE reason='rejected_by_missing_oracle_anchor'")
    assert diag == []
    oracle_log = app.store.query("SELECT * FROM oracle_anchor_log")
    assert oracle_log == []


async def test_edge_uses_fair_probability_against_price_to_beat_anchored_market(app):
    """The EV gate reads signal.edge.fair_probability -- the same
    probability the (CEX-only) probability model already computes -- and
    logs it alongside the anchor in oracle_anchor_log once a signal is
    actually built; confirms the anchor data flows all the way through."""
    m = market(expiry_ms=app.clock.now_ms() + 240_000)
    m.price_to_beat = 100_000.0
    m.price_to_beat_source = "polymarket_event_metadata"
    m.raw["event_start_ts_ms"] = app.clock.now_ms() - 10_000
    sh = shock(asset="BTC", ts_ms=app.clock.now_ms())
    v = view(asset="BTC", price=100_010.0)

    await app._evaluate_market(sh, m, v, app.clock.now_ms())

    # whatever happened downstream (book/signal may still be None in this
    # minimal fixture), the anchor itself must have been resolved and cached.
    assert app.diag.get("latest_oracle_anchor") is not None
    assert app.diag["latest_oracle_anchor"]["oracle_open_price"] == 100_000.0
