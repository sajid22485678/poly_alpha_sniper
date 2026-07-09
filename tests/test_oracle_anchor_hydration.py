from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import Secrets, load_config
from poly_alpha_sniper.core.contracts import RejectReason, TradingMode
from poly_alpha_sniper.discovery.market_discovery import MarketDiscovery
from poly_alpha_sniper.discovery.market_mapper import map_raw_market
from poly_alpha_sniper.reporting.agent_export import build_oracle_status
from poly_alpha_sniper.strategy.oracle_anchor import resolve_oracle_anchor, validate_oracle_anchor
from poly_alpha_sniper.tests.helpers import NOW_MS, market, shock, view

MAX_AGE_MS = 300_000
MAX_BASIS = 0.02


def _iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _raw_market(mid: str = "m-anchor", event: dict | None = None, **extra) -> dict:
    suffix = "".join(ch for ch in mid if ch.isalnum())
    raw = {
        "question": "Bitcoin Up or Down - July 9, 1:00PM-1:05PM ET",
        "slug": f"btc-updown-5m-{suffix}",
        "id": mid,
        "conditionId": "0x" + "11" * 32,
        "outcomes": '["Up", "Down"]',
        "clobTokenIds": f'["111000{suffix}", "222000{suffix}"]',
        "orderMinSize": 5,
        "orderPriceMinTickSize": 0.01,
        "negRisk": False,
        "liquidityNum": 1000.0,
        "volume24hr": 5000.0,
        "active": True,
        "closed": False,
        "acceptingOrders": True,
        "endDate": _iso(NOW_MS + 240_000),
        "eventStartTime": _iso(NOW_MS - 60_000),
        "description": "This market resolves from the price at the start of the range.",
    }
    if event is not None:
        raw["events"] = [event]
    raw.update(extra)
    return raw


def _cfg():
    c = load_config()
    c.telegram.enabled = False
    c.dashboard.auth_enabled = False
    c.oracle_ev.enabled = True
    return c


def test_extracts_price_to_beat_from_nested_event_metadata():
    raw = _raw_market(event={
        "id": "event-1",
        "eventMetadata": {"priceToBeat": "100123.45"},
        "resolutionSource": "https://data.chain.link/streams/btc-usd",
    })

    m = map_raw_market(raw, NOW_MS)

    assert m.price_to_beat == pytest.approx(100123.45)
    assert m.price_to_beat_source == "polymarket_event_metadata"
    assert m.resolution_source_url == "https://data.chain.link/streams/btc-usd"
    diag = m.raw["oracle_anchor_diagnostics"]
    assert "events[0].eventMetadata.priceToBeat" in diag["fields_checked"]
    assert diag["final_anchor_status"] == "available"


def test_parses_metadata_json_string():
    raw = _raw_market(event={
        "id": "event-2",
        "eventMetadata": json.dumps({
            "price_to_beat": "100987.65",
            "resolutionSource": "https://data.chain.link/streams/btc-usd",
        }),
    })

    m = map_raw_market(raw, NOW_MS)

    assert m.price_to_beat == pytest.approx(100987.65)
    assert m.price_to_beat_source == "polymarket_event_metadata"
    assert m.resolution_source_url == "https://data.chain.link/streams/btc-usd"


async def test_shallow_market_triggers_hydration():
    calls = []

    async def fake_gamma(_params):
        return [_raw_market(mid="shallow", event={"id": "event-shallow"})]

    async def hydrate(raw):
        calls.append(raw["id"])
        return {"id": "event-shallow", "eventMetadata": {"priceToBeat": 100000.0}}

    disc = MarketDiscovery(_cfg(), SimClock(NOW_MS), fake_gamma, event_hydrator=hydrate)
    markets = await disc.refresh()

    assert calls == ["shallow"]
    diag = markets[0].raw["oracle_anchor_diagnostics"]
    assert diag["hydration_attempted"] is True
    assert diag["hydration_success"] is True
    assert markets[0].price_to_beat == 100000.0


async def test_hydration_success_makes_anchor_available():
    async def fake_gamma(_params):
        return [_raw_market(mid="hydrated", event={"id": "event-hydrated"})]

    async def hydrate(_raw):
        return {"id": "event-hydrated", "eventMetadata": {"priceToBeat": "100010.0"}}

    disc = MarketDiscovery(_cfg(), SimClock(NOW_MS), fake_gamma, event_hydrator=hydrate)
    markets = await disc.refresh()

    anchor = resolve_oracle_anchor(markets[0], cex_price=100020.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)

    assert anchor.available is True
    assert anchor.oracle_open_price == 100010.0
    assert anchor.hydration_attempted is True
    assert anchor.hydration_success is True


async def test_hydration_failure_remains_rejected():
    async def fake_gamma(_params):
        return [_raw_market(mid="missing", event={"id": "event-missing"})]

    async def hydrate(_raw):
        return {}

    disc = MarketDiscovery(_cfg(), SimClock(NOW_MS), fake_gamma, event_hydrator=hydrate)
    markets = await disc.refresh()
    anchor = resolve_oracle_anchor(markets[0], cex_price=100020.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)
    ok, reason = validate_oracle_anchor(anchor, markets[0], NOW_MS, MAX_AGE_MS, MAX_BASIS)

    assert anchor.available is False
    assert anchor.hydration_attempted is True
    assert anchor.hydration_success is False
    assert not ok
    assert reason == RejectReason.MISSING_ORACLE_ANCHOR


def test_resolution_source_url_alone_is_not_accepted_as_anchor():
    raw = _raw_market(event={
        "id": "event-url-only",
        "resolutionSource": "https://www.binance.com/en/trade/BTC_USDT",
    })
    m = map_raw_market(raw, NOW_MS)
    anchor = resolve_oracle_anchor(m, cex_price=100020.0, cex_ts_ms=NOW_MS, now_ms=NOW_MS)
    status = build_oracle_status(
        {"diagnostics": {"latest_oracle_anchor": asdict(anchor)}}, _cfg(), NOW_MS)

    assert m.price_to_beat is None
    assert anchor.available is False
    assert status["available"] is False
    assert status["metadata_url"] == "https://www.binance.com/en/trade/BTC_USDT"
    assert status["settlement_anchor"] == "price_to_beat"
    ok, reason = validate_oracle_anchor(anchor, m, NOW_MS, MAX_AGE_MS, MAX_BASIS)
    assert not ok
    assert reason == RejectReason.MISSING_ORACLE_ANCHOR


@pytest.fixture()
def app(tmp_path):
    from poly_alpha_sniper.core.app import App

    c = _cfg()
    db = (tmp_path / "oracle_anchor_hydration.db").as_posix()
    a = App(c, Secrets(env={"DASHBOARD_AUTH_ENABLED": "false",
                            "DATABASE_URL": f"sqlite:///{db}"}))
    a.build()
    yield a
    a.store.close()


async def test_fired_shock_without_anchor_remains_rejected(app):
    m = market(expiry_ms=app.clock.now_ms() + 240_000)
    assert m.price_to_beat is None

    await app._evaluate_market(shock(asset="BTC", ts_ms=app.clock.now_ms()),
                               m, view(asset="BTC"), app.clock.now_ms())

    assert app.store.query("SELECT * FROM predictions") == []
    assert app.store.query("SELECT * FROM orders") == []
    diag = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE reason='rejected_by_missing_oracle_anchor'")
    assert len(diag) == 1


async def test_missing_anchor_is_retried_but_valid_anchor_is_cached():
    calls = 0

    async def fake_gamma(_params):
        return [_raw_market(mid="retry", event={"id": "event-retry"})]

    async def hydrate(_raw):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {}
        return {"id": "event-retry", "eventMetadata": {"priceToBeat": 100050.0}}

    disc = MarketDiscovery(_cfg(), SimClock(NOW_MS), fake_gamma, event_hydrator=hydrate)
    first = await disc.refresh()
    second = await disc.refresh()
    third = await disc.refresh()

    assert first[0].price_to_beat is None
    assert second[0].price_to_beat == 100050.0
    assert third[0].price_to_beat == 100050.0
    assert calls == 2


async def test_null_embedded_metadata_still_recovered_via_events_hydration():
    """Verified against live Gamma (2026-07-10): a /markets row can embed an
    event with eventMetadata=null while the /events?id= endpoint already exposes
    the populated priceToBeat (Polymarket publishes it per-market, with a delay,
    to /events before it lands on /markets). Hydration must therefore fire even
    when the embedded event carries a null eventMetadata slot -- skipping it
    would drop a genuinely recoverable anchor."""
    calls = []

    async def fake_gamma(_params):
        return [_raw_market(mid="nullmeta",
                            event={"id": "event-null", "eventMetadata": None})]

    async def hydrate(raw):
        calls.append(raw.get("id"))
        return {"id": "event-null", "eventMetadata": {"priceToBeat": 77.97}}

    disc = MarketDiscovery(_cfg(), SimClock(NOW_MS), fake_gamma, event_hydrator=hydrate)
    markets = await disc.refresh()

    assert calls == ["nullmeta"]  # hydration DID fire despite the null slot
    assert markets[0].price_to_beat == pytest.approx(77.97)
    diag = markets[0].raw["oracle_anchor_diagnostics"]
    assert diag["hydration_success"] is True
    assert diag["final_anchor_status"] == "available"


async def test_anchor_recovered_from_markets_when_published_late_no_hydration_needed():
    """When priceToBeat lands on the /markets embedded event on a later refresh,
    discovery picks it up directly -- hydration is a fallback, not the only
    path. (Here the embedded event gains the anchor between refreshes.)"""
    published = {"v": False}

    async def fake_gamma(_params):
        meta = {"priceToBeat": 100050.0} if published["v"] else None
        return [_raw_market(mid="late", event={"id": "event-late", "eventMetadata": meta})]

    async def hydrate(_raw):
        return {}  # /events has nothing; recovery must come from /markets itself

    disc = MarketDiscovery(_cfg(), SimClock(NOW_MS), fake_gamma, event_hydrator=hydrate)
    first = await disc.refresh()
    published["v"] = True  # Polymarket publishes the anchor on /markets
    second = await disc.refresh()

    assert first[0].price_to_beat is None
    assert second[0].price_to_beat == 100050.0


def test_live_remains_disabled():
    c = _cfg()
    assert c.mode.trading_mode == "shadow_live"
    assert c.mode.dry_run is True
    assert TradingMode(c.mode.trading_mode).is_live is False


def test_oracle_hydration_code_does_not_place_or_cancel_orders():
    for rel in ("discovery/market_discovery.py", "connectors/polymarket_gamma.py",
                "discovery/market_mapper.py"):
        src = Path(rel).read_text(encoding="utf-8")
        assert "place_order" not in src
        assert "cancel_order" not in src
        assert "cancel_orders" not in src


def test_oracle_hydration_code_does_not_touch_env_or_secrets():
    for rel in ("discovery/market_discovery.py", "connectors/polymarket_gamma.py",
                "discovery/market_mapper.py", "strategy/oracle_anchor.py"):
        src = Path(rel).read_text(encoding="utf-8")
        assert ".env" not in src
        assert "load_secrets" not in src
