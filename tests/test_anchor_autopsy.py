"""Oracle anchor autopsy: exact current/next window classification, precise
source paths and missing reasons, and discovery wiring. Diagnostics only --
never a gate, never fabricates an anchor."""
from __future__ import annotations

import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.discovery.anchor_autopsy import build_anchor_autopsy
from poly_alpha_sniper.discovery.market_discovery import MarketDiscovery
from poly_alpha_sniper.discovery.market_mapper import extract_oracle_anchor_metadata
from poly_alpha_sniper.tests.helpers import NOW_MS

NOW_S = NOW_MS // 1000
CUR_START = NOW_S - 100          # current window opened 100s ago
NEXT_START = CUR_START + 300     # the immediately following window
PREV_START = CUR_START - 300     # the PREVIOUS window (already over)


def _raw(asset="btc", start=CUR_START, ptb=100_000.0, **extra):
    raw = {
        "id": f"m-{asset}-{start}",
        "slug": f"{asset}-updown-5m-{start}",
        # unique per row -- merge_markets dedups identical condition ids
        "conditionId": "0x" + f"{start}{asset}".encode().hex().ljust(64, "0")[:64],
        "question": f"{asset.upper()} Up or Down",
        "events": [{"id": f"e-{asset}-{start}",
                    "eventMetadata": {"priceToBeat": ptb} if ptb is not None else None}],
    }
    raw.update(extra)
    return raw


def _autopsy(rows, **kw):
    return build_anchor_autopsy(rows, ["BTC", "ETH", "SOL"], NOW_MS, **kw)


# ---------------------------------------------------------------------------
# Window classification: current / next exact, previous never counts
# ---------------------------------------------------------------------------

def test_current_and_next_window_classified_exactly():
    out = _autopsy([_raw(start=CUR_START), _raw(start=NEXT_START)])
    cur, nxt = out["assets"]["BTC"]["current"], out["assets"]["BTC"]["next"]
    assert cur["slug"].endswith(str(CUR_START)) and cur["exact_window_match"] is True
    assert nxt["slug"].endswith(str(NEXT_START)) and nxt["exact_window_match"] is True
    assert cur["final_anchor_available"] is True
    assert cur["final_anchor_source_path"] == "events[0].eventMetadata.priceToBeat"


def test_previous_window_never_becomes_current():
    """A market from the PREVIOUS 5-min window must not appear as current --
    a stale previous anchor can never be served for the live window."""
    out = _autopsy([_raw(start=PREV_START)])
    cur = out["assets"]["BTC"]["current"]
    assert cur["slug"] is None
    assert cur["final_missing_reason"] == "NOT_RECORDED"


def test_nearest_future_window_wins_next_slot():
    out = _autopsy([_raw(start=NEXT_START + 300), _raw(start=NEXT_START)])
    assert out["assets"]["BTC"]["next"]["slug"].endswith(str(NEXT_START))


def test_wrong_asset_rows_never_cross_assets():
    out = _autopsy([_raw(asset="eth", start=CUR_START)])
    assert out["assets"]["BTC"]["current"]["slug"] is None      # BTC untouched
    assert out["assets"]["ETH"]["current"]["final_anchor_available"] is True


# ---------------------------------------------------------------------------
# Missing reasons + field presence
# ---------------------------------------------------------------------------

def test_upstream_not_published_reported_with_field_presence():
    out = _autopsy([_raw(ptb=None)])
    cur = out["assets"]["BTC"]["current"]
    assert cur["final_anchor_available"] is False
    assert cur["final_missing_reason"] == "UPSTREAM_NOT_PUBLISHED"
    fp = cur["field_presence"]
    assert fp["events_count"] == 1
    assert fp["events0_eventMetadata_present"] is False   # slot is null
    assert fp["events0_eventMetadata_priceToBeat_present"] is False
    assert cur["final_anchor_source_path"] is None


def test_hydration_failed_reason_and_retry_count_surface():
    raw = _raw(ptb=None, _anchor_hydration_attempted=True,
               _anchor_hydration_success=False)
    out = _autopsy([raw], retry_counts={raw["id"]: 4})
    cur = out["assets"]["BTC"]["current"]
    assert cur["final_missing_reason"] == "HYDRATION_FAILED"
    assert cur["retry_count"] == 4
    assert cur["hydration_attempted"] is True and cur["hydration_success"] is False


def test_last_successful_anchor_surfaces_but_is_diagnostic_only():
    out = _autopsy([_raw(ptb=None)],
                   last_success={"BTC": {"price_to_beat": 99_950.0,
                                         "ts_ms": NOW_MS - 60_000, "slug": "old"}})
    cur = out["assets"]["BTC"]["current"]
    assert cur["last_successful_anchor_for_asset"]["price_to_beat"] == 99_950.0
    assert cur["last_successful_anchor_age_s"] == 60
    assert cur["final_anchor_available"] is False  # never substituted as anchor


def test_metadata_json_string_source_path_exact():
    import json
    raw = _raw(ptb=None)
    raw["events"][0]["eventMetadata"] = json.dumps({"price_to_beat": 123.45})
    out = _autopsy([raw])
    cur = out["assets"]["BTC"]["current"]
    assert cur["final_price_to_beat"] == pytest.approx(123.45)
    assert cur["final_anchor_source_path"] == "events[0].eventMetadata.price_to_beat"


def test_resolution_source_url_never_becomes_anchor_in_autopsy():
    raw = _raw(ptb=None, resolutionSource="https://data.chain.link/streams/btc-usd")
    out = _autopsy([raw])
    cur = out["assets"]["BTC"]["current"]
    assert cur["final_anchor_available"] is False
    assert cur["final_price_to_beat"] is None


def test_extractor_diag_source_path_matches_hit():
    price, _s, _u, diag = extract_oracle_anchor_metadata(_raw())
    assert price == 100_000.0
    assert diag["source_path"] == "events[0].eventMetadata.priceToBeat"
    _p2, _s2, _u2, diag2 = extract_oracle_anchor_metadata({"id": "x", "priceToBeat": 5.0})
    assert diag2["source_path"] == "priceToBeat"


# ---------------------------------------------------------------------------
# Discovery wiring: autopsy rebuilt per refresh, prehydrates next window
# ---------------------------------------------------------------------------

async def test_discovery_rebuilds_autopsy_and_prehydrates_next():
    cfg = load_config()
    hydrated: list[str] = []

    async def fake_gamma(_params):
        return [_raw(start=CUR_START, ptb=None), _raw(start=NEXT_START, ptb=None)]

    async def hydrate(raw):
        hydrated.append(raw["slug"])
        return {"id": raw["events"][0]["id"], "eventMetadata": {"priceToBeat": 101.0}}

    disc = MarketDiscovery(cfg, SimClock(NOW_MS), fake_gamma, event_hydrator=hydrate)
    await disc.refresh()

    # BOTH current and next windows were hydration-attempted (prehydration)
    assert f"btc-updown-5m-{CUR_START}" in hydrated
    assert f"btc-updown-5m-{NEXT_START}" in hydrated
    autopsy = disc.anchor_autopsy["assets"]["BTC"]
    assert autopsy["current"]["final_anchor_available"] is True
    assert autopsy["next"]["final_anchor_available"] is True
    assert autopsy["next"]["exact_window_match"] is True
