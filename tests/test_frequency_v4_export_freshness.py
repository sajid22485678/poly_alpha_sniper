"""End-to-end freshness contract for the cached Frequency V4 export.

The section cache is only acceptable if the published payload stays honest.
These tests drive the real export against a real store and assert the parts an
operator relies on: critical fields are re-read on every build, cached ones
carry their true age, a stale or missing analytical section is visible and
fails readiness closed, and the payload stays deterministic, JSON-safe,
schema-compatible and atomically published.
"""
from __future__ import annotations

import json

import pytest

from poly_alpha_sniper.lite_frequency_v4 import export as export_module
from poly_alpha_sniper.lite_frequency_v4.export import (
    EXPORT_FILENAME,
    build_frequency_v4_dashboard,
    write_frequency_v4_dashboard,
)
from poly_alpha_sniper.lite_frequency_v4.export_cache import (
    ExportSectionCache,
    SECTION_POLICY,
)
from poly_alpha_sniper.lite_frequency_v4.export_profile import EXPORT_PROFILE
from tests.test_frequency_v4_export import (
    CACHED_OK_INTEGRITY,
    _healthy_runtime_state,
    _store_with_health,
)
from tests.test_frequency_v4_store import NOW

from poly_alpha_sniper.lite_frequency_v4.config import FrequencyV4Config


@pytest.fixture()
def built(tmp_path):
    store, session, context = _store_with_health(tmp_path)
    cfg = FrequencyV4Config()
    cache = ExportSectionCache()

    def build(now_ms=NOW, *, use_cache=True):
        return build_frequency_v4_dashboard(
            store, now_ms=now_ms, config=cfg,
            runtime_state=_healthy_runtime_state(session),
            session_id=session, integrity=CACHED_OK_INTEGRITY,
            section_cache=cache if use_cache else None,
        )

    try:
        yield build, store, session, cfg, cache
    finally:
        store.close()


# -- the contract is published ------------------------------------------------


def test_every_cached_section_publishes_its_full_provenance(built):
    build, *_ = built
    payload = build()
    freshness = payload["export_freshness"]
    assert freshness["export_generated_at_ms"] == NOW
    assert freshness["complete"] is True
    assert set(freshness["sections"]) == set(SECTION_POLICY)
    for name, record in freshness["sections"].items():
        assert set(record) >= {
            "tier", "source_as_of_ms", "age_ms", "cache_hit",
            "refresh_reason", "refresh_duration_ms", "max_age_ms", "stale",
            "available", "source_version",
        }, name
        assert record["available"] is True
        assert record["stale"] is False
        assert record["age_ms"] == 0
        assert record["source_as_of_ms"] == NOW
        assert record["refresh_reason"] == "first"


def test_an_uncached_export_declares_itself_fully_refreshed(built):
    build, *_ = built
    freshness = build(use_cache=False)["export_freshness"]
    assert freshness["complete"] is True
    assert freshness["sections"] == {}
    assert freshness["cache_hits"] == 0
    assert freshness["max_section_age_ms"] == 0


def test_a_reused_section_reports_its_real_age_not_the_export_time(built):
    build, *_ = built
    build(NOW)
    payload = build(NOW + 7_000)
    freshness = payload["export_freshness"]
    assert freshness["export_generated_at_ms"] == NOW + 7_000
    performance = freshness["sections"]["performance"]
    assert performance["cache_hit"] is True
    assert performance["refresh_reason"] == "unchanged_source"
    assert performance["source_as_of_ms"] == NOW
    assert performance["age_ms"] == 7_000
    assert freshness["max_section_age_ms"] == 7_000
    # The payload's own age contract is untouched: this export is brand new.
    assert payload["export_age_ms"] == 0
    assert payload["generated_ts_ms"] == NOW + 7_000


# -- critical fields never go stale -------------------------------------------


def test_critical_fields_are_recomputed_on_every_build(built):
    build, store, session, _cfg, _cache = built
    first = build(NOW)
    second = build(NOW + 3_000)
    critical = (
        "generated_ts_ms", "heartbeat_ts_ms", "runtime_heartbeat_ts_ms",
        "dry_run", "live_enabled", "real_orders_possible",
        "live_adapter_present", "kill_switch_engaged", "fixed_shares",
    )
    for key in critical:
        assert key in second
    assert second["generated_ts_ms"] != first["generated_ts_ms"]
    # None of the always-fresh sections may appear in the cache contract.
    cached = set(second["export_freshness"]["sections"])
    for never_cached in (
        "ledger", "open_positions", "runtime_health", "runtime_sessions",
        "candidates", "universe_active_windows", "integrity",
        "latest_checkpoint", "database_size",
    ):
        assert never_cached not in cached


def test_safety_tuple_is_identical_with_and_without_the_cache(built):
    build, *_ = built
    cached = build(NOW)
    fresh = build(NOW, use_cache=False)
    for key in (
        "dry_run", "live_enabled", "real_orders_possible",
        "live_adapter_present", "kill_switch_engaged", "fixed_shares",
        "strategy_id", "mode",
    ):
        assert cached[key] == fresh[key]


# -- work is actually avoided -------------------------------------------------


def test_an_unchanged_source_avoids_the_expensive_recomputation(built):
    build, store, *_ = built
    EXPORT_PROFILE.reset()
    try:
        with EXPORT_PROFILE.build():
            build(NOW)
        first = EXPORT_PROFILE.snapshot(top=500)["statements"]
        with EXPORT_PROFILE.build():
            build(NOW + 1_000)
        second = EXPORT_PROFILE.snapshot(top=500)["statements"]
    finally:
        EXPORT_PROFILE.reset()

    def calls(table: str, statements) -> int:
        return sum(v["count"] for k, v in statements.items() if table in k)

    # The two whole-history performance joins ran once and were not repeated.
    assert calls("legacy_performance", second) == calls(
        "legacy_performance", first)
    assert calls("metrics.execution.decisions", second) == calls(
        "metrics.execution.decisions", first)
    assert calls("insufficient_rejects", second) == calls(
        "insufficient_rejects", first)


def test_a_changed_source_reruns_only_the_affected_section(built):
    build, store, session, *_ = built
    build(NOW)
    store.record_reject({
        "session_id": session, "reject_ts_ms": NOW + 10,
        "taxonomy": "ECONOMIC", "reason": "negative_edge",
        "recoverable": False,
    })
    payload = build(NOW + 1_000)
    sections = payload["export_freshness"]["sections"]
    assert sections["rejects"]["refresh_reason"] == "source_changed"
    assert sections["rejects"]["cache_hit"] is False
    # A section that does not depend on reject rows keeps its cached read.
    assert sections["performance"]["cache_hit"] is True
    assert sections["legacy_performance"]["cache_hit"] is True


# -- degradation is visible ---------------------------------------------------


def _break_rejects(monkeypatch, message: str = "database is locked") -> None:
    from poly_alpha_sniper.lite_frequency_v4 import metrics as metrics_module

    def boom(*args, **kwargs):
        raise RuntimeError(message)

    monkeypatch.setattr(metrics_module, "reject_taxonomy", boom)


def _break_sources(monkeypatch, message: str = "database is locked") -> None:
    from poly_alpha_sniper.lite_frequency_v4.store import V4Store

    def boom(*args, **kwargs):
        raise RuntimeError(message)

    monkeypatch.setattr(V4Store, "latest_source_health", boom)


def test_a_stale_section_is_marked_and_fails_readiness_closed(built, monkeypatch):
    build, *_ = built
    build(NOW)
    _break_rejects(monkeypatch)
    payload = build(NOW + 25_000)
    freshness = payload["export_freshness"]
    assert freshness["stale_sections"] == ["rejects"]
    assert freshness["degraded_sections"] == ["rejects"]
    # A retained value is still a value: the export is complete but degraded.
    assert freshness["complete"] is True
    record = freshness["sections"]["rejects"]
    assert record["stale"] is True
    assert record["source_as_of_ms"] == NOW
    assert record["age_ms"] == 25_000
    assert "RuntimeError" in record["error"]
    reasons = payload["persistence"]["operational_degraded_reasons"]
    assert "export_section_stale:rejects" in reasons
    assert payload["persistence"]["operational_ready"] is False
    # The retained taxonomy is the one that was actually read at NOW.
    assert payload["reject_taxonomy"]["all"] is not None


def test_a_required_section_with_no_valid_value_fails_the_export_closed(
    built, monkeypatch
):
    build, *_ = built
    _break_rejects(monkeypatch, "gone")
    with pytest.raises(RuntimeError, match="gone"):
        build(NOW)


def test_an_excessively_stale_required_section_stops_publishing(
    built, monkeypatch
):
    build, *_ = built
    build(NOW)
    _break_rejects(monkeypatch, "still gone")
    # Inside the stale limit the last value is retained and marked...
    assert build(NOW + 25_000)["export_freshness"]["stale_sections"] == [
        "rejects"]
    # ...but past it there is nothing honest left to publish.
    with pytest.raises(RuntimeError, match="still gone"):
        build(NOW + 20_000 * 4 + 1)


def test_an_unavailable_optional_section_clears_completeness(built, monkeypatch):
    build, *_ = built
    _break_sources(monkeypatch, "gone")
    payload = build(NOW)
    freshness = payload["export_freshness"]
    assert freshness["complete"] is False
    assert freshness["unavailable_sections"] == ["sources"]
    assert freshness["sections"]["sources"]["available"] is False
    assert "RuntimeError" in freshness["sections"]["sources"]["error"]
    reasons = payload["persistence"]["operational_degraded_reasons"]
    assert "export_section_unavailable:sources" in reasons
    assert payload["persistence"]["operational_ready"] is False
    # Degraded to an empty shape, never a fabricated one.
    assert payload["sources"] == []


def test_one_failing_optional_section_still_publishes_the_export(
    built, monkeypatch
):
    """A broken optional section must not stop heartbeat publication."""

    build, *_ = built
    _break_sources(monkeypatch, "slow")
    payload = build(NOW)
    # Every heartbeat-critical field is present and current despite the failure.
    assert payload["generated_ts_ms"] == NOW
    assert payload["heartbeat_ts_ms"] == NOW
    assert payload["runtime_heartbeat_ts_ms"] == NOW
    assert payload["export_age_ms"] == 0
    assert payload["export_freshness"]["complete"] is False


# -- payload discipline -------------------------------------------------------


def _strip_measured(payload: dict) -> dict:
    """Drop the only non-reproducible fields: measured refresh durations."""

    clone = json.loads(json.dumps(payload, allow_nan=False))
    for record in clone.get("export_freshness", {}).get(
            "sections", {}).values():
        record.pop("refresh_duration_ms", None)
    return clone


def test_export_stays_deterministic_for_one_instant(built):
    """Same instant, same cache state, same payload."""

    build, *_ = built
    first = build(NOW, use_cache=False)
    second = build(NOW, use_cache=False)
    assert json.dumps(first, sort_keys=True, allow_nan=False) == json.dumps(
        second, sort_keys=True, allow_nan=False)


def test_a_cached_export_differs_only_by_its_declared_provenance(built):
    build, *_ = built
    first = build(NOW)
    second = build(NOW)
    a, b = _strip_measured(first), _strip_measured(second)
    # The second build reused every section, and says so.  Nothing else moved.
    assert b["export_freshness"]["cache_hits"] == len(
        b["export_freshness"]["sections"])
    a.pop("export_freshness")
    b.pop("export_freshness")
    assert json.dumps(a, sort_keys=True, allow_nan=False) == json.dumps(
        b, sort_keys=True, allow_nan=False)


def test_export_payload_is_json_safe_and_schema_compatible(built):
    build, *_ = built
    payload = build(NOW)
    encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
    assert json.loads(encoded)["schema_version"] == 3
    # Additive only: every key the dashboard reads is still present.
    for key in (
        "schema_version", "generated_ts_ms", "export_age_ms", "runtime",
        "persistence", "database", "sources", "market_universe", "frequency",
        "execution", "pnl", "compounding_preview", "exposure", "breakdowns",
        "integrity", "acceptance_gate", "recent_entries", "open_positions",
        "terminal_trades", "reject_reasons", "safety",
    ):
        assert key in payload, key


def test_cached_and_uncached_exports_agree_on_content(built):
    build, *_ = built
    cached = build(NOW)
    fresh = build(NOW, use_cache=False)
    cached.pop("export_freshness")
    fresh.pop("export_freshness")
    assert json.dumps(cached, sort_keys=True, allow_nan=False) == json.dumps(
        fresh, sort_keys=True, allow_nan=False)


def test_atomic_publication_keeps_the_last_valid_export_on_failure(
    tmp_path, monkeypatch
):
    store, session, _context = _store_with_health(tmp_path)
    export_dir = tmp_path / "export" / "poly_alpha_frequency_v4"
    export_dir.mkdir(parents=True)
    monkeypatch.setattr(
        export_module, "canonical_export_dir", lambda: export_dir)
    cache = ExportSectionCache()
    cfg = FrequencyV4Config()
    try:
        write_frequency_v4_dashboard(
            store, export_dir, now_ms=NOW, config=cfg,
            runtime_state=_healthy_runtime_state(session), session_id=session,
            integrity=CACHED_OK_INTEGRITY, section_cache=cache)
        good = (export_dir / EXPORT_FILENAME).read_text(encoding="utf-8")

        monkeypatch.setattr(
            export_module, "_atomic_write",
            lambda *a, **k: (_ for _ in ()).throw(OSError("locked")))
        with pytest.raises(OSError):
            write_frequency_v4_dashboard(
                store, export_dir, now_ms=NOW + 5_000, config=cfg,
                runtime_state=_healthy_runtime_state(session),
                session_id=session, integrity=CACHED_OK_INTEGRITY,
                section_cache=cache)
        assert (export_dir / EXPORT_FILENAME).read_text(
            encoding="utf-8") == good
        assert not list(export_dir.glob("*.tmp*"))
    finally:
        store.close()


def test_a_restarted_runtime_starts_with_an_empty_cache(built):
    """No cached value can survive a restart: the cache is in memory only."""

    build, store, session, cfg, cache = built
    build(NOW)
    assert cache.counters()["tracked_sections"] > 0
    # A fresh engine constructs a fresh cache; model that exactly.
    restarted = ExportSectionCache()
    payload = build_frequency_v4_dashboard(
        store, now_ms=NOW + 1_000, config=cfg,
        runtime_state=_healthy_runtime_state(session), session_id=session,
        integrity=CACHED_OK_INTEGRITY, section_cache=restarted)
    for record in payload["export_freshness"]["sections"].values():
        assert record["refresh_reason"] == "first"
        assert record["cache_hit"] is False
        assert record["age_ms"] == 0


def test_no_read_transaction_survives_a_cached_export(tmp_path):
    from poly_alpha_sniper.lite_frequency_v4.store import V4ReadOnlyStore

    store, session, _context = _store_with_health(tmp_path)
    path = store.path
    store.close()
    reader = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    cache = ExportSectionCache()
    try:
        for tick in (NOW, NOW + 1_000, NOW + 2_000):
            build_frequency_v4_dashboard(
                reader, now_ms=tick, config=FrequencyV4Config(),
                runtime_state=_healthy_runtime_state(session),
                session_id=session, integrity=CACHED_OK_INTEGRITY,
                section_cache=cache)
            assert reader.connection.in_transaction is False
    finally:
        reader.close()
