"""Source-versioned section cache and export freshness contract.

The cache exists to stop the export re-deriving results that did not change.
These tests pin the part that matters: it must never present a cached value as
a fresh one, never reuse across a source change, never hold a stale value
without saying so, and never let a failed analytical section masquerade as a
complete export.
"""
from __future__ import annotations

import json

import pytest

from lite_frequency_v4.export_cache import (
    CADENCE_ONLY_VERSION,
    ExportSectionCache,
    SECTION_POLICY,
    SectionResolver,
    TIER_ANALYTICAL,
    TIER_CRITICAL,
    TIER_HISTORICAL,
    UNKNOWN_COMPONENT,
    policy,
    source_version_sql,
    summarize,
    version_is_established,
    version_token,
)


@pytest.fixture()
def cache() -> ExportSectionCache:
    return ExportSectionCache()


class Counter:
    """A build function that records how many times it actually ran."""

    def __init__(self, value=None) -> None:
        self.calls = 0
        self.value = value if value is not None else {"v": 0}

    def __call__(self):
        self.calls += 1
        return self.value


# -- versions -----------------------------------------------------------------


def test_version_token_is_deterministic_and_group_scoped():
    components = {
        "pnl_max": 726, "pnl_rows": 726, "pnl_flags": 2904,
        "entry_max": 732, "entry_rows": 732, "entry_unresolved": 0,
        "exit_max": 726, "decision_max": 267500, "maker_max": 57096,
    }
    first = version_token(components, "trade_evidence")
    assert first == version_token(components, "trade_evidence")
    # A group must not be sensitive to a component outside it.
    moved = dict(components, decision_max=999999)
    assert version_token(moved, "trade_evidence") == first
    assert version_token(moved, "decisions") != version_token(
        components, "decisions")


def test_version_token_without_groups_is_cadence_only():
    assert version_token({}, ) == CADENCE_ONLY_VERSION
    assert version_is_established(CADENCE_ONLY_VERSION)


def test_missing_component_makes_the_version_unestablished():
    token = version_token({"pnl_max": 1}, "trade_evidence")
    assert UNKNOWN_COMPONENT in token
    assert not version_is_established(token)


def test_every_declared_section_has_a_coherent_policy():
    for name in SECTION_POLICY:
        tier, max_age, min_interval, groups = policy(name)
        assert tier in (TIER_ANALYTICAL, TIER_HISTORICAL)
        assert 1_000 <= max_age <= 300_000
        # The staleness ceiling must never be tighter than the refresh floor,
        # or a section could be simultaneously due and forbidden to refresh.
        assert 0 <= min_interval <= max_age
        assert isinstance(groups, tuple)
    # An undeclared section still gets a safe, floor-free default.
    assert policy("not_declared") == (TIER_ANALYTICAL, 30_000, 0, ())


def test_a_refresh_floor_bounds_a_continuously_changing_source(cache):
    """A version that moves every build must not defeat the cache."""

    build = Counter()
    # Source advances on every call, as `decisions` does in production.
    for tick, version in enumerate(("v1", "v2", "v3", "v4"), start=0):
        state = cache.resolve(
            "execution", now_ms=1_000 + tick * 5_000, version=version,
            build=build, max_age_ms=60_000, min_refresh_interval_ms=20_000)
    assert build.calls == 1
    assert state.cache_hit is True
    assert state.refresh_reason == "within_refresh_interval"
    # The lag is declared, not implied.
    assert state.source_changed is True
    assert state.age_ms == 15_000
    assert state.provenance()["source_changed_since"] is True
    assert state.provenance()["min_refresh_interval_ms"] == 20_000
    # Past the floor the changed source is picked up.
    state = cache.resolve(
        "execution", now_ms=1_000 + 20_000, version="v5", build=build,
        max_age_ms=60_000, min_refresh_interval_ms=20_000)
    assert build.calls == 2
    assert state.refresh_reason == "source_changed"
    assert state.source_changed is False
    assert state.age_ms == 0


def test_an_unchanged_source_inside_the_floor_is_exact_not_lagging(cache):
    build = Counter()
    cache.resolve("execution", now_ms=1_000, version="v1", build=build,
                  max_age_ms=60_000, min_refresh_interval_ms=20_000)
    state = cache.resolve("execution", now_ms=6_000, version="v1",
                          build=build, max_age_ms=60_000,
                          min_refresh_interval_ms=20_000)
    assert state.refresh_reason == "unchanged_source"
    assert state.source_changed is False


def test_the_staleness_ceiling_still_wins_over_the_refresh_floor(cache):
    build = Counter()
    cache.resolve("execution", now_ms=1_000, version="v1", build=build,
                  max_age_ms=60_000, min_refresh_interval_ms=20_000)
    state = cache.resolve("execution", now_ms=61_000, version="v1",
                          build=build, max_age_ms=60_000,
                          min_refresh_interval_ms=20_000)
    assert build.calls == 2
    assert state.refresh_reason == "max_age"


def test_decision_histogram_rewrite_is_equivalent_and_seek_only(tmp_path):
    """The loose index scan must match GROUP BY exactly and never scan."""

    import sqlite3

    from poly_alpha_sniper.lite_frequency_v4.metrics import (
        DECISION_ACTIONS_12H,
    )
    from poly_alpha_sniper.lite_frequency_v4.store import V4Store

    path = tmp_path / "decisions.db"
    store = V4Store(path)
    conn = store.connection
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_probe ON decisions(action,decision_ts_ms)")
    rows = [
        ("NO_ACTION", 1_000), ("NO_ACTION", 5_000), ("NO_ACTION", 50),
        ("CROSS_SPREAD", 6_000), ("CROSS_SPREAD", 10),
        ("SKIP", 20), ("SAFETY_FAIL", 7_000),
    ]
    # Seed through raw SQL so the test needs no full candidate graph.
    conn.execute("PRAGMA foreign_keys=OFF")
    for index, (action, ts) in enumerate(rows, start=1):
        conn.execute(
            "INSERT INTO decisions(decision_id,candidate_id,decision_seq,"
            "decision_ts_ms,monotonic_ns,phase,action,economic_gate_passed,"
            "exact_depth_passed,evidence_fresh,reason) "
            "VALUES(?,?,?,?,?,'INITIAL',?,0,0,0,'t')",
            (index, index, index, ts, ts, action),
        )
    conn.commit()

    for cutoff in (0, 100, 5_500, 99_999):
        grouped = conn.execute(
            "SELECT action,COUNT(*) count FROM decisions WHERE decision_ts_ms>=? "
            "GROUP BY action ORDER BY count DESC,action", (cutoff,)).fetchall()
        loose = [
            (action, count) for action, count in conn.execute(
                DECISION_ACTIONS_12H, (cutoff,)).fetchall() if count
        ]
        loose.sort(key=lambda kv: (-kv[1], kv[0]))
        assert [tuple(row) for row in grouped] == loose, cutoff

    plan = " / ".join(
        str(row[-1]) for row in conn.execute(
            "EXPLAIN QUERY PLAN " + DECISION_ACTIONS_12H, (0,)))
    assert "SCAN decisions" not in plan
    assert "SEARCH" in plan
    store.close()


def test_source_version_sql_touches_no_large_table_body():
    sql = source_version_sql().lower()
    # The large tables contribute only a maximum row id, never a scan.
    for table in ("decisions", "maker_observations", "event_buckets"):
        assert f"from {table}" in sql
        assert f"count(*) from {table}" not in sql
    assert "count(*) from pnl_records" in sql


# -- reuse and invalidation ---------------------------------------------------


def test_unchanged_source_within_max_age_reuses_without_rebuilding(cache):
    build = Counter()
    first = cache.resolve("performance", now_ms=1_000, version="v1",
                          build=build, max_age_ms=30_000)
    second = cache.resolve("performance", now_ms=6_000, version="v1",
                           build=build, max_age_ms=30_000)
    assert build.calls == 1
    assert first.cache_hit is False and first.refresh_reason == "first"
    assert second.cache_hit is True
    assert second.refresh_reason == "unchanged_source"
    assert second.value is first.value


def test_changed_source_version_invalidates_the_section(cache):
    build = Counter()
    cache.resolve("performance", now_ms=1_000, version="v1", build=build,
                  max_age_ms=30_000)
    state = cache.resolve("performance", now_ms=2_000, version="v2",
                          build=build, max_age_ms=30_000)
    assert build.calls == 2
    assert state.cache_hit is False
    assert state.refresh_reason == "source_changed"


def test_max_age_forces_a_refresh_even_with_an_unchanged_version(cache):
    build = Counter()
    cache.resolve("execution", now_ms=1_000, version="v1", build=build,
                  max_age_ms=20_000)
    state = cache.resolve("execution", now_ms=21_000, version="v1",
                          build=build, max_age_ms=20_000)
    assert build.calls == 2
    assert state.refresh_reason == "max_age"
    assert state.age_ms == 0


def test_cache_keys_do_not_collide_across_sections(cache):
    a, b = Counter({"a": 1}), Counter({"b": 2})
    cache.resolve("performance", now_ms=1_000, version="same", build=a)
    cache.resolve("legacy_performance", now_ms=1_000, version="same", build=b)
    again = cache.resolve("performance", now_ms=1_100, version="same",
                          build=a)
    assert again.value == {"a": 1}
    assert cache.resolve("legacy_performance", now_ms=1_100, version="same",
                         build=b).value == {"b": 2}
    assert a.calls == 1 and b.calls == 1


def test_a_critical_section_is_never_reused(cache):
    build = Counter()
    for tick in (1_000, 1_100, 1_200):
        state = cache.resolve("ledger", now_ms=tick, version="v1",
                              build=build, tier=TIER_CRITICAL)
        assert state.cache_hit is False
        assert state.refresh_reason == "critical_always_refresh"
    assert build.calls == 3
    assert cache.counters()["cache_hits"] == 0


# -- honesty ------------------------------------------------------------------


def test_a_cached_section_reports_its_true_source_timestamp_and_age(cache):
    build = Counter()
    cache.resolve("performance", now_ms=10_000, version="v1", build=build,
                  max_age_ms=30_000)
    state = cache.resolve("performance", now_ms=22_500, version="v1",
                          build=build, max_age_ms=30_000)
    assert state.source_as_of_ms == 10_000
    assert state.generated_at_ms == 22_500
    assert state.age_ms == 12_500
    assert state.provenance()["source_as_of_ms"] == 10_000
    assert state.provenance()["age_ms"] == 12_500


def test_a_fresh_section_reports_zero_age(cache):
    state = cache.resolve("performance", now_ms=10_000, version="v1",
                          build=Counter(), max_age_ms=30_000)
    assert state.age_ms == 0
    assert state.source_as_of_ms == state.generated_at_ms == 10_000


def test_a_failed_refresh_retains_the_last_value_marked_stale(cache):
    good = Counter({"n": 1})
    cache.resolve("rejects", now_ms=1_000, version="v1", build=good,
                  max_age_ms=20_000)

    def boom():
        raise RuntimeError("database is locked")

    state = cache.resolve("rejects", now_ms=25_000, version="v2", build=boom,
                          max_age_ms=20_000)
    assert state.available is True
    assert state.stale is True
    assert state.value == {"n": 1}
    assert state.refresh_reason == "refresh_failed_retained_stale"
    assert state.source_as_of_ms == 1_000
    assert state.age_ms == 24_000
    assert "RuntimeError" in (state.error or "")


def test_an_excessively_stale_optional_section_is_dropped_not_served(cache):
    cache.resolve("sources", now_ms=1_000, version="v1", build=Counter(),
                  max_age_ms=20_000, required=False, empty=list)

    def boom():
        raise RuntimeError("still locked")

    # Past stale_limit_ms (4x max_age by default) the value is not served.
    state = cache.resolve("sources", now_ms=1_000 + 80_001, version="v2",
                          build=boom, max_age_ms=20_000, required=False,
                          empty=list)
    assert state.available is False
    assert state.value == []
    assert state.refresh_reason == "refresh_failed_unavailable"
    # The dropped entry must not resurrect on the next failure either.
    state = cache.resolve("sources", now_ms=1_000 + 80_002, version="v2",
                          build=boom, max_age_ms=20_000, required=False,
                          empty=list)
    assert state.available is False


def test_an_excessively_stale_required_section_fails_closed(cache):
    cache.resolve("rejects", now_ms=1_000, version="v1", build=Counter(),
                  max_age_ms=20_000)

    def boom():
        raise RuntimeError("still locked")

    # Inside the stale limit the last value stands in, explicitly marked.
    state = cache.resolve("rejects", now_ms=1_000 + 40_000, version="v2",
                          build=boom, max_age_ms=20_000)
    assert state.stale is True and state.available is True
    # Past it there is nothing honest left, so the export fails closed.
    with pytest.raises(RuntimeError, match="still locked"):
        cache.resolve("rejects", now_ms=1_000 + 80_001, version="v2",
                      build=boom, max_age_ms=20_000)


def test_a_required_section_with_no_prior_value_fails_closed(cache):
    def boom():
        raise RuntimeError("first read failed")

    with pytest.raises(RuntimeError, match="first read failed"):
        cache.resolve("performance", now_ms=1_000, version="v1", build=boom)


def test_a_failed_critical_section_fails_closed_by_propagating(cache):
    def boom():
        raise RuntimeError("no ledger")

    with pytest.raises(RuntimeError):
        cache.resolve("ledger", now_ms=1_000, version="v1", build=boom,
                      tier=TIER_CRITICAL)


# -- resolver -----------------------------------------------------------------


def test_resolver_without_a_cache_always_builds_fresh():
    build = Counter()
    for tick in (1_000, 2_000, 3_000):
        resolver = SectionResolver(None, components={"pnl_max": 1},
                                   now_ms=tick)
        value = resolver.section("performance", build=build, groups=())
        assert value is build.value
        assert resolver.states["performance"].refresh_reason == "uncached"
        assert resolver.states["performance"].cache_hit is False
    assert build.calls == 3


def test_resolver_reuses_across_builds_when_the_version_holds(cache):
    build = Counter()
    components = {"decision_max": 10, "maker_max": 5}
    for tick in (1_000, 3_000, 5_000):
        resolver = SectionResolver(cache, components=components, now_ms=tick)
        resolver.section("execution", build=build, groups=("decisions", "maker"),
                         max_age_ms=20_000)
    assert build.calls == 1

    resolver = SectionResolver(
        cache, components={"decision_max": 11, "maker_max": 5}, now_ms=7_000)
    resolver.section("execution", build=build, groups=("decisions", "maker"),
                     max_age_ms=20_000)
    assert build.calls == 2


def test_an_unestablished_version_never_authorizes_reuse(cache):
    build = Counter()
    for tick in (1_000, 2_000, 3_000):
        resolver = SectionResolver(cache, components={}, now_ms=tick)
        resolver.section("performance", build=build,
                         groups=("trade_evidence",), max_age_ms=60_000)
    assert build.calls == 3


def test_resolver_composes_a_dependent_section_version(cache):
    frequency, gate = Counter({"f": 1}), Counter({"g": 1})
    components = {"bucket_max": 1, "funnel_rows": 1, "funnel_updated": 1,
                  "entry_max": 1, "entry_rows": 1, "entry_unresolved": 0,
                  "pnl_max": 1, "pnl_rows": 1, "pnl_flags": 1, "exit_max": 1}
    resolver = SectionResolver(cache, components=components, now_ms=1_000)
    resolver.section("frequency", build=frequency, groups=("event_buckets",))
    resolver.section("acceptance_gate", build=gate, groups=("trade_evidence",),
                     extra_version=resolver.version_of("frequency"))
    assert gate.calls == 1

    # The gate's own sources are unchanged, but frequency moved: the gate must
    # not be reused against a newer generation of its inputs.
    moved = dict(components, bucket_max=2)
    resolver = SectionResolver(cache, components=moved, now_ms=2_000)
    resolver.section("frequency", build=frequency, groups=("event_buckets",))
    resolver.section("acceptance_gate", build=gate, groups=("trade_evidence",),
                     extra_version=resolver.version_of("frequency"))
    assert gate.calls == 2


# -- summary ------------------------------------------------------------------


def test_summary_reports_completeness_and_the_oldest_section(cache):
    resolver = SectionResolver(cache, components={"reject_max": 1},
                               now_ms=1_000)
    resolver.section("rejects", build=Counter(), groups=("rejects",),
                     max_age_ms=20_000)
    resolver.section("performance", build=Counter(), groups=(),
                     max_age_ms=30_000)
    resolver = SectionResolver(cache, components={"reject_max": 1},
                               now_ms=13_000)
    resolver.section("rejects", build=Counter(), groups=("rejects",),
                     max_age_ms=20_000)
    resolver.section("performance", build=Counter(), groups=(),
                     max_age_ms=30_000)
    summary = resolver.summary()
    assert summary["complete"] is True
    assert summary["cache_hits"] == 2
    assert summary["max_section_age_ms"] == 12_000
    assert summary["stale_sections"] == []
    assert summary["unavailable_sections"] == []
    assert set(summary["sections"]) == {"rejects", "performance"}
    assert summary["sections"]["rejects"]["cache_hit"] is True


def test_summary_marks_an_unavailable_optional_section_incomplete(cache):
    def boom():
        raise RuntimeError("gone")

    resolver = SectionResolver(cache, components={}, now_ms=1_000)
    value = resolver.section("sources", build=boom, groups=())
    summary = resolver.summary()
    assert value == []  # degraded to an empty shape, never fabricated
    assert summary["complete"] is False
    assert summary["unavailable_sections"] == ["sources"]
    assert summary["degraded_sections"] == ["sources"]


def test_the_resolver_fails_a_required_section_closed(cache):
    def boom():
        raise RuntimeError("gone")

    resolver = SectionResolver(cache, components={}, now_ms=1_000)
    with pytest.raises(RuntimeError, match="gone"):
        resolver.section("performance", build=boom, groups=("trade_evidence",))


def test_required_and_optional_sections_are_declared_consistently():
    from lite_frequency_v4.export_cache import EMPTY_SECTION, REQUIRED_SECTIONS

    assert REQUIRED_SECTIONS <= set(SECTION_POLICY)
    assert set(EMPTY_SECTION) <= set(SECTION_POLICY)
    # Every section is exactly one of required or optional-with-a-shape.
    assert REQUIRED_SECTIONS.isdisjoint(EMPTY_SECTION)
    assert REQUIRED_SECTIONS | set(EMPTY_SECTION) == set(SECTION_POLICY)


def test_summary_is_json_safe(cache):
    resolver = SectionResolver(cache, components={"reject_max": 1},
                               now_ms=1_000)
    resolver.section("rejects", build=Counter(), groups=("rejects",))
    json.dumps(resolver.summary(), allow_nan=False)


def test_summarize_handles_an_empty_build():
    summary = summarize({}, now_ms=42)
    assert summary == {
        "export_generated_at_ms": 42, "complete": True, "sections": {},
        "cache_hits": 0, "refreshed": 0, "max_section_age_ms": 0,
        "stale_sections": [], "degraded_sections": [],
        "unavailable_sections": [],
    }


def test_counters_track_hits_refreshes_and_degraded_reuse(cache):
    build = Counter()
    cache.resolve("rejects", now_ms=1_000, version="v1", build=build,
                  max_age_ms=20_000)
    cache.resolve("rejects", now_ms=2_000, version="v1", build=build,
                  max_age_ms=20_000)

    def boom():
        raise RuntimeError("x")

    cache.resolve("rejects", now_ms=25_000, version="v1", build=boom,
                  max_age_ms=20_000)
    counters = cache.counters()
    assert counters["refreshes"] == 1
    assert counters["cache_hits"] == 1
    assert counters["degraded_reuses"] == 1
    assert counters["tracked_sections"] == 1


def test_clear_empties_the_cache_and_the_next_build_refreshes(cache):
    build = Counter()
    cache.resolve("rejects", now_ms=1_000, version="v1", build=build)
    cache.clear()
    state = cache.resolve("rejects", now_ms=1_100, version="v1", build=build)
    assert build.calls == 2
    assert state.refresh_reason == "first"
    assert cache.counters()["cache_hits"] == 0
