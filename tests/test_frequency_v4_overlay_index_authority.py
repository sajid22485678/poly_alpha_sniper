"""The overlay-index contract, and that nothing may join it unratified.

``_PERFORMANCE_INDEXES`` creates indexes on every writable open, outside
``SCHEMA_SQL``, outside every migration and outside the managed-V5 census.  Six
of them are declared nowhere else, so nothing attested them: the 2026-07-25
base-schema authority pins ``SCHEMA_SQL`` and requires a resolution for changes
*to it*, and says nothing about objects created beside it.  Five of those six
predate that ratification; the sixth is ``ix_cex_latest_by_key``.

They were ratified together on 2026-08-02 as additive, non-unique, non-partial,
performance-only, semantics-preserving objects.  The ratification is explicitly
not blanket authority for the mechanism, and these tests are what makes that
true rather than merely stated.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from poly_alpha_sniper.lite_frequency_v4 import store as store_module
from poly_alpha_sniper.lite_frequency_v4.store import (
    V4_OVERLAY_INDEX_AUTHORITY_COUNT,
    V4_OVERLAY_INDEX_AUTHORITY_SHA256,
    V4_RATIFIED_OVERLAY_ONLY_INDEXES,
    V4SchemaError,
    V4Store,
    _declared_indexes,
    overlay_index_fingerprint,
    overlay_index_records,
    verify_overlay_index_authority,
)


OVERLAY = V4Store._PERFORMANCE_INDEXES


def test_the_shipped_overlay_is_exactly_what_was_ratified():
    verify_overlay_index_authority(OVERLAY)
    assert len(OVERLAY) == V4_OVERLAY_INDEX_AUTHORITY_COUNT
    assert overlay_index_fingerprint(OVERLAY) == V4_OVERLAY_INDEX_AUTHORITY_SHA256


def test_the_six_ratified_objects_are_the_ones_declared_nowhere_else():
    """The ratified set must be derived from reality, not asserted alongside it."""

    names = {record["name"] for record in overlay_index_records(OVERLAY)}
    assert names - _declared_indexes() == V4_RATIFIED_OVERLAY_ONLY_INDEXES
    assert V4_RATIFIED_OVERLAY_ONLY_INDEXES == {
        "ix_candidates_retention",
        "ix_cex_latest_by_key",
        "ix_latency_candidate",
        "ix_maker_observations_candidate",
        "ix_rejects_candidate",
        "ix_retention_runs_time",
    }
    # And the other five really are declared in SCHEMA_SQL or a migration, so
    # the ratification is not quietly covering something broader than it says.
    assert names & _declared_indexes() == {
        "ix_cex_retention",
        "ix_books_retention",
        "ix_candidate_cex_evidence_obs",
        "ix_candidate_book_evidence_snap",
        "ix_candidates_trigger_source",
    }


def test_every_ratified_object_is_additive_non_unique_and_non_partial():
    for record, statement in zip(overlay_index_records(OVERLAY), OVERLAY):
        assert statement.startswith("CREATE INDEX IF NOT EXISTS ")
        assert "UNIQUE" not in statement.upper()
        assert " WHERE " not in f" {statement.upper()} "
        assert record["table"]
        assert record["name"] in statement


def test_ix_cex_latest_by_key_ddl_is_pinned_verbatim():
    """The object the audit named, at the shape it was ratified in."""

    by_name = {r["name"]: r["sql"] for r in overlay_index_records(OVERLAY)}
    assert by_name["ix_cex_latest_by_key"] == (
        "CREATE INDEX IF NOT EXISTS ix_cex_latest_by_key "
        "ON cex_observations ( session_id , provider , instrument , "
        "provider_ts_ms DESC , cex_observation_id DESC )"
    )
    # It indexes the read it exists to serve, in the order that read asks for.
    # If that query ever stops asking for this order the index stops being
    # semantics-neutral cover for it, and the pin above stops meaning anything.
    source = Path(store_module.__file__).read_text(encoding="utf-8")
    assert (
        "ORDER BY provider_ts_ms DESC,cex_observation_id DESC LIMIT 1"
        in source)


def test_an_unratified_addition_fails_closed():
    """Future additions need their own resolution -- enforced, not requested."""

    with pytest.raises(V4SchemaError, match="new human resolution is required"):
        verify_overlay_index_authority(
            tuple(OVERLAY)
            + ("CREATE INDEX IF NOT EXISTS ix_unratified "
               "ON candidates(window_id)",))


def test_a_removal_fails_closed():
    with pytest.raises(V4SchemaError, match="expected 11 statements"):
        verify_overlay_index_authority(tuple(OVERLAY)[:-1])


def test_an_edit_to_a_ratified_object_fails_closed():
    """Same count, same names, different definition: still not what was approved."""

    edited = tuple(
        statement.replace(
            "ON cex_observations(session_id,provider,instrument,"
            "provider_ts_ms DESC,cex_observation_id DESC)",
            "ON cex_observations(session_id,provider,instrument,"
            "provider_ts_ms ASC,cex_observation_id DESC)")
        for statement in OVERLAY
    )
    assert edited != tuple(OVERLAY)
    with pytest.raises(V4SchemaError, match="fingerprint"):
        verify_overlay_index_authority(edited)


def test_a_unique_or_partial_overlay_index_is_refused_by_kind():
    """Those change insertion or conflict semantics, not only performance."""

    with pytest.raises(V4SchemaError, match="CREATE INDEX IF NOT EXISTS"):
        verify_overlay_index_authority(
            tuple(OVERLAY)[:-1]
            + ("CREATE UNIQUE INDEX IF NOT EXISTS ix_latency_candidate "
               "ON latency_metrics(candidate_id)",))

    partial = tuple(OVERLAY)[:-1] + (
        "CREATE INDEX IF NOT EXISTS ix_latency_candidate "
        "ON latency_metrics(candidate_id) WHERE candidate_id IS NOT NULL",)
    with pytest.raises(V4SchemaError, match="non-partial"):
        verify_overlay_index_authority(partial)


def test_a_duplicate_definition_fails_closed():
    with pytest.raises(V4SchemaError, match="duplicate definitions"):
        verify_overlay_index_authority(tuple(OVERLAY)[:-1] + (OVERLAY[0],))


def test_a_real_store_open_creates_exactly_the_ratified_objects(tmp_path):
    """End to end: the objects in a fresh database are the ratified ones."""

    store = V4Store(tmp_path / "overlay.db")
    try:
        live = {
            str(row["name"]) for row in store.query(
                "SELECT name FROM sqlite_schema WHERE type='index' "
                "AND sql IS NOT NULL")
        }
        assert V4_RATIFIED_OVERLAY_ONLY_INDEXES <= live
        # Nothing outside SCHEMA_SQL, the migrations and the ratified six.
        assert not (live - _declared_indexes()
                    - V4_RATIFIED_OVERLAY_ONLY_INDEXES)
        # Idempotent: re-running the overlay changes nothing.
        before = store.query(
            "SELECT name,sql FROM sqlite_schema WHERE type='index' "
            "ORDER BY name")
        store._ensure_performance_indexes()
        assert store.query(
            "SELECT name,sql FROM sqlite_schema WHERE type='index' "
            "ORDER BY name") == before
    finally:
        store.close()


def test_an_unratified_overlay_can_never_reach_a_database(tmp_path, monkeypatch):
    """The check guards the write, not just the test suite."""

    monkeypatch.setattr(
        V4Store, "_PERFORMANCE_INDEXES",
        tuple(OVERLAY) + ("CREATE INDEX IF NOT EXISTS ix_smuggled "
                          "ON candidates(window_id)",))
    with pytest.raises(V4SchemaError):
        V4Store(tmp_path / "refused.db")
