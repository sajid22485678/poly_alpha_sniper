from poly_alpha_sniper.lite_frequency_v4.replay import replay_candidate, replay_entries
from poly_alpha_sniper.lite_frequency_v4.store import V4Store

from .test_frequency_v4_store import (
    create_entry,
    entry_payload,
    seed_candidate_entry_context,
    seed_market_window,
    seed_session,
)


def test_persisted_candidate_replay_is_stable_and_complete(tmp_path):
    store = V4Store(tmp_path / "replay.db")
    try:
        seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        create_entry(store, entry_payload(context, evidence))

        first = replay_candidate(store, evidence["candidate_id"])
        second = replay_candidate(store, evidence["candidate_id"])
        assert first.valid is True
        assert first.deterministic_hash == second.deterministic_hash
        assert first.coherent_probability is True
        assert first.economic_formula_valid is True
        assert first.selected_side_valid is True
        assert replay_entries(store) == {
            "entries": 1,
            "valid": 1,
            "invalid": 0,
            "deterministic_hashes": [first.deterministic_hash],
            "errors": [],
        }
    finally:
        store.close()


def test_replay_fails_closed_on_economic_evidence_drift(tmp_path):
    store = V4Store(tmp_path / "replay-drift.db")
    try:
        seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        with store.transaction(immediate=True) as conn:
            conn.execute(
                """UPDATE fair_value_sides SET net_edge=net_edge+0.001
                   WHERE fair_value_calculation_id=?""",
                (evidence["fair_id"],),
            )
        result = replay_candidate(store, evidence["candidate_id"])
        assert result.valid is False
        assert result.economic_formula_valid is False
        assert any(error.startswith("net_edge_formula_mismatch")
                   for error in result.errors)
    finally:
        store.close()
