"""Phase 2B C1 cluster arbitration regression tests.

Hermetic: every test builds a scratch managed-v5 database under tmp_path
and exercises the arbitration state machine, cluster occupancy, candidate
identity/ranking, and replay idempotency.  No production path is touched.
"""
import sqlite3

import pytest

from poly_alpha_sniper.lite_frequency_v4 import cluster as C
from poly_alpha_sniper.lite_frequency_v4.store import V4Store


COHORT = "dynamic_universe_phase1_post_activation"
SESSION = "session-cluster-test"
NONCE = "abcdef0123456789abcdef0123456789"


def _store(tmp_path):
    store = V4Store(tmp_path / "cluster.db")
    # Seed the FK targets the managed-v5 arbitration tables reference: one
    # authoritative cohort and one runtime session.  Uses the store's own
    # write path so the cohort/session rows are schema-valid.
    with store.transaction() as conn:
        conn.execute(
            "INSERT INTO cohorts(cohort, activation_ts_ms, starting_equity_usd, "
            "max_exposure_pct, fixed_shares, authoritative, peak_committed_usd, "
            "peak_exposure_pct, created_ts_ms) VALUES(?,?,?,?,?,?,?,?,?)",
            (COHORT, 0, 13.0, 1.0, 5.0, 1, 0.0, 0.0, 0))
        conn.execute(
            "INSERT INTO runtime_sessions(session_id, strategy_id, mode, launch_nonce, "
            "pid, git_commit, config_hash, started_ts_ms, cohort) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (SESSION, "lite_frequency_v4", "lite_frequency_v4_shadow", NONCE,
             12345, "0" * 40, "c" * 64, 0, COHORT))
        # Seed asset_windows rows for the window_ids the tests reference via
        # selected_window_id / cluster_locks.window_id (FK targets).
        for wid, asset in ((1, "ETH"), (2, "BTC"), (3, "SOL"), (4, "ADA")):
            conn.execute(
                "INSERT INTO asset_windows(window_id, asset, window_open_ts_ms, "
                "window_close_ts_ms, created_ts_ms, updated_ts_ms) VALUES(?,?,?,?,?,?)",
                (wid, asset, 10_000, 310_000, 0, 0))
    return store


def _reopen(tmp_path):
    """Reopen an already-seeded store without re-seeding.

    ``_store`` re-INSERTs the authoritative cohort; calling it twice on the
    same path collides with the ``ix_cohorts_single_authoritative`` partial
    unique index (correct C1.A behaviour).  Reopening must construct the
    store only.
    """
    return V4Store(tmp_path / "cluster.db")


def _id(cluster_ts=10_000):
    return C.ArbitrationIdentity(cohort=COHORT, cluster_open_ts_ms=cluster_ts)


def _candidate(asset="ETH", window_id=1, net_edge=0.02, depth=3.0, side="YES", prob=0.6):
    return C.Candidate(
        asset=asset, window_id=window_id, selected_side=side,
        fair_probability=prob, net_edge=net_edge, depth=depth)


def _begin(store, identity, *, fp, ts=1):
    with store.transaction() as conn:
        return C.begin_arbitration(
            conn, identity, arbitration_started_ts_ms=ts,
            arbitration_cutoff_ts_ms=ts + 5_000, candidate_set_fingerprint=fp,
            actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=ts)


def _prepare_mark_entered(
        store, identity, selected, *, lock_candidate=None,
        lock_window_id=None, promote=True,
):
    """Persist the select -> reserve -> lock path used by ENTERED tests."""
    lock_candidate = selected if lock_candidate is None else lock_candidate
    lock_window_id = (
        selected.window_id if lock_window_id is None else lock_window_id
    )
    _begin(store, identity, fp=_fp([selected]))
    with store.transaction() as conn:
        C.select_candidate(
            conn, identity, candidate=selected, candidate_rank=1, reason="selected",
            actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2,
        )
        C.begin_reserving(
            conn, identity, reason="reserving", actor=C.ACTOR_ENGINE,
            session_id=SESSION, event_ts_ms=3,
        )
        C.reserve_cluster_lock(
            conn, identity, window_id=lock_window_id, candidate=lock_candidate,
            session_id=SESSION, owner_launch_nonce=NONCE,
            idempotency_key="mark-entered-lock", now_ms=4,
        )
        if promote:
            C.promote_lock_to_entered(conn, identity, now_ms=5)


# --- candidate identity / ranking ----------------------------------------

def test_candidate_key_is_content_derived_and_deterministic():
    a = _candidate()
    b = _candidate()
    assert C.candidate_key(a) == C.candidate_key(b)
    assert C.candidate_key(a) != C.candidate_key(_candidate(net_edge=0.021))


def test_candidate_key_independent_of_object_identity_and_order():
    cs = [_candidate("ETH", 1), _candidate("BTC", 2, net_edge=0.03)]
    assert C.candidate_set_fingerprint(cs) == C.candidate_set_fingerprint(list(reversed(cs)))


def test_nine_decimal_canonicalization():
    # Two genuinely distinct numbers that differ only below the 9th decimal:
    # both round to 0.020000000 under fixed nine-decimal formatting.
    a = _candidate(net_edge=0.0200000001)
    b = _candidate(net_edge=0.0200000004)
    # nine-decimal formatting collapses sub-1e-9 differences
    assert C.candidate_key(a) == C.candidate_key(b)


def test_non_finite_values_rejected():
    for bad in (float("nan"), float("inf"), -float("inf")):
        with pytest.raises(C.ClusterArbitrationError):
            C.candidate_key(_candidate(net_edge=bad))


def test_ranking_net_edge_then_depth_then_asset_then_key():
    cs = [
        _candidate("ETH", 1, net_edge=0.01, depth=9.0),
        _candidate("BTC", 2, net_edge=0.02, depth=3.0),
        _candidate("SOL", 3, net_edge=0.02, depth=3.0),  # ties BTC; asset asc -> BTC first
        _candidate("ADA", 4, net_edge=0.02, depth=5.0),
    ]
    ranked = C.rank_candidates(cs)
    order = [c.asset for _, c in ranked]
    assert order == ["ADA", "BTC", "SOL", "ETH"]
    assert [r for r, _ in ranked] == [1, 2, 3, 4]


def test_ranking_quantizes_to_1e_6():
    # differences below the 1e-6 quantum are tied
    cs = [_candidate("A", 1, net_edge=0.0200001), _candidate("B", 2, net_edge=0.0200002)]
    ranked = C.rank_candidates(cs)
    # within quantum -> tie broken by asset asc
    assert [c.asset for _, c in ranked] == ["A", "B"]


# --- arbitration state machine -------------------------------------------

def _fp(candidates):
    return C.candidate_set_fingerprint(candidates)


def test_first_event_is_none_to_collecting(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        state = _begin(store, identity, fp=_fp([_candidate()]), ts=1)
        assert state.status == C.COLLECTING
        assert state.events[0].seq == 1
        assert (state.events[0].from_status, state.events[0].to_status) == (C.NONE, C.COLLECTING)
        assert state.events[0].event_ts_ms == 1
    finally:
        store.close()


def test_begin_is_idempotent_for_same_fingerprint(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        fp = _fp([_candidate()])
        s1 = _begin(store, identity, fp=fp)
        s2 = _begin(store, identity, fp=fp)
        assert s1.events == s2.events
    finally:
        store.close()


def test_begin_divergent_fingerprint_rejected(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        _begin(store, identity, fp=_fp([_candidate()]))
        with pytest.raises(C.ClusterArbitrationError):
            _begin(store, identity, fp=_fp([_candidate("BTC", 2)]))
    finally:
        store.close()


def test_every_legal_transition(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="selected",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2_000)
            C.begin_reserving(conn, identity, reason="reserving",
                              actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=3_000)
            # reserve + enter the lock
            C.reserve_cluster_lock(conn, identity, window_id=cand.window_id,
                                   candidate=cand, session_id=SESSION,
                                   owner_launch_nonce=NONCE, idempotency_key="k1", now_ms=3_500)
            C.promote_lock_to_entered(conn, identity, now_ms=4_000)
            state = C.mark_entered(conn, identity, entered_entry_id=None,
                                   reason="entered", actor=C.ACTOR_ENGINE,
                                   session_id=SESSION, event_ts_ms=4_000)
        assert state.status == C.ENTERED
        seqs = [e.seq for e in state.events]
        assert seqs == [1, 2, 3, 4]
    finally:
        store.close()


@pytest.mark.parametrize("from_status, to_status", [
    (C.NONE, C.SELECTED),
    (C.COLLECTING, C.RESERVING),
    (C.COLLECTING, C.ENTERED),
    (C.SELECTED, C.COLLECTING),
    (C.RESERVING, C.CANCELLED),
    (C.RESERVING, C.EXHAUSTED),
    (C.ENTERED, C.SELECTED),
    (C.EXHAUSTED, C.SELECTED),
])
def test_illegal_transitions_rejected(tmp_path, from_status, to_status):
    # Build a *valid* history that legitimately lands in from_status by
    # replaying legal transitions through the public API, then attempt the
    # illegal one.  Direct fixture INSERTs were broken three ways:
    #   - VALUES(?,?,?,?,?,?,?,?) had 8 placeholders for 9 columns;
    #   - status='NONE' is rejected by the cluster_arbitrations CHECK (NONE
    #     is the absence of a row, not a persistable status);
    #   - ENTERED/EXHAUSTED are terminal and can never be a legal from_status.
    # The transition guard itself is correct; these cases must refuse at the
    # intended gate with the exact reason.
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        if from_status != C.NONE:
            # Drive the arbitration to from_status via the legal API.
            with store.transaction() as conn:
                C.begin_arbitration(
                    conn, identity, arbitration_started_ts_ms=1,
                    arbitration_cutoff_ts_ms=6_000, candidate_set_fingerprint=_fp([cand]),
                    actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=1)
                if from_status in (C.SELECTED, C.RESERVING, C.ENTERED, C.EXHAUSTED):
                    C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="selected",
                                       actor=C.ACTOR_ENGINE, session_id=SESSION,
                                       event_ts_ms=2_000)
                if from_status in (C.RESERVING, C.ENTERED):
                    C.begin_reserving(conn, identity, reason="reserving",
                                      actor=C.ACTOR_ENGINE, session_id=SESSION,
                                      event_ts_ms=3_000)
                    C.reserve_cluster_lock(conn, identity, window_id=cand.window_id,
                                           candidate=cand, session_id=SESSION,
                                           owner_launch_nonce=NONCE, idempotency_key="k1",
                                           now_ms=3_500)
                if from_status == C.ENTERED:
                    C.promote_lock_to_entered(conn, identity, now_ms=4_000)
                    C.mark_entered(conn, identity, entered_entry_id=None,
                                   reason="entered", actor=C.ACTOR_ENGINE,
                                   session_id=SESSION, event_ts_ms=4_000)
                if from_status == C.EXHAUSTED:
                    C.mark_exhausted(conn, identity, reason="none_left",
                                     actor=C.ACTOR_ENGINE, session_id=SESSION,
                                     event_ts_ms=4_000)
        # Now attempt the illegal transition and assert the exact refusal.
        with store.transaction() as conn:
            with pytest.raises(C.ClusterArbitrationError, match=_expected_refusal(from_status)):
                C._advance(conn, identity, to_status=to_status, reason="illegal",
                           actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=9_000)
    finally:
        store.close()


def _expected_refusal(from_status: str) -> str:
    # NONE -> no arbitration row exists; every other case is the transition
    # guard (illegal pair) or the terminal-status refusal.
    if from_status == C.NONE:
        return "no arbitration for"
    if from_status in (C.ENTERED, C.EXHAUSTED):
        return f"terminal arbitration {from_status} cannot transition"
    return "illegal arbitration transition"


def test_terminal_states_never_transition(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="s",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            C.mark_exhausted(conn, identity, reason="none", actor=C.ACTOR_ENGINE,
                             session_id=SESSION, event_ts_ms=3)
            with pytest.raises(C.ClusterArbitrationError):
                C._advance(conn, identity, to_status=C.SELECTED, reason="x",
                           actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=4)
    finally:
        store.close()


def test_monotonic_unique_event_sequence(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="s",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            C.begin_reserving(conn, identity, reason="r", actor=C.ACTOR_ENGINE,
                              session_id=SESSION, event_ts_ms=3)
            state = C.load_arbitration(conn, identity)
        seqs = [e.seq for e in state.events]
        assert seqs == sorted(set(seqs)) == [1, 2, 3]
    finally:
        store.close()


def test_one_arbitration_per_cluster_and_one_lock_per_cluster(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="s",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            C.begin_reserving(conn, identity, reason="r", actor=C.ACTOR_ENGINE,
                              session_id=SESSION, event_ts_ms=3)
            C.reserve_cluster_lock(conn, identity, window_id=cand.window_id,
                                   candidate=cand, session_id=SESSION,
                                   owner_launch_nonce=NONCE, idempotency_key="k", now_ms=4)
            # a second lock row for the same cluster is rejected by the PK
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO cluster_locks(cohort, cluster_open_ts_ms, window_id,"
                    "candidate_key, session_id, owner_launch_nonce, state, idempotency_key,"
                    "reserved_ts_ms, updated_ts_ms) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (COHORT, identity.cluster_open_ts_ms, 99, "other", SESSION, NONCE,
                     "RESERVED", "otherkey", 5, 5))
    finally:
        store.close()


def test_reserved_lock_released_when_unentered(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="s",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            C.reserve_cluster_lock(conn, identity, window_id=cand.window_id,
                                   candidate=cand, session_id=SESSION,
                                   owner_launch_nonce=NONCE, idempotency_key="k", now_ms=3)
            assert C._load_lock(conn, identity) is not None
            C.release_unentered_lock(conn, identity, now_ms=4)
            assert C._load_lock(conn, identity) is None
    finally:
        store.close()


def test_entered_lock_permanent_cannot_release(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.reserve_cluster_lock(conn, identity, window_id=cand.window_id,
                                   candidate=cand, session_id=SESSION,
                                   owner_launch_nonce=NONCE, idempotency_key="k", now_ms=2)
            C.promote_lock_to_entered(conn, identity, now_ms=3)
            with pytest.raises(C.ClusterArbitrationError):
                C.release_unentered_lock(conn, identity, now_ms=4)
    finally:
        store.close()


def test_no_fallback_after_entered(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="s",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            C.begin_reserving(conn, identity, reason="r", actor=C.ACTOR_ENGINE,
                              session_id=SESSION, event_ts_ms=3)
            C.reserve_cluster_lock(conn, identity, window_id=cand.window_id,
                                   candidate=cand, session_id=SESSION,
                                   owner_launch_nonce=NONCE, idempotency_key="k", now_ms=4)
            C.promote_lock_to_entered(conn, identity, now_ms=5)
            C.mark_entered(conn, identity, entered_entry_id=None, reason="entered",
                           actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=6)
            # After ENTERED, reserving a different candidate must fail.
            other = _candidate("BTC", 2)
            with pytest.raises(C.ClusterArbitrationError):
                C.reserve_cluster_lock(conn, identity, window_id=other.window_id,
                                       candidate=other, session_id=SESSION,
                                       owner_launch_nonce=NONCE, idempotency_key="k2",
                                       now_ms=7)
    finally:
        store.close()


def test_reserving_to_selected_fallback_allows_repick(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="s",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            C.begin_reserving(conn, identity, reason="r", actor=C.ACTOR_ENGINE,
                              session_id=SESSION, event_ts_ms=3)
            state = C.revert_to_selected(conn, identity, reason="rejected",
                                         actor=C.ACTOR_ENGINE, session_id=SESSION,
                                         event_ts_ms=4)
            assert state.status == C.SELECTED
    finally:
        store.close()


def test_shutdown_preserves_collecting(tmp_path):
    # An arbitration left in COLLECTING survives a "shutdown" (no code mutates
    # it on close).  Reopen and confirm the row is intact.
    store = _store(tmp_path)
    identity = _id()
    try:
        _begin(store, identity, fp=_fp([_candidate()]))
    finally:
        store.close()
    store2 = _reopen(tmp_path)
    try:
        with store2.transaction() as conn:
            state = C.load_arbitration(conn, identity)
        assert state is not None and state.status == C.COLLECTING
    finally:
        store2.close()


def test_duplicate_replay_idempotent(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="s",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            s1 = C.replay_arbitration(conn, identity)
            s2 = C.replay_arbitration(conn, identity)
        assert s1 == s2
    finally:
        store.close()


def test_divergent_replay_rejected(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="s",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            # Tamper: set stored status to a value the event log does not reach.
            conn.execute(
                "UPDATE cluster_arbitrations SET status=? "
                "WHERE cohort=? AND cluster_open_ts_ms=?",
                (C.ENTERED, COHORT, identity.cluster_open_ts_ms))
            with pytest.raises(C.ClusterArbitrationError):
                C.replay_arbitration(conn, identity)
    finally:
        store.close()


def test_contradictory_event_history_rejected(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        with store.transaction() as conn:
            # Stored status COLLECTING, but the event chain is internally
            # inconsistent: seq=1 is a legal NONE->COLLECTING, then seq=2 is a
            # pairwise-legal SELECTED->RESERVING whose from_status disagrees
            # with the chain's current state.  Each row passes the schema
            # CHECKs individually; only replay detects the inconsistency.
            conn.execute(
                "INSERT INTO cluster_arbitrations(cohort, cluster_open_ts_ms,"
                "arbitration_started_ts_ms, arbitration_cutoff_ts_ms, ranking_version,"
                "candidate_set_fingerprint, status, created_session_id, created_ts_ms,"
                "updated_ts_ms) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (COHORT, identity.cluster_open_ts_ms, 1, 2, 1, "fp", C.COLLECTING,
                 SESSION, 1, 1))
            conn.execute(
                "INSERT INTO cluster_arbitration_events(cohort, cluster_open_ts_ms,"
                "seq, from_status, to_status, reason, actor, session_id, event_ts_ms) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (COHORT, identity.cluster_open_ts_ms, 1, C.NONE, C.COLLECTING,
                 "start", C.ACTOR_ENGINE, SESSION, 1))
            conn.execute(
                "INSERT INTO cluster_arbitration_events(cohort, cluster_open_ts_ms,"
                "seq, from_status, to_status, reason, actor, session_id, event_ts_ms) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (COHORT, identity.cluster_open_ts_ms, 2, C.SELECTED, C.RESERVING,
                 "bad", C.ACTOR_ENGINE, SESSION, 2))
            with pytest.raises(C.ClusterArbitrationError,
                               match="event seq=2 from_status SELECTED != current COLLECTING"):
                C.replay_arbitration(conn, identity)
    finally:
        store.close()


def test_actor_vocabulary_enforced(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        with store.transaction() as conn:
            with pytest.raises(C.ClusterArbitrationError):
                C.begin_arbitration(
                    conn, identity, arbitration_started_ts_ms=1,
                    arbitration_cutoff_ts_ms=2, candidate_set_fingerprint=_fp([_candidate()]),
                    actor="BAD_ACTOR", session_id=SESSION, event_ts_ms=1)
    finally:
        store.close()


def test_cancel_only_for_non_entered(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            state = C.cancel_arbitration(
                conn, identity, reason=C.REASON_CLUSTER_WINDOW_EXPIRED,
                actor=C.ACTOR_RECOVERY, session_id=SESSION, event_ts_ms=9)
            assert state.status == C.CANCELLED
    finally:
        store.close()


def test_cancel_entered_rejected(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="s",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            C.begin_reserving(conn, identity, reason="r", actor=C.ACTOR_ENGINE,
                              session_id=SESSION, event_ts_ms=3)
            C.reserve_cluster_lock(conn, identity, window_id=cand.window_id,
                                   candidate=cand, session_id=SESSION,
                                   owner_launch_nonce=NONCE, idempotency_key="k", now_ms=4)
            C.promote_lock_to_entered(conn, identity, now_ms=5)
            C.mark_entered(conn, identity, entered_entry_id=None, reason="e",
                           actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=6)
            with pytest.raises(C.ClusterArbitrationError):
                C.cancel_arbitration(conn, identity, reason="x",
                                     actor=C.ACTOR_RECOVERY, session_id=SESSION,
                                     event_ts_ms=7)
    finally:
        store.close()


# --- regression tests for confirmed production defects --------------------
# Each pins a specific C1 fix by asserting the EXACT refusal reason (not just
# the exception type), so a future regression cannot pass for the wrong cause.


def test_regression_C1D_01_status_journal_disagree_refuses_to_mutate(tmp_path):
    """C1D-01: _advance must refuse when stored status disagrees with journal."""
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            # Force the stored status to SELECTED while the journal only
            # records NONE->COLLECTING.
            conn.execute(
                "UPDATE cluster_arbitrations SET status=? "
                "WHERE cohort=? AND cluster_open_ts_ms=?",
                (C.SELECTED, COHORT, identity.cluster_open_ts_ms))
            with pytest.raises(C.ClusterArbitrationError,
                               match="disagrees with journal terminal"):
                C._advance(conn, identity, to_status=C.RESERVING, reason="x",
                           actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=5)
            # Prove no mutation happened: the journal still has exactly one
            # event and the stored status is unchanged from the tampered value
            # (the write path must never paper over a disagreement).
            count = conn.execute(
                "SELECT COUNT(*) FROM cluster_arbitration_events "
                "WHERE cohort=? AND cluster_open_ts_ms=?",
                (COHORT, identity.cluster_open_ts_ms)).fetchone()[0]
            assert count == 1
    finally:
        store.close()


def test_regression_C1E_02_release_refused_when_arbitration_entered(tmp_path):
    """C1E-02: release_unentered_lock refuses when arbitration is ENTERED,
    even if the lock row was tampered back to RESERVED (crash window)."""
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="s",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            C.begin_reserving(conn, identity, reason="r", actor=C.ACTOR_ENGINE,
                              session_id=SESSION, event_ts_ms=3)
            C.reserve_cluster_lock(conn, identity, window_id=cand.window_id,
                                   candidate=cand, session_id=SESSION,
                                   owner_launch_nonce=NONCE, idempotency_key="k", now_ms=4)
            C.promote_lock_to_entered(conn, identity, now_ms=5)
            C.mark_entered(conn, identity, entered_entry_id=None, reason="e",
                           actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=6)
            # Tamper the lock row back to RESERVED; arbitration is still ENTERED.
            conn.execute(
                "UPDATE cluster_locks SET state='RESERVED' "
                "WHERE cohort=? AND cluster_open_ts_ms=?",
                (COHORT, identity.cluster_open_ts_ms))
            with pytest.raises(C.ClusterArbitrationError,
                               match="arbitration is ENTERED"):
                C.release_unentered_lock(conn, identity, now_ms=7)
            # Lock row must survive the refused release.
            assert C._load_lock(conn, identity) is not None
    finally:
        store.close()


def test_regression_C1E_05_mark_entered_requires_lock(tmp_path):
    """C1E-05: mark_entered refuses when no cluster_locks row exists."""
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1, reason="s",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            C.begin_reserving(conn, identity, reason="r", actor=C.ACTOR_ENGINE,
                              session_id=SESSION, event_ts_ms=3)
            # Note: NO reserve_cluster_lock / promote_lock_to_entered call.
            with pytest.raises(C.ClusterArbitrationError,
                               match="no cluster lock exists"):
                C.mark_entered(conn, identity, entered_entry_id=None, reason="e",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=4)
            # Status must remain RESERVING (not ENTERED).
            state = C.load_arbitration(conn, identity)
            assert state.status == C.RESERVING
    finally:
        store.close()


@pytest.mark.parametrize(
    "case,expected_error",
    [
        (
            "different_candidate_and_window",
            "cannot mark ENTERED: lock.window_id does not match "
            "arbitration.selected_window_id",
        ),
        (
            "same_window_different_candidate",
            "cannot mark ENTERED: lock.candidate_key does not match "
            "arbitration.selected_candidate_key",
        ),
        (
            "same_candidate_different_window",
            "cannot mark ENTERED: lock.window_id does not match "
            "arbitration.selected_window_id",
        ),
    ],
)
def test_mark_entered_refuses_mismatched_lock_without_mutation(
        tmp_path, case, expected_error,
):
    selected = _candidate("ETH", 1)
    if case == "different_candidate_and_window":
        lock_candidate = _candidate("BTC", 2)
        lock_window_id = 2
    elif case == "same_window_different_candidate":
        lock_candidate = _candidate("BTC", 1)
        lock_window_id = 1
    else:
        # reserve_cluster_lock accepts window_id separately, so this represents
        # the same candidate key incorrectly attached to another window.
        lock_candidate = selected
        lock_window_id = 2

    store = _store(tmp_path)
    try:
        identity = _id()
        _prepare_mark_entered(
            store, identity, selected, lock_candidate=lock_candidate,
            lock_window_id=lock_window_id, promote=True,
        )
        with store.transaction() as conn:
            before_state = C.load_arbitration(conn, identity)
            before_lock = C._load_lock(conn, identity)
            assert before_state is not None
            assert before_lock is not None
            assert before_state.status == C.RESERVING
            assert before_state.selected_candidate_key == C.candidate_key(selected)
            assert before_state.selected_window_id == selected.window_id
            assert before_lock.state == C.LOCK_ENTERED
            assert before_lock.candidate_key == C.candidate_key(lock_candidate)
            assert before_lock.window_id == lock_window_id
            before_event_count = len(before_state.events)

            with pytest.raises(C.ClusterArbitrationError) as exc_info:
                C.mark_entered(
                    conn, identity, entered_entry_id=None, reason="entered",
                    actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=6,
                )
            assert str(exc_info.value) == expected_error

            after_state = C.load_arbitration(conn, identity)
            after_lock = C._load_lock(conn, identity)
            assert after_state == before_state
            assert after_lock == before_lock
            assert after_state.status == C.RESERVING
            assert after_state.selected_candidate_key == C.candidate_key(selected)
            assert after_state.selected_window_id == selected.window_id
            assert len(after_state.events) == before_event_count
            assert not any(
                event.to_status == C.ENTERED for event in after_state.events
            )
    finally:
        store.close()


def test_mark_entered_refuses_reserved_lock_state_without_mutation(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        selected = _candidate("ETH", 1)
        _prepare_mark_entered(store, identity, selected, promote=False)
        with store.transaction() as conn:
            before_state = C.load_arbitration(conn, identity)
            before_lock = C._load_lock(conn, identity)
            assert before_state is not None
            assert before_lock is not None
            assert before_state.status == C.RESERVING
            assert before_lock.state == C.LOCK_RESERVED
            before_event_count = len(before_state.events)

            with pytest.raises(C.ClusterArbitrationError) as exc_info:
                C.mark_entered(
                    conn, identity, entered_entry_id=None, reason="entered",
                    actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=6,
                )
            assert str(exc_info.value) == (
                "cannot mark ENTERED: lock.state must be ENTERED (got RESERVED)"
            )

            after_state = C.load_arbitration(conn, identity)
            after_lock = C._load_lock(conn, identity)
            assert after_state == before_state
            assert after_lock == before_lock
            assert len(after_state.events) == before_event_count
            assert not any(
                event.to_status == C.ENTERED for event in after_state.events
            )
    finally:
        store.close()


def test_mark_entered_accepts_matching_entered_lock(tmp_path):
    store = _store(tmp_path)
    try:
        identity = _id()
        selected = _candidate("ETH", 1)
        _prepare_mark_entered(store, identity, selected, promote=True)
        with store.transaction() as conn:
            before_state = C.load_arbitration(conn, identity)
            before_lock = C._load_lock(conn, identity)
            assert before_state is not None
            assert before_lock is not None
            assert before_lock.state == C.LOCK_ENTERED

            entered = C.mark_entered(
                conn, identity, entered_entry_id=None, reason="entered",
                actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=6,
            )

            assert entered.status == C.ENTERED
            assert entered.selected_candidate_key == C.candidate_key(selected)
            assert entered.selected_window_id == selected.window_id
            assert len(entered.events) == len(before_state.events) + 1
            assert entered.events[-1].from_status == C.RESERVING
            assert entered.events[-1].to_status == C.ENTERED
            assert C._load_lock(conn, identity) == before_lock
    finally:
        store.close()


def test_regression_C1D_10_divergent_rebegin_refused(tmp_path):
    """C1D-10: begin_arbitration rejects a divergent arbitration window."""
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))  # start=1, cutoff=5001
        with store.transaction() as conn:
            with pytest.raises(C.ClusterArbitrationError,
                               match="arbitration window mismatch"):
                C.begin_arbitration(
                    conn, identity, arbitration_started_ts_ms=999,
                    arbitration_cutoff_ts_ms=999_999,
                    candidate_set_fingerprint=_fp([cand]),
                    actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=999)
    finally:
        store.close()


def test_regression_C1F_01_signed_zero_canonical_identity():
    """C1F-01: _nine collapses signed zero so equal candidates share a key."""
    assert C._nine(0.0) == C._nine(-0.0) == "0.000000000"
    a = _candidate(net_edge=0.0)
    b = _candidate(net_edge=-0.0)
    assert C.candidate_key(a) == C.candidate_key(b)


def test_regression_C1F_02_quantization_does_not_flip_ranking():
    """C1F-02: representation-stable quantizer; AAA ties ZZZ and sorts first."""
    aaa = _candidate("AAA", 1, net_edge=0.0400005)
    zzz = _candidate("ZZZ", 2, net_edge=0.040001)
    ranked = C.rank_candidates([aaa, zzz])
    order = [c.asset for _, c in ranked]
    assert order == ["AAA", "ZZZ"]
    # Both quantize to the same grid point.
    assert C._quantize(0.0400005) == C._quantize(0.040001)


def test_regression_C1F_03_04_nested_floats_and_unknown_type_rejected():
    """C1F-03/04: nested floats use nine-decimal encoding; unknown types rejected."""
    c = C.Candidate(asset="ETH", window_id=1, selected_side="YES",
                    fair_probability=0.6, net_edge=0.02, depth=3.0,
                    market_identity={"x": 0.1 + 0.2})
    enc = C.candidate_content_payload(c)["market_identity"]["x"]
    assert enc == "0.300000000"
    assert isinstance(enc, str)

    class Custom:
        pass
    with pytest.raises(C.ClusterArbitrationError, match="unsupported market_identity"):
        C._canonical_mapping({"k": Custom()})


def test_regression_C1D_07_select_candidate_persists_supplied_rank(tmp_path):
    """C1D-07: select_candidate persists the caller-supplied rank, not a
    hardcoded 1.  Selecting the second-ranked candidate records rank=2."""
    store = _store(tmp_path)
    try:
        identity = _id()
        high = _candidate("AAA", 1, net_edge=0.05)
        low = _candidate("BBB", 2, net_edge=0.03)
        ranked = dict((c.asset, r) for r, c in C.rank_candidates([high, low]))
        assert ranked == {"AAA": 1, "BBB": 2}
        _begin(store, identity, fp=_fp([high, low]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=low,
                               candidate_rank=ranked["BBB"], reason="picked-low",
                               actor=C.ACTOR_ENGINE, session_id=SESSION, event_ts_ms=2)
            state = C.load_arbitration(conn, identity)
        assert state.selected_rank == 2
        # The persisted event must carry rank 2 too.
        ev = state.events[-1]
        assert ev.candidate_rank == 2
    finally:
        store.close()


@pytest.mark.parametrize("reason,actor,allowed", [
    # Authoritative matrix (09_FINAL_OPERATOR_RUNBOOK.md).
    (C.REASON_OPERATOR_CANCELLED, C.ACTOR_OPERATOR_TOOL, True),
    (C.REASON_OPERATOR_CANCELLED, C.ACTOR_ENGINE, False),
    (C.REASON_OPERATOR_CANCELLED, C.ACTOR_RECOVERY, False),
    (C.REASON_CLUSTER_WINDOW_EXPIRED, C.ACTOR_ENGINE, True),
    (C.REASON_CLUSTER_WINDOW_EXPIRED, C.ACTOR_RECOVERY, True),
    (C.REASON_CLUSTER_WINDOW_EXPIRED, C.ACTOR_OPERATOR_TOOL, False),
    (C.REASON_CLUSTER_EVIDENCE_INVALIDATED, C.ACTOR_ENGINE, True),
    (C.REASON_CLUSTER_EVIDENCE_INVALIDATED, C.ACTOR_RECOVERY, True),
    (C.REASON_CLUSTER_EVIDENCE_INVALIDATED, C.ACTOR_OPERATOR_TOOL, False),
    ("some_other_reason", C.ACTOR_ENGINE, False),
])
def test_regression_C1D_04_cancellation_actor_reason_matrix(tmp_path, reason, actor, allowed):
    """C1D-04: cancel_arbitration enforces the authoritative actor/reason matrix."""
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            if allowed:
                state = C.cancel_arbitration(conn, identity, reason=reason,
                                             actor=actor, session_id=SESSION,
                                             event_ts_ms=9)
                assert state.status == C.CANCELLED
            else:
                with pytest.raises(C.ClusterArbitrationError,
                                   match="cancellation reason"):
                    C.cancel_arbitration(conn, identity, reason=reason,
                                         actor=actor, session_id=SESSION,
                                         event_ts_ms=9)
                # No mutation: status remains COLLECTING.
                state = C.load_arbitration(conn, identity)
                assert state.status == C.COLLECTING
    finally:
        store.close()


# --- concurrency (C1D-12) -------------------------------------------------
# The schema's UNIQUE(cohort, cluster_open_ts_ms, seq) makes a double advance
# impossible; the loser must surface as a classified ClusterArbitrationError,
# not a raw sqlite3.OperationalError/IntegrityError.


def test_regression_C1D_12_record_event_classifies_seq_collision(tmp_path, monkeypatch):
    """A UNIQUE seq collision at the event-INSERT boundary is classified.

    Deterministic: simulate the race window by forcing ``_next_seq`` to
    return an already-occupied seq (what two concurrent writers both
    observe).  The INSERT must raise ``ClusterArbitrationError``, not a
    raw ``sqlite3.IntegrityError``.  This is the classification boundary
    the concurrency contract relies on.
    """
    store = _store(tmp_path)
    try:
        identity = _id()
        cand = _candidate()
        _begin(store, identity, fp=_fp([cand]))
        with store.transaction() as conn:
            C.select_candidate(conn, identity, candidate=cand, candidate_rank=1,
                               reason="s", actor=C.ACTOR_ENGINE,
                               session_id=SESSION, event_ts_ms=2)
            # The chain is at seq=2 (SELECTED).  Force _next_seq to return 2
            # (already occupied) to simulate the loser of a race that
            # observed MAX(seq)=1 before the winner wrote seq=2.
            monkeypatch.setattr(C, "_next_seq", lambda c, i: 2)
            with pytest.raises(C.ClusterArbitrationError,
                               match="concurrent arbitration event write lost"):
                C.begin_reserving(conn, identity, reason="r",
                                  actor=C.ACTOR_ENGINE, session_id=SESSION,
                                  event_ts_ms=3)
            # No partial mutation: the row UPDATE never ran (status still
            # SELECTED) and exactly one seq=2 row exists.
            state = C.load_arbitration(conn, identity)
            assert state.status == C.SELECTED
            n2 = conn.execute(
                "SELECT COUNT(*) FROM cluster_arbitration_events "
                "WHERE cohort=? AND cluster_open_ts_ms=? AND seq=2",
                (COHORT, identity.cluster_open_ts_ms)).fetchone()[0]
            assert n2 == 1
    finally:
        store.close()


def test_regression_C1D_12_two_stores_no_double_advance(tmp_path):
    """Two store connections racing an advance never produce two seq=N rows.

    Uses transaction(immediate=True) per the concurrency contract.  At most
    one writer can hold the IMMEDIATE lock at a time; the UNIQUE constraint
    plus the classification wrapper guarantee no double advance and no raw
    sqlite3 error escapes either writer.
    """
    import threading
    store = _store(tmp_path)
    db_path = store.path
    identity = _id()
    cand = _candidate()
    _begin(store, identity, fp=_fp([cand]))
    with store.transaction() as conn:
        C.select_candidate(conn, identity, candidate=cand, candidate_rank=1,
                           reason="s", actor=C.ACTOR_ENGINE,
                           session_id=SESSION, event_ts_ms=2)
    store.close()

    outcomes = {"a": None, "b": None, "errors": []}

    def attempt(key):
        # V4Store enforces owner-thread affinity, so the store must be
        # created inside the worker thread that uses it.
        s = V4Store(db_path)
        try:
            with s.transaction(immediate=True) as conn:
                C.begin_reserving(conn, identity, reason=f"r-{key}",
                                  actor=C.ACTOR_ENGINE, session_id=SESSION,
                                  event_ts_ms=10 if key == "a" else 11)
                outcomes[key] = "advanced"
        except C.ClusterArbitrationError as e:
            outcomes[key] = f"classified: {e}"
        except Exception as e:  # pragma: no cover - failure of the test itself
            outcomes["errors"].append((key, type(e).__name__, str(e)))
        finally:
            s.close()

    t_a = threading.Thread(target=attempt, args=("a",))
    t_b = threading.Thread(target=attempt, args=("b",))
    t_a.start(); t_b.start()
    t_a.join(); t_b.join()

    assert outcomes["errors"] == [], f"raw error escaped: {outcomes['errors']}"
    # Exactly one writer advanced; the other was classified (or blocked
    # safely).  Crucially neither produced a raw sqlite3 error.
    assert outcomes["a"] is not None and outcomes["b"] is not None
    advances = sum(1 for v in (outcomes["a"], outcomes["b"]) if v == "advanced")
    assert advances == 1, f"expected exactly one advance, got {outcomes}"

    # No double advance in the journal: exactly one seq=3 row, and the
    # stored status reached RESERVING consistently with the journal.
    verifier = V4Store(db_path)
    try:
        with verifier.transaction() as conn:
            n3 = conn.execute(
                "SELECT COUNT(*) FROM cluster_arbitration_events "
                "WHERE cohort=? AND cluster_open_ts_ms=? AND seq=3",
                (identity.cohort, identity.cluster_open_ts_ms)).fetchone()[0]
            assert n3 == 1
            state = C.load_arbitration(conn, identity)
            assert state.status == C.RESERVING
            # The full chain must replay cleanly (no partial mutation).
            C.replay_arbitration(conn, identity)
    finally:
        verifier.close()


# --- C1F-02 quantization contract (Human Authority Ruling 1) -------------
#
# These tests pin the quantization *contract*, not the implementation
# against itself.  The authority rule is:
#   - quantum 0.000001 exactly;
#   - conversion from a decimal representation of the finite input
#     (NOT binary-float division by the quantum);
#   - rounding mode ROUND_HALF_UP;
#   - positive and negative exact half-grid values round away from zero;
#   - signed zero canonicalizes to positive zero;
#   - deterministic, representation-stable, symmetric around zero.
# The literals below are the exact integers the contract must produce;
# they must not be changed to match a different (e.g. banker's-rounding
# or float-division) implementation.

@pytest.mark.parametrize("value, expected", [
    # signed zero canonicalizes to positive zero -> 0
    (0.0, 0),
    (-0.0, 0),
    # strictly inside the first grid cell (|v| < 0.5 quantum) -> 0
    (0.0000004, 0),
    (-0.0000004, 0),
    # exact positive half-grid -> rounds away from zero -> 1
    (0.0000005, 1),
    # exact negative half-grid -> rounds away from zero -> -1
    (-0.0000005, -1),
    # strictly past the half-grid (|v| > 0.5 quantum) -> +/-1
    (0.0000006, 1),
    (-0.0000006, -1),
    # exactly on the first grid line -> +/-1
    (0.0000010, 1),
    (-0.0000010, -1),
    # values near the strategy range: just below, on, and just after a
    # half-grid boundary at the 1e-6 scale.
    (0.0400004, 40000),
    (0.0400005, 40001),
    (0.0400010, 40001),
])
def test_regression_C1F_02_quantize_literal_contract(value, expected):
    """C1F-02: the quantizer pins the half-up contract on both sides of zero.

    Every literal here is the exact expected integer under the ratified
    rule (Decimal + ROUND_HALF_UP, away-from-zero at exact half-grid,
    signed zero -> 0).  These are contract assertions against the rule,
    not self-consistency: changing the implementation to Python ``round()``,
    banker's rounding, float-division, or direction-dependent logic would
    flip at least one literal.
    """
    assert C._quantize(value) == expected


def test_regression_C1F_02_quantize_symmetric_around_zero():
    """C1F-02: quantization is symmetric around zero for grid and half-grid.

    For every tested magnitude, ``_quantize(-x) == -_quantize(x)``.  This
    rules out direction-dependent rounding (e.g. round-half-up for positives
    and round-half-down for negatives, or vice-versa).
    """
    for x in (0.0, 0.0000004, 0.0000005, 0.0000006, 0.0000010,
              0.0400004, 0.0400005, 0.0400010):
        assert C._quantize(-x) == -C._quantize(x), f"asymmetric at {x!r}"


def test_regression_C1F_02_quantize_rejects_non_finite():
    """C1F-02: the quantizer fail-closes on non-finite inputs."""
    for bad in (float("nan"), float("inf"), -float("inf")):
        with pytest.raises(C.ClusterArbitrationError):
            C._quantize(bad)


def test_regression_C1F_02_same_grid_bucket_falls_through_to_tie_break():
    """C1F-02 ranking rule 1: candidates in the same grid bucket tie on
    edge, then fall through to the documented deterministic tie-break
    (depth desc, then asset asc, then candidate key asc).

    AAA and BBB quantize to the same net_edge bucket (0.0200005 and
    0.0200009 both lie inside the same 1e-6 cell centered at 0.020001),
    so the ranking decision must NOT depend on the sub-quantum difference.
    With equal depth, asset ascending puts AAA first.
    """
    aaa = _candidate("AAA", 1, net_edge=0.0200005, depth=3.0)
    bbb = _candidate("BBB", 2, net_edge=0.0200009, depth=3.0)
    # Precondition: both edges collapse to the same quantized integer.
    assert C._quantize(aaa.net_edge) == C._quantize(bbb.net_edge)
    ranked = C.rank_candidates([bbb, aaa])  # supply reversed to prove order independence
    assert [c.asset for _, c in ranked] == ["AAA", "BBB"]
    # Same bucket -> equal edge rank component; depth equal -> asset asc decides.
    assert [r for r, _ in ranked] == [1, 2]


def test_regression_C1F_02_opposite_sides_of_half_grid_rank_by_quantized_int():
    """C1F-02 ranking rule 2: candidates on opposite sides of a half-grid
    boundary rank strictly by their literal quantized integers.

    LOW has net_edge 0.0400004 -> 40000; HIGH has 0.0400005 -> 40001.
    HIGH's quantized integer is strictly larger, so HIGH must rank 1 even
    though its asset sorts after LOW alphabetically.
    """
    low = _candidate("AAA", 1, net_edge=0.0400004, depth=3.0)   # -> 40000
    high = _candidate("ZZZ", 2, net_edge=0.0400005, depth=3.0)  # -> 40001
    # Precondition: literal contract puts them on opposite sides.
    assert C._quantize(low.net_edge) == 40000
    assert C._quantize(high.net_edge) == 40001
    ranked = C.rank_candidates([low, high])
    assert [c.asset for _, c in ranked] == ["ZZZ", "AAA"]
    assert [r for r, _ in ranked] == [1, 2]


def test_regression_C1F_02_negative_boundary_ranking_pinned():
    """C1F-02 ranking rule 3: negative boundary behavior is pinned even
    though current strategy inputs are normally non-negative.

    A candidate at exactly -0.0000005 quantizes to -1 (away from zero),
    which is strictly less than a candidate at 0.0000004 (-> 0).  Ranking
    is by net_edge descending, so the non-negative candidate must rank 1.
    This pins the negative half-grid contract so a future direction-flip
    or signedness bug cannot silently invert ordering.
    """
    neg = _candidate("NEG", 1, net_edge=-0.0000005, depth=3.0)   # -> -1
    pos = _candidate("POS", 2, net_edge=0.0000004, depth=3.0)    # -> 0
    # Precondition: literal negative-boundary contract.
    assert C._quantize(neg.net_edge) == -1
    assert C._quantize(pos.net_edge) == 0
    ranked = C.rank_candidates([neg, pos])
    # net_edge desc: 0 (POS) > -1 (NEG) -> POS first.
    assert [c.asset for _, c in ranked] == ["POS", "NEG"]
    assert [r for r, _ in ranked] == [1, 2]


def test_regression_C1F_02_quantize_not_float_division():
    """C1F-02: the result is representation-stable, not float-division-derived.

    A naive ``int(round(v / 1e-6))`` compounds float-division error with
    banker's rounding and can drift; the ratified Decimal-based quantizer
    must produce the exact contracted integer.  We assert the quantizer
    output equals the literal and (incidentally) differs from a
    banker's-rounding float computation wherever the two would diverge.
    """
    # 0.0000005 is the canonical half-grid point: float division +
    # banker's rounding yields 0 here, but ROUND_HALF_UP yields 1.
    assert C._quantize(0.0000005) == 1
    # Sanity: the float-division + banker's path would produce something
    # other than the contracted literal at this exact half-grid point.
    import math
    naive = int(round(0.0000005 / 1e-6))  # banker's rounding -> 0
    assert naive == 0  # confirms the naive path differs from the contract
    assert C._quantize(0.0000005) != naive
