"""Phase 1 cohort-separation and safety regression tests (requirements 27-38)."""
import pytest

from poly_alpha_sniper.lite_frequency_v4.config import (
    ACTIVE_COHORT,
    LEGACY_COHORT,
    RUNTIME_LABEL,
    FrequencyV4Config,
    load_frequency_v4_config,
    validate_frequency_v4_config,
)
from poly_alpha_sniper.lite_frequency_v4.export import build_frequency_v4_dashboard
from poly_alpha_sniper.lite_frequency_v4.ledger import compute_capital_ledger
from poly_alpha_sniper.lite_frequency_v4.metrics import (
    acceptance_gate,
    build_metrics,
    performance_metrics,
    rolling_frequency,
)
from poly_alpha_sniper.lite_frequency_v4.persistence import (
    V4PersistenceCommand,
    V4PersistenceIdempotencyConflict,
    V4PersistenceWriter,
)
from poly_alpha_sniper.lite_frequency_v4.store import V4Store

from tests.test_frequency_v4_store import (
    NOW,
    create_entry,
    entry_payload,
    seed_candidate_entry_context,
    seed_market_window,
    seed_session,
)


LEGACY_SESSION = "legacy-session"


def seed_legacy_closed_trade(store: V4Store, *, net_pnl=2.50):
    """One terminal legacy trade under the legacy cohort, evidence-complete."""
    store.ensure_cohort({
        "cohort": LEGACY_COHORT,
        "activation_ts_ms": NOW - 14_400_000,
        "starting_equity_usd": 13.0,
        "max_exposure_pct": 0.75,
        "authoritative": 0,
        "label": "NON_AUTHORITATIVE_LEGACY_MIXED_UNIVERSE",
    })
    store.record_runtime_session({
        "session_id": LEGACY_SESSION,
        "launch_nonce": f"nonce-{LEGACY_SESSION}",
        "pid": 999,
        "git_commit": "0" * 40,
        "config_hash": "1" * 64,
        "started_ts_ms": NOW - 14_400_000,
        "cohort": LEGACY_COHORT,
    })
    context = seed_market_window(
        store, asset="BTC", open_ts=NOW - 3_600_000,
        session_id=LEGACY_SESSION, suffix="legacy")
    context["session_id"] = LEGACY_SESSION
    evidence = seed_candidate_entry_context(store, context)
    entry_id = store.create_entry(
        entry_payload(context, evidence, idem="legacy-entry"),
        max_concurrent_positions=6, cohort=LEGACY_COHORT,
        starting_equity_usd=13.0, max_exposure_pct=0.75,
        exit_fee_buffer_usd=0.1075,
    )
    store.record_resolution_attempt({
        "entry_id": entry_id, "attempt_no": 1,
        "attempt_ts_ms": context["close_ts"] + 1,
        "source": "POLYMARKET_OFFICIAL", "result": "RESOLVED",
        "observed_outcome": "YES", "evidence_hash": "f" * 64,
        "verified": True,
    })
    position = store.query_one(
        "SELECT position_id FROM positions WHERE entry_id=?", (entry_id,))
    store.close_position({
        "position_id": position["position_id"],
        "exit_ts_ms": context["close_ts"] + 2,
        "exit_source": "OFFICIAL_RESOLUTION",
        "shares": 5.0,
        "payout_usd": 5.0,
        "gross_pnl": 2.55,
        "exit_fee": 0.0,
        "net_pnl": net_pnl,
        "evidence_verified": True,
        "resolution_outcome": "YES",
        "reason": "official_resolution",
    })
    return entry_id


def _seed_mixed_history(tmp_path):
    """Legacy closed trade + active-cohort session in one store."""
    store = V4Store(tmp_path / "cohorts.db")
    seed_session(store)  # registers the ACTIVE cohort and its session
    legacy_entry = seed_legacy_closed_trade(store)
    return store, legacy_entry


# Requirement 27: legacy rows remain immutable (content preserved verbatim).
def test_legacy_rows_remain_immutable(tmp_path):
    store, legacy_entry = _seed_mixed_history(tmp_path)
    try:
        row = store.query_one(
            "SELECT * FROM entries WHERE entry_id=?", (legacy_entry,))
        assert row["status"] == "CLOSED"
        pnl = store.query_one(
            "SELECT net_pnl,verified FROM pnl_records WHERE entry_id=?",
            (legacy_entry,))
        assert pnl == {"net_pnl": 2.50, "verified": 1}
        session = store.query_one(
            "SELECT cohort FROM runtime_sessions WHERE session_id=?",
            (LEGACY_SESSION,))
        assert session == {"cohort": LEGACY_COHORT}
    finally:
        store.close()


# Requirement 28: legacy PnL does not enter new-cohort equity.
def test_legacy_pnl_never_enters_new_cohort_equity(tmp_path):
    store, _ = _seed_mixed_history(tmp_path)
    try:
        ledger = compute_capital_ledger(store.query, cohort=ACTIVE_COHORT,
                                        fee_rate=0.07, fee_buffer_usd=0.02)
        assert ledger.starting_equity_usd == 130.0
        assert ledger.realized_net_pnl_usd == 0.0
        assert ledger.current_equity_usd == 130.0
        legacy = compute_capital_ledger(store.query, cohort=LEGACY_COHORT,
                                        fee_rate=0.07, fee_buffer_usd=0.02)
        assert legacy.realized_net_pnl_usd == pytest.approx(2.50)
    finally:
        store.close()


# Requirements 29-30: legacy trades count toward neither the authoritative
# 300-trade gate nor any authoritative metric; only post-activation trades do.
def test_legacy_trades_excluded_from_authoritative_gate_and_metrics(tmp_path):
    store, _ = _seed_mixed_history(tmp_path)
    try:
        authoritative = performance_metrics(store, cohort=ACTIVE_COHORT)
        assert authoritative["verified_terminal"]["count"] == 0
        assert authoritative["all_terminal"]["count"] == 0
        legacy_all = performance_metrics(store)
        assert legacy_all["verified_terminal"]["count"] == 1

        frequencies = rolling_frequency(store, NOW)
        gate = acceptance_gate(store, frequencies, authoritative,
                               cohort=ACTIVE_COHORT)
        assert gate["verified_terminal_trades"] == 0
        assert gate["cohort"] == ACTIVE_COHORT
        assert "fewer_than_300_verified_terminal_trades" in gate["blockers"]

        metrics = build_metrics(store, NOW, cohort=ACTIVE_COHORT)
        assert metrics["performance"]["verified_terminal"]["count"] == 0
        assert metrics["compound_preview"]["sample_size"] == 0
        assert metrics["compound_preview"]["starting_equity_usd"] == 130.0
        assert metrics["compound_preview"]["label"] == (
            "READ_ONLY_THEORETICAL_PREVIEW_DOES_NOT_INFLUENCE_EXECUTION")
        assert metrics["compound_preview"]["influences_sizing"] is False
        legacy_view = metrics["legacy_non_authoritative"]
        assert legacy_view["label"] == "NON_AUTHORITATIVE_LEGACY_ALL_HISTORY"
        assert legacy_view["performance"]["verified_terminal"]["count"] == 1
    finally:
        store.close()


# Requirement 31 + reporting: the dashboard payload separates the cohorts and
# carries the authoritative runtime label and $130 ledger.
def test_dashboard_payload_separates_cohorts(tmp_path):
    store, _ = _seed_mixed_history(tmp_path)
    try:
        cfg = FrequencyV4Config()
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=cfg, session_id="session-v4")
        assert payload["runtime_label"] == RUNTIME_LABEL
        assert payload["cohort"]["authoritative"] == ACTIVE_COHORT
        assert payload["cohort"]["legacy"] == LEGACY_COHORT
        assert payload["cohort"]["legacy_metrics_are_non_authoritative"] is True
        capital = payload["authoritative_capital"]
        assert capital["ledger_available"] is True
        assert capital["ledger"]["starting_equity_usd"] == 130.0
        assert capital["ledger"]["current_equity_usd"] == 130.0
        assert capital["ledger"]["max_exposure_pct"] == 1.0
        assert capital["fixed_shares"] == 5.0
        assert payload["performance"]["verified_terminal"]["count"] == 0
        assert payload["legacy_non_authoritative"]["performance"][
            "verified_terminal"]["count"] == 1
        assert payload["universe"]["dynamic_universe_enabled"] is True
        assert payload["universe"]["hardcoded_to_required_assets_only"] is False
        assert payload["acceptance_gate"]["verified_terminal_trades"] == 0
    finally:
        store.close()


# Requirements 32-38: safety locks stay engaged and no real-order surface
# exists anywhere in the configuration or persisted session contract.
def test_safety_locks_remain_engaged_and_shadow_only(tmp_path):
    cfg = load_frequency_v4_config()
    assert cfg.dry_run is True                        # 32
    assert cfg.live_enabled is False                  # 33
    assert cfg.real_orders_possible is False          # 34
    assert cfg.kill_switch_engaged is True            # 35
    assert cfg.fixed_shares == 5.0                    # 36
    assert cfg.live_adapter_present is False          # 37
    assert cfg.research_equity_usd == 130.0
    assert cfg.exposure_cap_pct == 1.0
    validate_frequency_v4_config(cfg)

    # 38: the persisted session contract cannot represent a live runtime.
    store = V4Store(tmp_path / "safety.db")
    try:
        import sqlite3
        with pytest.raises(sqlite3.IntegrityError):
            with store.transaction(immediate=True) as conn:
                conn.execute(
                    """INSERT INTO runtime_sessions(
                       session_id,strategy_id,mode,launch_nonce,pid,git_commit,
                       config_hash,started_ts_ms,dry_run,live_enabled,
                       real_orders_possible,live_adapter_present,
                       kill_switch_engaged,fixed_shares)
                       VALUES('live','lite_frequency_v4',
                       'lite_frequency_v4_shadow','x',1,'c','h',1,0,1,1,1,0,10.0)""")
    finally:
        store.close()


# Regression: every launch journals ensure_cohort with a fresh payload (its
# own start timestamp/commit), so the journal idempotency key must be
# session-scoped.  A constant key made the second launch fail fatally with a
# payload-hash idempotency conflict; the cohort row itself stays idempotent
# via the store's INSERT OR IGNORE.
def test_restart_cohort_registration_journals_cleanly_across_sessions(tmp_path):
    path = tmp_path / "cohort-journal.db"

    def cohort_command(session_id, started_ts_ms, *, idempotency_key=None):
        return V4PersistenceCommand(
            command_id=f"{session_id}:000000000001:ensure-cohort",
            method="ensure_cohort",
            args=({
                "cohort": ACTIVE_COHORT,
                "activation_ts_ms": started_ts_ms,
                "activation_commit": "a" * 40,
                "starting_equity_usd": 13.0,
                "max_exposure_pct": 1.0,
                "authoritative": 1,
            },),
            ordering_key="global",
            idempotency_key=(
                idempotency_key
                or f"cohort-activate:{ACTIVE_COHORT}:{session_id}"),
        )

    first = V4PersistenceWriter(path)
    first.execute_sync(cohort_command("session-one", NOW - 60_000), timeout_s=5.0)
    first.close()
    # A later launch with a different payload must journal cleanly under its
    # own session-scoped key and must not move the original activation.
    second = V4PersistenceWriter(path)
    second.execute_sync(cohort_command("session-two", NOW), timeout_s=5.0)
    second.close()
    verifier = V4Store(path)
    try:
        rows = verifier.query("SELECT * FROM cohorts WHERE cohort=?",
                              (ACTIVE_COHORT,))
        assert len(rows) == 1
        assert rows[0]["activation_ts_ms"] == NOW - 60_000
    finally:
        verifier.close()
    # The constant-key form is exactly what the journal must refuse: same key,
    # different payload is an integrity conflict, never a silent overwrite.
    conflicting = V4PersistenceWriter(path)
    constant = f"cohort-activate:{ACTIVE_COHORT}"
    conflicting.execute_sync(
        cohort_command("session-three", NOW + 60_000,
                       idempotency_key=constant), timeout_s=5.0)
    with pytest.raises(V4PersistenceIdempotencyConflict):
        conflicting.execute_sync(
            cohort_command("session-four", NOW + 120_000,
                           idempotency_key=constant), timeout_s=5.0)
    conflicting.close()


# Recovery: cohort identity and activation survive restart untouched.
def test_cohort_identity_survives_restart(tmp_path):
    path = tmp_path / "restart-cohort.db"
    store = V4Store(path)
    try:
        seed_session(store)
        first = store.query_one(
            "SELECT * FROM cohorts WHERE cohort=?", (ACTIVE_COHORT,))
    finally:
        store.close()
    reopened = V4Store(path)
    try:
        # A later runtime re-registering the cohort cannot move activation.
        reopened.ensure_cohort({
            "cohort": ACTIVE_COHORT, "activation_ts_ms": NOW + 999,
            "starting_equity_usd": 13.0, "max_exposure_pct": 1.0,
        })
        second = reopened.query_one(
            "SELECT * FROM cohorts WHERE cohort=?", (ACTIVE_COHORT,))
        assert second == first
        assert reopened.query_one(
            "SELECT COUNT(*) n FROM cohorts WHERE cohort=?",
            (ACTIVE_COHORT,)) == {"n": 1}
    finally:
        reopened.close()
