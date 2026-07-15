from concurrent.futures import ThreadPoolExecutor
import sqlite3
import time

import pytest

from poly_alpha_sniper.lite_frequency_v4.contracts import CexObservation, SourceEvent
from poly_alpha_sniper.lite_frequency_v4.store import (
    EXPECTED_TABLES,
    ExposureLimitExceeded,
    V4BackgroundWriteDeferred,
    V4SchemaError,
    V4Store,
    WindowReservationConflict,
)


NOW = 2_000_000_000_000


def seed_session(store: V4Store, *, session_id: str = "session-v4", started=NOW-7_200_000):
    store.record_runtime_session({
        "session_id": session_id,
        "launch_nonce": f"nonce-{session_id}",
        "pid": 12345,
        "git_commit": "a" * 40,
        "config_hash": "b" * 64,
        "started_ts_ms": started,
    })
    return session_id


def seed_market_window(
    store: V4Store, *, asset="BTC", open_ts=NOW-300_000,
    session_id="session-v4", suffix="1",
):
    close_ts = open_ts + 300_000
    market_id = store.upsert_market({
        "polymarket_market_id": f"pm-{suffix}",
        "asset": asset,
        "slug": f"{asset.lower()}-updown-5m-{open_ts//1000}",
        "question": f"{asset} Up or Down",
        "duration_ms": 300_000,
        "open_ts_ms": open_ts,
        "close_ts_ms": close_ts,
        "status": "ACTIVE",
        "accepting_orders": True,
        "first_seen_ts_ms": open_ts-60_000,
        "last_seen_ts_ms": open_ts,
    })
    identity_id = store.record_market_identity({
        "market_id": market_id,
        "event_id": f"event-{suffix}",
        "condition_id": f"condition-{suffix}",
        "yes_token_id": f"yes-{suffix}",
        "no_token_id": f"no-{suffix}",
        "association_valid": True,
        "token_pair_valid": True,
        "ambiguous": False,
        "verification_reason": "exact_identity",
        "verified_ts_ms": open_ts,
    })
    window_id = store.ensure_asset_window({
        "asset": asset,
        "window_open_ts_ms": open_ts,
        "window_close_ts_ms": close_ts,
        "lifecycle_status": "ACTIVE",
        "created_ts_ms": open_ts-60_000,
        "updated_ts_ms": open_ts,
    })
    store.link_window_market({
        "window_id": window_id,
        "market_identity_id": identity_id,
        "eligibility_status": "ELIGIBLE",
        "selected": True,
        "linked_ts_ms": open_ts,
    })
    store.update_window_funnel(
        window_id, open_ts, available=1, eligible=1,
        available_ts_ms=open_ts, eligible_ts_ms=open_ts,
    )
    return {
        "session_id": session_id, "asset": asset, "market_id": market_id,
        "identity_id": identity_id, "window_id": window_id,
        "open_ts": open_ts, "close_ts": close_ts,
        "yes_token": f"yes-{suffix}", "no_token": f"no-{suffix}",
    }


def seed_candidate_entry_context(store: V4Store, context: dict, *, seq=1, side="YES"):
    ts = context["open_ts"] + 10_000
    event = store.record_source_event({
        "session_id": context["session_id"],
        "source": "POLYMARKET_CLOB",
        "channel": f"market:{context['identity_id']}",
        "event_type": "book",
        "asset": context["asset"],
        "market_identity_id": context["identity_id"],
        "token_id": context["yes_token"],
        "provider_ts_ms": ts-5,
        "receipt_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "sequence_no": seq,
        "payload": {"bids": [[0.48, 10]], "asks": [[0.49, 10]]},
    }, now_ms=ts)
    book_id = store.record_book_snapshot({
        "source_event_id": event["source_event_id"],
        "market_identity_id": context["identity_id"],
        "token_id": context["yes_token" if side == "YES" else "no_token"],
        "outcome_side": side,
        "provider_ts_ms": ts-5,
        "receipt_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "sequence_no": seq,
        "best_bid": 0.48,
        "best_ask": 0.49,
        "spread": 0.01,
        "bid_depth_5": 10,
        "ask_depth_5": 10,
        "bids_json": [[0.48, 10]],
        "asks_json": [[0.49, 10]],
        "hydrated": True,
        "stale": False,
    })
    cex = store.record_cex_observation({
        "source_event_id": event["source_event_id"],
        "session_id": context["session_id"],
        "provider": "OKX",
        "instrument": f"{context['asset']}-USDT",
        "asset": context["asset"],
        "price": 100.0,
        "bid": 99.9,
        "ask": 100.1,
        "provider_ts_ms": ts-4,
        "receipt_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "sequence_no": seq,
        "fresh": True,
    })
    candidate_id = store.record_candidate({
        "session_id": context["session_id"],
        "window_id": context["window_id"],
        "market_identity_id": context["identity_id"],
        "trigger_source_event_id": event["source_event_id"],
        "evaluation_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "evaluation_seq": seq,
        "status": "POSITIVE_EDGE",
        "regime": "TREND",
        "selected_side": side,
        "fair_probability_yes": 0.55 if side == "YES" else 0.45,
        "fair_probability_no": 0.45 if side == "YES" else 0.55,
        "calibrated": False,
        "reliability": 0.7,
        "positive_edge": True,
        "dominant_model": "lead_lag_impulse",
    })
    store.link_candidate_book(candidate_id, side, book_id, 5)
    store.link_candidate_cex(candidate_id, cex["cex_observation_id"], "PRIMARY", 0, 4)
    store.record_model_contribution({
        "candidate_id": candidate_id,
        "model_name": "lead_lag_impulse",
        "model_version": "v1",
        "correlation_group": "CEX_MOMENTUM",
        "direction": side,
        "raw_score": 0.6,
        "estimated_probability": 0.55,
        "evidence_age_ms": 4,
        "confidence": 0.7,
        "reliability": 0.7,
        "expected_net_edge": 0.025,
        "regime_weight": 0.8,
        "gated": False,
        "model_contribution": 0.48,
        "calibrated": False,
    })
    fair_id = store.record_fair_value({
        "candidate_id": candidate_id,
        "phase": "FINAL",
        "calculation_seq": 1,
        "calculated_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "regime": "TREND",
        "fair_probability_yes": 0.55 if side == "YES" else 0.45,
        "fair_probability_no": 0.45 if side == "YES" else 0.55,
        "calibrated": False,
        "calibration_label": "UNCALIBRATED",
    }, [{
        "outcome_side": side,
        "token_id": context["yes_token" if side == "YES" else "no_token"],
        "book_snapshot_id": book_id,
        "executable_vwap": 0.49,
        "worst_consumed_price": 0.49,
        "spread": 0.01,
        "depth_shares": 10,
        "exact_five_share_depth": True,
        "estimated_fee": 0.005,
        "execution_buffer": 0.003,
        "latency_buffer": 0.002,
        "uncertainty_buffer": 0.005,
        "net_edge": 0.045,
        "evidence_fresh": True,
        "selected": True,
    }])
    decision_id = store.record_decision({
        "candidate_id": candidate_id,
        "fair_value_calculation_id": fair_id,
        "decision_seq": 1,
        "decision_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "phase": "FINAL",
        "action": "CROSS_SPREAD",
        "selected_side": side,
        "selected_net_edge": 0.045,
        "economic_gate_passed": True,
        "exact_depth_passed": True,
        "evidence_fresh": True,
        "reason": "strong_positive_net_edge",
    })
    store.reserve_window({
        "window_id": context["window_id"],
        "session_id": context["session_id"],
        "market_identity_id": context["identity_id"],
        "owner_launch_nonce": f"nonce-{context['session_id']}",
        "outcome_side": side,
        "state": "RESERVED",
        "candidate_id": candidate_id,
        "decision_id": decision_id,
        "reserved_ts_ms": ts,
        "updated_ts_ms": ts,
    })
    return {
        "candidate_id": candidate_id, "fair_id": fair_id,
        "decision_id": decision_id, "book_id": book_id,
        "event_id": event["source_event_id"], "cex_id": cex["cex_observation_id"],
        "entry_ts": ts,
    }


def entry_payload(context: dict, evidence: dict, *, idem="entry-one"):
    return {
        "session_id": context["session_id"],
        "window_id": context["window_id"],
        "market_identity_id": context["identity_id"],
        "candidate_id": evidence["candidate_id"],
        "decision_id": evidence["decision_id"],
        "fair_value_calculation_id": evidence["fair_id"],
        "book_snapshot_id": evidence["book_id"],
        "outcome_side": "YES",
        "token_id": context["yes_token"],
        "entry_ts_ms": evidence["entry_ts"] + 1,
        "entry_mode": "CROSS_SPREAD",
        "executable_vwap": 0.49,
        "worst_consumed_price": 0.49,
        "depth_shares": 10,
        "gross_cost": 2.45,
        "estimated_fee": 0.05,
        "execution_buffer": 0.003,
        "latency_buffer": 0.002,
        "uncertainty_buffer": 0.005,
        "selected_net_edge": 0.045,
        "execution_verified": True,
        "idempotency_key": idem,
    }


def create_entry(store: V4Store, payload: dict) -> int:
    return store.create_entry(
        payload,
        max_concurrent_positions=4,
        global_exposure_cap_usd=10.0,
        per_asset_exposure_cap_usd=5.0,
    )


def test_fresh_schema_has_all_normalized_tables_wal_fk_and_integrity(tmp_path):
    sentinel = tmp_path / "advanced.db"
    sentinel.write_bytes(b"advanced-sentinel")
    path = tmp_path / "poly_alpha_frequency_v4.db"
    store = V4Store(path)
    try:
        tables = {row["name"] for row in store.query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )}
        assert EXPECTED_TABLES <= tables
        assert store.query_one("PRAGMA journal_mode")["journal_mode"].lower() == "wal"
        assert store.query_one("PRAGMA foreign_keys")["foreign_keys"] == 1
        assert store.integrity_check() == {"integrity": "ok", "foreign_key_violations": []}
    finally:
        store.close()
    assert sentinel.read_bytes() == b"advanced-sentinel"
    reopened = V4Store(path)
    reopened.close()


def test_non_v4_database_is_refused_without_migration(tmp_path):
    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE lite_trades(id INTEGER PRIMARY KEY)")
    conn.close()
    with pytest.raises(V4SchemaError, match="refusing non-v4"):
        V4Store(path)


def test_source_event_dedup_future_regression_and_unchanged_fresh_tick(tmp_path):
    store = V4Store(tmp_path / "v4.db")
    try:
        session = seed_session(store)
        base = {
            "session_id": session, "source": "OKX", "channel": "tickers:BTC-USDT",
            "event_type": "ticker", "asset": "BTC", "provider_ts_ms": NOW-10,
            "receipt_ts_ms": NOW, "monotonic_ns": NOW*1_000_000,
            "sequence_no": 1, "payload": {"last": "100"},
        }
        first = store.record_source_event(base, now_ms=NOW)
        duplicate = store.record_source_event(base, now_ms=NOW)
        future = store.record_source_event({**base, "dedupe_key": "future", "sequence_no": 2,
                                            "provider_ts_ms": NOW+2_000}, now_ms=NOW)
        assert first["accepted"] and first["inserted"]
        assert duplicate["duplicate"] and duplicate["classification"] == "DUPLICATE"
        assert not future["accepted"] and future["classification"] == "FUTURE_EVENT"
        one = store.record_cex_observation({
            "session_id": session, "provider": "OKX", "instrument": "BTC-USDT",
            "asset": "BTC", "price": 100.0, "bid": 99.9, "ask": 100.1,
            "provider_ts_ms": NOW, "receipt_ts_ms": NOW,
            "monotonic_ns": NOW*1_000_000, "fresh": True,
        })
        unchanged = store.record_cex_observation({
            "session_id": session, "provider": "OKX", "instrument": "BTC-USDT",
            "asset": "BTC", "price": 100.0, "bid": 99.9, "ask": 100.1,
            "provider_ts_ms": NOW+100, "receipt_ts_ms": NOW+100,
            "monotonic_ns": (NOW+100)*1_000_000, "fresh": True,
        })
        assert one["classification"] == "NEW_TICK"
        assert unchanged["classification"] == "NO_NEW_TICK"
        assert unchanged["fresh"] is True
        with pytest.raises(ValueError, match="secret-like field"):
            store.record_source_event({
                **base, "dedupe_key": "secret-payload", "sequence_no": 3,
                "payload": {"api_key": "must-never-persist"},
            }, now_ms=NOW)
        counts = store.query_one("SELECT SUM(raw_count) raw,SUM(unique_count) uniq,SUM(duplicate_count) dup FROM event_buckets")
        assert counts == {"raw": 3, "uniq": 2, "dup": 1}
    finally:
        store.close()


def test_contract_ingestion_sequence_semantics_and_zero_future_tolerance(tmp_path):
    store = V4Store(tmp_path / "contract-events.db")
    try:
        session = seed_session(store)

        def event(key, sequence, provider_ts, receipt_ts, *, channel="books", epoch=4):
            return SourceEvent(
                source="POLYMARKET_CLOB",
                channel=channel,
                event_type="book",
                event_key=key,
                payload_hash=f"hash-{key}",
                provider_ts_ms=provider_ts,
                receipt_ts_ms=receipt_ts,
                receipt_monotonic_ns=receipt_ts * 1_000_000,
                sequence=sequence,
                connection_epoch=epoch,
                asset="BTC",
                market_id="pm-contract",
                condition_id="condition-contract",
                token_id="yes-contract",
                window_open_ms=NOW - 300_000,
                payload_json='{"kind":"book"}',
            )

        first = store.record_source_event(
            event("event-10-a", 10, NOW - 10, NOW), session_id=session
        )
        equal = store.record_source_event(
            event("event-10-b", 10, NOW - 9, NOW + 1), session_id=session
        )
        jump = store.record_source_event(
            event("event-42", 42, NOW - 8, NOW + 2), session_id=session
        )
        regressed = store.record_source_event(
            event("event-41", 41, NOW - 7, NOW + 3), session_id=session
        )

        assert first["accepted"] is True
        assert equal["accepted"] is True
        assert jump["accepted"] is True
        assert regressed["accepted"] is False
        assert regressed["classification"] == "REGRESSED_SEQUENCE"

        contiguous_first = store.record_source_event(
            event("contiguous-100", 100, NOW - 6, NOW + 4, channel="ordered"),
            session_id=session,
            sequence_contiguous=True,
        )
        contiguous_gap = store.record_source_event(
            event("contiguous-102", 102, NOW - 5, NOW + 5, channel="ordered"),
            session_id=session,
            sequence_contiguous=True,
        )
        assert contiguous_first["accepted"] is True
        assert contiguous_first["sequence_contiguous"] is True
        assert contiguous_gap["accepted"] is False
        assert contiguous_gap["classification"] == "SEQUENCE_GAP"

        future = store.record_source_event(
            event("future-default-zero", 1, NOW + 11, NOW + 10, channel="future"),
            session_id=session,
        )
        assert future["accepted"] is False
        assert future["classification"] == "FUTURE_EVENT"

        stored = store.query_one(
            """SELECT external_market_id,condition_id,window_open_ts_ms,
                      sequence_no,connection_epoch,payload_json
               FROM source_events WHERE dedupe_key='event-10-a'"""
        )
        assert stored == {
            "external_market_id": "pm-contract",
            "condition_id": "condition-contract",
            "window_open_ts_ms": NOW - 300_000,
            "sequence_no": 10,
            "connection_epoch": 4,
            "payload_json": '{"kind":"book"}',
        }
        with pytest.raises(ValueError, match="future_tolerance_ms"):
            store.record_source_event(
                event("bad-tolerance", 2, NOW, NOW + 20, channel="future"),
                session_id=session,
                future_tolerance_ms=-1,
            )
    finally:
        store.close()


def test_cex_contract_aliases_and_zero_future_tolerance(tmp_path):
    store = V4Store(tmp_path / "contract-cex.db")
    try:
        session = seed_session(store)
        first = store.record_cex_observation(
            CexObservation(
                provider="OKX", asset="BTC", instrument="BTC-USDT",
                price=100.0, provider_ts_ms=NOW, receipt_ts_ms=NOW,
                receipt_monotonic_ns=NOW * 1_000_000, event_id="tick-1",
                event_type="trade", side="buy", sequence=10,
                connection_epoch=2, bid=99.9, ask=100.1, size=1.25,
            ),
            session_id=session,
        )
        unchanged = store.record_cex_observation(
            CexObservation(
                provider="OKX", asset="BTC", instrument="BTC-USDT",
                price=100.0, provider_ts_ms=NOW + 1, receipt_ts_ms=NOW + 1,
                receipt_monotonic_ns=(NOW + 1) * 1_000_000, event_id="tick-2",
                event_type="ticker", sequence=11, connection_epoch=2,
                bid=99.9, ask=100.1,
            ),
            session_id=session,
        )
        future = store.record_cex_observation(
            CexObservation(
                provider="OKX", asset="BTC", instrument="BTC-USDT",
                price=100.2, provider_ts_ms=NOW + 3, receipt_ts_ms=NOW + 2,
                receipt_monotonic_ns=(NOW + 2) * 1_000_000, event_id="tick-3",
                event_type="ticker", sequence=12, connection_epoch=2,
                bid=100.1, ask=100.3,
            ),
            session_id=session,
        )

        assert first == {
            "cex_observation_id": first["cex_observation_id"],
            "inserted": True,
            "classification": "NEW_TICK",
            "fresh": True,
        }
        assert unchanged["classification"] == "NO_NEW_TICK"
        assert unchanged["fresh"] is True
        assert future["inserted"] is True
        assert future["classification"] == "INVALID"
        assert future["fresh"] is False
        stored = store.query_one(
            """SELECT event_id,event_type,trade_side,size,sequence_no,
                      connection_epoch,invalid_reason
               FROM cex_observations WHERE event_id='tick-1'"""
        )
        assert stored == {
            "event_id": "tick-1",
            "event_type": "trade",
            "trade_side": "buy",
            "size": 1.25,
            "sequence_no": 10,
            "connection_epoch": 2,
            "invalid_reason": None,
        }
        invalid = store.query_one(
            "SELECT invalid_reason FROM cex_observations WHERE event_id='tick-3'"
        )
        assert invalid == {"invalid_reason": "future_provider_timestamp"}
    finally:
        store.close()


def test_atomic_entry_race_allows_exactly_one_asset_window_entry(tmp_path):
    path = tmp_path / "race.db"
    seed_store = V4Store(path)
    try:
        seed_session(seed_store)
        context = seed_market_window(seed_store)
        evidence = seed_candidate_entry_context(seed_store, context)
    finally:
        seed_store.close()
    payloads = [entry_payload(context, evidence, idem=f"contender-{n}") for n in (1, 2)]

    def attempt(payload):
        store = V4Store(path)
        try:
            try:
                return ("ok", create_entry(store, payload))
            except WindowReservationConflict as exc:
                return ("blocked", exc.reason)
        finally:
            store.close()

    verifier = None
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(attempt, payloads))
        verifier = V4Store(path)
        assert sorted(result[0] for result in results) == ["blocked", "ok"]
        assert verifier.query_one("SELECT COUNT(*) count FROM entries")["count"] == 1
        entry = verifier.query_one("SELECT * FROM entries")
        assert entry["shares"] == 5.0
        assert entry["maker_fill_assumed"] == 0
        assert verifier.query_one(
            "SELECT COUNT(*) count FROM positions WHERE status='OPEN'"
        )["count"] == 1
        assert verifier.integrity_check()["foreign_key_violations"] == []
    finally:
        if verifier is not None:
            verifier.close()


def test_entry_risk_limit_rolls_back_without_partial_rows(tmp_path):
    store = V4Store(tmp_path / "risk.db")
    try:
        seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        with pytest.raises(ValueError, match="maker fill may never be assumed"):
            store.create_entry(
                {**entry_payload(context, evidence), "maker_fill_assumed": True},
                max_concurrent_positions=4, global_exposure_cap_usd=10.0,
                per_asset_exposure_cap_usd=5.0,
            )
        with pytest.raises(ExposureLimitExceeded, match="global_exposure_cap"):
            store.create_entry(
                entry_payload(context, evidence), max_concurrent_positions=4,
                global_exposure_cap_usd=1.0, per_asset_exposure_cap_usd=5.0,
            )
        assert store.query_one("SELECT COUNT(*) count FROM entries")["count"] == 0
        assert store.query_one("SELECT COUNT(*) count FROM positions")["count"] == 0
        assert store.query_one("SELECT state FROM window_locks")["state"] == "RESERVED"
    finally:
        store.close()


def test_management_post_close_book_exit_forbidden_and_official_close_reconciles(tmp_path):
    store = V4Store(tmp_path / "manage.db")
    try:
        seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        entry_id = create_entry(store, entry_payload(context, evidence))
        position = store.open_positions()[0]
        with pytest.raises(ValueError, match="post-close book exit"):
            store.record_management_decision({
                "position_id": position["position_id"], "decision_seq": 1,
                "decision_ts_ms": context["close_ts"], "monotonic_ns": NOW*1_000_000,
                "updated_fair_probability": 0.6, "executable_exit_value": 2.0,
                "hold_to_resolution_value": 3.0, "remaining_time_ms": 0,
                "spread": 0.01, "depth_shares": 10, "estimated_fee": 0.01,
                "uncertainty": 0.01, "thesis_state": "CLOSED",
                "action": "EXIT_BOOK", "reason": "forbidden_stale_exit",
            })
        store.record_resolution_attempt({
            "entry_id": entry_id, "attempt_no": 1, "attempt_ts_ms": context["close_ts"]+1,
            "source": "POLYMARKET_OFFICIAL", "result": "RESOLVED",
            "observed_outcome": "YES", "evidence_hash": "f"*64,
            "verified": True,
        })
        exit_id = store.close_position({
            "position_id": position["position_id"],
            "exit_ts_ms": context["close_ts"]+2,
            "exit_source": "OFFICIAL_RESOLUTION",
            "shares": 5.0,
            "payout_usd": 5.0,
            "gross_pnl": 2.55,
            "exit_fee": 0.0,
            "net_pnl": 2.50,
            "evidence_verified": True,
            "resolution_outcome": "YES",
            "reason": "official_resolution",
        })
        assert exit_id > 0
        pnl = store.query_one("SELECT * FROM pnl_records WHERE entry_id=?", (entry_id,))
        assert pnl["total_fees"] == pytest.approx(0.05)
        assert pnl["net_pnl"] == pytest.approx(2.50)
        assert pnl["verified"] == 1
        assert store.open_positions() == []
    finally:
        store.close()


def test_bounded_retention_deletes_only_old_unpinned_raw_evidence(tmp_path):
    store = V4Store(tmp_path / "retention.db")
    try:
        session = seed_session(store, started=NOW-20_000_000)
        common = {
            "session_id": session, "source": "OKX", "channel": "trades:BTC-USDT",
            "event_type": "trade", "asset": "BTC",
            "receipt_ts_ms": NOW-10_000_000, "monotonic_ns": NOW*1_000_000,
        }
        first = store.record_source_event({
            **common, "provider_ts_ms": NOW-10_000_010, "sequence_no": 1,
            "dedupe_key": "old-pinned", "payload": {"price": 100},
        }, now_ms=NOW-10_000_000)
        store.record_source_event({
            **common, "provider_ts_ms": NOW-10_000_009, "sequence_no": 2,
            "dedupe_key": "old-raw", "payload": {"price": 101},
        }, now_ms=NOW-10_000_000)
        store.pin_source_event(first["source_event_id"])
        result = store.compact_raw_evidence(NOW, retention_ms=1_000_000, batch_size=1)
        assert result["source_events"] == 1
        rows = store.query("SELECT dedupe_key,retention_class,pin_count FROM source_events")
        assert rows == [{"dedupe_key": "old-pinned", "retention_class": "TRADE_EVIDENCE", "pin_count": 1}]
        assert result["integrity"] == "ok"
        assert result["foreign_key_violations"] == []
    finally:
        store.close()


def test_reservation_release_requires_exact_owner_and_never_releases_entry(tmp_path):
    store = V4Store(tmp_path / "reservation-release.db")
    try:
        seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        lock = store.query_one(
            "SELECT * FROM window_locks WHERE window_id=?",
            (context["window_id"],),
        )

        for overrides in (
            {"session_id": "another-session"},
            {"owner_launch_nonce": "another-nonce"},
            {"idempotency_key": "another-idempotency-key"},
        ):
            owner = {
                "window_id": context["window_id"],
                "session_id": context["session_id"],
                "owner_launch_nonce": f"nonce-{context['session_id']}",
                "idempotency_key": lock["idempotency_key"],
                **overrides,
            }
            assert store.release_window_reservation(**owner) is False
            assert store.query_one(
                "SELECT COUNT(*) count FROM window_locks WHERE window_id=?",
                (context["window_id"],),
            )["count"] == 1

        exact_owner = {
            "window_id": context["window_id"],
            "session_id": context["session_id"],
            "owner_launch_nonce": f"nonce-{context['session_id']}",
            "idempotency_key": lock["idempotency_key"],
        }
        assert store.release_window_reservation(**exact_owner) is True
        assert store.release_window_reservation(**exact_owner) is False

        store.reserve_window({
            "window_id": context["window_id"],
            "session_id": context["session_id"],
            "market_identity_id": context["identity_id"],
            "owner_launch_nonce": exact_owner["owner_launch_nonce"],
            "outcome_side": "YES",
            "state": "RESERVED",
            "candidate_id": evidence["candidate_id"],
            "decision_id": evidence["decision_id"],
            "reserved_ts_ms": evidence["entry_ts"],
            "updated_ts_ms": evidence["entry_ts"],
            "idempotency_key": exact_owner["idempotency_key"],
        })
        create_entry(store, entry_payload(context, evidence))
        assert store.release_window_reservation(**exact_owner) is False
        assert store.query_one(
            "SELECT state FROM window_locks WHERE window_id=?",
            (context["window_id"],),
        ) == {"state": "ENTERED"}
    finally:
        store.close()


def test_atomic_per_asset_open_position_cap_across_windows(tmp_path):
    path = tmp_path / "per-asset-race.db"
    seed_store = V4Store(path)
    try:
        seed_session(seed_store)
        first_context = seed_market_window(
            seed_store, open_ts=NOW - 600_000, suffix="asset-cap-one"
        )
        second_context = seed_market_window(
            seed_store, open_ts=NOW - 300_000, suffix="asset-cap-two"
        )
        first_evidence = seed_candidate_entry_context(
            seed_store, first_context, seq=1
        )
        second_evidence = seed_candidate_entry_context(
            seed_store, second_context, seq=2
        )
    finally:
        seed_store.close()

    def attempt(context, evidence, contender):
        store = V4Store(path)
        try:
            try:
                entry_id = store.create_entry(
                    entry_payload(
                        context, evidence, idem=f"asset-cap-{contender}"
                    ),
                    max_concurrent_positions=10,
                    global_exposure_cap_usd=100.0,
                    per_asset_exposure_cap_usd=100.0,
                    max_open_per_asset=1,
                )
                return "ok", entry_id
            except ExposureLimitExceeded as exc:
                return "blocked", exc.reason
        finally:
            store.close()

    verifier = None
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = (
                pool.submit(attempt, first_context, first_evidence, "one"),
                pool.submit(attempt, second_context, second_evidence, "two"),
            )
            results = [future.result() for future in futures]
        verifier = V4Store(path)
        assert sorted(result[0] for result in results) == ["blocked", "ok"]
        assert {result[1] for result in results if result[0] == "blocked"} == {
            "max_open_per_asset"
        }
        assert verifier.query_one("SELECT COUNT(*) count FROM entries") == {"count": 1}
        assert verifier.query_one(
            "SELECT COUNT(*) count FROM positions WHERE asset='BTC' AND status='OPEN'"
        ) == {"count": 1}
        assert verifier.integrity_check() == {
            "integrity": "ok",
            "foreign_key_violations": [],
        }
    finally:
        if verifier is not None:
            verifier.close()


def test_event_bucket_compaction_preserves_totals_and_is_idempotent(tmp_path):
    store = V4Store(tmp_path / "bucket-compaction.db")
    try:
        minute_start = (NOW - 180_000) // 60_000 * 60_000
        observations = (
            (minute_start + 1_000, True, False, False),
            (minute_start + 2_000, False, True, False),
            (minute_start + 2_000, False, False, True),
        )
        for receipt_ts_ms, unique, duplicate, invalid in observations:
            store.record_event_count(
                receipt_ts_ms=receipt_ts_ms,
                source="OKX",
                channel="tickers:BTC-USDT",
                asset="BTC",
                event_type="ticker",
                classification="COUNTED",
                unique=unique,
                duplicate=duplicate,
                invalid=invalid,
            )
        store.record_event_count(
            receipt_ts_ms=NOW,
            source="OKX",
            channel="tickers:BTC-USDT",
            asset="BTC",
            event_type="ticker",
            classification="COUNTED",
            unique=True,
            duplicate=False,
            invalid=False,
        )

        assert store.compact_event_buckets(NOW, detail_retention_ms=10_000) == 2
        minute = store.query_one(
            """SELECT raw_count,unique_count,duplicate_count,invalid_count
               FROM event_buckets WHERE bucket_ms=60000 AND bucket_start_ts_ms=?""",
            (minute_start,),
        )
        assert minute == {
            "raw_count": 3,
            "unique_count": 1,
            "duplicate_count": 1,
            "invalid_count": 1,
        }
        assert store.query_one(
            "SELECT COUNT(*) count FROM event_buckets WHERE bucket_ms=1000"
        ) == {"count": 1}
        assert store.compact_event_buckets(NOW, detail_retention_ms=10_000) == 0
        assert store.query_one(
            "SELECT raw_count FROM event_buckets WHERE bucket_ms=60000"
        ) == {"raw_count": 3}
    finally:
        store.close()


def test_batched_event_counts_flush_exact_aggregates_atomically(tmp_path):
    store = V4Store(tmp_path / "batched-event-counts.db")
    try:
        store.record_event_count_batch([{
            "receipt_ts_ms": NOW,
            "source": "polymarket",
            "channel": "market:book:btc",
            "asset": "BTC",
            "event_type": "book",
            "classification": "REJECT_STALE",
            "raw_count": 25,
            "unique_count": 24,
            "duplicate_count": 1,
            "invalid_count": 25,
        }])
        row = store.query_one(
            "SELECT raw_count,unique_count,duplicate_count,invalid_count "
            "FROM event_buckets"
        )
        assert row == {
            "raw_count": 25,
            "unique_count": 24,
            "duplicate_count": 1,
            "invalid_count": 25,
        }

        with pytest.raises(ValueError, match="invalid event bucket counts"):
            store.record_event_count_batch([{
                "receipt_ts_ms": NOW+1_000,
                "source": "polymarket", "channel": "market", "asset": "BTC",
                "event_type": "book", "classification": "INVALID",
                "raw_count": 1, "unique_count": 2,
                "duplicate_count": 0, "invalid_count": 1,
            }])
        assert store.query_one(
            "SELECT COUNT(*) AS n FROM event_buckets")["n"] == 1
    finally:
        store.close()


def test_raw_row_cap_deletes_oldest_unlinked_rows_and_preserves_trade_evidence(tmp_path):
    store = V4Store(tmp_path / "raw-row-cap.db")
    try:
        session = seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        for sequence in range(1_001):
            receipt_ts_ms = NOW - 1_000_000 + sequence
            store.record_source_event({
                "session_id": session,
                "source": "OKX",
                "channel": "raw-row-cap",
                "event_type": "ticker",
                "asset": "BTC",
                "provider_ts_ms": receipt_ts_ms,
                "receipt_ts_ms": receipt_ts_ms,
                "monotonic_ns": receipt_ts_ms * 1_000_000,
                "sequence_no": sequence,
                "dedupe_key": f"raw-row-cap-{sequence}",
                "payload": {"price": 100 + sequence / 10_000},
            }, now_ms=receipt_ts_ms)

        protected_id = evidence["event_id"]
        before = store.query_one(
            """SELECT COUNT(*) count FROM source_events
               WHERE retention_class='RAW' AND pin_count=0"""
        )["count"]
        assert before == 1_002
        deleted = store.enforce_raw_row_cap(1_000)
        assert deleted == {
            "source_events": 2,
            "book_snapshots": 0,
            "cex_observations": 0,
        }
        assert store.query_one(
            """SELECT COUNT(*) count FROM source_events
               WHERE retention_class='RAW' AND pin_count=0"""
        ) == {"count": 1_000}
        assert store.query_one(
            "SELECT dedupe_key FROM source_events WHERE source_event_id=?",
            (protected_id,),
        ) is not None
        assert store.enforce_raw_row_cap(1_000) == {
            "source_events": 0,
            "book_snapshots": 0,
            "cex_observations": 0,
        }
    finally:
        store.close()


def test_late_admitted_evidence_bypasses_regression_without_double_counting(tmp_path):
    store = V4Store(tmp_path / "late-admitted.db")
    try:
        session = seed_session(store)
        common = {
            "session_id": session,
            "source": "POLYMARKET_CLOB",
            "channel": "market:late-admitted",
            "event_type": "book",
            "asset": "BTC",
        }
        current = store.record_source_event({
            **common,
            "dedupe_key": "current-evidence",
            "provider_ts_ms": NOW - 10,
            "receipt_ts_ms": NOW,
            "monotonic_ns": NOW * 1_000_000,
            "sequence_no": 20,
            "payload": {"version": "current"},
        }, now_ms=NOW)
        assert current["accepted"] is True

        store.record_event_count(
            receipt_ts_ms=NOW + 1,
            source=common["source"],
            channel=common["channel"],
            asset=common["asset"],
            event_type=common["event_type"],
            classification="ACCEPTED",
            unique=True,
            duplicate=False,
            invalid=False,
        )
        late_payload = {
            **common,
            "dedupe_key": "late-trade-evidence",
            "provider_ts_ms": NOW - 1_000,
            "receipt_ts_ms": NOW + 1,
            "monotonic_ns": (NOW + 1) * 1_000_000,
            "sequence_no": 10,
            "payload": {"version": "late-but-already-admitted"},
        }
        late = store.record_source_event(
            late_payload,
            now_ms=NOW + 1,
            count_in_bucket=False,
            admitted_at_receipt=True,
        )
        duplicate = store.record_source_event(
            late_payload,
            now_ms=NOW + 1,
            count_in_bucket=False,
            admitted_at_receipt=True,
        )

        assert late["accepted"] is True
        assert late["admitted_at_receipt"] is True
        assert duplicate["duplicate"] is True
        assert duplicate["classification"] == "DUPLICATE"
        assert store.query_one(
            """SELECT last_provider_ts_ms,last_sequence FROM source_cursors
               WHERE session_id=? AND source=? AND channel=? AND connection_epoch=0""",
            (session, common["source"], common["channel"]),
        ) == {"last_provider_ts_ms": NOW - 10, "last_sequence": 20}
        assert store.query_one(
            """SELECT SUM(raw_count) raw,SUM(unique_count) uniq,
                      SUM(duplicate_count) dup
               FROM event_buckets"""
        ) == {"raw": 2, "uniq": 2, "dup": 0}
        assert store.query_one("SELECT COUNT(*) count FROM source_events") == {
            "count": 2
        }
    finally:
        store.close()


def test_entry_bundle_ack_contains_committed_position_without_followup_read(tmp_path):
    store = V4Store(tmp_path / "entry-position-ack.db")
    try:
        seed_session(store)
        context = seed_market_window(store, suffix="position-ack")
        evidence = seed_candidate_entry_context(store, context)
        reservation = store.query_one(
            "SELECT * FROM window_locks WHERE window_id=?", (context["window_id"],))
        result = store.reserve_and_create_entry_bundle(
            reservation,
            entry_payload(context, evidence, idem="position-ack-entry"),
            max_concurrent_positions=4,
            global_exposure_cap_usd=10.0,
            per_asset_exposure_cap_usd=5.0,
        )
        assert result["entry_id"] > 0
        assert result["position_id"] == result["position"]["position_id"]
        assert result["position"]["entry_id"] == result["entry_id"]
        assert result["position"]["status"] == "OPEN"
        assert result["position"]["shares"] == 5.0
    finally:
        store.close()


def test_validated_cex_feature_horizons_are_not_reclassified_by_bundle_order(tmp_path):
    store = V4Store(tmp_path / "validated-horizons.db")
    try:
        session = seed_session(store)
        context = seed_market_window(store, suffix="validated-horizons")
        base = {
            "session_id": session,
            "provider": "OKX",
            "instrument": "BTC-USDT",
            "asset": "BTC",
            "event_type": "ticker",
            "price": 100.0,
            "bid": 99.9,
            "ask": 100.1,
            "receipt_ts_ms": NOW,
            "monotonic_ns": NOW * 1_000_000,
            "classification": "NEW_TICK",
            "fresh": True,
        }
        source_reference = {
            "value": {
                "session_id": session,
                "source": "OKX",
                "channel": "tickers:BTC-USDT",
                "event_type": "ticker",
                "asset": "BTC",
                "dedupe_key": "validated-horizon-source",
                "provider_ts_ms": NOW-10,
                "receipt_ts_ms": NOW,
                "monotonic_ns": NOW * 1_000_000,
                "sequence_no": 20,
                "payload": {"price": 100},
            },
            "kwargs": {"session_id": session},
        }
        result = store.persist_evaluation_bundle({
            "cex": [
                {"value": {**base, "event_id": "newest", "provider_ts_ms": NOW-10,
                            "sequence_no": 20},
                 "kwargs": {"session_id": session,
                            "already_validated_at_receipt": True},
                 "source_event": source_reference,
                 "role": "POINT_IN_TIME_FEATURE", "horizon_ms": 100,
                 "evidence_age_ms": 10},
                {"value": {**base, "event_id": "older", "provider_ts_ms": NOW-1_000,
                            "sequence_no": 10},
                 "kwargs": {"session_id": session,
                            "already_validated_at_receipt": True},
                 "source_event": source_reference,
                 "role": "POINT_IN_TIME_FEATURE", "horizon_ms": 1_000,
                 "evidence_age_ms": 1_000},
            ],
            "candidate": {
                "session_id": session,
                "window_id": context["window_id"],
                "market_identity_id": context["identity_id"],
                "evaluation_ts_ms": NOW,
                "monotonic_ns": NOW * 1_000_000,
                "evaluation_seq": 99,
                "status": "NO_EDGE",
                "regime": "QUIET",
                "fair_probability_yes": 0.5,
                "fair_probability_no": 0.5,
                "calibrated": False,
                "reliability": 0.5,
                "positive_edge": False,
            },
            "fair_value": {
                "calculation": {
                    "phase": "FINAL",
                    "calculation_seq": 1,
                    "calculated_ts_ms": NOW,
                    "monotonic_ns": NOW * 1_000_000,
                    "regime": "QUIET",
                    "fair_probability_yes": 0.5,
                    "fair_probability_no": 0.5,
                    "calibrated": False,
                    "calibration_label": "UNCALIBRATED",
                },
                "sides": [],
            },
            "decision": {
                "decision_seq": 1,
                "decision_ts_ms": NOW,
                "monotonic_ns": NOW * 1_000_000,
                "phase": "FINAL",
                "action": "SKIP",
                "economic_gate_passed": False,
                "exact_depth_passed": False,
                "evidence_fresh": True,
                "reason": "no_positive_edge",
            },
        })
        assert len(result["cex_observation_ids"]) == 2
        assert result["cex_source_event_ids"][0] == result["cex_source_event_ids"][1]
        assert store.query(
            "SELECT event_id,classification,fresh,invalid_reason "
            "FROM cex_observations ORDER BY provider_ts_ms DESC"
        ) == [
            {"event_id": "newest", "classification": "NEW_TICK",
             "fresh": 1, "invalid_reason": None},
            {"event_id": "older", "classification": "NEW_TICK",
             "fresh": 1, "invalid_reason": None},
        ]
        assert store.query_one(
            "SELECT COUNT(*) count FROM candidate_cex_evidence "
            "WHERE candidate_id=?", (result["candidate_id"],)
        ) == {"count": 2}
        assert store.query_one(
            "SELECT duplicate_count FROM source_events WHERE dedupe_key=?",
            ("validated-horizon-source",),
        ) == {"duplicate_count": 0}
    finally:
        store.close()


def test_reference_only_source_resolution_never_inflates_duplicate_telemetry(tmp_path):
    store = V4Store(tmp_path / "reference-only-source.db")
    try:
        session = seed_session(store)
        event = {
            "session_id": session,
            "source": "OKX",
            "channel": "tickers:BTC-USDT",
            "event_type": "ticker",
            "asset": "BTC",
            "dedupe_key": "already-admitted-event",
            "provider_ts_ms": NOW-1,
            "receipt_ts_ms": NOW,
            "monotonic_ns": NOW * 1_000_000,
            "sequence_no": 1,
            "payload": {"price": 100},
        }
        first = store.record_source_event(
            event, now_ms=NOW, count_in_bucket=False,
            admitted_at_receipt=True, reference_only=True)
        second = store.record_source_event(
            event, now_ms=NOW, count_in_bucket=False,
            admitted_at_receipt=True, reference_only=True)
        row = store.query_one(
            "SELECT duplicate_count,last_duplicate_receipt_ts_ms "
            "FROM source_events WHERE source_event_id=?",
            (first["source_event_id"],),
        )
        assert first["inserted"] is True
        assert second["inserted"] is False
        assert second["duplicate"] is False
        assert second["accepted"] is True
        assert row == {"duplicate_count": 0,
                       "last_duplicate_receipt_ts_ms": None}
        assert store.query_one(
            "SELECT COUNT(*) count FROM event_buckets") == {"count": 0}
    finally:
        store.close()


def test_startup_reconciliation_abandons_only_proven_absent_owner_makers(tmp_path):
    store = V4Store(tmp_path / "maker-startup-reconcile.db")
    try:
        seed_session(store, session_id="absent-session")
        absent_context = seed_market_window(
            store, session_id="absent-session", suffix="absent-maker")
        absent_evidence = seed_candidate_entry_context(store, absent_context)
        seed_session(store, session_id="current-session")
        current_context = seed_market_window(
            store, session_id="current-session", open_ts=NOW-600_000,
            suffix="current-maker")
        current_evidence = seed_candidate_entry_context(store, current_context)

        def maker(context, evidence):
            return store.record_maker_observation({
                "window_id": context["window_id"],
                "candidate_id": evidence["candidate_id"],
                "decision_id": evidence["decision_id"],
                "initial_fair_value_calculation_id": evidence["fair_id"],
                "initial_book_snapshot_id": evidence["book_id"],
                "maker_start_ts_ms": evidence["entry_ts"],
                "maker_deadline_ts_ms": evidence["entry_ts"] + 1_000,
                "start_monotonic_ns": evidence["entry_ts"] * 1_000_000,
                "maker_target_price": 0.48,
                "chase_cap_price": 0.50,
                "initial_net_edge": 0.015,
                "maker_fill_assumed": False,
            })

        absent_maker = maker(absent_context, absent_evidence)
        current_maker = maker(current_context, current_evidence)
        result = store.reconcile_startup_state(
            current_launch_nonce="nonce-current-session",
            proven_absent_launch_nonces=("nonce-absent-session",),
            reconciled_ts_ms=NOW+5_000,
        )
        assert result["unfinished_maker_observations"] == 2
        assert result["reconciled_abandoned_maker_observations"] == 1
        assert result["unfinished_makers_left_fail_closed"] == 1
        assert store.query_one(
            "SELECT maker_end_ts_ms,outcome,reason,maker_fill_assumed "
            "FROM maker_observations WHERE maker_observation_id=?",
            (absent_maker,),
        ) == {
            "maker_end_ts_ms": NOW+5_000,
            "outcome": "ABANDONED",
            "reason": "STARTUP_RECONCILED",
            "maker_fill_assumed": 0,
        }
        assert store.query_one(
            "SELECT maker_end_ts_ms,outcome,reason,maker_fill_assumed "
            "FROM maker_observations WHERE maker_observation_id=?",
            (current_maker,),
        ) == {
            "maker_end_ts_ms": None,
            "outcome": None,
            "reason": None,
            "maker_fill_assumed": 0,
        }
        assert store.query_one("SELECT COUNT(*) count FROM entries") == {"count": 0}
    finally:
        store.close()


def test_bounded_retention_prunes_only_nontrade_graph_and_linked_raw_evidence(tmp_path):
    store = V4Store(tmp_path / "bounded-graph-retention.db")
    try:
        seed_session(store)
        trade_context = seed_market_window(
            store, open_ts=NOW-600_000, suffix="retained-trade")
        trade_evidence = seed_candidate_entry_context(store, trade_context, seq=1)
        trade_entry = create_entry(
            store, entry_payload(trade_context, trade_evidence,
                                 idem="retained-trade-entry"))

        raw_context = seed_market_window(
            store, open_ts=NOW-900_000, suffix="pruned-nontrade")
        raw_evidence = seed_candidate_entry_context(store, raw_context, seq=2)
        raw_lock = store.query_one(
            "SELECT * FROM window_locks WHERE window_id=?", (raw_context["window_id"],))
        assert store.release_window_reservation(
            window_id=raw_context["window_id"],
            session_id=raw_context["session_id"],
            owner_launch_nonce=raw_lock["owner_launch_nonce"],
            idempotency_key=raw_lock["idempotency_key"],
        )

        for _ in range(30):
            result = store.bounded_retention_step(
                cutoff_ts_ms=NOW-100_000,
                max_rows=250,
                deadline_monotonic=time.monotonic()+1.0,
                protect_trade_evidence=True,
                raw_event_max_rows=250_000,
                now_ms=NOW,
            )
            if result["action"] == "no_eligible_rows":
                break

        assert store.query_one(
            "SELECT candidate_id FROM candidates WHERE candidate_id=?",
            (raw_evidence["candidate_id"],),
        ) is None
        assert store.query_one(
            "SELECT source_event_id FROM source_events WHERE source_event_id=?",
            (raw_evidence["event_id"],),
        ) is None
        assert store.query_one(
            "SELECT book_snapshot_id FROM book_snapshots WHERE book_snapshot_id=?",
            (raw_evidence["book_id"],),
        ) is None
        assert store.query_one(
            "SELECT cex_observation_id FROM cex_observations "
            "WHERE cex_observation_id=?", (raw_evidence["cex_id"],),
        ) is None
        assert store.query_one(
            "SELECT candidate_id FROM candidates WHERE candidate_id=?",
            (trade_evidence["candidate_id"],),
        ) is not None
        assert store.query_one(
            "SELECT entry_id FROM entries WHERE entry_id=?", (trade_entry,)
        ) == {"entry_id": trade_entry}
        assert store.query_one(
            "SELECT retention_class,pin_count FROM source_events "
            "WHERE source_event_id=?", (trade_evidence["event_id"],)
        )["retention_class"] == "TRADE_EVIDENCE"
        assert store.integrity_check() == {
            "integrity": "ok", "foreign_key_violations": []}
    finally:
        store.close()


def test_journal_payload_compaction_keeps_idempotency_and_protected_commands(tmp_path):
    store = V4Store(tmp_path / "journal-retention.db")
    try:
        seed_session(store)
        context = seed_market_window(
            store, open_ts=NOW-600_000, suffix="journal-trade")
        evidence = seed_candidate_entry_context(store, context)
        create_entry(store, entry_payload(context, evidence, idem="journal-trade-entry"))

        def insert_command(name, *, status="COMMITTED", terminal=0,
                           window_id=None, trade_id=None):
            payload_hash = (name[0] * 64)[:64]
            values = {
                "command_id": name,
                "command_type": "EVALUATION",
                "method": "persist_evaluation_bundle",
                "idempotency_key": f"idem-{name}",
                "ordering_key": f"order-{name}",
                "priority": 10,
                "terminal": terminal,
                "associated_window_id": window_id,
                "associated_trade_id": trade_id,
                "payload_hash": payload_hash,
                "payload_json": '{"large":"' + ("x" * 1000) + '"}',
                "status": status,
                "attempt_count": 1,
                "submitted_ts_ms": NOW-20_000,
                "started_ts_ms": NOW-19_000,
            }
            if status == "COMMITTED":
                values.update({
                    "committed_ts_ms": NOW-18_000,
                    "completed_ts_ms": NOW-18_000,
                    "result_json": '{"candidate_id":1}',
                })
            with store.transaction(immediate=True) as conn:
                return store._insert("persistence_commands", values, conn=conn)

        insert_command("compactable")
        insert_command("terminal", terminal=1)
        insert_command("trade", window_id=context["window_id"])
        insert_command("ambiguous", status="EXECUTING")
        before = {
            row["command_id"]: row for row in store.query(
                "SELECT command_id,idempotency_key,payload_hash,payload_json,"
                "result_json,status FROM persistence_commands")
        }

        for _ in range(20):
            result = store.bounded_retention_step(
                cutoff_ts_ms=0,
                max_rows=10,
                deadline_monotonic=time.monotonic()+1.0,
                protect_trade_evidence=True,
                journal_payload_retention_ms=1_000,
                now_ms=NOW,
            )
            if result["metrics"].get("journal_payloads_compacted"):
                break

        after = {
            row["command_id"]: row for row in store.query(
                "SELECT command_id,idempotency_key,payload_hash,payload_json,"
                "result_json,status FROM persistence_commands")
        }
        assert after["compactable"]["payload_json"].startswith(
            '{"compacted":true,"payload_hash":')
        for field in ("idempotency_key", "payload_hash", "result_json", "status"):
            assert after["compactable"][field] == before["compactable"][field]
        for name in ("terminal", "trade", "ambiguous"):
            assert after[name] == before[name]
    finally:
        store.close()


def test_bounded_retention_deadline_prevents_any_transaction(tmp_path):
    store = V4Store(tmp_path / "retention-deadline.db")
    try:
        seed_session(store)
        before = store.transaction_counters
        result = store.bounded_retention_step(
            cutoff_ts_ms=NOW,
            max_rows=10,
            deadline_monotonic=time.monotonic()-1.0,
            protect_trade_evidence=True,
            now_ms=NOW,
        )
        assert result["deadline_exhausted"] is True
        assert result["budget_units"] == 0
        assert store.transaction_counters == before
    finally:
        store.close()


def test_bounded_retention_deadline_caps_sqlite_lock_wait(tmp_path):
    path = tmp_path / "retention-lock-deadline.db"
    owner = V4Store(path)
    contender = V4Store(path)
    try:
        seed_session(owner)
        started = time.monotonic()
        with owner.transaction(immediate=True):
            result = contender.bounded_retention_step(
                cutoff_ts_ms=NOW,
                max_rows=10,
                deadline_monotonic=time.monotonic()+0.02,
                protect_trade_evidence=True,
                now_ms=NOW,
            )
        elapsed = time.monotonic() - started
        assert result["deadline_exhausted"] is True
        assert result["budget_units"] == 0
        assert elapsed < 0.5
        assert contender.query_one("PRAGMA busy_timeout") == {
            "timeout": contender.busy_timeout_ms}
    finally:
        contender.close()
        owner.close()


def test_bounded_retention_rolls_buckets_and_bounds_maintenance_metadata(tmp_path):
    store = V4Store(tmp_path / "bounded-metadata.db")
    try:
        seed_session(store)
        store.record_event_count(
            receipt_ts_ms=NOW-10_000,
            source="OKX", channel="ticker", asset="BTC",
            event_type="ticker", classification="ACCEPTED",
            unique=True, duplicate=False, invalid=False,
        )
        store.record_event_count(
            receipt_ts_ms=NOW,
            source="OKX", channel="ticker", asset="BTC",
            event_type="ticker", classification="ACCEPTED",
            unique=True, duplicate=False, invalid=False,
        )
        sample = {
            "worker_thread_id": 123,
            "state": "HEALTHY",
            "queue_depth": 0,
            "queue_capacity": 100,
            "queue_high_water": 1,
            "oldest_queue_age_ms": 0,
            "commands_submitted": 1,
            "commands_committed": 1,
            "commands_failed": 0,
            "commands_retried": 0,
            "idempotent_replays": 0,
            "queue_full_count": 0,
            "timeout_count": 0,
            "transactions_started": 1,
            "transactions_committed": 1,
            "transactions_rolled_back": 0,
        }
        for ts in (NOW-10_000, NOW):
            store.record_persistence_worker_sample(
                {**sample, "sample_ts_ms": ts})
            with store.transaction(immediate=True) as conn:
                store._insert("checkpoint_runs", {
                    "started_ts_ms": ts,
                    "completed_ts_ms": ts,
                    "mode": "PASSIVE",
                    "reason": "test",
                    "before_wal_bytes": 0,
                    "after_wal_bytes": 0,
                    "duration_ms": 0.0,
                    "database_bytes": 0,
                    "success": 1,
                }, conn=conn)
                store._insert("retention_runs", {
                    "started_ts_ms": ts,
                    "completed_ts_ms": ts,
                    "raw_cutoff_ts_ms": max(0, ts-1_000),
                    "requested_batch_size": 1,
                }, conn=conn)

        aggregate_metrics = {}
        for _ in range(20):
            result = store.bounded_retention_step(
                cutoff_ts_ms=0,
                max_rows=10,
                deadline_monotonic=time.monotonic()+1.0,
                protect_trade_evidence=True,
                event_bucket_detail_retention_ms=1_000,
                metadata_retention_ms=1_000,
                metadata_max_rows=1_000,
                now_ms=NOW,
            )
            aggregate_metrics.update(result["metrics"])
            if result["action"] == "no_eligible_rows":
                break

        assert aggregate_metrics["event_bucket_detail_rows_compacted"] == 1
        assert aggregate_metrics["persistence_worker_samples_deleted"] == 1
        assert aggregate_metrics["checkpoint_runs_deleted"] == 1
        assert aggregate_metrics["retention_runs_deleted"] == 1
        assert store.query_one(
            "SELECT COUNT(*) count FROM event_buckets WHERE bucket_ms=1000"
        ) == {"count": 1}
        assert store.query_one(
            "SELECT SUM(raw_count) raw FROM event_buckets WHERE bucket_ms=60000"
        ) == {"raw": 1}
        assert store.query_one(
            "SELECT COUNT(*) count FROM persistence_worker_samples"
        ) == {"count": 1}
        assert store.query_one(
            "SELECT COUNT(*) count FROM checkpoint_runs") == {"count": 1}
        assert store.query_one(
            "SELECT COUNT(*) count FROM retention_runs") == {"count": 1}
    finally:
        store.close()


def test_background_gate_defers_outer_write_and_checkpoint_without_leak(tmp_path):
    path = tmp_path / "background-gate.db"
    initial = V4Store(path)
    seed_session(initial)
    initial.close()
    calls = {"admit": 0, "release": 0}

    def deny():
        calls["admit"] += 1
        return False

    def release():
        calls["release"] += 1

    store = V4Store(
        path,
        background_write_admission=deny,
        background_write_release=release,
    )
    try:
        with pytest.raises(V4BackgroundWriteDeferred):
            store.record_runtime_health({
                "session_id": "session-v4",
                "sample_ts_ms": NOW,
                "heartbeat_ts_ms": NOW,
                "pid": 123,
                "state": "RUNNING",
                "loop_lag_ms": 0,
            })
        with pytest.raises(V4BackgroundWriteDeferred):
            store.checkpoint(mode="PASSIVE", reason="critical_pending")
        assert calls == {"admit": 2, "release": 0}
        assert store.query_one(
            "SELECT COUNT(*) count FROM runtime_health") == {"count": 0}
        assert store.query_one(
            "SELECT COUNT(*) count FROM checkpoint_runs") == {"count": 0}
    finally:
        store.close()


def test_background_gate_releases_once_per_outer_transaction(tmp_path):
    calls = {"admit": 0, "release": 0}

    def admit():
        calls["admit"] += 1
        return True

    def release():
        calls["release"] += 1

    store = V4Store(
        tmp_path / "background-gate-release.db",
        background_write_admission=admit,
        background_write_release=release,
    )
    try:
        seed_session(store)
        assert calls == {"admit": 1, "release": 1}
        with store.transaction(immediate=True):
            store.record_runtime_health({
                "session_id": "session-v4",
                "sample_ts_ms": NOW,
                "heartbeat_ts_ms": NOW,
                "pid": 123,
                "state": "RUNNING",
                "loop_lag_ms": 0,
            })
        assert calls == {"admit": 2, "release": 2}
    finally:
        store.close()
