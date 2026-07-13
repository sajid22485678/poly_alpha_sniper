from concurrent.futures import ThreadPoolExecutor
import sqlite3

import pytest

from poly_alpha_sniper.lite_frequency_v4.contracts import CexObservation, SourceEvent
from poly_alpha_sniper.lite_frequency_v4.store import (
    EXPECTED_TABLES,
    ExposureLimitExceeded,
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
    one = V4Store(path)
    seed_session(one)
    context = seed_market_window(one)
    evidence = seed_candidate_entry_context(one, context)
    two = V4Store(path)
    payloads = [entry_payload(context, evidence, idem=f"contender-{n}") for n in (1, 2)]

    def attempt(pair):
        store, payload = pair
        try:
            return ("ok", create_entry(store, payload))
        except WindowReservationConflict as exc:
            return ("blocked", exc.reason)

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(attempt, ((one, payloads[0]), (two, payloads[1]))))
        assert sorted(result[0] for result in results) == ["blocked", "ok"]
        assert one.query_one("SELECT COUNT(*) count FROM entries")["count"] == 1
        entry = one.query_one("SELECT * FROM entries")
        assert entry["shares"] == 5.0
        assert entry["maker_fill_assumed"] == 0
        assert one.query_one("SELECT COUNT(*) count FROM positions WHERE status='OPEN'")["count"] == 1
        assert one.integrity_check()["foreign_key_violations"] == []
    finally:
        one.close()
        two.close()


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
    one = V4Store(path)
    seed_session(one)
    first_context = seed_market_window(
        one, open_ts=NOW - 600_000, suffix="asset-cap-one"
    )
    second_context = seed_market_window(
        one, open_ts=NOW - 300_000, suffix="asset-cap-two"
    )
    first_evidence = seed_candidate_entry_context(one, first_context, seq=1)
    second_evidence = seed_candidate_entry_context(one, second_context, seq=2)
    two = V4Store(path)

    def attempt(store, context, evidence, contender):
        try:
            entry_id = store.create_entry(
                entry_payload(context, evidence, idem=f"asset-cap-{contender}"),
                max_concurrent_positions=10,
                global_exposure_cap_usd=100.0,
                per_asset_exposure_cap_usd=100.0,
                max_open_per_asset=1,
            )
            return "ok", entry_id
        except ExposureLimitExceeded as exc:
            return "blocked", exc.reason

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = (
                pool.submit(attempt, one, first_context, first_evidence, "one"),
                pool.submit(attempt, two, second_context, second_evidence, "two"),
            )
            results = [future.result() for future in futures]
        assert sorted(result[0] for result in results) == ["blocked", "ok"]
        assert {result[1] for result in results if result[0] == "blocked"} == {
            "max_open_per_asset"
        }
        assert one.query_one("SELECT COUNT(*) count FROM entries") == {"count": 1}
        assert one.query_one(
            "SELECT COUNT(*) count FROM positions WHERE asset='BTC' AND status='OPEN'"
        ) == {"count": 1}
        assert one.integrity_check() == {
            "integrity": "ok",
            "foreign_key_violations": [],
        }
    finally:
        one.close()
        two.close()


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
