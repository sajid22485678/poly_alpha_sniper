"""Phase 1 authoritative $13 capital-ledger regression tests (requirements 13-26)."""
import pytest

from poly_alpha_sniper.lite_frequency_v4.config import ACTIVE_COHORT
from poly_alpha_sniper.lite_frequency_v4.ledger import compute_capital_ledger
from poly_alpha_sniper.lite_frequency_v4.risk import conservative_exit_fee_buffer
from poly_alpha_sniper.lite_frequency_v4.store import (
    ExposureLimitExceeded,
    MIN_EXIT_FEE_BUFFER_USD,
    V4Store,
    V4StoreError,
)

from tests.test_frequency_v4_store import (
    NOW,
    create_entry,
    entry_payload,
    seed_candidate_entry_context,
    seed_market_window,
    seed_session,
)


BUFFER = conservative_exit_fee_buffer(0.07, 0.02)


def _ledger(store):
    return compute_capital_ledger(store.query, cohort=ACTIVE_COHORT,
                                  fee_rate=0.07, fee_buffer_usd=0.02)


def _open_entry(store, *, asset="BTC", suffix="1", open_ts=NOW - 300_000,
                seq=1, idem=None):
    context = seed_market_window(store, asset=asset, suffix=suffix,
                                 open_ts=open_ts)
    evidence = seed_candidate_entry_context(store, context, seq=seq)
    entry_id = create_entry(store, entry_payload(
        context, evidence, idem=idem or f"entry-{suffix}"))
    return context, evidence, entry_id


def _close(store, context, entry_id, *, net_pnl, payout):
    position = store.query_one(
        "SELECT * FROM positions WHERE entry_id=?", (entry_id,))
    store.record_resolution_attempt({
        "entry_id": entry_id, "attempt_no": 1,
        "attempt_ts_ms": context["close_ts"] + 1,
        "source": "POLYMARKET_OFFICIAL", "result": "RESOLVED",
        "observed_outcome": "YES" if payout > 0 else "NO",
        "evidence_hash": "f" * 64, "verified": True,
    })
    store.close_position({
        "position_id": position["position_id"],
        "exit_ts_ms": context["close_ts"] + 2,
        "exit_source": "OFFICIAL_RESOLUTION",
        "shares": 5.0,
        "payout_usd": payout,
        "gross_pnl": payout - 2.45,
        "exit_fee": 0.0,
        "net_pnl": net_pnl,
        "evidence_verified": True,
        "resolution_outcome": "YES" if payout > 0 else "NO",
        "reason": "official_resolution",
    })


# Requirement 13: the new cohort begins with exactly 13.00 USD.
def test_new_cohort_begins_with_exactly_thirteen_dollars(tmp_path):
    store = V4Store(tmp_path / "thirteen.db")
    try:
        seed_session(store)
        ledger = _ledger(store)
        assert ledger.starting_equity_usd == 13.0
        assert ledger.current_equity_usd == 13.0
        assert ledger.available_cash_usd == 13.0
        assert ledger.committed_total_usd == 0.0
    finally:
        store.close()


# Requirements 14-16: maximum exposure is exactly 100%; committed capital may
# reach 100% of equity but never exceed it.
def test_exposure_may_reach_but_never_exceed_100_percent(tmp_path):
    store = V4Store(tmp_path / "full-exposure.db")
    try:
        # Equity sized so one 5-share entry commits EXACTLY 100%:
        # gross 2.45 + entry fee 0.05 + exit buffer.
        equity = round(2.45 + 0.05 + 0.1075, 10)
        seed_session(store, starting_equity=equity)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        entry_id = store.create_entry(
            entry_payload(context, evidence),
            max_concurrent_positions=4, cohort=ACTIVE_COHORT,
            starting_equity_usd=equity, max_exposure_pct=1.0,
            exit_fee_buffer_usd=0.1075,
        )
        assert entry_id > 0
        ledger = compute_capital_ledger(store.query, cohort=ACTIVE_COHORT,
                                        fee_rate=0.07, fee_buffer_usd=0.02)
        assert ledger.max_exposure_pct == 1.0
        assert ledger.exposure_pct == pytest.approx(1.0)
        assert ledger.available_cash_usd == pytest.approx(0.0)
        assert ledger.invariant_committed_within_equity

        # A second entry must fail closed: committed can never exceed equity.
        second = seed_market_window(store, asset="ETH", suffix="2")
        second_evidence = seed_candidate_entry_context(store, second, seq=2)
        with pytest.raises(ExposureLimitExceeded, match="insufficient_capital"):
            store.create_entry(
                entry_payload(second, second_evidence, idem="second"),
                max_concurrent_positions=4, cohort=ACTIVE_COHORT,
                starting_equity_usd=equity, max_exposure_pct=1.0,
                exit_fee_buffer_usd=0.1075,
            )
        assert store.query_one(
            "SELECT peak_exposure_pct FROM cohorts WHERE cohort=?",
            (ACTIVE_COHORT,))["peak_exposure_pct"] == pytest.approx(1.0)
    finally:
        store.close()


# Requirements 17-18: every asset and every execution path share one ledger.
def test_all_assets_share_one_global_ledger(tmp_path):
    store = V4Store(tmp_path / "one-ledger.db")
    try:
        seed_session(store)
        _open_entry(store, asset="BTC", suffix="a", seq=1)
        after_one = _ledger(store)
        _open_entry(store, asset="XRP", suffix="b", seq=2,
                    open_ts=NOW - 300_000)
        after_two = _ledger(store)
        per_entry = round(2.45 + 0.05 + BUFFER, 10)
        assert after_one.committed_total_usd == pytest.approx(per_entry)
        assert after_two.committed_total_usd == pytest.approx(2 * per_entry)
        assert after_two.available_cash_usd == pytest.approx(13.0 - 2 * per_entry)
        # The bundle path uses the same ledger and the same cohort scope.
        third = seed_market_window(store, asset="DOGE", suffix="c")
        third_evidence = seed_candidate_entry_context(store, third, seq=3)
        reservation = store.query_one(
            "SELECT * FROM window_locks WHERE window_id=?",
            (third["window_id"],))
        result = store.reserve_and_create_entry_bundle(
            reservation, entry_payload(third, third_evidence, idem="bundle"),
            max_concurrent_positions=6, cohort=ACTIVE_COHORT,
            starting_equity_usd=13.0, max_exposure_pct=1.0,
            exit_fee_buffer_usd=BUFFER,
        )
        assert result["entry_id"] > 0
        assert _ledger(store).committed_total_usd == pytest.approx(3 * per_entry)
    finally:
        store.close()


# Requirements 19-20: unaffordable five-share orders fail closed and fee +
# buffer components are part of the committed calculation.
def test_unaffordable_order_fails_closed_with_fees_and_buffers(tmp_path):
    store = V4Store(tmp_path / "unaffordable.db")
    try:
        # gross 2.45 + fee 0.05 affordable alone, but not with the mandatory
        # conservative exit-fee buffer: the buffer must be part of the check.
        equity = 2.50
        seed_session(store, starting_equity=equity)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        with pytest.raises(ExposureLimitExceeded, match="insufficient_capital"):
            store.create_entry(
                entry_payload(context, evidence),
                max_concurrent_positions=4, cohort=ACTIVE_COHORT,
                starting_equity_usd=equity, max_exposure_pct=1.0,
                exit_fee_buffer_usd=BUFFER,
            )
        # The store refuses a caller trying to shrink the buffer below the
        # conservative worst-case floor.
        with pytest.raises(V4StoreError, match="exit fee buffer"):
            store.create_entry(
                entry_payload(context, evidence),
                max_concurrent_positions=4, cohort=ACTIVE_COHORT,
                starting_equity_usd=equity, max_exposure_pct=1.0,
                exit_fee_buffer_usd=MIN_EXIT_FEE_BUFFER_USD / 2,
            )
    finally:
        store.close()


# Requirements 21-22: reservations reduce available cash; releasing a
# reservation frees the capital exactly once.
def test_reservations_hold_and_release_capital_exactly_once(tmp_path):
    store = V4Store(tmp_path / "reservations.db")
    try:
        seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        held = 2.6075
        with store.transaction(immediate=True) as conn:
            conn.execute(
                "UPDATE window_locks SET reserved_commitment_usd=? WHERE window_id=?",
                (held, context["window_id"]))
        ledger = _ledger(store)
        assert ledger.reserved_order_usd == pytest.approx(held)
        assert ledger.available_cash_usd == pytest.approx(13.0 - held)
        lock = store.query_one(
            "SELECT * FROM window_locks WHERE window_id=?",
            (context["window_id"],))
        release = dict(
            window_id=context["window_id"], session_id=context["session_id"],
            owner_launch_nonce=lock["owner_launch_nonce"],
            idempotency_key=lock["idempotency_key"])
        assert store.release_window_reservation(**release) is True
        assert _ledger(store).available_cash_usd == pytest.approx(13.0)
        # Releasing again is a no-op: capital cannot be freed twice.
        assert store.release_window_reservation(**release) is False
        assert _ledger(store).available_cash_usd == pytest.approx(13.0)
        _ = evidence
    finally:
        store.close()


# Requirement 23: unresolved positions retain their committed capital.
def test_unresolved_positions_retain_committed_capital(tmp_path):
    store = V4Store(tmp_path / "unresolved.db")
    try:
        seed_session(store)
        context, _, entry_id = _open_entry(store)
        before = _ledger(store)
        store.mark_unresolved_final(
            entry_id, context["close_ts"] + 10, "resolution_unavailable")
        after = _ledger(store)
        assert after.unresolved_capital_usd == pytest.approx(2.50)
        # Cost stays committed; only the per-position exit buffer (which an
        # unresolvable position can never spend) is released.
        assert after.committed_total_usd == pytest.approx(
            before.committed_total_usd - BUFFER)
        assert after.current_equity_usd == 13.0  # no fabricated outcome
    finally:
        store.close()


# Requirement 24: restart recovery restores balances exactly.
def test_restart_restores_ledger_exactly(tmp_path):
    path = tmp_path / "restart.db"
    store = V4Store(path)
    try:
        seed_session(store)
        context, _, entry_id = _open_entry(store)
        _close(store, context, entry_id, net_pnl=2.50, payout=5.0)
        _open_entry(store, asset="ETH", suffix="again", seq=2)
        before = _ledger(store)
        counts = store.reconcile_startup_state(
            current_launch_nonce="nonce-session-v4")
        assert counts["consistency_errors"] == 0
        assert counts["cohort_ledger_violations"] == 0
    finally:
        store.close()
    reopened = V4Store(path)
    try:
        after = _ledger(reopened)
        assert after == before
        assert after.current_equity_usd == pytest.approx(15.50)
        assert after.realized_net_pnl_usd == pytest.approx(2.50)
    finally:
        reopened.close()


# Requirements 25-26: no leverage or synthetic cash; available cash can never
# go negative even after losses.
def test_losses_shrink_equity_without_negative_cash_or_synthetic_topups(tmp_path):
    store = V4Store(tmp_path / "loss.db")
    try:
        seed_session(store)
        context, _, entry_id = _open_entry(store)
        _close(store, context, entry_id, net_pnl=-2.50, payout=0.0)
        ledger = _ledger(store)
        assert ledger.current_equity_usd == pytest.approx(10.50)
        assert ledger.available_cash_usd == pytest.approx(10.50)
        assert ledger.available_cash_usd >= 0
        assert ledger.committed_total_usd == 0.0
        # The shrunken equity is now the hard cap for new commitments.
        for index, asset in enumerate(("ETH", "XRP", "DOGE", "BNB")):
            if _ledger(store).available_cash_usd < 2.45 + 0.05 + BUFFER:
                ctx = seed_market_window(store, asset=asset,
                                         suffix=f"fill-{index}")
                ev = seed_candidate_entry_context(store, ctx, seq=10 + index)
                with pytest.raises(ExposureLimitExceeded):
                    create_entry(store, entry_payload(
                        ctx, ev, idem=f"fill-{index}"))
                break
            _open_entry(store, asset=asset, suffix=f"fill-{index}",
                        seq=10 + index)
        final = _ledger(store)
        assert final.available_cash_usd >= 0
        assert final.committed_total_usd <= final.current_equity_usd + 1e-9
    finally:
        store.close()
