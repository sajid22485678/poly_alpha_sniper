"""Phase 1 dynamic-universe eligibility regression tests (requirements 1-12)."""
import pytest

from poly_alpha_sniper.lite_frequency_v4.contracts import AnchorStatus, MarketIdentity
from poly_alpha_sniper.lite_frequency_v4.export import _universe
from poly_alpha_sniper.lite_frequency_v4.store import (
    UniverseEligibilityError,
    V4Store,
)
from poly_alpha_sniper.lite_frequency_v4.universe import (
    EligibilityDecision,
    UNIVERSE_POLICY_VERSION,
    evaluate_market_identity,
    evaluate_persisted_market,
    reconstruct_market_identity,
)

from tests.test_frequency_v4_store import (
    NOW,
    create_entry,
    entry_payload,
    seed_candidate_entry_context,
    seed_market_window,
    seed_session,
)


def _identity(asset="XRP", *, open_ts=NOW - 300_000, **overrides) -> MarketIdentity:
    fields = dict(
        asset=asset,
        slug=f"{asset.lower()}-updown-5m-{open_ts // 1000}",
        market_id="m-1",
        event_id="e-1",
        condition_id="c-1",
        yes_token_id="tok-yes",
        no_token_id="tok-no",
        window_open_ms=open_ts,
        window_close_ms=open_ts + 300_000,
    )
    fields.update(overrides)
    return MarketIdentity(**fields)


# Requirement 1: eligible crypto assets pass without hardcoded asset names.
def test_arbitrary_crypto_assets_pass_without_hardcoded_names():
    for asset in ("XRP", "DOGE", "PEPE", "HYPE", "BNB", "SUI"):
        decision = evaluate_market_identity(_identity(asset))
        assert decision.eligible, (asset, decision.reasons)
        assert decision.policy_version == UNIVERSE_POLICY_VERSION
    # There is no whitelist: BTC has no special status either.
    assert evaluate_market_identity(_identity("BTC")).eligible


# Requirement 2: unsupported or ambiguous markets fail closed.
def test_unsupported_duration_or_contract_fails_closed():
    fifteen = _identity(
        "BTC", slug=f"btc-updown-15m-{(NOW - 300_000) // 1000}")
    decision = evaluate_market_identity(fifteen)
    assert not decision.eligible
    assert "contract_not_understood" in decision.reasons

    class Unaligned:  # duration != 300000 must be rejected at the policy too
        asset = "BTC"
        slug = f"btc-updown-5m-{(NOW - 300_000) // 1000}"
        market_id, event_id, condition_id = "m", "e", "c"
        yes_token_id, no_token_id = "y", "n"
        window_open_ms = NOW - 300_000
        window_close_ms = NOW + 300_000
        active, accepting_orders, closed, archived = True, True, False, False
        anchor_status = AnchorStatus.FIELD_MISSING

    bad = evaluate_market_identity(Unaligned())
    assert not bad.eligible and "duration_unsupported" in bad.reasons

    unknown_family = evaluate_market_identity(_identity(
        "BTC", slug="will-btc-hit-1m-2026"))
    assert not unknown_family.eligible

    not_executable = evaluate_market_identity(
        _identity("BTC", accepting_orders=False))
    assert not not_executable.eligible
    assert "market_not_executable" in not_executable.reasons


# Requirement 3: invalid event/condition/token association fails closed.
def test_invalid_associations_fail_closed():
    market_row = {
        "asset": "BTC", "slug": f"btc-updown-5m-{(NOW - 300_000) // 1000}",
        "polymarket_market_id": "m-1", "open_ts_ms": NOW - 300_000,
        "close_ts_ms": NOW, "status": "ACTIVE", "accepting_orders": 1,
    }
    identity_row = {
        "market_id": 1, "event_id": "e-1", "condition_id": "c-1",
        "yes_token_id": "y", "no_token_id": "n",
        "association_valid": 1, "token_pair_valid": 1, "ambiguous": 0,
    }
    assert evaluate_persisted_market(market_row, identity_row).eligible
    for corrupt in (
        {"association_valid": 0},
        {"token_pair_valid": 0},
        {"ambiguous": 1},
        {"yes_token_id": ""},
        {"yes_token_id": "n"},  # identical to the NO token
        {"event_id": ""},
    ):
        decision = evaluate_persisted_market(
            market_row, {**identity_row, **corrupt})
        assert not decision.eligible, corrupt
        assert decision.reasons
    assert not evaluate_persisted_market(None, identity_row).eligible
    assert not evaluate_persisted_market(market_row, None).eligible


# Requirement 4: corrupt/required-anchor evidence fails closed.
def test_anchor_evidence_failures_fail_closed():
    # This contract family requires no anchor: absent or unpublished passes.
    for status in (AnchorStatus.FIELD_MISSING, AnchorStatus.NOT_YET_PUBLISHED,
                   AnchorStatus.UNANCHORED):
        assert evaluate_market_identity(
            _identity("BTC", anchor_status=status)).eligible
    # Corrupted anchor metadata is a data-integrity rejection.
    corrupt = evaluate_market_identity(
        _identity("BTC", anchor_status=AnchorStatus.PARSE_FAILED))
    assert not corrupt.eligible and "anchor_evidence_corrupt" in corrupt.reasons
    # An anchored market must carry a positive price-to-beat.
    anchored = evaluate_market_identity(_identity(
        "BTC", anchor_status=AnchorStatus.ANCHORED, price_to_beat=64000.0))
    assert anchored.eligible
    # A persisted ANCHORED row without a price fails reconstruction closed.
    market_row = {
        "asset": "BTC", "slug": f"btc-updown-5m-{(NOW - 300_000) // 1000}",
        "polymarket_market_id": "m-1", "open_ts_ms": NOW - 300_000,
        "close_ts_ms": NOW, "status": "ACTIVE", "accepting_orders": 1,
    }
    identity_row = {
        "market_id": 1, "event_id": "e", "condition_id": "c",
        "yes_token_id": "y", "no_token_id": "n",
        "association_valid": 1, "token_pair_valid": 1, "ambiguous": 0,
    }
    broken = evaluate_persisted_market(
        market_row, identity_row, {"status": "ANCHORED", "price_to_beat": None})
    assert not broken.eligible
    assert "identity_reconstruction_failed" in broken.reasons


# Requirements 5-7: stale evidence and fee-negative candidates fail closed at
# the store's atomic entry gates (decision row must have passed freshness and
# economic gates; the entry row must carry positive fee-net edge).
def test_stale_or_fee_negative_evidence_fails_closed_at_entry(tmp_path):
    store = V4Store(tmp_path / "stale.db")
    try:
        seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        with store.transaction(immediate=True) as conn:
            conn.execute(
                "UPDATE decisions SET evidence_fresh=0 WHERE decision_id=?",
                (evidence["decision_id"],))
        with pytest.raises(ValueError, match="economic/depth/freshness"):
            create_entry(store, entry_payload(context, evidence))
        with store.transaction(immediate=True) as conn:
            conn.execute(
                "UPDATE decisions SET evidence_fresh=1 WHERE decision_id=?",
                (evidence["decision_id"],))
        fee_negative = {**entry_payload(context, evidence),
                        "selected_net_edge": -0.01}
        with pytest.raises(ValueError, match="positive fee-net edge"):
            create_entry(store, fee_negative)
        assert store.query_one("SELECT COUNT(*) n FROM entries")["n"] == 0
    finally:
        store.close()


def _seed_observed_only_market(store, **kwargs):
    """Seed a market whose persisted state the canonical policy rejects."""
    context = seed_market_window(store, **kwargs)
    with store.transaction(immediate=True) as conn:
        conn.execute(
            "UPDATE markets SET accepting_orders=0 WHERE market_id=?",
            (context["market_id"],))
        conn.execute(
            """UPDATE window_market_links SET eligibility_status='OBSERVED_ONLY',
               reject_reason='market_not_executable' WHERE window_id=?""",
            (context["window_id"],))
        conn.execute(
            "UPDATE window_funnel SET eligible=0 WHERE window_id=?",
            (context["window_id"],))
    return context


# Requirements 8-11: a non-eligible market cannot reserve exposure, consume
# concurrency, or reach execution — even via direct candidate injection.
def test_rejected_market_cannot_execute_reserve_or_consume_slots(tmp_path):
    store = V4Store(tmp_path / "boundary.db")
    try:
        seed_session(store)
        rejected = _seed_observed_only_market(store)
        injected = seed_candidate_entry_context(store, rejected)
        with pytest.raises(UniverseEligibilityError, match="market_not_executable"):
            create_entry(store, entry_payload(rejected, injected))
        # Fail-closed rollback: no entry, no position, no committed capital,
        # no concurrency slot consumed.
        assert store.query_one("SELECT COUNT(*) n FROM entries")["n"] == 0
        assert store.query_one("SELECT COUNT(*) n FROM positions")["n"] == 0

        # An eligible market in the same cohort still has the full ledger and
        # every concurrency slot available.
        eligible = seed_market_window(
            store, asset="ETH", open_ts=NOW - 300_000, suffix="ok")
        okay = seed_candidate_entry_context(store, eligible, seq=2)
        entry_id = create_entry(
            store, entry_payload(eligible, okay, idem="eligible-entry")
            | {"token_id": eligible["yes_token"]})
        assert entry_id > 0
    finally:
        store.close()


# Requirement 11 (variant): tampering with committed identity rows after the
# candidate exists still fails closed inside the entry transaction.
def test_identity_tamper_after_candidate_fails_closed(tmp_path):
    store = V4Store(tmp_path / "tamper.db")
    try:
        seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        with store.transaction(immediate=True) as conn:
            conn.execute(
                "UPDATE market_identities SET association_valid=0 "
                "WHERE market_identity_id=?", (context["identity_id"],))
        with pytest.raises(UniverseEligibilityError, match="association_invalid"):
            create_entry(store, entry_payload(context, evidence))
        assert store.query_one("SELECT COUNT(*) n FROM positions")["n"] == 0
    finally:
        store.close()


# Requirement 12: eligibility rejection reasons are persisted and reported.
def test_rejection_reasons_are_persisted_and_reported(tmp_path):
    store = V4Store(tmp_path / "reporting.db")
    try:
        seed_session(store)
        _seed_observed_only_market(store, open_ts=NOW - 60_000)
        universe = _universe(store, NOW)
        assert universe["dynamic_universe_enabled"] is True
        assert universe["universe_policy_version"] == UNIVERSE_POLICY_VERSION
        assert universe["hardcoded_to_required_assets_only"] is False
        assert universe["execution_fail_closed"] is True
        assert universe["observed_only_market_count_active"] == 1
        rejection = universe["universe_rejections_active"][0]
        assert rejection["eligibility_status"] == "OBSERVED_ONLY"
        assert rejection["reject_reason"] == "market_not_executable"
        assert universe["observed_only_assets_active"] == ["BTC"]
    finally:
        store.close()


def test_reconstruct_market_identity_round_trip():
    market_row = {
        "asset": "DOGE", "slug": f"doge-updown-5m-{(NOW - 300_000) // 1000}",
        "polymarket_market_id": "pm-doge", "open_ts_ms": NOW - 300_000,
        "close_ts_ms": NOW, "status": "ACTIVE", "accepting_orders": 1,
    }
    identity_row = {
        "event_id": "e", "condition_id": "c",
        "yes_token_id": "y", "no_token_id": "n",
    }
    identity = reconstruct_market_identity(market_row, identity_row)
    assert identity is not None and identity.asset == "DOGE"
    decision = evaluate_market_identity(identity)
    assert isinstance(decision, EligibilityDecision) and decision.eligible
