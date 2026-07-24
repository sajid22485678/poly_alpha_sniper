"""Phase 2B C1 cluster arbitration: deterministic candidate identity,
ranking, the persisted arbitration state machine, and cluster occupancy.

C1 scope only.  This module implements the content-derived candidate key
and set fingerprint, ranking version 1, and the persisted arbitration
transitions and cluster-lock occupancy over the managed-v5 schema (see
``store.PHASE2B_SCHEMA_V5_SQL``).  It deliberately implements **no**
operator cancellation CLI, no successor activation, no cutover/hold
semantics, and no change to startup authority: an ordinary shutdown or
restart preserves ``COLLECTING`` arbitration rows, and only a proven
unentered ``RESERVED`` lock can be released.

The arbitration state machine (legal transitions only):

    NONE       -> COLLECTING
    COLLECTING -> SELECTED | CANCELLED
    SELECTED   -> RESERVING | EXHAUSTED | CANCELLED
    RESERVING  -> ENTERED | SELECTED

The first event for a cluster is exactly ``seq=1``, ``NONE -> COLLECTING``;
event sequences are gap-free and unique per cluster; terminal states never
transition.  ``ENTERED`` occupancy is permanent; no fallback candidate may
enter after ``ENTERED``.

All writes go through a caller-supplied transactional connection within a
``store.transaction()``; this module never commits or opens its own
database.  Reads use the same connection.

**Concurrency contract (C1.D-12).** Event-sequence advancement is a
read-modify-write over ``cluster_arbitration_events.seq``.  The schema's
``UNIQUE(cohort, cluster_open_ts_ms, seq)`` constraint makes a double
advance impossible, but a deferred ``BEGIN`` can surface the loser as a
raw ``sqlite3.OperationalError`` / ``IntegrityError`` instead of a
classified fail-closed result.  To keep arbitration deterministic:

  * every caller MUST wrap arbitration mutations in
    ``store.transaction(immediate=True)`` (the engine's persistence
    writer already owns an outer IMMEDIATE transaction; ``cluster.py``
    must never commit);
  * this module classifies a concurrent-write failure at the event-INSERT
    boundary as a ``ClusterArbitrationError`` so no raw sqlite3 error
    escapes the arbitration API.

``transaction()``'s default is deliberately left deferred so unrelated
write paths do not gain excessive lock duration.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal, localcontext
from typing import Any, Iterable, Mapping, Optional, Sequence

# ---- arbitration statuses ------------------------------------------------

NONE = "NONE"
COLLECTING = "COLLECTING"
SELECTED = "SELECTED"
RESERVING = "RESERVING"
ENTERED = "ENTERED"
EXHAUSTED = "EXHAUSTED"
CANCELLED = "CANCELLED"

TERMINAL_STATUSES = frozenset({ENTERED, EXHAUSTED, CANCELLED})

# Legal (from_status -> {to_status, ...}) transition table.  This mirrors
# the table-level CHECK constraint on cluster_arbitration_events exactly.
LEGAL_TRANSITIONS: Mapping[str, frozenset[str]] = {
    NONE: frozenset({COLLECTING}),
    COLLECTING: frozenset({SELECTED, CANCELLED}),
    SELECTED: frozenset({RESERVING, EXHAUSTED, CANCELLED}),
    RESERVING: frozenset({ENTERED, SELECTED}),
}

# Actor vocabulary (mirrors the schema CHECK).  C1 uses ENGINE and
# RECOVERY only; OPERATOR_TOOL is reserved for the (out-of-scope) operator
# cancellation CLI and is honored here only for replay validation.
ACTOR_ENGINE = "ENGINE"
ACTOR_RECOVERY = "RECOVERY"
ACTOR_OPERATOR_TOOL = "OPERATOR_TOOL"
ACTORS = frozenset({ACTOR_ENGINE, ACTOR_RECOVERY, ACTOR_OPERATOR_TOOL})

# Authoritative cancellation actor/reason matrix
# (09_FINAL_OPERATOR_RUNBOOK.md "Authoritative cancellation actor/reason
# matrix").  ``operator_cancelled`` is OPERATOR_TOOL-only; the two evidence
# reasons are ENGINE/RECOVERY-only; nothing else transitions to CANCELLED.
REASON_OPERATOR_CANCELLED = "operator_cancelled"
REASON_CLUSTER_WINDOW_EXPIRED = "cluster_window_expired"
REASON_CLUSTER_EVIDENCE_INVALIDATED = "cluster_evidence_invalidated"
CANCELLATION_MATRIX: Mapping[str, frozenset[str]] = {
    REASON_OPERATOR_CANCELLED: frozenset({ACTOR_OPERATOR_TOOL}),
    REASON_CLUSTER_WINDOW_EXPIRED: frozenset({ACTOR_ENGINE, ACTOR_RECOVERY}),
    REASON_CLUSTER_EVIDENCE_INVALIDATED: frozenset({ACTOR_ENGINE, ACTOR_RECOVERY}),
}
CANCELLATION_REASONS = frozenset(CANCELLATION_MATRIX)

RANKING_VERSION = 1
QUANTUM = 1e-6


class ClusterArbitrationError(RuntimeError):
    """Fail-closed arbitration error."""


@dataclass(frozen=True)
class Candidate:
    """A rankable cluster candidate.

    Identity is content-derived: the candidate key is computed from the
    canonical sorted-key JSON of these fields with fixed nine-decimal
    numeric formatting, then lowercased SHA-256.  ``net_edge`` and
    ``depth`` are required finite numbers; ``asset`` is upper-cased by the
    caller to match the persisted market identity convention.
    """

    asset: str
    window_id: int
    selected_side: str            # 'YES' or 'NO'
    fair_probability: float       # probability of the selected side
    net_edge: float
    depth: float
    # Optional content extensions that disambiguate otherwise-identical
    # candidates (market identity tokens, evaluation timestamp).  These are
    # part of the content key so replay is deterministic.
    market_identity: Mapping[str, Any] = field(default_factory=dict)
    evaluation_ts_ms: Optional[int] = None


def _require_finite(name: str, value: Any) -> float:
    """Reject non-finite or non-numeric inputs with a precise error."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ClusterArbitrationError(f"candidate {name} must be a number: {value!r}")
    f = float(value)
    if not math.isfinite(f):
        raise ClusterArbitrationError(f"candidate {name} must be finite: {value!r}")
    return f


def _nine(value: float) -> str:
    """Fixed nine-decimal numeric formatting for deterministic content keys.

    Signed zero is normalized first: ``-0.0`` and ``+0.0`` are numerically
    equal and must produce the identical canonical string ``"0.000000000"``
    so two equal candidates never diverge in key space (C1.F: identical
    content -> identical keys).  Callers must pre-validate finiteness.
    """
    # ``+0.0`` is the canonical zero; adding 0.0 collapses -0.0 to +0.0
    # without changing any other value.
    if value == 0.0:
        value = value + 0.0
    return f"{value:.9f}"


def candidate_content_payload(candidate: Candidate) -> dict[str, Any]:
    """Canonical, sorted-key content mapping for a candidate.

    Floats are fixed nine-decimal formatted strings so identical candidate
    content produces identical keys across replay, order, process, and
    migration paths.  Non-finite values are rejected.
    """
    probability = _require_finite("fair_probability", candidate.fair_probability)
    net_edge = _require_finite("net_edge", candidate.net_edge)
    depth = _require_finite("depth", candidate.depth)
    if candidate.selected_side not in ("YES", "NO"):
        raise ClusterArbitrationError(
            f"selected_side must be YES or NO: {candidate.selected_side!r}")
    asset = str(candidate.asset).upper()
    return {
        "asset": asset,
        "window_id": int(candidate.window_id),
        "selected_side": str(candidate.selected_side),
        "fair_probability": _nine(probability),
        "net_edge": _nine(net_edge),
        "depth": _nine(depth),
        "market_identity": _canonical_mapping(candidate.market_identity),
        "evaluation_ts_ms": candidate.evaluation_ts_ms,
    }


def _canonical_mapping(value: Any) -> Any:
    """Recursively normalize nested mappings/lists for canonical encoding.

    Nested floats are routed through :func:`_nine` so a candidate's
    ``market_identity`` uses the same nine-decimal encoding as the
    top-level numeric fields (C1.F: one canonical numeric path).  Unknown
    types are rejected rather than falling back to ``str()``, which would
    admit process-nondeterministic content.
    """
    if isinstance(value, Mapping):
        return {str(k): _canonical_mapping(value[k]) for k in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_canonical_mapping(v) for v in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ClusterArbitrationError(f"non-finite value in market_identity: {value!r}")
        return _nine(value)
    if value is None:
        return None
    if isinstance(value, str):
        return str(value)
    raise ClusterArbitrationError(
        f"unsupported market_identity value type {type(value).__name__}: {value!r}")


def canonical_candidate_json(candidate: Candidate) -> str:
    """Canonical sorted-key JSON for a single candidate (compact, ascii)."""
    return json.dumps(
        candidate_content_payload(candidate),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def candidate_key(candidate: Candidate) -> str:
    """Lowercase SHA-256 over the candidate's canonical content JSON."""
    return hashlib.sha256(canonical_candidate_json(candidate).encode("utf-8")).hexdigest()


def candidate_set_fingerprint(candidates: Sequence[Candidate]) -> str:
    """Order-independent lowercase SHA-256 fingerprint of a candidate set.

    Computed over the sorted multiset of per-candidate content keys so the
    fingerprint is deterministic regardless of input order, process, or
    replay path.
    """
    if not candidates:
        raise ClusterArbitrationError("candidate set must be non-empty")
    keys = sorted(candidate_key(c) for c in candidates)
    payload = json.dumps(
        {"ranking_version": RANKING_VERSION, "candidate_keys": keys},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _quantize(value: float) -> int:
    """Quantize a float to the 1e-6 grid as an integer key (for ranking).

    Representation-stable: uses :class:`decimal.Decimal` with explicit
    half-up rounding so the result is independent of float-division error
    and of the rounding mode of the host's float ``round()`` (banker's
    rounding).  Authority (C1.F) ranks by net edge "quantized to 1e-6";
    the rounding mode is half-up (a value exactly halfway between two grid
    points rounds away from zero).
    """
    f = _require_finite("rank_value", value)
    # Decimal(repr(float)) preserves the float's short repr exactly, so the
    # grid mapping is deterministic across processes.  12 digits of working
    # precision is plenty for a 1e-6 grid on values near 1.0.
    with localcontext() as ctx:
        ctx.prec = 12
        scaled = (Decimal(repr(f)) / Decimal(str(QUANTUM))).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP)
    return int(scaled)


def rank_candidates(candidates: Sequence[Candidate]) -> list[tuple[int, Candidate]]:
    """Rank candidates by ranking version 1.

    Order: net edge quantized 1e-6 descending, then depth quantized 1e-6
    descending, then asset ascending, then candidate key ascending.
    Returns ``(rank, candidate)`` pairs with rank starting at 1.
    """
    keyed = []
    for c in candidates:
        # Pre-validate (raises on non-finite).
        candidate_content_payload(c)
        keyed.append((
            -_quantize(c.net_edge),   # descending
            -_quantize(c.depth),      # descending
            str(c.asset).upper(),     # ascending
            candidate_key(c),         # ascending
            c,
        ))
    keyed.sort(key=lambda item: item[:4])
    return [(rank, item[4]) for rank, item in enumerate(keyed, start=1)]


# ---- persisted arbitration state machine ---------------------------------


@dataclass(frozen=True)
class ArbitrationIdentity:
    cohort: str
    cluster_open_ts_ms: int


@dataclass(frozen=True)
class ArbitrationEvent:
    seq: int
    from_status: str
    to_status: str
    candidate_key: Optional[str]
    candidate_rank: Optional[int]
    reason: str
    actor: str
    session_id: str
    event_ts_ms: int


@dataclass(frozen=True)
class ArbitrationState:
    identity: ArbitrationIdentity
    status: str
    ranking_version: int
    candidate_set_fingerprint: Optional[str]
    selected_candidate_key: Optional[str]
    selected_rank: Optional[int]
    selected_window_id: Optional[int]
    selected_asset: Optional[str]
    events: tuple[ArbitrationEvent, ...]


def _check_transition(from_status: str, to_status: str) -> None:
    allowed = LEGAL_TRANSITIONS.get(from_status)
    if allowed is None or to_status not in allowed:
        raise ClusterArbitrationError(
            f"illegal arbitration transition: {from_status} -> {to_status}")


def _validate_actor(actor: str) -> None:
    if actor not in ACTORS:
        raise ClusterArbitrationError(f"unknown arbitration actor: {actor!r}")


def _next_seq(conn: Any, identity: ArbitrationIdentity) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) + 1 AS n FROM cluster_arbitration_events "
        "WHERE cohort=? AND cluster_open_ts_ms=?",
        (identity.cohort, identity.cluster_open_ts_ms),
    ).fetchone()
    return int(row[0] if not isinstance(row, dict) else row["n"])


def _row_get(row: Any, key: str) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    # sqlite3.Row supports name-based access
    try:
        return row[key]
    except (IndexError, KeyError):
        return None


def load_arbitration(conn: Any, identity: ArbitrationIdentity) -> Optional[ArbitrationState]:
    """Load an arbitration + its full gap-free event history, or None."""
    arb = conn.execute(
        "SELECT cohort, cluster_open_ts_ms, ranking_version, candidate_set_fingerprint, "
        "selected_candidate_key, selected_rank, selected_window_id, selected_asset, status "
        "FROM cluster_arbitrations WHERE cohort=? AND cluster_open_ts_ms=?",
        (identity.cohort, identity.cluster_open_ts_ms),
    ).fetchone()
    if arb is None:
        return None
    event_rows = conn.execute(
        "SELECT seq, from_status, to_status, candidate_key, candidate_rank, reason, actor, "
        "session_id, event_ts_ms FROM cluster_arbitration_events "
        "WHERE cohort=? AND cluster_open_ts_ms=? ORDER BY seq",
        (identity.cohort, identity.cluster_open_ts_ms),
    ).fetchall()
    events = tuple(
        ArbitrationEvent(
            seq=int(_row_get(r, "seq")),
            from_status=str(_row_get(r, "from_status")),
            to_status=str(_row_get(r, "to_status")),
            candidate_key=_row_get(r, "candidate_key"),
            candidate_rank=_row_get(r, "candidate_rank"),
            reason=str(_row_get(r, "reason")),
            actor=str(_row_get(r, "actor")),
            session_id=str(_row_get(r, "session_id")),
            event_ts_ms=int(_row_get(r, "event_ts_ms")),
        )
        for r in event_rows
    )
    return ArbitrationState(
        identity=identity,
        status=str(_row_get(arb, "status")),
        ranking_version=int(_row_get(arb, "ranking_version")),
        candidate_set_fingerprint=_row_get(arb, "candidate_set_fingerprint"),
        selected_candidate_key=_row_get(arb, "selected_candidate_key"),
        selected_rank=_row_get(arb, "selected_rank"),
        selected_window_id=_row_get(arb, "selected_window_id"),
        selected_asset=_row_get(arb, "selected_asset"),
        events=events,
    )


def _validate_initial_event(events: tuple[ArbitrationEvent, ...]) -> None:
    """The first event must be seq=1 NONE->COLLECTING."""
    if not events:
        raise ClusterArbitrationError("arbitration has no events")
    first = events[0]
    if first.seq != 1 or first.from_status != NONE or first.to_status != COLLECTING:
        raise ClusterArbitrationError(
            f"initial arbitration event is invalid: seq={first.seq} "
            f"{first.from_status}->{first.to_status}")
    prev_seq = 0
    for ev in events:
        if ev.seq != prev_seq + 1:
            raise ClusterArbitrationError(
                f"arbitration event sequence is not gap-free at seq={ev.seq}")
        prev_seq = ev.seq


def _validate_chain_consistency(
        state: ArbitrationState, *, terminal_must_equal_status: bool = True,
) -> None:
    """Validate the full event chain, not only its first event.

    Walks every transition pair through :func:`_check_transition` and tracks
    the terminal state the journal reaches.  When ``terminal_must_equal_status``
    is set (the write-path default), the journal's terminal state must equal
    the stored ``status``; a disagreement means the persisted row and the
    journal disagree and **no mutation may proceed** -- otherwise the write
    path could manufacture permanently unrecoverable state (C1.D-01).
    """
    events = state.events
    _validate_initial_event(events)
    current = NONE
    for ev in events:
        if ev.seq == 1:
            if ev.from_status != NONE or ev.to_status != COLLECTING:
                raise ClusterArbitrationError("invalid initial arbitration event")
        else:
            if ev.from_status != current:
                raise ClusterArbitrationError(
                    f"event seq={ev.seq} from_status {ev.from_status} "
                    f"!= current {current}")
        _check_transition(ev.from_status, ev.to_status)
        current = ev.to_status
    if terminal_must_equal_status and current != state.status:
        raise ClusterArbitrationError(
            f"arbitration status {state.status} disagrees with journal "
            f"terminal {current}; refusing to mutate inconsistent state")


def _record_event(
    conn: Any, identity: ArbitrationIdentity, *, from_status: str, to_status: str,
    candidate_key: Optional[str], candidate_rank: Optional[int], reason: str,
    actor: str, session_id: str, event_ts_ms: int,
) -> int:
    seq = _next_seq(conn, identity)
    if seq == 1 and not (from_status == NONE and to_status == COLLECTING):
        raise ClusterArbitrationError(
            f"first arbitration event must be NONE->COLLECTING (got {from_status}->{to_status})")
    try:
        conn.execute(
            "INSERT INTO cluster_arbitration_events("
            "cohort, cluster_open_ts_ms, seq, from_status, to_status, candidate_key, "
            "candidate_rank, reason, actor, session_id, event_ts_ms) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (identity.cohort, identity.cluster_open_ts_ms, seq, from_status, to_status,
             candidate_key, candidate_rank, reason, actor, session_id, event_ts_ms),
        )
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        # A concurrent writer won the seq slot (UNIQUE(cohort,
        # cluster_open_ts_ms, seq)) or took the write lock first.  The
        # schema makes a double advance impossible; classify the loser as
        # a fail-closed arbitration error so no raw sqlite3 error escapes
        # the API.  Callers MUST run inside transaction(immediate=True)
        # so this is a deterministic, bounded outcome, not a retry loop.
        raise ClusterArbitrationError(
            f"concurrent arbitration event write lost for seq={seq}: {exc}"
        ) from exc
    return seq


def begin_arbitration(
    conn: Any, identity: ArbitrationIdentity, *, arbitration_started_ts_ms: int,
    arbitration_cutoff_ts_ms: int, candidate_set_fingerprint: str, actor: str,
    session_id: str, event_ts_ms: int,
) -> ArbitrationState:
    """Create a new arbitration at COLLECTING (the only legal first state).

    Idempotent: if the arbitration already exists in COLLECTING with the
    same candidate-set fingerprint, this is a verified no-op.  A divergent
    existing arbitration fails closed.
    """
    _validate_actor(actor)
    if event_ts_ms != arbitration_started_ts_ms:
        # The first event's timestamp equals arbitration_started_ts_ms.
        raise ClusterArbitrationError(
            "initial event timestamp must equal arbitration_started_ts_ms")
    if arbitration_cutoff_ts_ms < arbitration_started_ts_ms:
        raise ClusterArbitrationError("arbitration cutoff precedes start")
    existing = load_arbitration(conn, identity)
    if existing is not None:
        if existing.status != COLLECTING:
            raise ClusterArbitrationError(
                f"arbitration already exists in terminal/advanced state {existing.status}")
        if (existing.candidate_set_fingerprint != candidate_set_fingerprint
                or existing.ranking_version != RANKING_VERSION):
            raise ClusterArbitrationError(
                "re-begin diverges from the existing COLLECTING arbitration")
        # Divergent payloads fail closed (C1.E): the idempotent re-begin
        # must carry the same arbitration window as the stored row, not
        # merely the same candidate-set fingerprint.
        stored = conn.execute(
            "SELECT arbitration_started_ts_ms, arbitration_cutoff_ts_ms "
            "FROM cluster_arbitrations WHERE cohort=? AND cluster_open_ts_ms=?",
            (identity.cohort, identity.cluster_open_ts_ms),
        ).fetchone()
        stored_start = int(_row_get(stored, "arbitration_started_ts_ms"))
        stored_cutoff = int(_row_get(stored, "arbitration_cutoff_ts_ms"))
        if (stored_start != int(arbitration_started_ts_ms)
                or stored_cutoff != int(arbitration_cutoff_ts_ms)):
            raise ClusterArbitrationError(
                "re-begin diverges from the existing COLLECTING arbitration "
                "(arbitration window mismatch)")
        return existing
    conn.execute(
        "INSERT INTO cluster_arbitrations("
        "cohort, cluster_open_ts_ms, arbitration_started_ts_ms, arbitration_cutoff_ts_ms, "
        "ranking_version, candidate_set_fingerprint, status, created_session_id, "
        "created_ts_ms, updated_ts_ms) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (identity.cohort, identity.cluster_open_ts_ms, arbitration_started_ts_ms,
         arbitration_cutoff_ts_ms, RANKING_VERSION, candidate_set_fingerprint,
         COLLECTING, session_id, event_ts_ms, event_ts_ms),
    )
    _record_event(
        conn, identity, from_status=NONE, to_status=COLLECTING,
        candidate_key=None, candidate_rank=None, reason="collection_started",
        actor=actor, session_id=session_id, event_ts_ms=event_ts_ms,
    )
    return load_arbitration(conn, identity)  # type: ignore[return-value]


def _require_current(conn: Any, identity: ArbitrationIdentity) -> ArbitrationState:
    """Load and fully validate an arbitration before any mutation.

    The journal is the authoritative arbitration history (C1.D), so the
    write path must verify the *entire* event chain -- not only the first
    event -- agrees with the stored ``status`` before advancing.  A row
    whose status and journal disagree is fail-closed: no mutation proceeds.
    """
    state = load_arbitration(conn, identity)
    if state is None:
        raise ClusterArbitrationError(f"no arbitration for {identity}")
    _validate_chain_consistency(state)
    return state


def _advance(
    conn: Any, identity: ArbitrationIdentity, *, to_status: str, reason: str,
    actor: str, session_id: str, event_ts_ms: int,
    candidate_key: Optional[str] = None, candidate_rank: Optional[int] = None,
    selected_window_id: Optional[int] = None, selected_asset: Optional[str] = None,
    entered_entry_id: Optional[int] = None, exhausted_reason: Optional[str] = None,
) -> ArbitrationState:
    state = _require_current(conn, identity)
    if state.status in TERMINAL_STATUSES:
        raise ClusterArbitrationError(
            f"terminal arbitration {state.status} cannot transition")
    _validate_actor(actor)
    _check_transition(state.status, to_status)
    _record_event(
        conn, identity, from_status=state.status, to_status=to_status,
        candidate_key=candidate_key, candidate_rank=candidate_rank, reason=reason,
        actor=actor, session_id=session_id, event_ts_ms=event_ts_ms,
    )
    sets = []
    bindings: list[Any] = []
    sets.append("status=?"); bindings.append(to_status)
    sets.append("updated_ts_ms=?"); bindings.append(event_ts_ms)
    if candidate_key is not None:
        sets.append("selected_candidate_key=?"); bindings.append(candidate_key)
    if candidate_rank is not None:
        sets.append("selected_rank=?"); bindings.append(candidate_rank)
    if selected_window_id is not None:
        sets.append("selected_window_id=?"); bindings.append(selected_window_id)
    if selected_asset is not None:
        sets.append("selected_asset=?"); bindings.append(selected_asset)
    if entered_entry_id is not None:
        sets.append("entered_entry_id=?"); bindings.append(entered_entry_id)
    if exhausted_reason is not None:
        sets.append("exhausted_reason=?"); bindings.append(exhausted_reason)
    bindings.extend([identity.cohort, identity.cluster_open_ts_ms])
    conn.execute(
        f"UPDATE cluster_arbitrations SET {', '.join(sets)} "
        f"WHERE cohort=? AND cluster_open_ts_ms=?",
        bindings,
    )
    return _require_current(conn, identity)


def select_candidate(
    conn: Any, identity: ArbitrationIdentity, *, candidate: Candidate,
    candidate_rank: int, reason: str, actor: str, session_id: str,
    event_ts_ms: int,
) -> ArbitrationState:
    """COLLECTING -> SELECTED, recording the chosen candidate key + rank.

    ``candidate_rank`` is the authoritative rank the caller computed via
    :func:`rank_candidates` over the full candidate set (C1.D: the journal
    is the authoritative history, so the persisted rank must be the real
    ranking, not a placeholder).  A fallback re-pick after RESERVING thus
    records its true rank rather than always 1.
    """
    return _advance(
        conn, identity, to_status=SELECTED, reason=reason, actor=actor,
        session_id=session_id, event_ts_ms=event_ts_ms,
        candidate_key=candidate_key(candidate),
        candidate_rank=int(candidate_rank),
        selected_window_id=int(candidate.window_id),
        selected_asset=str(candidate.asset).upper(),
    )


def begin_reserving(
    conn: Any, identity: ArbitrationIdentity, *, reason: str, actor: str,
    session_id: str, event_ts_ms: int,
) -> ArbitrationState:
    """SELECTED -> RESERVING."""
    return _advance(
        conn, identity, to_status=RESERVING, reason=reason, actor=actor,
        session_id=session_id, event_ts_ms=event_ts_ms)


def revert_to_selected(
    conn: Any, identity: ArbitrationIdentity, *, reason: str, actor: str,
    session_id: str, event_ts_ms: int,
) -> ArbitrationState:
    """RESERVING -> SELECTED (reservation rejected; allow fallback re-pick)."""
    return _advance(
        conn, identity, to_status=SELECTED, reason=reason, actor=actor,
        session_id=session_id, event_ts_ms=event_ts_ms)


def mark_entered(
    conn: Any, identity: ArbitrationIdentity, *, entered_entry_id: Optional[int],
    reason: str, actor: str, session_id: str, event_ts_ms: int,
) -> ArbitrationState:
    """RESERVING -> ENTERED (permanent occupancy).

    Requires an existing cluster lock (RESERVED or ENTERED) for the
    cluster: ENTERED occupancy is permanent and must rest on occupancy
    evidence, never on the absence of a lock row (C1.E).

    ``entered_entry_id`` is nullable in the authoritative DDL
    (``cluster_arbitrations.entered_entry_id INTEGER REFERENCES
    entries(entry_id)`` -- no NOT NULL).  Pure state-machine tests pass
    ``None`` because the authoritative ``entries`` row lives behind a deep
    FK chain (market_identities->markets, candidates, decisions,
    fair_value_calculations, book_snapshots) that is not part of the
    arbitration contract; the production writer supplies the real id once
    the entries row exists.
    """
    lock = _load_lock(conn, identity)
    if lock is None:
        raise ClusterArbitrationError(
            "cannot mark ENTERED: no cluster lock exists for the cluster")
    return _advance(
        conn, identity, to_status=ENTERED, reason=reason, actor=actor,
        session_id=session_id, event_ts_ms=event_ts_ms,
        entered_entry_id=None if entered_entry_id is None else int(entered_entry_id))


def mark_exhausted(
    conn: Any, identity: ArbitrationIdentity, *, reason: str, actor: str,
    session_id: str, event_ts_ms: int,
) -> ArbitrationState:
    """SELECTED -> EXHAUSTED (all ranked candidates failed reservation)."""
    return _advance(
        conn, identity, to_status=EXHAUSTED, reason=reason, actor=actor,
        session_id=session_id, event_ts_ms=event_ts_ms, exhausted_reason=reason)


def cancel_arbitration(
    conn: Any, identity: ArbitrationIdentity, *, reason: str, actor: str,
    session_id: str, event_ts_ms: int,
) -> ArbitrationState:
    """COLLECTING|SELECTED -> CANCELLED.

    Enforces the authoritative cancellation actor/reason matrix
    (09_FINAL_OPERATOR_RUNBOOK.md): ``operator_cancelled`` is
    ``OPERATOR_TOOL``-only; ``cluster_window_expired`` and
    ``cluster_evidence_invalidated`` are ``ENGINE``/``RECOVERY``-only;
    no other reason may transition to ``CANCELLED``.  ``ENTERED`` and
    ``EXHAUSTED`` are terminal and cannot be cancelled.
    """
    state = _require_current(conn, identity)
    if state.status not in (COLLECTING, SELECTED):
        raise ClusterArbitrationError(
            f"cannot cancel arbitration in state {state.status}")
    allowed_actors = CANCELLATION_MATRIX.get(reason)
    if allowed_actors is None:
        raise ClusterArbitrationError(
            f"unknown cancellation reason {reason!r}; allowed reasons: "
            f"{sorted(CANCELLATION_REASONS)}")
    if actor not in allowed_actors:
        raise ClusterArbitrationError(
            f"cancellation reason {reason!r} is not permitted for actor "
            f"{actor!r}; allowed actors: {sorted(allowed_actors)}")
    return _advance(
        conn, identity, to_status=CANCELLED, reason=reason, actor=actor,
        session_id=session_id, event_ts_ms=event_ts_ms, exhausted_reason=reason)


# ---- cluster-lock occupancy ---------------------------------------------


LOCK_RESERVED = "RESERVED"
LOCK_ENTERED = "ENTERED"


@dataclass(frozen=True)
class ClusterLock:
    identity: ArbitrationIdentity
    window_id: int
    candidate_key: str
    session_id: str
    owner_launch_nonce: str
    state: str
    idempotency_key: str
    reserved_ts_ms: int
    updated_ts_ms: int


def _load_lock(conn: Any, identity: ArbitrationIdentity) -> Optional[ClusterLock]:
    row = conn.execute(
        "SELECT cohort, cluster_open_ts_ms, window_id, candidate_key, session_id, "
        "owner_launch_nonce, state, idempotency_key, reserved_ts_ms, updated_ts_ms "
        "FROM cluster_locks WHERE cohort=? AND cluster_open_ts_ms=?",
        (identity.cohort, identity.cluster_open_ts_ms),
    ).fetchone()
    if row is None:
        return None
    return ClusterLock(
        identity=identity,
        window_id=int(_row_get(row, "window_id")),
        candidate_key=str(_row_get(row, "candidate_key")),
        session_id=str(_row_get(row, "session_id")),
        owner_launch_nonce=str(_row_get(row, "owner_launch_nonce")),
        state=str(_row_get(row, "state")),
        idempotency_key=str(_row_get(row, "idempotency_key")),
        reserved_ts_ms=int(_row_get(row, "reserved_ts_ms")),
        updated_ts_ms=int(_row_get(row, "updated_ts_ms")),
    )


def reserve_cluster_lock(
    conn: Any, identity: ArbitrationIdentity, *, window_id: int, candidate: Candidate,
    session_id: str, owner_launch_nonce: str, idempotency_key: str, now_ms: int,
) -> ClusterLock:
    """Create the single RESERVED cluster lock for a cluster.

    Exactly one cluster_locks row per cohort/cluster (enforced by the
    primary key).  If a lock already exists, it must be the same reservation
    (idempotent) or the call fails closed.  An ENTERED lock is permanent and
    refuses any new reservation.
    """
    existing = _load_lock(conn, identity)
    if existing is not None:
        if existing.state == LOCK_ENTERED:
            raise ClusterArbitrationError(
                "cluster already has permanent ENTERED occupancy")
        # Idempotent only if the same candidate/session/nonce.
        key = candidate_key(candidate)
        if (existing.candidate_key != key
                or existing.session_id != session_id
                or existing.owner_launch_nonce != owner_launch_nonce):
            raise ClusterArbitrationError(
                "cluster already reserved by a different owner/candidate")
        return existing
    conn.execute(
        "INSERT INTO cluster_locks(cohort, cluster_open_ts_ms, window_id, candidate_key, "
        "session_id, owner_launch_nonce, state, idempotency_key, reserved_ts_ms, "
        "updated_ts_ms) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (identity.cohort, identity.cluster_open_ts_ms, int(window_id),
         candidate_key(candidate), session_id, owner_launch_nonce, LOCK_RESERVED,
         idempotency_key, now_ms, now_ms),
    )
    return _load_lock(conn, identity)  # type: ignore[return-value]


def promote_lock_to_entered(
    conn: Any, identity: ArbitrationIdentity, *, now_ms: int,
) -> ClusterLock:
    """Promote the cluster's RESERVED lock to permanent ENTERED occupancy."""
    existing = _load_lock(conn, identity)
    if existing is None:
        raise ClusterArbitrationError("no cluster lock to promote")
    if existing.state == LOCK_ENTERED:
        return existing
    conn.execute(
        "UPDATE cluster_locks SET state=?, updated_ts_ms=? "
        "WHERE cohort=? AND cluster_open_ts_ms=? AND state=?",
        (LOCK_ENTERED, now_ms, identity.cohort, identity.cluster_open_ts_ms,
         LOCK_RESERVED),
    )
    return _load_lock(conn, identity)  # type: ignore[return-value]


def release_unentered_lock(
    conn: Any, identity: ArbitrationIdentity, *, now_ms: int,
) -> None:
    """Release a RESERVED cluster lock only when proven unentered.

    An ENTERED lock is permanent occupancy evidence and is never released.
    Release also refuses when the *arbitration* is ENTERED even if the lock
    row still reads RESERVED (a crash window can leave the lock row stale):
    ENTERED occupancy is permanent regardless of which side lags (C1.E).
    """
    existing = _load_lock(conn, identity)
    if existing is None:
        return
    if existing.state == LOCK_ENTERED:
        raise ClusterArbitrationError(
            "ENTERED cluster occupancy is permanent and cannot be released")
    # Cross-check the arbitration status: a RESERVED lock row against an
    # ENTERED arbitration is an inconsistent crash window, and releasing
    # the lock there would permit a duplicate entry.
    arb = load_arbitration(conn, identity)
    if arb is not None and arb.status == ENTERED:
        raise ClusterArbitrationError(
            "cannot release cluster lock: arbitration is ENTERED "
            "(permanent occupancy)")
    conn.execute(
        "DELETE FROM cluster_locks WHERE cohort=? AND cluster_open_ts_ms=? AND state=?",
        (identity.cohort, identity.cluster_open_ts_ms, LOCK_RESERVED),
    )


# ---- replay / recovery ---------------------------------------------------


def replay_arbitration(conn: Any, identity: ArbitrationIdentity) -> ArbitrationState:
    """Re-derive and validate an arbitration purely from persisted state.

    Verifies the initial event, gap-free monotonic sequence, every
    transition pair is legal, and terminal states never transition.
    Returns the validated state; raises on any contradiction.  Shares the
    same full-chain walk as the write path so a contradiction is detected
    identically on replay and on advance.
    """
    state = load_arbitration(conn, identity)
    if state is None:
        raise ClusterArbitrationError(f"no arbitration to replay for {identity}")
    _validate_chain_consistency(state)
    return state
