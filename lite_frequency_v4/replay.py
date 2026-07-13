"""Deterministic persisted-evidence replay checks for Frequency V4."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any


@dataclass(frozen=True, slots=True)
class ReplayResult:
    candidate_id: int
    deterministic_hash: str
    coherent_probability: bool
    economic_formula_valid: bool
    selected_side_valid: bool
    evidence_complete: bool
    errors: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return not self.errors


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False, default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def replay_candidate(store: Any, candidate_id: int) -> ReplayResult:
    """Recheck one candidate solely from immutable persisted evidence.

    This does not invent missing feature history.  It verifies the canonical
    probability complement, full fee/buffer edge equation, selected-side
    uniqueness, evidence links, and stable serialization.
    """

    candidate = store.query_one(
        "SELECT * FROM candidates WHERE candidate_id=?", (int(candidate_id),))
    if candidate is None:
        raise ValueError("unknown v4 candidate")
    calculations = store.query(
        """SELECT * FROM fair_value_calculations WHERE candidate_id=?
           ORDER BY calculated_ts_ms,fair_value_calculation_id""",
        (int(candidate_id),),
    )
    models = store.query(
        """SELECT * FROM model_contributions WHERE candidate_id=?
           ORDER BY model_name""", (int(candidate_id),))
    decisions = store.query(
        """SELECT * FROM decisions WHERE candidate_id=?
           ORDER BY decision_seq,decision_id""", (int(candidate_id),))
    books = store.query(
        """SELECT cbe.outcome_side,b.* FROM candidate_book_evidence cbe
           JOIN book_snapshots b USING(book_snapshot_id)
           WHERE cbe.candidate_id=? ORDER BY cbe.outcome_side""",
        (int(candidate_id),),
    )
    cex = store.query(
        """SELECT cce.evidence_role,cce.horizon_ms,c.*
           FROM candidate_cex_evidence cce JOIN cex_observations c
           USING(cex_observation_id) WHERE cce.candidate_id=?
           ORDER BY c.provider_ts_ms,c.cex_observation_id""",
        (int(candidate_id),),
    )
    errors: list[str] = []
    coherent = True
    formula = True
    selected_valid = True
    sides_by_calc: dict[int, list[dict[str, Any]]] = {}
    for calculation in calculations:
        calc_id = int(calculation["fair_value_calculation_id"])
        sides = store.query(
            """SELECT * FROM fair_value_sides
               WHERE fair_value_calculation_id=? ORDER BY outcome_side""",
            (calc_id,),
        )
        sides_by_calc[calc_id] = sides
        yes = float(calculation["fair_probability_yes"])
        no = float(calculation["fair_probability_no"])
        if not (math.isfinite(yes) and math.isfinite(no)
                and 0.0 < yes < 1.0 and 0.0 < no < 1.0
                and abs(yes + no - 1.0) <= 1e-6):
            coherent = False
            errors.append(f"probability_incoherent:{calc_id}")
        selected = [row for row in sides if bool(row["selected"])]
        if len(selected) > 1:
            selected_valid = False
            errors.append(f"multiple_selected_sides:{calc_id}")
        for side in sides:
            values = (
                side["executable_vwap"], side["net_edge"],
                side["book_snapshot_id"],
            )
            if any(value is None for value in values):
                continue
            probability = yes if side["outcome_side"] == "YES" else no
            expected = (
                probability - float(side["executable_vwap"])
                - float(side["estimated_fee"])
                - float(side["execution_buffer"])
                - float(side["latency_buffer"])
                - float(side["uncertainty_buffer"])
            )
            if not math.isfinite(expected) or abs(expected - float(side["net_edge"])) > 1e-8:
                formula = False
                errors.append(
                    f"net_edge_formula_mismatch:{calc_id}:{side['outcome_side']}")
        if selected:
            executable = [row for row in sides if row["net_edge"] is not None]
            if executable and float(selected[0]["net_edge"]) < max(
                    float(row["net_edge"]) for row in executable) - 1e-10:
                selected_valid = False
                errors.append(f"selected_side_not_best:{calc_id}")
    evidence_complete = bool(calculations and models and decisions and books and cex)
    if not evidence_complete:
        errors.append("candidate_evidence_incomplete")
    evidence = {
        "candidate": candidate,
        "calculations": calculations,
        "sides": sides_by_calc,
        "models": models,
        "decisions": decisions,
        "books": books,
        "cex": cex,
    }
    return ReplayResult(
        candidate_id=int(candidate_id),
        deterministic_hash=_canonical_hash(evidence),
        coherent_probability=coherent,
        economic_formula_valid=formula,
        selected_side_valid=selected_valid,
        evidence_complete=evidence_complete,
        errors=tuple(errors),
    )


def replay_entries(store: Any) -> dict[str, Any]:
    """Replay every actual entry candidate and summarize fail-closed status."""

    rows = store.query("SELECT entry_id,candidate_id FROM entries ORDER BY entry_id")
    results = [replay_candidate(store, int(row["candidate_id"])) for row in rows]
    return {
        "entries": len(results),
        "valid": sum(result.valid for result in results),
        "invalid": sum(not result.valid for result in results),
        "deterministic_hashes": [result.deterministic_hash for result in results],
        "errors": [error for result in results for error in result.errors],
    }
