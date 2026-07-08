"""Token mapping validation: which token means UP/ABOVE (positive condition)?

Polymarket convention: `clobTokenIds[i]` corresponds to `outcomes[i]`.
We require exactly two outcomes with an unambiguous recognized label pair.
Confidence < 95 => REJECT_TOKEN_MAPPING_UNCLEAR (master rule).
"""
from __future__ import annotations

import json

from poly_alpha_sniper.core.contracts import ParsedMarket, RejectReason, TokenMappingResult

POSITIVE_LABELS = {"yes", "up", "above", "higher"}
NEGATIVE_LABELS = {"no", "down", "below", "lower"}


def _as_list(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def validate_token_mapping(raw_market: dict, parsed: ParsedMarket) -> TokenMappingResult:
    checks: list[str] = []
    outcomes = _as_list(raw_market.get("outcomes"))
    token_ids = _as_list(raw_market.get("clobTokenIds") or raw_market.get("clob_token_ids"))

    if len(outcomes) != 2:
        return TokenMappingResult(ok=False, confidence=0.0,
                                  reject_reason=RejectReason.TOKEN_MAPPING_UNCLEAR,
                                  checks=[f"outcomes count {len(outcomes)} != 2"])
    checks.append("two_outcomes")
    if len(token_ids) != 2 or not all(token_ids):
        return TokenMappingResult(ok=False, confidence=0.0,
                                  reject_reason=RejectReason.TOKEN_MAPPING_UNCLEAR,
                                  checks=checks + ["missing token ids"])
    checks.append("two_token_ids")

    labels = [str(o).strip().lower() for o in outcomes]
    l0, l1 = labels[0], labels[1]

    if l0 in POSITIVE_LABELS and l1 in NEGATIVE_LABELS:
        up_means_yes = True
    elif l0 in NEGATIVE_LABELS and l1 in POSITIVE_LABELS:
        # first token is the negative outcome: YES-token (index 0) = DOWN/BELOW
        up_means_yes = False
    else:
        return TokenMappingResult(ok=False, confidence=0.0,
                                  reject_reason=RejectReason.TOKEN_MAPPING_UNCLEAR,
                                  checks=checks + [f"unrecognized outcome labels {labels}"])
    checks.append(f"labels_recognized:{l0}/{l1}")

    confidence = 100.0
    # label pair should be internally consistent (yes/no or up/down or above/below)
    pairs = ({"yes", "no"}, {"up", "down"}, {"above", "below"}, {"higher", "lower"})
    if {l0, l1} not in pairs:
        confidence -= 20
        checks.append("mixed label pair")
    # parsed direction wording should agree with label family for threshold markets
    if parsed.market_type.value == "THRESHOLD" and {l0, l1} == {"up", "down"}:
        confidence -= 10
        checks.append("threshold market with up/down labels")

    ok = confidence >= 95
    return TokenMappingResult(
        ok=ok, confidence=confidence, direction_up_means_yes=up_means_yes,
        reject_reason="" if ok else RejectReason.TOKEN_MAPPING_UNCLEAR,
        checks=checks)
