"""RESEARCH / SHADOW-ONLY: diagnostics and challenger evaluation only. Never places orders, never affects the baseline trading decision or live readiness.

Deterministic weighted fair CEX price across exchanges.

WHY: the bot needs a single "fair" reference price for the underlying asset,
built from several CEX quote feeds that can individually be stale, frozen,
or plainly wrong. This module fuses those quotes with a transparent
exponential age-decay weighting plus median-based outlier rejection, and
reports a confidence score and a staleness EV penalty so downstream shadow
analytics can quantify how trustworthy the reference is.

WHY NOT A KALMAN FILTER: a Kalman fusion of multi-exchange quotes was
considered and deliberately deferred. This package is stdlib-only (no
numpy for the covariance algebra), and — more importantly for a
research/shadow module — a weighted-mean estimator is auditable
line-by-line: every weight can be recomputed by hand from the quote ages,
whereas Kalman gains hide state that is hard to reconstruct in a
post-mortem. Transparency beats optimality for diagnostics.

NOTE ON CLAMPING: this module outputs asset *prices*, not probabilities,
so the package-wide [0.01, 0.99] probability clamp does not apply to
``fair_price``. ``confidence`` is a quality score explicitly clamped to
[0.0, 1.0] (0.0 is meaningful: "no usable source").
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

# Half-life of quote trust, in ms. WHY: a quote 3s old gets half the weight
# of a live one — crypto microstructure moves fast enough that 3s is a
# reasonable trust half-life, and 0.5**(age/3000) is trivially auditable.
HALF_LIFE_MS = 3000.0

# Relative deviation from the cross-source median beyond which a source is
# an outlier (only applied with >= 3 sources, where a median is meaningful).
OUTLIER_DEVIATION_PCT = 0.005  # 0.5%

# Max pairwise deviation (as a fraction of the lowest kept price) at which
# agreement-confidence bottoms out at 0. WHY: 2% cross-exchange dispersion
# on a liquid asset means the feeds effectively disagree.
AGREEMENT_CAP_PCT = 0.02

# Per-half-life EV penalty while DEGRADED. WHY: mirrors config
# cex_freshness.degraded_adverse_selection_buffer_add = 0.015 — the live
# pipeline widens its adverse-selection buffer by 1.5c when CEX data is
# degraded, so the shadow fair-price engine charges the same toll.
DEGRADED_PENALTY_PER_HALF_LIFE = 0.015

# FAIL_CLOSED / NO_SOURCE penalty. WHY: an "inf-like" 1.0 (the full price
# range of a binary market) guarantees any EV net of this penalty is
# negative — trading on such data should never look attractive.
FAIL_CLOSED_PENALTY = 1.0


@dataclass
class FairPrice:
    """Fused fair-price estimate plus everything needed to audit it.

    ``stale_bucket`` is one of 'FRESH' | 'DEGRADED' | 'FAIL_CLOSED' |
    'NO_SOURCE'. ``source_weights`` includes outlier sources with weight
    0.0 so the audit trail shows *why* a source contributed nothing.
    """

    fair_price: Optional[float]
    confidence: float
    source_weights: dict[str, float] = field(default_factory=dict)
    outlier_sources: list[str] = field(default_factory=list)
    n_sources: int = 0
    freshest_source: Optional[str] = None
    stale_bucket: str = "NO_SOURCE"
    ev_stale_penalty: float = FAIL_CLOSED_PENALTY


def _no_source() -> FairPrice:
    """Explicit neutral result: nothing usable, never fabricate a price."""
    return FairPrice(
        fair_price=None,
        confidence=0.0,
        source_weights={},
        outlier_sources=[],
        n_sources=0,
        freshest_source=None,
        stale_bucket="NO_SOURCE",
        ev_stale_penalty=FAIL_CLOSED_PENALTY,
    )


def _median(values: list[float]) -> float:
    """Median without statistics-module import churn; input is non-empty."""
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2 == 1:
        return ordered[mid]
    return 0.5 * (ordered[mid - 1] + ordered[mid])


def compute_fair_price(
    quotes: dict[str, dict],
    now_ms: int,
    live_max_age_ms: int = 1500,
    degraded_max_age_ms: int = 8000,
) -> FairPrice:
    """Fuse per-exchange quotes into one auditable fair price.

    Each quote is ``{'price': float, 'ts_ms': int}``. Deterministic: same
    inputs always yield the same FairPrice (ties broken alphabetically).

    Steps (each documented inline):
      1. drop non-positive / non-finite prices — a zero or negative CEX
         price is a feed bug, never information;
      2. drop quotes older than 2x the degraded threshold — beyond that a
         quote is archaeology, not data;
      3. median-based outlier rejection (>= 3 sources only);
      4. exponential age-decay weights, zeroed for outliers, normalized;
      5. fair price = weighted mean of surviving prices;
      6. staleness bucket from the freshest surviving source's age;
      7. confidence from source count, cross-source agreement, freshness;
      8. EV staleness penalty mirroring the live pipeline's buffers.
    """
    if not quotes:
        return _no_source()

    hard_max_age_ms = degraded_max_age_ms * 2

    # Steps 1 + 2: sanitize. Ages are clamped at 0 so a slightly-future
    # timestamp (clock skew between us and the exchange) reads as "live"
    # rather than producing a negative age that would inflate its weight.
    survivors: dict[str, tuple[float, float]] = {}  # name -> (price, age_ms)
    for name in sorted(quotes):  # sorted: deterministic iteration order
        quote = quotes[name]
        try:
            price = float(quote["price"])
            ts_ms = int(quote["ts_ms"])
        except (KeyError, TypeError, ValueError):
            continue  # malformed entry: skip, never guess
        if not math.isfinite(price) or price <= 0.0:
            continue  # step 1: non-positive price is a feed bug
        age_ms = max(0.0, float(now_ms - ts_ms))
        if age_ms > hard_max_age_ms:
            continue  # step 2: too old to say anything about "now"
        survivors[name] = (price, age_ms)

    if not survivors:
        return _no_source()

    n_sources = len(survivors)

    # Freshest source: min age, alphabetical tie-break for determinism.
    freshest_source = min(survivors, key=lambda k: (survivors[k][1], k))
    freshest_age = survivors[freshest_source][1]

    # Step 3: outlier detection. A median needs >= 3 points to be robust;
    # with exactly 2 sources we mark none and let the age-decay weights of
    # step 4 naturally trust the freshest source more.
    outliers: list[str] = []
    if n_sources >= 3:
        med = _median([p for p, _ in survivors.values()])
        for name, (price, _) in survivors.items():
            # med > 0 is guaranteed: all survivor prices are positive.
            if abs(price - med) / med > OUTLIER_DEVIATION_PCT:
                outliers.append(name)
        if len(outliers) == n_sources:
            # Degenerate dispersion (e.g. two price clusters straddling the
            # median): rather than fabricating from nothing, fall back to
            # the single freshest source — the least-stale opinion is the
            # best remaining one — and keep the rest flagged.
            outliers = [n for n in outliers if n != freshest_source]

    outlier_set = set(outliers)

    # Step 4: weights. 0.5**(age/half_life) is strictly positive for every
    # non-outlier, so the normalizing sum below can never be zero.
    raw_weights: dict[str, float] = {}
    for name, (_, age_ms) in survivors.items():
        if name in outlier_set:
            raw_weights[name] = 0.0
        else:
            raw_weights[name] = 0.5 ** (age_ms / HALF_LIFE_MS)
    total_weight = sum(raw_weights.values())
    source_weights = {n: w / total_weight for n, w in raw_weights.items()}

    # Step 5: fair price = weighted mean of non-outlier prices.
    fair_price = sum(
        source_weights[name] * price for name, (price, _) in survivors.items()
    )

    # Step 6: staleness bucket from the freshest age. Freshness measures
    # pipeline health (is *any* feed alive?), independent of agreement.
    if freshest_age <= live_max_age_ms:
        stale_bucket = "FRESH"
    elif freshest_age <= degraded_max_age_ms:
        stale_bucket = "DEGRADED"
    else:
        stale_bucket = "FAIL_CLOSED"

    # Step 7: confidence = product of three [0, 1] factors. A product (not
    # a mean) because each factor is a necessary condition — one dead axis
    # (single source, disagreement, or staleness) should crater confidence.
    #   count:     min(1, n/3)   — 3+ independent sources = full credit;
    #   agreement: 1 - (max pairwise deviation pct / 2% cap), floored at 0;
    #              with one usable price agreement is unmeasurable, so a
    #              neutral 0.5 is used rather than fabricating consensus;
    #   freshness: same half-life decay as the weights.
    kept_prices = [
        price for name, (price, _) in survivors.items() if name not in outlier_set
    ]
    count_factor = min(1.0, n_sources / 3.0)
    if len(kept_prices) >= 2:
        lo, hi = min(kept_prices), max(kept_prices)
        spread_pct = (hi - lo) / lo  # lo > 0 guaranteed
        agreement_factor = max(0.0, 1.0 - min(1.0, spread_pct / AGREEMENT_CAP_PCT))
    else:
        agreement_factor = 0.5
    freshness_factor = 0.5 ** (freshest_age / HALF_LIFE_MS)
    confidence = max(0.0, min(1.0, count_factor * agreement_factor * freshness_factor))

    # Step 8: EV staleness penalty. FRESH costs nothing; DEGRADED charges
    # the config's degraded_adverse_selection_buffer_add (0.015) scaled by
    # how many half-lives stale the best feed is (floored at 1x so entering
    # DEGRADED always costs at least the full buffer); FAIL_CLOSED charges
    # an inf-like 1.0 so no EV can survive it.
    if stale_bucket == "FRESH":
        ev_stale_penalty = 0.0
    elif stale_bucket == "DEGRADED":
        ev_stale_penalty = DEGRADED_PENALTY_PER_HALF_LIFE * max(
            1.0, freshest_age / HALF_LIFE_MS
        )
    else:
        ev_stale_penalty = FAIL_CLOSED_PENALTY

    return FairPrice(
        fair_price=fair_price,
        confidence=confidence,
        source_weights=source_weights,
        outlier_sources=sorted(outliers),
        n_sources=n_sources,
        freshest_source=freshest_source,
        stale_bucket=stale_bucket,
        ev_stale_penalty=ev_stale_penalty,
    )
