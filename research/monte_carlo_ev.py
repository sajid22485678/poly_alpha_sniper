"""RESEARCH / SHADOW-ONLY: diagnostics and challenger evaluation only. Never places orders, never affects the baseline trading decision or live readiness.

Seeded Monte Carlo EV validator for HOT/FIRED candidates (challenger).

WHY: the baseline edge model produces a point estimate of win probability
and EV. Before trusting a HOT/FIRED candidate in shadow analysis we want an
independent, distribution-aware sanity check: simulate the underlying price
to expiry under a simple geometric-Brownian-motion model and ask (a) how
often the candidate actually finishes in the money, and (b) how bad the EV
looks at the pessimistic tail of our own sampling uncertainty. A challenger
that only looks good at the mean but collapses at the 5th-percentile win
rate is fragile and should be flagged in diagnostics.

Model (documented so results are reproducible and auditable):
- Per path the terminal price is
      terminal = current * exp((drift - vol^2 / 2) * T + vol * sqrt(T) * g)
  with g ~ N(0, 1) drawn from random.Random(seed), T = time_remaining_s in
  seconds and vol/drift expressed per second. This is exact GBM terminal
  sampling — one gaussian per path, no intermediate steps needed because
  only the terminal value matters for a binary market.
- p_up = fraction of paths with terminal >= price_to_beat.
- Per-path payout for the chosen side: win -> (1 - executable_price),
  lose -> -executable_price, and every path additionally pays
  fee_rate * executable_price (fees are charged whether or not we win).
- ev_mean = arithmetic mean of per-path payouts.
- ev_p05 (CHOSEN DESIGN, documented): rather than the 5th percentile of the
  two-point per-path payout distribution (which is degenerate — it just
  snaps to the losing payout whenever p_win < 95%), we recompute EV at the
  5th-percentile *win probability* via the one-sided normal approximation
  to the binomial:  p_lo = p_hat - 1.645 * sqrt(p_hat * (1 - p_hat) / n).
  This answers "if our sampled win rate is optimistic by sampling noise,
  what does EV look like?", which is the honest worst-tail question for a
  binary payoff. ev_p05 <= ev_mean always holds.

Determinism: identical inputs + seed produce a bit-identical McResult.

Degenerate guard: time_remaining_s <= 0, vol_per_s <= 0, current_price <= 0
or n_paths <= 0 make Monte Carlo meaningless, so we return a deterministic
result instead of fabricating randomness: the terminal price is resolved
analytically (no time left -> current price; zero vol -> pure drift), then
p_up is 1.0 / 0.0 / 0.5 by comparing that terminal against price_to_beat
(0.5 = exact tie, i.e. genuinely unresolved -> neutral), clamped to the
probability band, with n_paths reported as 0 and ev_p05 == ev_mean (there
is no sampling uncertainty to take a tail of).

All probability outputs are clamped to [0.01, 0.99]: we never claim
certainty on a binary market and it protects downstream odds math.
"""
from __future__ import annotations

import hashlib
import math
import random
from dataclasses import dataclass

PROB_FLOOR = 0.01
PROB_CEIL = 0.99

# One-sided 95% normal quantile used for the 5th-percentile win-probability
# bound. WHY 1.645: we only care about the pessimistic side of the sampling
# distribution, so a one-sided bound is the honest choice.
Z_ONE_SIDED_95 = 1.645

_VALID_SIDES = ("UP", "DOWN")


@dataclass(frozen=True)
class McResult:
    """Immutable Monte Carlo diagnostic for one HOT/FIRED candidate.

    p_up/p_down are band-clamped, so at extremes they may not sum to
    exactly 1.0 — that is deliberate (never claim certainty).
    """

    p_up: float
    p_down: float
    ev_mean: float
    ev_p05: float
    n_paths: int
    seed: int
    cache_key: str


def _clamp_prob(p: float) -> float:
    """Clamp into [0.01, 0.99]: no certainty claims, safe downstream odds."""
    return max(PROB_FLOOR, min(PROB_CEIL, p))


def _cache_key(
    current_price: float,
    price_to_beat: float,
    vol_per_s: float,
    drift_per_s: float,
    time_remaining_s: float,
    side: str,
    executable_price: float,
    fee_rate: float,
    n_paths: int,
    seed: int,
) -> str:
    """Short sha256 of the rounded inputs.

    WHY rounded: callers feed float telemetry with sub-microscopic jitter;
    rounding to 6 decimals makes near-identical requests share a cache slot
    without changing any economically meaningful digit.
    """
    blob = "|".join(
        (
            f"{round(current_price, 6)}",
            f"{round(price_to_beat, 6)}",
            f"{round(vol_per_s, 6)}",
            f"{round(drift_per_s, 6)}",
            f"{round(time_remaining_s, 6)}",
            side,
            f"{round(executable_price, 6)}",
            f"{round(fee_rate, 6)}",
            f"{n_paths}",
            f"{seed}",
        )
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:12]


def simulate(
    current_price: float,
    price_to_beat: float,
    vol_per_s: float,
    drift_per_s: float,
    time_remaining_s: float,
    side: str,
    executable_price: float,
    fee_rate: float = 0.0,
    n_paths: int = 500,
    seed: int = 42,
) -> McResult:
    """Seeded GBM Monte Carlo EV check for one candidate (diagnostics only).

    See the module docstring for the model, the ev_p05 design choice and
    the degenerate guard. Raises ValueError for an unknown side, because a
    silent default would attribute payouts to the wrong outcome.
    """
    side_u = side.upper()
    if side_u not in _VALID_SIDES:
        raise ValueError(f"side must be one of {_VALID_SIDES}, got {side!r}")

    key = _cache_key(
        current_price,
        price_to_beat,
        vol_per_s,
        drift_per_s,
        time_remaining_s,
        side_u,
        executable_price,
        fee_rate,
        n_paths,
        seed,
    )

    # Fees are paid on every path, win or lose.
    fee = fee_rate * executable_price
    win_payout = (1.0 - executable_price) - fee
    lose_payout = -executable_price - fee

    degenerate = (
        time_remaining_s <= 0.0
        or vol_per_s <= 0.0
        or current_price <= 0.0
        or n_paths <= 0
    )
    if degenerate:
        # Resolve the terminal analytically instead of fabricating noise.
        if time_remaining_s <= 0.0 or current_price <= 0.0:
            terminal = current_price
        else:  # vol <= 0 (or no paths): pure deterministic drift.
            terminal = current_price * math.exp(drift_per_s * time_remaining_s)
        if terminal > price_to_beat:
            p_up_raw = 1.0
        elif terminal < price_to_beat:
            p_up_raw = 0.0
        else:
            p_up_raw = 0.5  # exact tie: genuinely unresolved -> neutral.
        p_up = _clamp_prob(p_up_raw)
        p_down = _clamp_prob(1.0 - p_up_raw)
        p_win = p_up if side_u == "UP" else p_down
        # No sampling uncertainty exists, so the tail EV equals the mean EV;
        # EV uses the clamped probability so it stays consistent with the
        # reported p_up/p_down.
        ev = p_win * win_payout + (1.0 - p_win) * lose_payout
        return McResult(
            p_up=p_up,
            p_down=p_down,
            ev_mean=ev,
            ev_p05=ev,
            n_paths=0,
            seed=seed,
            cache_key=key,
        )

    rng = random.Random(seed)
    mu = (drift_per_s - 0.5 * vol_per_s * vol_per_s) * time_remaining_s
    sigma = vol_per_s * math.sqrt(time_remaining_s)
    wins_up = 0
    for _ in range(n_paths):
        terminal = current_price * math.exp(mu + sigma * rng.gauss(0.0, 1.0))
        if terminal >= price_to_beat:
            wins_up += 1

    p_up_raw = wins_up / n_paths  # n_paths > 0 guaranteed above.
    p_win_raw = p_up_raw if side_u == "UP" else 1.0 - p_up_raw

    # ev_mean is the literal mean of per-path payouts (uses the raw win
    # fraction — the two-point payout makes the mean collapse to this form).
    ev_mean = p_win_raw * win_payout + (1.0 - p_win_raw) * lose_payout

    # 5th-percentile win probability via the one-sided normal approximation
    # to the binomial (see module docstring for WHY this and not a payout
    # percentile), then band-clamped before recomputing EV.
    se = math.sqrt(p_win_raw * (1.0 - p_win_raw) / n_paths)
    p_lo = _clamp_prob(max(0.0, min(1.0, p_win_raw - Z_ONE_SIDED_95 * se)))
    ev_p05 = p_lo * win_payout + (1.0 - p_lo) * lose_payout

    return McResult(
        p_up=_clamp_prob(p_up_raw),
        p_down=_clamp_prob(1.0 - p_up_raw),
        ev_mean=ev_mean,
        ev_p05=min(ev_p05, ev_mean),
        n_paths=n_paths,
        seed=seed,
        cache_key=key,
    )
