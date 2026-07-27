"""Source-versioned section cache and freshness contract for the V4 export.

Measured on the live 9.08 GB evidence store under write load (806-sample and
368-sample identity-pinned reproductions), the dashboard export build spends
almost all of its time inside SQLite, and roughly **half of that time
recomputes results that did not change**.  Over 103 consecutive builds:

===========================================  ========  ==================
statement                                    total ms  result changed
===========================================  ========  ==================
metrics.legacy_performance.terminal_rows       64,391  1 of 103
metrics.performance.terminal_rows              58,564  1 of 103
insufficient_rejects                           24,892  0 of 103
metrics.compound_preview.verified_rows         13,735  1 of 103
metrics.frequency.*.event_buckets (x5)         24,275  4 of 103 each
acceptance_gate conflicts/unresolved/evidence   6,081  0 of 103
universe.universe_rejects                       5,586  0 of 103
terminal_trades                                 4,794  1 of 103
===========================================  ========  ==================

Re-deriving an all-history, explicitly non-authoritative performance
aggregate every five seconds is not a freshness requirement; it is waste that
lands directly on the export-age budget.  This module removes it **without
ever presenting a stale value as a fresh one**.

Contract
--------

Every section resolves to a :class:`SectionState` carrying its own provenance:
when its data was actually read (``source_as_of_ms``), how old that is
(``age_ms``), whether this export reused a previous read (``cache_hit``), why
(``refresh_reason``), and what the refresh cost (``refresh_duration_ms``).
The export publishes all of it, so no consumer has to assume.

Reuse requires **both**:

1. an unchanged source version -- a deterministic token derived from
   authoritative row ids, row counts and evidence flags, never from a hash of
   the result the query would have produced; and
2. an age within the section's declared ``max_age_ms``.

The age bound is not redundant with the version.  A version built from row
ids and counts cannot observe every in-place column update, so the bound is
what guarantees that any change a version misses still surfaces within a fixed
window.  A section is therefore never stale by more than ``max_age_ms``, and
its actual age is always published.

Critical sections are not represented here at all: the export computes them on
every build and lets a failure propagate.  A non-critical section whose
refresh fails may retain its last valid value, but only while that value is
within ``stale_limit_ms``, and only marked ``stale`` and listed in
``degraded_sections``.  Past that limit the value is dropped, the section is
listed in ``unavailable_sections``, and the export reports ``complete=False``.

The cache lives only in memory.  A restarted runtime starts empty, so the
first export after any restart refreshes every section (``refresh_reason``
``"first"``); there is no persisted state that could survive as stale.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, Optional


#: Sections the export must compute on every build; never cached.
TIER_CRITICAL = "CRITICAL"
#: Operational analytics: version-keyed with a short bounded age.
TIER_ANALYTICAL = "ANALYTICAL"
#: Whole-history aggregates that change only when a trade resolves.
TIER_HISTORICAL = "HISTORICAL"

#: Default multiple of ``max_age_ms`` a failed section may be retained for.
_STALE_LIMIT_FACTOR = 4
#: Hard ceiling on retaining a failed section, whatever its cadence.
_STALE_LIMIT_CEILING_MS = 120_000
#: Upper bound on tracked sections (the export declares ~14).
_MAX_SECTIONS = 64
#: Maximum retained length of a version token.
_VERSION_MAX = 200


class SectionUnavailable(RuntimeError):
    """A required section could not be produced and has no valid fallback."""


@dataclass(frozen=True, slots=True)
class SectionState:
    """One resolved export section and its complete provenance."""

    name: str
    tier: str
    value: Any
    version: str
    source_as_of_ms: int
    generated_at_ms: int
    age_ms: int
    cache_hit: bool
    refresh_reason: str
    refresh_duration_ms: float
    max_age_ms: int
    stale: bool
    available: bool
    error: Optional[str]

    def provenance(self) -> dict[str, Any]:
        """The published freshness record for this section."""

        return {
            "tier": self.tier,
            "source_as_of_ms": self.source_as_of_ms,
            "age_ms": self.age_ms,
            "cache_hit": self.cache_hit,
            "refresh_reason": self.refresh_reason,
            "refresh_duration_ms": round(self.refresh_duration_ms, 3),
            "max_age_ms": self.max_age_ms,
            "stale": self.stale,
            "available": self.available,
            "source_version": self.version,
            "error": self.error,
        }


@dataclass(slots=True)
class _Entry:
    value: Any
    version: str
    source_as_of_ms: int
    refresh_duration_ms: float


class ExportSectionCache:
    """Version-keyed, age-bounded reuse of expensive export sections."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, _Entry] = {}
        self._hits = 0
        self._refreshes = 0
        self._degraded = 0

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._hits = 0
            self._refreshes = 0
            self._degraded = 0

    def counters(self) -> dict[str, int]:
        with self._lock:
            return {
                "cache_hits": self._hits,
                "refreshes": self._refreshes,
                "degraded_reuses": self._degraded,
                "tracked_sections": len(self._entries),
            }

    def resolve(
        self,
        name: str,
        *,
        now_ms: int,
        version: str,
        build: Callable[[], Any],
        tier: str = TIER_ANALYTICAL,
        max_age_ms: int = 30_000,
        stale_limit_ms: Optional[int] = None,
        required: bool = True,
        empty: Callable[[], Any] = lambda: None,
        timer: Callable[[], float] = None,  # type: ignore[assignment]
    ) -> SectionState:
        """Return ``name``'s value, reusing the previous read when it is valid.

        ``version`` must be a deterministic token derived from authoritative
        source rows.  ``build`` is called only when a refresh is required.

        When a refresh fails, the last valid value is retained if it is still
        inside ``stale_limit_ms``, marked ``stale``.  With nothing retainable
        a ``required`` section raises -- the export fails closed and its last
        published file stands -- while an optional one degrades to ``empty()``
        so the payload keeps its shape, and is reported unavailable either way.
        """

        import time as _time

        clock = timer or _time.perf_counter
        now_ms = int(now_ms)
        token = str(version)[:_VERSION_MAX]
        limit = (
            int(stale_limit_ms) if stale_limit_ms is not None
            else min(_STALE_LIMIT_CEILING_MS,
                     max(int(max_age_ms), 1) * _STALE_LIMIT_FACTOR)
        )

        with self._lock:
            entry = self._entries.get(name)

        if tier == TIER_CRITICAL:
            reason = "critical_always_refresh"
        elif entry is None:
            reason = "first"
        elif entry.version != token:
            reason = "source_changed"
        elif now_ms - entry.source_as_of_ms >= int(max_age_ms):
            reason = "max_age"
        else:
            # Reuse: the source has not changed and the value is inside its
            # declared age bound.  This is the only path that skips work.
            with self._lock:
                self._hits += 1
            return SectionState(
                name=name, tier=tier, value=entry.value, version=entry.version,
                source_as_of_ms=entry.source_as_of_ms, generated_at_ms=now_ms,
                age_ms=max(0, now_ms - entry.source_as_of_ms), cache_hit=True,
                refresh_reason="unchanged_source",
                refresh_duration_ms=entry.refresh_duration_ms,
                max_age_ms=int(max_age_ms), stale=False, available=True,
                error=None,
            )

        started = clock()
        try:
            value = build()
        except Exception as exc:
            duration_ms = max(0.0, (clock() - started) * 1_000.0)
            detail = f"{type(exc).__name__}:{exc}"[:200]
            if tier == TIER_CRITICAL:
                raise
            entry_usable = (
                entry is not None
                and now_ms - entry.source_as_of_ms <= limit
            )
            if required and not entry_usable:
                # No valid value to stand in for a section the payload's shape
                # depends on: fail closed and let the last published export
                # remain, rather than publish a structurally incomplete one.
                raise
            age = (
                max(0, now_ms - entry.source_as_of_ms)
                if entry is not None else None
            )
            if entry is not None and age is not None and age <= limit:
                # Bounded degraded reuse, explicitly marked.  Never silent.
                with self._lock:
                    self._degraded += 1
                return SectionState(
                    name=name, tier=tier, value=entry.value,
                    version=entry.version,
                    source_as_of_ms=entry.source_as_of_ms,
                    generated_at_ms=now_ms, age_ms=age, cache_hit=True,
                    refresh_reason="refresh_failed_retained_stale",
                    refresh_duration_ms=duration_ms,
                    max_age_ms=int(max_age_ms), stale=True, available=True,
                    error=detail,
                )
            with self._lock:
                self._entries.pop(name, None)
            return SectionState(
                name=name, tier=tier, value=empty(), version="",
                source_as_of_ms=0, generated_at_ms=now_ms, age_ms=0,
                cache_hit=False, refresh_reason="refresh_failed_unavailable",
                refresh_duration_ms=duration_ms, max_age_ms=int(max_age_ms),
                stale=False, available=False, error=detail,
            )

        duration_ms = max(0.0, (clock() - started) * 1_000.0)
        with self._lock:
            self._refreshes += 1
            if tier != TIER_CRITICAL:
                if name not in self._entries and len(
                        self._entries) >= _MAX_SECTIONS:
                    # Bounded registry; an undeclared section simply is not
                    # cached rather than growing the map without limit.
                    pass
                else:
                    self._entries[name] = _Entry(
                        value=value, version=token, source_as_of_ms=now_ms,
                        refresh_duration_ms=duration_ms,
                    )
        return SectionState(
            name=name, tier=tier, value=value, version=token,
            source_as_of_ms=now_ms, generated_at_ms=now_ms, age_ms=0,
            cache_hit=False, refresh_reason=reason,
            refresh_duration_ms=duration_ms, max_age_ms=int(max_age_ms),
            stale=False, available=True, error=None,
        )


def source_version_sql() -> str:
    """One statement returning every authoritative source-version component.

    Each component is either a ``MAX`` over an ``INTEGER PRIMARY KEY`` (an
    O(log n) btree probe) or an aggregate over one of the small evidence
    tables (``pnl_records`` 726 rows, ``entries`` 732, ``exits`` 726,
    ``markets`` 4,258).  Nothing here touches the large tables whose scans the
    cache exists to avoid -- ``decisions`` (267,500), ``maker_observations``
    (57,096), ``event_buckets`` (902,099) and ``reject_events`` (34,855)
    contribute only their maximum row id.

    The flag sums make the trade-evidence version sensitive to in-place
    verification updates, which a row-id maximum alone would miss.
    """

    return """SELECT
      (SELECT COALESCE(MAX(pnl_record_id),0) FROM pnl_records) pnl_max,
      (SELECT COUNT(*) FROM pnl_records) pnl_rows,
      (SELECT COALESCE(SUM(verified + execution_evidence_complete
         + fee_evidence_complete + resolution_evidence_complete),0)
       FROM pnl_records) pnl_flags,
      (SELECT COALESCE(MAX(entry_id),0) FROM entries) entry_max,
      (SELECT COUNT(*) FROM entries) entry_rows,
      (SELECT COUNT(*) FROM entries WHERE status='UNRESOLVED_FINAL')
        entry_unresolved,
      (SELECT COALESCE(MAX(exit_id),0) FROM exits) exit_max,
      (SELECT COALESCE(MAX(decision_id),0) FROM decisions) decision_max,
      (SELECT COALESCE(MAX(maker_observation_id),0) FROM maker_observations)
        maker_max,
      (SELECT COALESCE(MAX(reject_event_id),0) FROM reject_events) reject_max,
      (SELECT COALESCE(MAX(rowid),0) FROM event_buckets) bucket_max,
      (SELECT COALESCE(MAX(market_id),0) FROM markets) market_max,
      (SELECT COUNT(*) FROM markets) market_rows,
      (SELECT COUNT(*) FROM window_funnel) funnel_rows,
      (SELECT COALESCE(MAX(updated_ts_ms),0) FROM window_funnel) funnel_updated
    """


#: Version component groups, so each section declares exactly what it depends
#: on and invalidates on nothing else.
VERSION_GROUPS: dict[str, tuple[str, ...]] = {
    "trade_evidence": (
        "pnl_max", "pnl_rows", "pnl_flags",
        "entry_max", "entry_rows", "entry_unresolved", "exit_max",
    ),
    "entries": ("entry_max", "entry_rows", "entry_unresolved"),
    "decisions": ("decision_max",),
    "maker": ("maker_max",),
    "rejects": ("reject_max",),
    "event_buckets": ("bucket_max", "funnel_rows", "funnel_updated"),
    "markets": ("market_max", "market_rows"),
}


#: Version of a section that depends on no source rows -- its refresh is
#: driven purely by its declared cadence.
CADENCE_ONLY_VERSION = "cadence"
#: Marker component for a source version that could not be established.
UNKNOWN_COMPONENT = "?"


def version_token(components: Any, *groups: str) -> str:
    """Build one deterministic version token from the named component groups.

    A component the probe could not supply becomes :data:`UNKNOWN_COMPONENT`;
    callers treat a token containing it as unestablished and refresh rather
    than reuse.  With no groups the section declares no row dependency and
    gets the stable :data:`CADENCE_ONLY_VERSION`.
    """

    data = dict(components or {})
    parts: list[str] = []
    for group in groups:
        for key in VERSION_GROUPS.get(group, ()):
            value = data.get(key)
            parts.append(UNKNOWN_COMPONENT if value is None else str(value))
    return ".".join(parts) if parts else CADENCE_ONLY_VERSION


def version_is_established(token: str) -> bool:
    """False when any component of ``token`` could not be determined."""

    return UNKNOWN_COMPONENT not in str(token).split(".")


class SectionResolver:
    """Resolves one export build's sections against a cache, or without one.

    Holds the single source-version probe result for the build, so every
    section derives its version from the same consistent read.
    """

    def __init__(
        self, cache: Optional[ExportSectionCache], *,
        components: Any, now_ms: int,
    ) -> None:
        self.cache = cache
        self.components = dict(components or {})
        self.now_ms = int(now_ms)
        self.states: dict[str, SectionState] = {}

    def section(
        self, name: str, *, build: Callable[[], Any],
        groups: tuple[str, ...] = (), tier: str = TIER_ANALYTICAL,
        max_age_ms: int = 30_000, extra_version: str = "",
    ) -> Any:
        """Resolve one section, returning its value and recording provenance."""

        token = version_token(self.components, *groups)
        if not version_is_established(token):
            # An unestablished version must never authorize reuse; making it
            # unique per build forces a refresh instead.
            token = f"unknown@{self.now_ms}"
        if extra_version:
            token = f"{token}#{extra_version}"
        if self.cache is None:
            state = _uncached_state(
                name, tier=tier, build=build, now_ms=self.now_ms,
                version=token, max_age_ms=max_age_ms,
            )
        else:
            state = self.cache.resolve(
                name, now_ms=self.now_ms, version=token, build=build,
                tier=tier, max_age_ms=max_age_ms,
                required=name in REQUIRED_SECTIONS,
                empty=EMPTY_SECTION.get(name, lambda: None),
            )
        self.states[name] = state
        return state.value

    def version_of(self, name: str) -> str:
        state = self.states.get(name)
        return state.version if state is not None else ""

    def summary(self) -> dict[str, Any]:
        return summarize(self.states, now_ms=self.now_ms)


def _uncached_state(
    name: str, *, tier: str, build: Callable[[], Any], now_ms: int,
    version: str, max_age_ms: int,
) -> SectionState:
    """Compute a section with no cache: every build is a fresh read."""

    import time as _time

    started = _time.perf_counter()
    value = build()
    duration_ms = max(0.0, (_time.perf_counter() - started) * 1_000.0)
    return SectionState(
        name=name, tier=tier, value=value, version=version,
        source_as_of_ms=int(now_ms), generated_at_ms=int(now_ms), age_ms=0,
        cache_hit=False, refresh_reason="uncached",
        refresh_duration_ms=duration_ms, max_age_ms=int(max_age_ms),
        stale=False, available=True, error=None,
    )


def summarize(states: dict[str, SectionState], *, now_ms: int) -> dict[str, Any]:
    """The published export freshness contract for one build."""

    sections = {name: state.provenance() for name, state in states.items()}
    stale = sorted(name for name, s in states.items() if s.stale)
    unavailable = sorted(
        name for name, s in states.items() if not s.available)
    degraded = sorted(set(stale) | set(unavailable))
    ages = [s.age_ms for s in states.values() if s.available]
    return {
        "export_generated_at_ms": int(now_ms),
        "complete": not unavailable,
        "sections": dict(sorted(sections.items())),
        "cache_hits": sum(1 for s in states.values() if s.cache_hit),
        "refreshed": sum(1 for s in states.values() if not s.cache_hit),
        "max_section_age_ms": max(ages) if ages else 0,
        "stale_sections": stale,
        "degraded_sections": degraded,
        "unavailable_sections": unavailable,
    }


#: Sections the payload's structure depends on.  If one cannot be produced and
#: has no retainable value, the export fails closed rather than publishing a
#: structurally incomplete document; the previous export file stands.
REQUIRED_SECTIONS = frozenset({
    "frequency", "performance", "acceptance_gate", "execution", "rejects",
    "compound_preview",
})

#: Shape an optional section degrades to when it is unavailable.  These are
#: empty, never fabricated: the section is simultaneously reported
#: ``available: false`` and named in ``unavailable_sections``.
EMPTY_SECTION: dict[str, Callable[[], Any]] = {
    "legacy_performance": lambda: None,
    "insufficient_rejects": lambda: 0,
    "universe_rejects": dict,
    "universe_durations": list,
    "recent_entries": list,
    "terminal_trades": list,
    "sources": list,
}

#: Declared refresh policy per cached section: (tier, max age, version groups).
#:
#: Critical runtime evidence -- the safety tuple, runtime/session state, the
#: capital ledger, open positions, integrity, database and WAL sizes, the live
#: five-minute window universe and the latest candidates -- is deliberately
#: absent: it is recomputed on every build and never served from here.
SECTION_POLICY: dict[str, tuple[str, int, tuple[str, ...]]] = {
    # Rolling funnel counters.  Sourced from event_buckets and window_funnel,
    # which the measured profile showed changing in 4 of 103 builds.
    "frequency": (TIER_ANALYTICAL, 20_000, ("event_buckets", "entries")),
    # Cohort-scoped terminal-trade performance: changes only when a trade
    # resolves, and the version observes verification flags in place.
    "performance": (TIER_ANALYTICAL, 30_000, ("trade_evidence",)),
    # Whole-history aggregate, explicitly labelled non-authoritative: the
    # single most expensive statement in the profile, changed once in 103.
    "legacy_performance": (TIER_HISTORICAL, 120_000, ("trade_evidence",)),
    "compound_preview": (TIER_ANALYTICAL, 30_000, ("trade_evidence",)),
    # Depends on frequency and performance as well as its own queries; its
    # version is composed from theirs so it can never mix generations.
    "acceptance_gate": (TIER_ANALYTICAL, 30_000, ("trade_evidence",)),
    # 12-hour decision/maker rollups over the two largest scanned tables.
    "execution": (TIER_ANALYTICAL, 20_000, ("decisions", "maker")),
    "rejects": (TIER_ANALYTICAL, 20_000, ("rejects",)),
    "insufficient_rejects": (TIER_ANALYTICAL, 30_000, ("rejects",)),
    "universe_rejects": (TIER_ANALYTICAL, 20_000, ("rejects",)),
    "universe_durations": (TIER_ANALYTICAL, 60_000, ("markets",)),
    "recent_entries": (TIER_ANALYTICAL, 15_000, ("entries",)),
    "terminal_trades": (TIER_ANALYTICAL, 15_000, ("trade_evidence",)),
    # Per-channel source health.  Every row carries its own sample_ts_ms, and
    # execution gating uses the engine's in-loop health, not this display copy.
    "sources": (TIER_ANALYTICAL, 15_000, ()),
}


def policy(name: str) -> tuple[str, int, tuple[str, ...]]:
    """Declared (tier, max_age_ms, version groups) for one section."""

    return SECTION_POLICY.get(name, (TIER_ANALYTICAL, 30_000, ()))


__all__ = [
    "CADENCE_ONLY_VERSION",
    "EMPTY_SECTION",
    "ExportSectionCache",
    "REQUIRED_SECTIONS",
    "SECTION_POLICY",
    "SectionResolver",
    "SectionState",
    "SectionUnavailable",
    "TIER_ANALYTICAL",
    "TIER_CRITICAL",
    "TIER_HISTORICAL",
    "UNKNOWN_COMPONENT",
    "VERSION_GROUPS",
    "policy",
    "source_version_sql",
    "summarize",
    "version_is_established",
    "version_token",
]
