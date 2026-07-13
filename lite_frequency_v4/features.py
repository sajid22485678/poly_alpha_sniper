"""Point-in-time feature buffers for the Frequency V4 event loop.

The buffers are deliberately small, in-memory views of evidence already
validated by the source adapters.  They never synthesize a price at a missing
timestamp: horizon and window-open references must exist at or before the
requested instant and within a bounded tolerance.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import math
import statistics
from typing import Iterable, Optional

from .contracts import BookState, CexFeatures, CexObservation
from .edge_models import BookFeaturePoint


HORIZONS_SECONDS = (1, 3, 5, 10, 15, 30)


@dataclass(frozen=True, slots=True)
class FeatureEvidence:
    """Feature result plus exact observations used by persistence."""

    features: CexFeatures
    observations: tuple[CexObservation, ...]


class CexFeatureBuffer:
    """Bounded CEX history with strict point-in-time feature extraction."""

    def __init__(self, *, max_points_per_asset: int = 20_000,
                 reference_tolerance_ms: int = 2_000) -> None:
        if max_points_per_asset < 100 or reference_tolerance_ms < 0:
            raise ValueError("invalid CEX feature buffer bounds")
        self.max_points = int(max_points_per_asset)
        self.reference_tolerance_ms = int(reference_tolerance_ms)
        self._rows: dict[str, deque[CexObservation]] = defaultdict(
            lambda: deque(maxlen=self.max_points))
        self._latest_move_ts: dict[str, int] = {}

    def append(self, observation: CexObservation) -> bool:
        """Append accepted evidence; duplicate/regressed evidence is ignored."""

        asset = observation.asset.upper()
        rows = self._rows[asset]
        if observation.provider_ts_ms > observation.receipt_ts_ms:
            return False
        if rows:
            prior = rows[-1]
            if observation.connection_epoch < prior.connection_epoch:
                return False
            if (observation.connection_epoch == prior.connection_epoch
                    and observation.provider_ts_ms < prior.provider_ts_ms):
                return False
            if observation.event_id == prior.event_id:
                return False
        rows.append(observation)
        if not observation.unchanged:
            self._latest_move_ts[asset] = observation.provider_ts_ms
        return True

    def assets(self) -> tuple[str, ...]:
        return tuple(sorted(asset for asset, rows in self._rows.items() if rows))

    def latest(self, asset: str) -> Optional[CexObservation]:
        rows = self._rows.get(str(asset).upper())
        return rows[-1] if rows else None

    def clear(self) -> None:
        """Invalidate all connection-scoped evidence on provider loss."""

        self._rows.clear()
        self._latest_move_ts.clear()

    def invalidate(self, asset: str) -> None:
        """Drop one asset's executable evidence (e.g. on ingest overflow).

        Fail-closed counterpart to :meth:`clear` used when a single asset's
        ingestion path is compromised: the buffer must not keep serving a
        possibly-inconsistent point-in-time history for that asset until fresh
        admitted evidence rebuilds it.
        """

        normalized = str(asset).upper()
        self._rows.pop(normalized, None)
        self._latest_move_ts.pop(normalized, None)

    @staticmethod
    def _at_or_before(rows: Iterable[CexObservation], target_ms: int,
                      tolerance_ms: int) -> Optional[CexObservation]:
        chosen: Optional[CexObservation] = None
        for row in reversed(tuple(rows)):
            if row.provider_ts_ms <= int(target_ms):
                chosen = row
                break
        if chosen is None or int(target_ms) - chosen.provider_ts_ms > tolerance_ms:
            return None
        return chosen

    def build(self, asset: str, *, now_ms: int, window_open_ms: int,
              max_age_ms: int) -> FeatureEvidence:
        asset = str(asset).upper()
        rows = tuple(self._rows.get(asset, ()))
        latest = rows[-1] if rows else None
        if latest is None:
            return FeatureEvidence(CexFeatures(
                asset=asset, provider="okx", provider_ts_ms=0,
                receipt_ts_ms=int(now_ms), latest_move_ts_ms=None,
                evidence_age_ms=0, sample_count=0, classification="NO_HISTORY",
                valid=False, invalidation_reason="cex_history_missing",
            ), ())

        age = int(now_ms) - latest.provider_ts_ms
        if age < 0:
            return FeatureEvidence(CexFeatures(
                asset=asset, provider=latest.provider,
                provider_ts_ms=latest.provider_ts_ms,
                receipt_ts_ms=latest.receipt_ts_ms,
                latest_move_ts_ms=self._latest_move_ts.get(asset),
                evidence_age_ms=0, sample_count=len(rows),
                classification="INVALID", valid=False,
                invalidation_reason="future_cex_evidence",
                evidence_event_ids=(latest.event_id,),
            ), (latest,))

        used: list[CexObservation] = [latest]
        returns: dict[int, Optional[float]] = {}
        for seconds in HORIZONS_SECONDS:
            reference = self._at_or_before(
                rows, latest.provider_ts_ms - seconds * 1_000,
                self.reference_tolerance_ms,
            )
            if reference is None:
                returns[seconds] = None
                continue
            returns[seconds] = latest.price / reference.price - 1.0
            used.append(reference)

        prior = rows[-2] if len(rows) > 1 else None
        tick_return = (latest.price / prior.price - 1.0) if prior else None
        previous_short = None
        if prior is not None:
            prior_ref = self._at_or_before(
                rows[:-1], prior.provider_ts_ms - 1_000,
                self.reference_tolerance_ms,
            )
            if prior_ref is not None:
                previous_short = prior.price / prior_ref.price - 1.0
                used.append(prior_ref)
        acceleration = (
            float(returns[1]) - previous_short
            if returns.get(1) is not None and previous_short is not None else None
        )

        cutoff = latest.provider_ts_ms - 30_000
        recent = [row for row in rows if cutoff <= row.provider_ts_ms <= latest.provider_ts_ms]
        log_returns = [
            math.log(current.price / previous.price)
            for previous, current in zip(recent, recent[1:])
            if previous.price > 0.0 and current.price > 0.0
        ]
        volatility = statistics.pstdev(log_returns) if len(log_returns) >= 2 else None

        open_reference = self._at_or_before(
            rows, int(window_open_ms), self.reference_tolerance_ms)
        window_price = open_reference.price if open_reference is not None else None
        window_return = (
            latest.price / window_price - 1.0
            if window_price is not None and latest.provider_ts_ms >= int(window_open_ms)
            else None
        )
        if open_reference is not None:
            used.append(open_reference)

        unique = tuple(dict.fromkeys(row.event_id for row in used))
        by_id = {row.event_id: row for row in used}
        valid = age <= int(max_age_ms)
        reason = "" if valid else "stale_cex_evidence"
        features = CexFeatures(
            asset=asset,
            provider=latest.provider,
            provider_ts_ms=latest.provider_ts_ms,
            receipt_ts_ms=latest.receipt_ts_ms,
            latest_move_ts_ms=self._latest_move_ts.get(asset),
            evidence_age_ms=age,
            returns=returns,
            tick_return=tick_return,
            acceleration=acceleration,
            volatility=volatility,
            window_open_price=window_price,
            window_return=window_return,
            sample_count=len(recent),
            classification=latest.classification,
            valid=valid,
            invalidation_reason=reason,
            evidence_event_ids=unique,
        )
        return FeatureEvidence(features, tuple(by_id[event_id] for event_id in unique))


class BookHistoryBuffer:
    """Bounded paired-book history used only by point-in-time models."""

    def __init__(self, *, max_points_per_window: int = 512) -> None:
        if max_points_per_window < 3:
            raise ValueError("book history must retain at least three points")
        self.max_points = int(max_points_per_window)
        self._rows: dict[str, deque[BookFeaturePoint]] = defaultdict(
            lambda: deque(maxlen=self.max_points))

    @staticmethod
    def _mid(book: BookState) -> Optional[float]:
        if book.best_bid is None or book.best_ask is None:
            return None
        return (book.best_bid + book.best_ask) / 2.0

    @staticmethod
    def _depth(levels) -> float:
        return sum(level.shares for level in tuple(levels)[:5])

    def append_pair(self, window_key: str, yes: BookState, no: BookState,
                    *, yes_trade_flow: float = 0.0,
                    no_trade_flow: float = 0.0) -> bool:
        yes_mid, no_mid = self._mid(yes), self._mid(no)
        if yes_mid is None or no_mid is None:
            return False
        provider_ts = max(yes.provider_ts_ms, no.provider_ts_ms)
        receipt_ts = max(yes.receipt_ts_ms, no.receipt_ts_ms)
        point = BookFeaturePoint(
            provider_ts_ms=provider_ts,
            receipt_ts_ms=receipt_ts,
            yes_mid=yes_mid,
            no_mid=no_mid,
            yes_bid_depth=self._depth(yes.bids),
            yes_ask_depth=self._depth(yes.asks),
            no_bid_depth=self._depth(no.bids),
            no_ask_depth=self._depth(no.asks),
            yes_trade_flow=float(yes_trade_flow),
            no_trade_flow=float(no_trade_flow),
        )
        rows = self._rows[str(window_key)]
        if rows and point.provider_ts_ms < rows[-1].provider_ts_ms:
            return False
        if rows and point == rows[-1]:
            return False
        rows.append(point)
        return True

    def points(self, window_key: str, *, now_ms: int) -> tuple[BookFeaturePoint, ...]:
        return tuple(point for point in self._rows.get(str(window_key), ())
                     if point.provider_ts_ms <= int(now_ms)
                     and point.receipt_ts_ms <= int(now_ms))

    def remove(self, window_key: str) -> None:
        self._rows.pop(str(window_key), None)
