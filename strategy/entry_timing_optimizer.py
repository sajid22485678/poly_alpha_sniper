"""Entry timing: NOW / WAIT / SKIP from lag assessment + book freshness."""
from __future__ import annotations


def decide_timing(lag: dict, book_age_ms: float, cfg) -> tuple[str, str]:
    max_stale = cfg.polymarket.max_orderbook_staleness_ms
    gap = lag.get("repricing_gap", 0.0)
    speed = lag.get("repricing_speed_per_s", 0.0)
    min_edge = cfg.dynamic_edge.hard_min_edge

    if abs(gap) < min_edge / 2:
        return "SKIP", f"repricing complete (gap {gap:.4f} < {min_edge / 2:.4f})"
    if book_age_ms > max_stale:
        return "WAIT", f"book aging ({book_age_ms:.0f}ms > {max_stale}ms) — refresh first"
    if lag.get("lag_detected"):
        return "NOW", f"lag live: gap {gap:.4f}, speed {speed:.4f}/s"
    if speed * gap > 0 and abs(speed) > 0:
        return "SKIP", "market already repricing toward fair"
    return "WAIT", "gap present but lag unconfirmed"
