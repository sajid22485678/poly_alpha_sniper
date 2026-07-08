"""Overall stats report.

Run: python -m poly_alpha_sniper.reporting.stats_report
"""
from __future__ import annotations


def build_stats(store) -> dict:
    stats: dict = {}
    queries = {
        "predictions": "SELECT COUNT(*) AS n FROM predictions",
        "signals": "SELECT COUNT(*) AS n FROM signals",
        "orders": "SELECT COUNT(*) AS n FROM orders",
        "fills": "SELECT COUNT(*) AS n FROM fills",
        "exits": "SELECT COUNT(*) AS n FROM exits",
        "panic_events": "SELECT COUNT(*) AS n FROM panic_events",
        "incidents": "SELECT COUNT(*) AS n FROM incident_reports",
    }
    for name, sql in queries.items():
        try:
            stats[name] = store.query(sql)[0]["n"]
        except Exception:  # noqa: BLE001
            stats[name] = 0
    try:
        pnl_rows = store.query("SELECT pnl_usd FROM exits")
        pnls = [float(r["pnl_usd"] or 0) for r in pnl_rows]
        stats["total_pnl_usd"] = round(sum(pnls), 3)
        stats["winrate"] = round(sum(1 for p in pnls if p > 0) / len(pnls), 3) if pnls else 0.0
    except Exception:  # noqa: BLE001
        stats["total_pnl_usd"] = 0.0
        stats["winrate"] = 0.0
    return stats


def render_text(stats: dict) -> str:
    lines = ["POLY ALPHA SNIPER — STATS"]
    lines += [f"{k}: {v}" for k, v in stats.items()]
    return "\n".join(lines)


def main() -> None:
    import os
    from poly_alpha_sniper.storage.db import get_store
    store = get_store(os.environ.get("DATABASE_URL", ""))
    from poly_alpha_sniper.storage.migrations import run_migrations
    run_migrations(store)
    print(render_text(build_stats(store)))


if __name__ == "__main__":
    main()
