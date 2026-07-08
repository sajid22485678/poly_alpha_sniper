"""Daily aggregate report from the store."""
from __future__ import annotations

from collections import Counter


def build_daily(store, day: str = "") -> dict:
    """day: 'YYYY-MM-DD' (UTC); empty = all time (used when day column absent)."""
    try:
        exits = store.query("SELECT * FROM exits ORDER BY ts_ms DESC LIMIT 500")
        predictions = store.query(
            "SELECT decision, reject_reason, tier FROM predictions "
            "ORDER BY ts_ms DESC LIMIT 2000")
        fq = store.query("SELECT score FROM fill_quality ORDER BY ts_ms DESC LIMIT 200")
    except Exception:  # noqa: BLE001
        return {"text": "daily report: database unavailable", "trades": 0}

    pnls = [float(e.get("pnl_usd") or 0) for e in exits]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    gross_win, gross_loss = sum(wins), abs(sum(losses))
    pf = gross_win / gross_loss if gross_loss > 1e-9 else (2.0 if gross_win else 0.0)
    reject_counts = Counter(str(p.get("reject_reason") or "") for p in predictions
                            if p.get("decision") == "REJECT")
    tier_counts = Counter(str(p.get("tier") or "?") for p in predictions)

    report = {
        "trades": len(exits),
        "pnl_usd": round(sum(pnls), 3),
        "winrate": round(len(wins) / len(pnls), 3) if pnls else 0.0,
        "profit_factor": round(pf, 2),
        "predictions": len(predictions),
        "top_rejects": dict(reject_counts.most_common(5)),
        "tier_distribution": dict(tier_counts),
        "avg_fill_quality": round(sum(float(f.get("score") or 0) for f in fq) / len(fq), 1)
                            if fq else None,
    }
    report["text"] = (
        f"DAILY: trades={report['trades']} pnl=${report['pnl_usd']} "
        f"wr={report['winrate']:.0%} pf={report['profit_factor']} "
        f"preds={report['predictions']}")
    return report
