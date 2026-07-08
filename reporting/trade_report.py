"""Per-trade report builders from journal/exit rows."""
from __future__ import annotations


def build_trade_report(entry_row: dict, exit_row: dict | None = None) -> dict:
    report = {
        "market": entry_row.get("market_title") or entry_row.get("market_id", ""),
        "asset": entry_row.get("asset", ""),
        "side": entry_row.get("side", ""),
        "tier": entry_row.get("tier", ""),
        "entry_price": entry_row.get("price", 0.0),
        "size_usd": entry_row.get("size_usd", 0.0),
        "edge_at_entry": entry_row.get("edge", 0.0),
        "confidence": entry_row.get("confidence", 0.0),
    }
    if exit_row:
        report.update({
            "exit_price": exit_row.get("price", 0.0),
            "pnl_usd": exit_row.get("pnl_usd", 0.0),
            "hold_seconds": exit_row.get("hold_seconds", 0.0),
            "exit_reason": exit_row.get("reason", ""),
        })
    return report


def render_text(report: dict) -> str:
    return "\n".join(f"{k}: {v}" for k, v in report.items())
