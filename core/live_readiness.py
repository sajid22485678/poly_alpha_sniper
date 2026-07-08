"""Live readiness — the LAST gate before real orders are possible.

Returns (ready, failed_gates). Every gate from the master spec is explicit.
This runs AFTER preflight and AFTER account reconciliation.
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import TradingMode
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("live_readiness")


async def check_live_readiness(cfg, secrets, client, panic, kill,
                               telegram_ok: bool, reconciled: bool) -> tuple[bool, list[str]]:
    failed: list[str] = []

    mode = TradingMode(cfg.mode.trading_mode)
    if not mode.is_live:
        failed.append(f"trading_mode {mode.value} is not live")
    if cfg.mode.dry_run:
        failed.append("dry_run is true")
    if not secrets.live_trading_enabled:
        failed.append("LIVE_TRADING_ENABLED is not true")
    if not secrets.i_understand_risk:
        failed.append("I_UNDERSTAND_REAL_MONEY_RISK is not true")
    if secrets.max_real_trade_usd <= 0:
        failed.append("MAX_REAL_TRADE_USD must be > 0")
    if cfg.risk.max_trade_usd > secrets.max_real_trade_usd:
        failed.append("config max_trade_usd exceeds MAX_REAL_TRADE_USD")

    if panic is not None and getattr(panic, "is_active", False):
        failed.append("panic mode active")
    if kill is not None and getattr(kill, "is_active", False):
        failed.append("kill switch active")

    if cfg.telegram.enabled and not telegram_ok:
        failed.append("Telegram critical alerts not working")

    if not reconciled:
        failed.append("account not reconciled (balance/positions/orders)")

    if client is None:
        failed.append("no trading client constructed")
    else:
        for method in ("place_order", "cancel_order", "cancel_all",
                       "get_open_orders", "get_balance_usd", "get_positions"):
            if not hasattr(client, method):
                failed.append(f"client missing {method} (emergency close not ready)")
        if hasattr(client, "get_balance_usd"):
            try:
                balance = await client.get_balance_usd()
                if balance < max(1.0, cfg.risk.min_trade_usd):
                    failed.append(f"balance ${balance:.2f} below minimum tradable")
            except Exception as exc:  # noqa: BLE001
                failed.append(f"balance fetch failed: {exc!r}")
        if hasattr(client, "get_open_orders"):
            try:
                await client.get_open_orders()
            except Exception as exc:  # noqa: BLE001
                failed.append(f"open orders fetch failed: {exc!r}")
        if hasattr(client, "get_positions"):
            try:
                await client.get_positions()
            except Exception as exc:  # noqa: BLE001
                failed.append(f"positions fetch failed: {exc!r}")

    ready = not failed
    log.info("live_readiness", extra={"extra": {"ready": ready, "failed": failed}})
    return ready, failed
