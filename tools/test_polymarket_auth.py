"""Polymarket connectivity + auth check. READ-ONLY.

Run: python -m poly_alpha_sniper.tools.test_polymarket_auth

Checks: public CLOB + Gamma reachability; then, if keys are present, balance,
open orders and positions. NEVER places or cancels orders. NEVER prints key
material.
"""
from __future__ import annotations

import asyncio
import sys


async def _main() -> int:
    from poly_alpha_sniper.core.config_loader import load_config, load_secrets
    from poly_alpha_sniper.connectors.polymarket_clob_public import PolymarketClobPublic
    from poly_alpha_sniper.connectors.polymarket_gamma import PolymarketGamma

    cfg = load_config()
    secrets = load_secrets()
    rc = 0

    print("== PUBLIC CONNECTIVITY ==")
    pub = PolymarketClobPublic(cfg)
    ok = await pub.get_ok()
    print(f"CLOB /ok: {'reachable' if ok else 'UNREACHABLE'}")
    gamma = PolymarketGamma(cfg)
    try:
        markets = await gamma.get_markets({"limit": 1, "closed": "false"})
        print(f"Gamma /markets: reachable ({len(markets)} sample rows)")
    except Exception as exc:  # noqa: BLE001
        print(f"Gamma /markets: FAILED ({type(exc).__name__})")
        rc = 1
    finally:
        await gamma.close()
        await pub.close()

    print("\n== AUTH CONNECTIVITY (read-only) ==")
    if not secrets.has("POLYMARKET_PRIVATE_KEY"):
        print("POLYMARKET_PRIVATE_KEY not set — skipping auth checks.")
        print("(Shadow/simulation modes do not need it.)")
        return rc
    try:
        from poly_alpha_sniper.connectors.polymarket_clob_private import create_live_client
        # This tool is explicitly read-only; live_gates_ok only unlocks
        # CONSTRUCTION here — we call exclusively read endpoints below.
        client = create_live_client(cfg, secrets, live_gates_ok=True)
    except RuntimeError as exc:
        print(f"client init failed: {exc}")
        return 1
    try:
        balance = await client.get_balance_usd()
        print(f"balance: ${balance:.2f} USDC")
    except Exception as exc:  # noqa: BLE001
        print(f"balance: FAILED ({type(exc).__name__})")
        rc = 1
    try:
        orders = await client.get_open_orders()
        print(f"open orders: {len(orders)}")
    except Exception as exc:  # noqa: BLE001
        print(f"open orders: FAILED ({type(exc).__name__})")
        rc = 1
    try:
        positions = await client.get_positions()
        print(f"positions: {len(positions)}")
        for p in positions[:5]:
            print(f"  {p.outcome.value} {p.shares:.2f} @ {p.avg_entry_price:.3f} "
                  f"(market {p.market_id[:16]}...)")
    except Exception as exc:  # noqa: BLE001
        print(f"positions: FAILED ({type(exc).__name__})")
        rc = 1
    print("\nNOTE: this tool never places or cancels orders.")
    return rc


def main() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    raise SystemExit(asyncio.run(_main()))


if __name__ == "__main__":
    main()
