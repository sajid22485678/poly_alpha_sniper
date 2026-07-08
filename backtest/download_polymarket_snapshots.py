"""Capture real Polymarket 5-min market books to CSV for research/backtests.

Run: python -m poly_alpha_sniper.backtest.download_polymarket_snapshots --minutes 30
Polls discovered 5-minute crypto markets' books every --interval-ms.
"""
from __future__ import annotations

import argparse
import asyncio
import sys

from poly_alpha_sniper.backtest.data_downloader import append_rows

FIELDS = ["ts_ms", "token_id", "side", "level", "price", "size"]


async def capture(minutes: float, interval_ms: int, out: str) -> None:
    from poly_alpha_sniper.core.clock import WallClock
    from poly_alpha_sniper.core.config_loader import load_config
    from poly_alpha_sniper.connectors.polymarket_clob_public import PolymarketClobPublic
    from poly_alpha_sniper.connectors.polymarket_gamma import PolymarketGamma
    from poly_alpha_sniper.discovery.market_discovery import MarketDiscovery

    cfg = load_config()
    clock = WallClock()
    gamma = PolymarketGamma(cfg)
    clob = PolymarketClobPublic(cfg, clock)
    discovery = MarketDiscovery(cfg, clock, gamma.get_markets)
    deadline = clock.now_ms() + int(minutes * 60_000)
    n = 0
    try:
        while clock.now_ms() < deadline:
            markets = await discovery.refresh()
            tokens = [t for m in markets for t in (m.yes_token_id, m.no_token_id) if t]
            print(f"tracking {len(tokens)} tokens across {len(markets)} markets")
            poll_until = min(deadline, clock.now_ms() + 60_000)
            while clock.now_ms() < poll_until:
                for token in tokens[:40]:
                    snap = await clob.get_book(token)
                    if snap is None:
                        continue
                    rows = []
                    for i, lvl in enumerate(snap.bids[:5]):
                        rows.append({"ts_ms": snap.ts_ms, "token_id": token,
                                     "side": "bid", "level": i,
                                     "price": lvl.price, "size": lvl.size})
                    for i, lvl in enumerate(snap.asks[:5]):
                        rows.append({"ts_ms": snap.ts_ms, "token_id": token,
                                     "side": "ask", "level": i,
                                     "price": lvl.price, "size": lvl.size})
                    append_rows(out, rows, FIELDS)
                    n += len(rows)
                await asyncio.sleep(interval_ms / 1000.0)
            print(f"{n} rows captured so far -> {out}")
    finally:
        await gamma.close()
        await clob.close()
    print(f"done: {n} rows -> {out}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=30)
    parser.add_argument("--interval-ms", type=int, default=1000)
    parser.add_argument("--out", default="data/poly.csv")
    args = parser.parse_args()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(capture(args.minutes, args.interval_ms, args.out))


if __name__ == "__main__":
    main()
