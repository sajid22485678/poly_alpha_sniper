"""Download Binance 1s klines for BTC/ETH/SOL into data/cex.csv (resumable).

Run: python -m poly_alpha_sniper.backtest.download_cex_data --days 7 [--out data/cex.csv]
NOTE: 12 months of 1s data is large; start with a few days.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time

import aiohttp

from poly_alpha_sniper.backtest.data_downloader import append_rows, fetch_json, last_ts_ms

BINANCE = "https://api.binance.com/api/v3/klines"
FIELDS = ["ts_ms", "asset", "price", "exchange"]


async def download(symbols: dict[str, str], days: float, out: str) -> None:
    end_ms = int(time.time() * 1000)
    start_default = end_ms - int(days * 86_400_000)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        for asset, symbol in symbols.items():
            resume = last_ts_ms(out)
            start = max(start_default, resume + 1000 if resume else start_default)
            cursor = start
            total = 0
            while cursor < end_ms:
                data = await fetch_json(session, BINANCE, {
                    "symbol": symbol, "interval": "1s", "limit": 1000,
                    "startTime": cursor})
                if not data:
                    break
                rows = [{"ts_ms": int(k[0]), "asset": asset,
                         "price": float(k[4]), "exchange": "binance"} for k in data]
                append_rows(out, rows, FIELDS)
                total += len(rows)
                cursor = int(data[-1][0]) + 1000
                await asyncio.sleep(0.25)  # polite pacing
                if total % 20_000 == 0:
                    print(f"{asset}: {total} rows...")
            print(f"{asset}: downloaded {total} rows -> {out}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=float, default=1.0)
    parser.add_argument("--out", default="data/cex.csv")
    args = parser.parse_args()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    from poly_alpha_sniper.core.config_loader import load_config
    cfg = load_config()
    asyncio.run(download(cfg.cex.symbols, args.days, args.out))


if __name__ == "__main__":
    main()
