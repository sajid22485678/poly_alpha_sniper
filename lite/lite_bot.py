"""Poly Alpha Lite V1 runtime: isolated, public-data, simulated shadow lane."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Optional

from .lite_book import LiteBookClient
from .lite_broker import LiteBroker
from .lite_cex import LiteCexFeed
from .lite_config import FIXED_SHARES, load_lite_config
from .lite_export import assert_lite_safety, write_lite_dashboard
from .lite_market import LiteGammaClient, LiteMarket, LiteMarketFinder, current_window_slug
from .lite_resolver import LiteResolver, book_exit_pnl, direct_book_exit_price
from .lite_store import LiteStore
from .lite_strategy import LiteStrategy

log = logging.getLogger("poly_alpha_lite_shadow")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        import psutil
        return bool(psutil.pid_exists(pid))
    except Exception:  # pragma: no cover - psutil is a declared dependency
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


class LiteRuntimeFiles:
    """Lite-only lock/state/heartbeat namespace; never uses core runtime files."""

    def __init__(self, runtime_dir: str, cfg):
        self.directory = Path(runtime_dir)
        self.lock_path = self.directory / "process.lock"
        self.state_path = self.directory / "state.json"
        self.heartbeat_path = self.directory / "heartbeat.json"
        self.stop_path = self.directory / "stop.request"
        self.cfg = cfg
        self.pid = os.getpid()
        self.started_ts_ms = _now_ms()
        self._held = False

    def acquire(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        if self.lock_path.exists():
            existing: dict[str, Any] = {}
            try:
                existing = json.loads(self.lock_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
            existing_pid = int(existing.get("pid") or 0)
            if _pid_alive(existing_pid):
                raise RuntimeError(
                    f"Lite shadow already has an active process lock (pid={existing_pid})"
                )
            self.lock_path.unlink(missing_ok=True)
        self.stop_path.unlink(missing_ok=True)
        lock = {
            "pid": self.pid,
            "mode": "lite_shadow",
            "started_ts_ms": self.started_ts_ms,
            "module": "lite.lite_bot",
        }
        encoded = json.dumps(lock, separators=(",", ":")).encode("utf-8")
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise RuntimeError("Lite shadow process lock was acquired concurrently") from exc
        try:
            os.write(fd, encoded)
        finally:
            os.close(fd)
        self._held = True

    def publish(self, state: dict[str, Any]) -> dict[str, Any]:
        now_ms = _now_ms()
        payload = {
            **state,
            "schema_version": 1,
            "running": True,
            "pid": self.pid,
            "mode": "lite_shadow",
            "dry_run": True,
            "live_enabled": False,
            "started_ts_ms": self.started_ts_ms,
            "heartbeat_ts_ms": now_ms,
        }
        _atomic_json(self.state_path, payload)
        _atomic_json(self.heartbeat_path, {
            "ts_ms": now_ms, "pid": self.pid, "mode": "lite_shadow",
        })
        return payload

    def stop_requested(self) -> bool:
        if not self.stop_path.exists():
            return False
        try:
            request = json.loads(self.stop_path.read_text(encoding="utf-8"))
            return (str(request.get("mode")) == "lite_shadow"
                    and int(request.get("target_pid") or 0) == self.pid)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return False

    def release(self, state: Optional[dict[str, Any]] = None) -> None:
        if state is not None:
            final = {
                **state,
                "schema_version": 1, "running": False, "pid": self.pid,
                "mode": "lite_shadow", "dry_run": True, "live_enabled": False,
                "started_ts_ms": self.started_ts_ms,
                "heartbeat_ts_ms": _now_ms(),
            }
            try:
                _atomic_json(self.state_path, final)
            except OSError:
                pass
        if self._held:
            try:
                current = json.loads(self.lock_path.read_text(encoding="utf-8"))
                if int(current.get("pid") or 0) == self.pid:
                    self.lock_path.unlink(missing_ok=True)
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                pass
        self.stop_path.unlink(missing_ok=True)
        self._held = False


class LiteBot:
    def __init__(self, cfg):
        assert_lite_safety(cfg)
        if float(cfg.fixed_order_shares) != FIXED_SHARES:
            raise RuntimeError("Lite fixed-share safety lock failed")
        self.cfg = cfg
        self.store = LiteStore(cfg.db_path)
        self.gamma = LiteGammaClient(cfg.gamma_base_url)
        self.market_finder = LiteMarketFinder(self.gamma.get_markets)
        self.cex = LiteCexFeed(cfg.assets)
        self.books = LiteBookClient(cfg.clob_base_url)
        self.strategy = LiteStrategy(cfg)
        self.broker = LiteBroker(self.store)
        self.resolver = LiteResolver(
            self.store, self.gamma.get_markets, self.books, cfg,
        )
        self.runtime = LiteRuntimeFiles(cfg.runtime_dir, cfg)
        self.stop_event = asyncio.Event()
        self.last_scan_ts_ms: Optional[int] = None
        self.last_trade_ts_ms: Optional[int] = self.store.last_trade_ts()
        self.current_markets: dict[str, dict[str, Any]] = {}

    def _runtime_state(self) -> dict[str, Any]:
        return {
            "last_scan_ts_ms": self.last_scan_ts_ms,
            "last_trade_ts_ms": self.last_trade_ts_ms,
            "open_positions": len(self.store.open_positions()),
            "db_path": str(Path(self.cfg.db_path).resolve()),
        }

    async def _manage_open_positions(self, now_ms: int) -> None:
        """Try a direct owned-token bid first; otherwise queue exact resolution."""
        for trade in self.store.open_positions():
            close_ts = int(trade.get("window_close_ts") or 0)
            exit_due = now_ms >= close_ts - int(float(self.cfg.exit_before_close_s) * 1000)
            if not exit_due:
                continue
            side = str(trade.get("side") or "")
            token_id = (trade.get("yes_token_id") if side == "BUY_YES"
                        else trade.get("no_token_id") if side == "BUY_NO" else "")
            quote = None
            if self.cfg.allow_book_exit and token_id:
                quote = await self.books.get_book(str(token_id))
            exit_price, reason = direct_book_exit_price(
                trade, quote, now_ms, int(self.cfg.book_max_age_ms),
                float(self.cfg.max_spread), float(self.cfg.min_depth_usd),
            )
            if exit_price is not None:
                pnl = book_exit_pnl(
                    float(trade["entry_price"]), FIXED_SHARES, exit_price,
                )
                self.store.complete_trade(
                    trade_id=int(trade["id"]), status="CLOSED_BOOK_EXIT",
                    exit_price=exit_price, exit_ts=now_ms, pnl=pnl,
                    resolution_source="book_exit",
                    resolution_reason="direct_token_book",
                )
            elif now_ms >= close_ts:
                self.store.mark_pending(int(trade["id"]), now_ms, reason)

    @staticmethod
    def _market_export(market: LiteMarket, now_ms: int) -> dict[str, Any]:
        return {
            "slug": market.slug,
            "market_id": market.market_id,
            "event_id": market.event_id,
            "condition_id": market.condition_id,
            "window_close_ts": market.window_close_s * 1000,
            "seconds_to_close": round(market.seconds_to_close(now_ms), 3),
            "anchor_available": market.anchor_available,
            "price_to_beat": market.price_to_beat,
        }

    def _cex_export(self, now_ms: int) -> dict[str, Any]:
        return {
            asset: self.cex.state(asset, now_ms, int(self.cfg.cex_max_age_ms))
            for asset in self.cfg.assets
        }

    async def _scan_asset(self, asset: str, now_ms: int) -> None:
        market, market_reason = await self.market_finder.current_market(asset, now_ms)
        if market is None:
            slug = current_window_slug(asset, now_ms)
            self.current_markets[asset] = {"slug": slug, "status": market_reason}
            self.store.record_reject(
                now_ms, asset, slug, market_reason or "no_market", self.cfg.reject_bucket_s,
            )
            return
        self.current_markets[asset] = self._market_export(market, now_ms)

        quotes = await self.books.get_books([market.yes_token_id, market.no_token_id])
        evaluated_ms = _now_ms()
        cex_price, cex_age_ms, cex_source = self.cex.latest(asset, evaluated_ms)
        momentum = self.cex.momentum_values(
            asset, list(self.cfg.momentum_windows_s), evaluated_ms,
        )
        open_rows = self.store.open_positions()
        window_rows = self.store.positions_for_window(market.window_close_s * 1000)
        by_id = {int(row["id"]): row for row in [*open_rows, *window_rows]}
        decision = self.strategy.evaluate(
            market=market,
            yes_book=quotes.get(market.yes_token_id),
            no_book=quotes.get(market.no_token_id),
            cex_price=cex_price,
            cex_age_ms=cex_age_ms,
            cex_source=cex_source,
            momentum_values=momentum,
            now_ms=evaluated_ms,
            open_positions=list(by_id.values()),
        )
        if not decision.accepted:
            self.store.record_reject(
                evaluated_ms, asset, market.slug, decision.reject_reason,
                self.cfg.reject_bucket_s,
            )
            return
        row = self.broker.open_trade(
            market, decision, evaluated_ms,
            cex_source=cex_source, cex_entry_price=cex_price,
        )
        self.store.record_reject(
            evaluated_ms, asset, market.slug, "opened", self.cfg.reject_bucket_s,
        )
        self.last_trade_ts_ms = int(row["entry_ts"])

    async def scan_once(self) -> None:
        now_ms = _now_ms()
        await self._manage_open_positions(now_ms)
        await self.resolver.resolve_due(now_ms)
        await self.cex.poll_once()
        async def scan_asset_safe(asset: str) -> None:
            try:
                await self._scan_asset(asset, _now_ms())
            except Exception as exc:  # one public-data failure never stops the loop
                log.warning("Lite scan failed for %s: %s", asset, type(exc).__name__)
                self.store.record_reject(
                    _now_ms(), asset, current_window_slug(asset, _now_ms()),
                    "no_market", self.cfg.reject_bucket_s,
                )
        await asyncio.gather(*(scan_asset_safe(asset) for asset in self.cfg.assets))
        self.last_scan_ts_ms = _now_ms()
        state = self.runtime.publish(self._runtime_state())
        try:
            write_lite_dashboard(
                self.store, self.cfg, self.last_scan_ts_ms,
                runtime_state=state,
                cex_feed_state=self._cex_export(self.last_scan_ts_ms),
                current_market_by_asset=self.current_markets,
            )
        except Exception as exc:  # export failure cannot affect trading state
            log.warning("Lite dashboard export failed: %s", type(exc).__name__)

    async def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set() and not self.runtime.stop_requested():
            self.runtime.publish(self._runtime_state())
            await asyncio.sleep(min(3.0, max(0.5, float(self.cfg.scan_interval_s))))

    async def run(self, once: bool = False) -> None:
        acquired = False
        heartbeat_task = None
        try:
            self.runtime.acquire()
            acquired = True
            self.runtime.publish(self._runtime_state())
            heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            while not self.stop_event.is_set() and not self.runtime.stop_requested():
                started = time.monotonic()
                await self.scan_once()
                if once:
                    break
                remaining = max(0.0, float(self.cfg.scan_interval_s) - (time.monotonic() - started))
                deadline = time.monotonic() + remaining
                while time.monotonic() < deadline:
                    if self.stop_event.is_set() or self.runtime.stop_requested():
                        break
                    await asyncio.sleep(min(0.5, deadline - time.monotonic()))
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
            await self.cex.close()
            await self.books.close()
            await self.gamma.close()
            state = self._runtime_state() if acquired else None
            self.store.close()
            if acquired:
                self.runtime.release(state)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


async def _run(args) -> None:
    cfg = load_lite_config(args.config or None)
    bot = LiteBot(cfg)
    loop = asyncio.get_running_loop()

    def stop_handler(*_args) -> None:
        bot.stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_handler)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, stop_handler)
    await bot.run(once=args.once)


def main() -> None:
    parser = argparse.ArgumentParser(description="Poly Alpha Lite V1 shadow-only runtime")
    parser.add_argument("--config", default="", help="optional Lite YAML path")
    parser.add_argument("--once", action="store_true", help="run one scan then stop")
    args = parser.parse_args()
    _configure_logging()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
