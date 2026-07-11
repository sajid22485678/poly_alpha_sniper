"""Poly Alpha Lite V1 runtime: isolated, public-data, simulated shadow lane."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from .lite_book import LiteBookClient
from .lite_broker import LiteBroker
from .lite_cex import LiteCexFeed
from .lite_config import FIXED_SHARES, load_lite_config
from .lite_export import assert_lite_safety, write_lite_dashboard
from .lite_market import LiteGammaClient, LiteMarket, LiteMarketFinder, current_window_slug
from .lite_resolver import LiteResolver, book_exit_pnl, direct_book_exit
from .lite_risk import assess_live_small_exposure, sweep_taker_fee
from .lite_store import LiteStore, WindowLockConflict
from .lite_strategy import LiteStrategy

log = logging.getLogger("poly_alpha_lite_shadow")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _current_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1],
            capture_output=True, text=True, timeout=3, check=True)
        commit = result.stdout.strip()
        return commit if len(commit) == 40 else "UNKNOWN"
    except (OSError, subprocess.SubprocessError):
        return "UNKNOWN"


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


def _process_create_time(pid: int) -> Optional[float]:
    try:
        import psutil
        return float(psutil.Process(int(pid)).create_time())
    except Exception:
        return None


class LiteRuntimeFiles:
    """Lite-only lock/state/heartbeat namespace; never uses core runtime files."""

    def __init__(self, runtime_dir: str, cfg):
        self.directory = Path(runtime_dir)
        self.lock_path = self.directory / "process.lock"
        self.state_path = self.directory / "state.json"
        self.heartbeat_path = self.directory / "heartbeat.json"
        self.stop_path = self.directory / "stop.request"
        self.guard_path = self.directory / "process.guard"
        self.cfg = cfg
        self.pid = os.getpid()
        self.started_ts_ms = _now_ms()
        supplied_nonce = str(os.environ.get("POLY_ALPHA_LITE_LAUNCH_NONCE", ""))
        self.launch_nonce = (supplied_nonce if len(supplied_nonce) == 32
                             and all(char in "0123456789abcdefABCDEF" for char in supplied_nonce)
                             else uuid.uuid4().hex)
        self.process_create_time = _process_create_time(self.pid)
        self._held = False
        self._guard_fd: Optional[int] = None

    def _acquire_os_guard(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.guard_path, os.O_CREAT | os.O_RDWR)
        try:
            if os.path.getsize(self.guard_path) == 0:
                os.write(fd, b"\0")
            os.lseek(fd, 0, os.SEEK_SET)
            if sys.platform == "win32":
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:  # pragma: no cover - Windows is the production target
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError) as exc:
            os.close(fd)
            raise RuntimeError("Lite shadow OS process guard is already held") from exc
        self._guard_fd = fd

    def _release_os_guard(self) -> None:
        fd, self._guard_fd = self._guard_fd, None
        if fd is None:
            return
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            if sys.platform == "win32":
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:  # pragma: no cover
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def acquire(self) -> None:
        try:
            self._acquire_os_guard()
            if self.lock_path.exists():
                existing: dict[str, Any] = {}
                try:
                    existing = json.loads(self.lock_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    existing = {}
                existing_pid = int(existing.get("pid") or 0)
                existing_create = existing.get("process_create_time")
                alive = _pid_alive(existing_pid)
                actual_create = _process_create_time(existing_pid) if alive else None
                same_process = (alive and (existing_create is None or actual_create is None
                                or abs(float(existing_create)-actual_create) < 0.01))
                if same_process:
                    raise RuntimeError(
                        f"Lite shadow already has an active process lock (pid={existing_pid})")
                self.lock_path.unlink(missing_ok=True)
            self.stop_path.unlink(missing_ok=True)
            lock = {
                "pid": self.pid, "mode": "lite_shadow",
                "started_ts_ms": self.started_ts_ms,
                "module": "lite.lite_bot", "launch_nonce": self.launch_nonce,
                "process_create_time": self.process_create_time,
                "current_commit": _current_commit(),
            }
            encoded = json.dumps(lock, separators=(",", ":")).encode("utf-8")
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, encoded)
            finally:
                os.close(fd)
            self._held = True
        except Exception:
            self._release_os_guard()
            raise

    def publish(self, state: dict[str, Any]) -> dict[str, Any]:
        now_ms = _now_ms()
        payload = {
            **state,
            "schema_version": 2,
            "running": True,
            "pid": self.pid,
            "mode": "lite_shadow",
            "dry_run": True,
            "live_enabled": False,
            "started_ts_ms": self.started_ts_ms,
            "launch_nonce": self.launch_nonce,
            "process_create_time": self.process_create_time,
            "heartbeat_ts_ms": now_ms,
        }
        _atomic_json(self.state_path, payload)
        _atomic_json(self.heartbeat_path, {
            "ts_ms": now_ms, "pid": self.pid, "mode": "lite_shadow",
            "launch_nonce": self.launch_nonce,
        })
        return payload

    def stop_requested(self) -> bool:
        if not self.stop_path.exists():
            return False
        try:
            request = json.loads(self.stop_path.read_text(encoding="utf-8"))
            return (str(request.get("mode")) == "lite_shadow"
                    and int(request.get("target_pid") or 0) == self.pid
                    and str(request.get("launch_nonce") or "") == self.launch_nonce)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return False

    def release(self, state: Optional[dict[str, Any]] = None) -> None:
        if state is not None:
            final = {
                **state,
                "schema_version": 2, "running": False, "pid": self.pid,
                "mode": "lite_shadow", "dry_run": True, "live_enabled": False,
                "started_ts_ms": self.started_ts_ms,
                "launch_nonce": self.launch_nonce,
                "process_create_time": self.process_create_time,
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
        self._release_os_guard()


class LiteBot:
    def __init__(self, cfg):
        assert_lite_safety(cfg)
        if float(cfg.fixed_order_shares) != FIXED_SHARES:
            raise RuntimeError("Lite fixed-share safety lock failed")
        self.cfg = cfg
        self.store = LiteStore(
            cfg.db_path,
            max_open_positions=int(cfg.max_open_positions),
            max_open_per_asset=int(cfg.max_open_per_asset),
            exposure_cap_usd=(float(cfg.live_small_equity_usd)
                              * float(cfg.equity_exposure_cap_pct)),
        )
        self.gamma = LiteGammaClient(cfg.gamma_base_url)
        self.market_finder = LiteMarketFinder(self.gamma.get_markets)
        self.cex = LiteCexFeed(cfg.assets)
        self.books = LiteBookClient(cfg.clob_base_url)
        self.strategy = LiteStrategy(cfg)
        self.broker = LiteBroker(
            self.store, cfg.crypto_taker_fee_rate, cfg.fee_buffer_usd,
            cfg.book_max_age_ms, cfg.max_spread)
        self.resolver = LiteResolver(
            self.store, self.gamma.get_market, self.gamma.get_event, cfg,
        )
        self.runtime = LiteRuntimeFiles(cfg.runtime_dir, cfg)
        self.stop_event = asyncio.Event()
        self.last_scan_ts_ms: Optional[int] = None
        self.last_trade_ts_ms: Optional[int] = self.store.last_trade_ts()
        self.current_markets: dict[str, dict[str, Any]] = {}
        self.current_commit = _current_commit()
        self.last_error: Optional[str] = None

    def _runtime_state(self) -> dict[str, Any]:
        return {
            "last_scan_ts_ms": self.last_scan_ts_ms,
            "last_trade_ts_ms": self.last_trade_ts_ms,
            "open_positions": len(self.store.open_positions()),
            "db_path": str(Path(self.cfg.db_path).resolve()),
            "current_commit": self.current_commit,
            "last_error": self.last_error,
        }

    async def _manage_open_positions(self, now_ms: int) -> None:
        """Use only pre-close, five-share owned-token bid sweeps."""
        self.store.finalize_window_locks(now_ms)
        due: list[dict] = []
        for trade in self.store.open_positions():
            close_ts = int(trade.get("window_close_ts") or 0)
            if now_ms >= close_ts:
                self.store.mark_pending(
                    int(trade["id"]), now_ms, "preclose_exit_unavailable")
                continue
            exit_due = now_ms >= close_ts - int(float(self.cfg.exit_before_close_s) * 1000)
            if not exit_due:
                continue
            self.store.mark_exit_pending(int(trade["id"]), now_ms)
            due.append(trade)

        async def exit_one(trade: dict) -> None:
            side = str(trade.get("side") or "")
            token_id = (trade.get("yes_token_id") if side == "BUY_YES"
                        else trade.get("no_token_id") if side == "BUY_NO" else "")
            quote = await self.books.get_book(str(token_id)) if (
                self.cfg.allow_book_exit and token_id) else None
            evidence_ms = _now_ms()
            sweep, reason = direct_book_exit(
                trade, quote, evidence_ms, int(self.cfg.book_max_age_ms),
                float(self.cfg.max_spread))
            if sweep is not None and quote is not None:
                exit_price = float(sweep.vwap)
                gross = book_exit_pnl(
                    float(trade["entry_price"]), FIXED_SHARES, exit_price)
                entry_fee = float(trade.get("entry_fee") or 0.0)
                fee_rate = float(trade.get("fee_rate") or self.cfg.crypto_taker_fee_rate)
                exit_fee = sweep_taker_fee(sweep, fee_rate)
                net = gross - entry_fee - exit_fee
                self.store.complete_trade(
                    trade_id=int(trade["id"]), status="CLOSED_BOOK_EXIT",
                    exit_price=exit_price, exit_ts=evidence_ms, pnl=net,
                    gross_pnl=gross, exit_fee=exit_fee,
                    resolution_source="book_exit",
                    resolution_reason="direct_token_five_share_bid_sweep",
                    resolution_verified=True,
                    exit_evidence={
                        "book_ts": quote.effective_ts_ms(),
                        "received_ts": quote.received_ts_ms,
                        "age_ms": quote.age_ms(evidence_ms),
                        "book_hash": quote.book_hash,
                        "best_bid": quote.best_bid, "best_ask": quote.best_ask,
                        "fill_shares": sweep.shares,
                        "bid_depth_shares": quote.total_bid_shares,
                        "fill_vwap": sweep.vwap,
                        "worst_price": sweep.worst_price,
                        "fill_levels": [list(level) for level in sweep.levels],
                        "spread": quote.spread,
                    },
                )
            elif evidence_ms >= int(trade["window_close_ts"]):
                self.store.mark_pending(int(trade["id"]), evidence_ms, reason)

        await asyncio.gather(*(exit_one(trade) for trade in due))

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
        features = self.cex.feature_snapshot(
            asset, list(self.cfg.momentum_windows_s), evaluated_ms)
        committed_rows = self.store.committed_positions()
        window_close_ts = market.window_close_s * 1000
        window_rows = self.store.positions_for_window(window_close_ts)
        by_id = {int(row["id"]): row for row in [*committed_rows, *window_rows]}
        self.store.record_decision(
            evaluated_ms, asset, market.slug, "candidate", self.cfg.reject_bucket_s)
        direction = self.strategy.choose_direction(
            features, cex_price=cex_price, market=market,
            yes_book=quotes.get(market.yes_token_id),
            no_book=quotes.get(market.no_token_id), cex_age_ms=cex_age_ms)
        self.current_markets[asset]["direction"] = {
            "output": direction.output, "side": direction.side,
            "yes_score": direction.yes_score, "no_score": direction.no_score,
            "score_difference": direction.score_difference,
            "confidence": direction.confidence, "reason": direction.reason,
        }
        if direction.side is None:
            self.store.record_decision(
                evaluated_ms, asset, market.slug, direction.output,
                self.cfg.reject_bucket_s)
            reject = {
                "BRIEF_CONFIRMATION_WAIT": "brief_confirmation_wait",
                "NO_TRADE_TRULY_FLAT": "truly_flat",
                "NO_TRADE_DATA_INVALID": "data_invalid",
            }[direction.output]
            self.store.record_reject(
                evaluated_ms, asset, market.slug, reject, self.cfg.reject_bucket_s)
            return

        lock = self.store.get_window_lock(asset, window_close_ts)
        new_lock = lock is None
        if lock is None:
            reserved, reason, lock = self.store.reserve_window_direction(
                market, direction, evaluated_ms)
            if not reserved:
                self.store.record_reject(
                    evaluated_ms, asset, market.slug, reason, self.cfg.reject_bucket_s)
                return
        elif str(lock.get("status")) not in ("DIRECTION_LOCKED", "WAIT_FOR_PULLBACK"):
            reason = ("opposite_side_blocked" if str(lock.get("side")) != direction.side
                      else "duplicate_same_side_blocked")
            self.store.record_reject(
                evaluated_ms, asset, market.slug, reason, self.cfg.reject_bucket_s)
            return
        elif str(lock.get("side")) != direction.side:
            self.store.mark_window_skipped(
                asset, window_close_ts, evaluated_ms, "thesis_invalidated_no_reversal")
            self.store.record_reject(
                evaluated_ms, asset, market.slug, "opposite_side_blocked",
                self.cfg.reject_bucket_s)
            return

        selected_book = (quotes.get(market.yes_token_id)
                         if direction.side == "BUY_YES"
                         else quotes.get(market.no_token_id))
        timing = self.strategy.optimize_entry(
            direction, selected_book, market, evaluated_ms,
            lock=None if new_lock else lock)
        self.current_markets[asset]["entry_decision"] = {
            "action": timing.action, "reason": timing.reason,
            "target_price": timing.target_price,
            "max_chase_price": timing.max_chase_price,
            "deadline_ts": timing.deadline_ts,
        }
        if timing.action == "WAIT_FOR_PULLBACK":
            initial_ask = None
            if selected_book is not None:
                sweep = selected_book.buy_sweep(FIXED_SHARES)
                initial_ask = sweep.vwap if sweep is not None else None
            self.store.update_window_lock(
                asset, window_close_ts, status="WAIT_FOR_PULLBACK",
                lifecycle_status="DIRECTION_LOCKED", entry_state="WAIT_FOR_PULLBACK",
                initial_ask=lock.get("initial_ask") or initial_ask,
                target_price=timing.target_price,
                max_chase_price=timing.max_chase_price,
                deadline_ts=timing.deadline_ts,
                last_reevaluate_ts=evaluated_ms,
                expected_improvement=timing.expected_improvement,
                wait_duration_ms=timing.wait_duration_ms,
                last_updated_ts=evaluated_ms)
            self.store.record_decision(
                evaluated_ms, asset, market.slug, "WAIT_FOR_PULLBACK",
                self.cfg.reject_bucket_s)
            return
        if timing.action != "ENTER_NOW":
            terminal_skip = timing.reason in (
                "thesis_invalidated", "max_chase_exceeded", "price_window")
            if terminal_skip:
                self.store.mark_window_skipped(
                    asset, window_close_ts, evaluated_ms, timing.reason,
                    missed=timing.missed_opportunity,
                    chase_prevented=timing.chase_prevented)
            self.store.record_reject(
                evaluated_ms, asset, market.slug, timing.reason,
                self.cfg.reject_bucket_s)
            return

        decision = self.strategy.evaluate(
            market=market,
            yes_book=quotes.get(market.yes_token_id),
            no_book=quotes.get(market.no_token_id),
            cex_price=cex_price,
            cex_age_ms=cex_age_ms,
            cex_source=cex_source,
            momentum_values=features,
            now_ms=evaluated_ms,
            open_positions=list(by_id.values()),
            window_lock=None if new_lock else lock,
        )
        if not decision.accepted:
            self.store.record_reject(
                evaluated_ms, asset, market.slug, decision.reject_reason,
                self.cfg.reject_bucket_s,
            )
            return
        risk = self.store.risk_snapshot(evaluated_ms)
        exposure = assess_live_small_exposure(
            entry_price=float(decision.entry_price),
            committed_exposure_usd=float(risk["committed_exposure_usd"]),
            equity_usd=float(self.cfg.live_small_equity_usd),
            available_balance_usd=(float(self.cfg.live_small_equity_usd)
                                   - float(risk["committed_exposure_usd"])),
            exposure_cap_pct=float(self.cfg.equity_exposure_cap_pct),
            fee_rate=float(self.cfg.crypto_taker_fee_rate),
            fee_buffer_usd=float(self.cfg.fee_buffer_usd),
            # Shadow collects live-small exposure behavior without being shut
            # down by historical legacy PnL. Daily/streak guards remain a
            # mandatory live activation check and are exported separately.
            kill_switch=False)
        if not exposure.allowed:
            self.store.record_reject(
                evaluated_ms, asset, market.slug, exposure.reason,
                self.cfg.reject_bucket_s)
            return
        try:
            row = self.broker.open_trade(
                market, decision, evaluated_ms,
                cex_source=cex_source, cex_entry_price=cex_price)
        except WindowLockConflict as exc:
            self.store.record_reject(
                evaluated_ms, asset, market.slug, exc.reason,
                self.cfg.reject_bucket_s)
            return
        self.store.record_decision(
            evaluated_ms, asset, market.slug,
            "WAIT_FOR_PULLBACK_ENTERED" if decision.wait_duration_ms else "ENTER_NOW",
            self.cfg.reject_bucket_s)
        self.last_trade_ts_ms = int(row["entry_ts"])

    async def scan_once(self) -> None:
        now_ms = _now_ms()
        await self.cex.poll_once()
        await asyncio.gather(
            self._manage_open_positions(_now_ms()),
            self.resolver.resolve_due(_now_ms()),
        )
        async def scan_asset_safe(asset: str) -> None:
            try:
                await self._scan_asset(asset, _now_ms())
            except Exception as exc:  # one public-data failure never stops the loop
                log.warning("Lite scan failed for %s: %s", asset, type(exc).__name__)
                self.last_error = f"scan:{asset}:{type(exc).__name__}"
                self.store.record_reject(
                    _now_ms(), asset, current_window_slug(asset, _now_ms()),
                    f"scan_exception:{type(exc).__name__}", self.cfg.reject_bucket_s,
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
