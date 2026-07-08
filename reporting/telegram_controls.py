"""Telegram admin/monitoring controls.

- Monitoring + emergency/admin ONLY. Trading is fully autonomous; there is NO
  /approve_trade, NO /reject_trade, no manual order confirmation.
- Only the configured TELEGRAM_CHAT_ID is honored; other chats are ignored
  silently (offset still advances).
- Dangerous commands (/resume /clear_panic /close_all /mode_live_micro) need a
  two-step confirmation: `/confirm <cmd>` within 60 s.
- Handlers are injected callables that route through the normal risk/panic
  APIs — controls can never bypass risk, live gates, kill switch, panic,
  reconciliation or config validation.

Update handling guarantees:
- On startup the pending backlog is FLUSHED (offset jumps past history) so
  restarts never replay old messages.
- Every update advances the offset exactly once — known, unknown, /start,
  ignored plain text, failed commands and unauthorized chats alike.
- update_id dedupe: the same update is never processed twice.
- Plain text (no leading '/') is ignored silently.
- Unknown slash commands get ONE help reply, then are throttled per chat.
"""
from __future__ import annotations

import asyncio
from collections import deque
from typing import Awaitable, Callable, Optional

import aiohttp

from poly_alpha_sniper.core.logger import get_logger, redact_text

log = get_logger("telegram_controls")

ALLOWED_COMMANDS = (
    "status", "health", "daily", "positions", "open_orders", "pnl", "mode",
    "pause", "resume", "panic", "clear_panic", "close_all", "cancel_orders",
    "mode_shadow", "mode_live_micro", "disable_live", "blacklist_market",
    "whitelist_market", "disable_asset", "enable_asset", "backup_now",
    "latency", "budget",
)
INFO_COMMANDS = ("start", "help")
DANGEROUS_COMMANDS = {"resume", "clear_panic", "close_all", "mode_live_micro"}
FORBIDDEN_COMMANDS = {"approve_trade", "reject_trade"}
CONFIRM_WINDOW_MS = 60_000
UNKNOWN_THROTTLE_MS = 30_000

Action = Callable[..., Awaitable[str]]


def help_text() -> str:
    info = ("Poly Alpha Sniper — autonomous bot; commands are monitoring/admin "
            "only (no manual trade approval exists).\n")
    cmds = " ".join("/" + c for c in ALLOWED_COMMANDS)
    danger = ", ".join("/" + c for c in sorted(DANGEROUS_COMMANDS))
    return (f"{info}Commands: /start /help {cmds}\n"
            f"Protected (need /confirm <cmd> within 60s): {danger}")


class TelegramControls:
    def __init__(self, secrets, cfg, clock, actions: dict[str, Action],
                 on_record: Optional[Callable[[dict], None]] = None):
        self.cfg = cfg
        self.clock = clock
        self.actions = actions
        self.on_record = on_record
        self._token = secrets.get("TELEGRAM_BOT_TOKEN")
        self._chat_id = str(secrets.get("TELEGRAM_CHAT_ID"))
        self._pending_confirm: dict[str, tuple[str, int]] = {}
        self._offset = 0
        self._stop = asyncio.Event()
        self.ignored_chats = 0
        self.require_confirmation = cfg.telegram.require_control_confirmation
        self._seen_ids: set[int] = set()
        self._seen_order: deque[int] = deque(maxlen=500)
        self._unknown_throttle_until: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Update processing (network-free; tests drive these directly)
    # ------------------------------------------------------------------
    def _mark_seen(self, update_id: int) -> bool:
        """Advance offset + dedupe. Returns False when already processed."""
        if update_id in self._seen_ids:
            self._offset = max(self._offset, update_id + 1)
            return False
        if len(self._seen_order) == self._seen_order.maxlen:
            self._seen_ids.discard(self._seen_order[0])
        self._seen_order.append(update_id)
        self._seen_ids.add(update_id)
        self._offset = max(self._offset, update_id + 1)
        return True

    async def process_update(self, update: dict) -> Optional[str]:
        """Single entrypoint: dedupe + offset for EVERY update, then route."""
        try:
            update_id = int(update.get("update_id", 0))
        except (TypeError, ValueError):
            update_id = 0
        if update_id and not self._mark_seen(update_id):
            return None  # duplicate — never processed twice
        return await self.handle_update(update)

    async def handle_update(self, update: dict) -> Optional[str]:
        msg = update.get("message") or update.get("edited_message") or {}
        chat_id = str((msg.get("chat") or {}).get("id", ""))
        text = str(msg.get("text") or "").strip()
        if not text.startswith("/"):
            return None  # plain text ("test", "hello") — ignored silently
        if chat_id != self._chat_id:
            self.ignored_chats += 1
            log.warning("unauthorized_chat_ignored", extra={"extra": {"count": self.ignored_chats}})
            return None  # silent
        parts = text[1:].split(None, 1)
        command = parts[0].lower().split("@")[0]
        arg = parts[1].strip() if len(parts) > 1 else ""
        reply = await self._dispatch(command, arg, chat_id)
        self._record(chat_id, command, arg, reply)
        return reply

    async def _dispatch(self, command: str, arg: str, chat_id: str = "") -> Optional[str]:
        now = self.clock.now_ms()

        if command in FORBIDDEN_COMMANDS:
            return "Manual trade approval does not exist. Trading is fully autonomous."

        if command in INFO_COMMANDS:
            extra = ""
            status_action = self.actions.get("status")
            if status_action is not None:
                try:
                    extra = "\n\nCurrent: " + str(await status_action())
                except Exception:  # noqa: BLE001
                    extra = ""
            return help_text() + extra

        if command == "confirm":
            target = arg.strip().lstrip("/").lower()
            pending = self._pending_confirm.pop(target, None)
            if pending is None:
                return f"nothing pending for '{target}'"
            orig_arg, expires = pending
            if now > expires:
                return f"confirmation for /{target} expired — issue the command again"
            return await self._execute(target, orig_arg)

        if command not in ALLOWED_COMMANDS:
            # one help reply, then throttle repeats from this chat
            until = self._unknown_throttle_until.get(chat_id, 0)
            if now < until:
                return None
            self._unknown_throttle_until[chat_id] = now + UNKNOWN_THROTTLE_MS
            return "Unknown command.\n" + help_text()

        if command in DANGEROUS_COMMANDS and self.require_confirmation:
            self._pending_confirm[command] = (arg, now + CONFIRM_WINDOW_MS)
            return (f"⚠️ /{command} is a protected command.\n"
                    f"Reply `/confirm {command}` within 60s to execute.")

        return await self._execute(command, arg)

    async def _execute(self, command: str, arg: str) -> str:
        action = self.actions.get(command)
        if action is None:
            return f"/{command} not wired in this build"
        try:
            if arg:
                return str(await action(arg))
            return str(await action())
        except TypeError:
            try:
                return str(await action())
            except Exception as exc:  # noqa: BLE001
                return f"error: {redact_text(repr(exc))[:120]}"
        except Exception as exc:  # noqa: BLE001
            log.error("control_action_failed", extra={"extra": {
                "command": command, "error": redact_text(repr(exc))[:150]}})
            return f"error executing /{command}: {redact_text(repr(exc))[:120]}"

    def _record(self, chat_id: str, command: str, arg: str, result: Optional[str]) -> None:
        if self.on_record is not None:
            try:
                self.on_record({"ts_ms": self.clock.now_ms(), "chat_id": chat_id,
                                "command": command, "arg": arg[:100],
                                "result": (result or "")[:200],
                                "confirmed": int(command == "confirm")})
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------
    # Long-poll loop
    # ------------------------------------------------------------------
    async def _flush_backlog(self, session: aiohttp.ClientSession) -> None:
        """Skip everything sent before this boot: restarts never replay history."""
        try:
            url = f"https://api.telegram.org/bot{self._token}/getUpdates"
            async with session.get(url, params={"offset": -1, "timeout": 0,
                                                "limit": 1}) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
            result = data.get("result", [])
            if result:
                last_id = int(result[-1].get("update_id", 0))
                self._offset = last_id + 1
                log.info("telegram_backlog_flushed", extra={"extra": {
                    "resume_offset": self._offset}})
        except Exception as exc:  # noqa: BLE001
            log.warning("backlog_flush_failed", extra={"extra": {
                "error": redact_text(repr(exc))[:100]}})

    async def run(self) -> None:
        if not self._token or not self._chat_id:
            log.info("controls_disabled_no_credentials")
            return
        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=40)) as session:
            await self._flush_backlog(session)
            while not self._stop.is_set():
                try:
                    url = f"https://api.telegram.org/bot{self._token}/getUpdates"
                    params = {"timeout": 25, "offset": self._offset}
                    async with session.get(url, params=params) as resp:
                        if resp.status == 409:
                            log.warning("telegram_conflict_another_poller_active")
                            await asyncio.sleep(10)
                            continue
                        if resp.status != 200:
                            await asyncio.sleep(5)
                            continue
                        data = await resp.json()
                    for update in data.get("result", []):
                        reply = await self.process_update(update)
                        if reply:
                            send_url = f"https://api.telegram.org/bot{self._token}/sendMessage"
                            async with session.post(send_url, json={
                                    "chat_id": self._chat_id, "text": reply[:4000]}) as _r:
                                pass
                except asyncio.CancelledError:
                    break
                except Exception as exc:  # noqa: BLE001
                    log.warning("controls_poll_error", extra={"extra": {
                        "error": redact_text(repr(exc))[:120]}})
                    await asyncio.sleep(5)

    def stop(self) -> None:
        self._stop.set()
