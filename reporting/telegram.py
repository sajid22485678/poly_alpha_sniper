"""Telegram client + message formatters (master templates).

- Token/chat come ONLY from Secrets; the token never appears in logs (URL is
  redacted on error paths).
- 1 msg/s throttle; cleanly disabled when unconfigured.
- Formatters are pure functions returning the exact master-spec layouts.
"""
from __future__ import annotations

import html
from typing import Optional

import aiohttp

from poly_alpha_sniper.core.logger import get_logger, redact_text

log = get_logger("telegram")


class TelegramClient:
    def __init__(self, secrets, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self._token = secrets.get("TELEGRAM_BOT_TOKEN")
        self._chat_id = secrets.get("TELEGRAM_CHAT_ID")
        self._enabled = bool(cfg.telegram.enabled and self._token and self._chat_id)
        self._last_send_ms = 0
        self._warned_disabled = False
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self._session

    async def send(self, text: str, critical: bool = False) -> bool:
        if not self._enabled:
            if not self._warned_disabled:
                log.info("telegram_disabled_or_unconfigured")
                self._warned_disabled = True
            return False
        # 1 msg/s throttle (critical messages skip the wait but still update it)
        now = self.clock.now_ms()
        if not critical and now - self._last_send_ms < 1000:
            await self.clock.sleep((1000 - (now - self._last_send_ms)) / 1000.0)
        self._last_send_ms = self.clock.now_ms()
        try:
            sess = await self._sess()
            url = f"https://api.telegram.org/bot{self._token}/sendMessage"
            payload = {"chat_id": self._chat_id, "text": text[:4000],
                       "disable_web_page_preview": True}
            async with sess.post(url, json=payload) as resp:
                ok = resp.status == 200
                if not ok:
                    body = (await resp.text())[:120]
                    log.warning("telegram_send_failed", extra={"extra": {
                        "status": resp.status, "body": redact_text(body)}})
                return ok
        except Exception as exc:  # noqa: BLE001
            log.warning("telegram_error", extra={"extra": {
                "error": redact_text(repr(exc))[:150]}})
            return False

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ---------------------------------------------------------------------------
# Formatters (pure; escape user-ish text; NEVER include secrets)
# ---------------------------------------------------------------------------

def _e(text) -> str:
    return html.escape(str(text))[:150]


def format_auto_entry(signal, gate, req, aggression_mode: str) -> str:
    return (
        "🚀 AUTO ENTRY\n"
        f"Tier: {gate.tier.value}\n"
        f"Aggression Mode: {aggression_mode}\n"
        f"Asset: {signal.asset}\n"
        f"Market: {_e(signal.market.title)}\n"
        f"Side: {req.side.value}\n"
        f"Entry: {req.price:.3f}\n"
        f"Size: ${req.size_usd:.2f} ({req.size_shares:.2f} shares)\n"
        f"Edge: {signal.edge.edge_after_slippage:.3f}\n"
        f"Confidence: {signal.fair.confidence:.0f}\n"
        f"Trade Quality: {signal.trade_quality:.0f}\n"
        f"Market Quality: {signal.market_quality.score:.0f}\n"
        f"Alpha Score: {signal.alpha_score:.0f}\n"
        f"Reason: {_e(gate.reason)}\n"
        f"Exit Plan: {_e(signal.exit_plan)}")


def format_auto_exit(position, decision, exit_price: float, pnl: float,
                     hold_s: float, remaining_shares: float = 0.0) -> str:
    cost = position.avg_entry_price * (position.shares or 1)
    roi = (pnl / cost * 100.0) if cost > 0 else 0.0
    return (
        "✅ AUTO EXIT\n"
        f"Market: {_e(position.market_id)}\n"
        f"Side: SELL_{position.outcome.value}\n"
        f"Exit Price: {exit_price:.3f}\n"
        f"PnL: ${pnl:+.3f}\n"
        f"ROI: {roi:+.1f}%\n"
        f"Hold Time: {hold_s:.0f}s\n"
        f"Exit Reason: {decision.reason.value if decision.reason else ''}\n"
        f"Remaining Position: {remaining_shares:.2f} shares")


def format_signal_5min(signal, aggression_mode: str, force_exit_s: float) -> str:
    return (
        "⚡ 5-MIN POLY ALPHA\n"
        f"Asset: {signal.asset}\n"
        f"Market: {_e(signal.market.title)}\n"
        f"Time to Expiry: {signal.seconds_to_expiry:.0f}s\n"
        f"Direction: {signal.direction.value}\n"
        f"Entry: {signal.edge.market_price:.3f}\n"
        f"Fair Probability: {signal.edge.fair_probability:.3f}\n"
        f"Edge: {signal.edge.edge_after_slippage:.3f}\n"
        f"Confidence: {signal.fair.confidence:.0f}\n"
        f"Market Quality: {signal.market_quality.score:.0f}\n"
        f"Trade Quality: {signal.trade_quality:.0f}\n"
        f"Alpha Score: {signal.alpha_score:.0f}\n"
        f"Tier: {signal.tier.value}\n"
        f"Aggression Mode: {aggression_mode}\n"
        f"Exit Plan: {_e(signal.exit_plan)}\n"
        f"Max Hold: {signal.exit_plan.split('maxhold')[-1].strip() if 'maxhold' in signal.exit_plan else 'per tier'}\n"
        f"Force Exit Before: {force_exit_s:.0f}s pre-expiry")


def format_rejected(signal, gate, reason: str, sizing_detail: Optional[dict] = None) -> str:
    # A min-order-size rejection means the signal already cleared the alpha gate
    # (edge/confidence/quality all passed) -- the real blocker is that this
    # bankroll's max_trade_usd can't buy Polymarket's share minimum at the
    # current ask. Reporting "edge/confidence" there would be actively wrong.
    if sizing_detail:
        import math
        d = sizing_detail
        suggested_max = math.ceil(d.get("min_required_usd", 0.0) * 100) / 100
        return (
            "⚠️ REJECTED — min order size\n"
            f"Reason: {_e(reason)}\n"
            f"Tier: {gate.tier.value}\n"
            f"Market: {_e(signal.market.title)}\n"
            f"Asset: {signal.asset}\n"
            f"Edge: {signal.edge.edge_after_slippage:.3f}\n"
            f"Confidence: {signal.fair.confidence:.0f}\n"
            f"Min Required Shares: {d.get('min_shares', 0):.2f}\n"
            f"Ask Price: {d.get('ask_price', 0):.4f}\n"
            f"Min Required USD: ${d.get('min_required_usd', 0):.2f}\n"
            f"Configured max_trade_usd: ${d.get('configured_max_trade_usd', 0):.2f}\n"
            f"Proposed Size USD: ${d.get('proposed_usd', 0):.2f}\n"
            f"Available Cash: ${d.get('available_cash_usd', 0):.2f}\n"
            f"Shortfall USD: ${d.get('shortfall_usd', 0):.2f}\n"
            f"What Must Improve: Increase max_trade_usd to at least ${suggested_max:.2f} "
            f"or skip due to small-bankroll mode.")
    improve = ", ".join(gate.failed_checks or gate.soft_penalties) or "edge/confidence"
    return (
        "⚠️ REJECTED\n"
        f"Reason: {_e(reason or gate.reason)}\n"
        f"Failed Checks: {', '.join(gate.failed_checks) or '-'}\n"
        f"Tier: {gate.tier.value}\n"
        f"Market: {_e(signal.market.title)}\n"
        f"Asset: {signal.asset}\n"
        f"Edge: {signal.edge.edge_after_slippage:.3f}\n"
        f"Confidence: {signal.fair.confidence:.0f}\n"
        f"What Must Improve: {_e(improve)}")


def format_mode_change(old: str, new: str, reason: str, snap) -> str:
    dd = 0.0
    if snap.equity_ath_usd > 0:
        dd = max(0.0, (snap.equity_ath_usd - snap.equity_usd) / snap.equity_ath_usd * 100)
    return (
        "MODE CHANGE:\n"
        f"{old} -> {new}\n"
        f"Reason: {_e(reason)}\n"
        f"Allowed Tiers: {'A_PLUS' if new == 'DEFENSIVE' else 'A_PLUS, A' if new == 'NORMAL' else 'A_PLUS, A, B'}\n"
        f"Current Drawdown: {dd:.1f}%\n"
        f"Loss Streak: {snap.consecutive_losses}")


def format_panic(trigger: str) -> str:
    return ("🚨 PANIC MODE ACTIVATED\n"
            f"Trigger: {_e(trigger)}\n"
            "Actions: entries disabled, orders cancelled, emergency close attempted.\n"
            "Manual reset required: /clear_panic then /resume")


def format_preflight(result) -> str:
    return "🛫 " + result.summary_text()[:3500]


def format_health(health: dict) -> str:
    return "💓 HEALTH\n" + "\n".join(f"{k}: {_e(v)}" for k, v in list(health.items())[:25])


def format_daily(report: dict) -> str:
    return "📊 DAILY REPORT\n" + "\n".join(
        f"{k}: {_e(v)}" for k, v in list(report.items())[:30] if k != "text")


def format_watchdog_restart(detail: str, count: int) -> str:
    return f"🐶 WATCHDOG RESTART #{count}\n{_e(detail)}"


def format_bad_fill(order_id: str, score: float, slippage_bps: float) -> str:
    return (f"⚠️ BAD FILL\nOrder: {_e(order_id[:12])}\nFill Quality: {score:.0f}/100\n"
            f"Slippage: {slippage_bps:.0f} bps")


def format_reconciliation_mismatch(mismatches: list[str]) -> str:
    listed = "\n".join(f"- {_e(m)}" for m in mismatches[:8])
    return ("🚨 RECONCILIATION MISMATCH\n" + listed
            + "\nKill switch + panic activated. Manual reset required.")


def format_backup(path: str, ok: bool) -> str:
    name = str(path).replace("\\", "/").split("/")[-1]
    return f"💾 BACKUP {'OK' if ok else 'FAILED'}: {_e(name)}"
