from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import Secrets
from poly_alpha_sniper.reporting.telegram_controls import (
    ALLOWED_COMMANDS, DANGEROUS_COMMANDS, TelegramControls)
from poly_alpha_sniper.tests.helpers import NOW_MS, cfg

CHAT = "12345"


def _controls(actions=None, clock=None, records=None):
    secrets = Secrets(env={"TELEGRAM_BOT_TOKEN": "t" * 40, "TELEGRAM_CHAT_ID": CHAT})
    return TelegramControls(secrets, cfg(), clock or SimClock(NOW_MS),
                            actions or {},
                            on_record=(records.append if records is not None else None))


def _update(text, chat=CHAT):
    return {"message": {"chat": {"id": chat}, "text": text}}


async def test_status_calls_injected_action():
    called = []

    async def status():
        called.append(1)
        return "all good"

    tc = _controls({"status": status})
    reply = await tc.handle_update(_update("/status"))
    assert reply == "all good"
    assert called


async def test_wrong_chat_silently_ignored():
    tc = _controls({"status": _mk("x")})
    reply = await tc.handle_update(_update("/status", chat="999"))
    assert reply is None
    assert tc.ignored_chats == 1


def _mk(result):
    async def action(*a):
        return result
    return action


async def test_dangerous_command_requires_confirmation():
    executed = []

    async def close_all():
        executed.append(1)
        return "closed"

    tc = _controls({"close_all": close_all})
    reply = await tc.handle_update(_update("/close_all"))
    assert "confirm" in reply.lower()
    assert not executed
    reply2 = await tc.handle_update(_update("/confirm close_all"))
    assert reply2 == "closed"
    assert executed


async def test_unconfirmed_expires():
    clock = SimClock(NOW_MS)

    async def resume():
        return "resumed"

    tc = _controls({"resume": resume}, clock=clock)
    await tc.handle_update(_update("/resume"))
    clock.advance_ms(61_000)
    reply = await tc.handle_update(_update("/confirm resume"))
    assert "expired" in reply


async def test_confirm_without_pending():
    tc = _controls({})
    reply = await tc.handle_update(_update("/confirm close_all"))
    assert "nothing pending" in reply


async def test_unknown_command_help():
    tc = _controls({})
    reply = await tc.handle_update(_update("/frobnicate"))
    assert "Unknown command" in reply
    assert "/status" in reply


async def test_no_manual_trade_approval_exists():
    assert "approve_trade" not in ALLOWED_COMMANDS
    assert "reject_trade" not in ALLOWED_COMMANDS
    tc = _controls({})
    reply = await tc.handle_update(_update("/approve_trade 123"))
    assert "autonomous" in reply.lower()


async def test_all_master_commands_allowed():
    for command in ("status", "health", "daily", "positions", "open_orders", "pnl",
                    "mode", "pause", "resume", "panic", "clear_panic", "close_all",
                    "cancel_orders", "mode_shadow", "mode_live_micro", "disable_live",
                    "blacklist_market", "whitelist_market", "disable_asset",
                    "enable_asset", "backup_now", "latency", "budget"):
        assert command in ALLOWED_COMMANDS
    assert DANGEROUS_COMMANDS == {"resume", "clear_panic", "close_all", "mode_live_micro"}


async def test_command_with_argument():
    async def blacklist_market(arg=""):
        return f"blacklisted {arg}"

    tc = _controls({"blacklist_market": blacklist_market})
    reply = await tc.handle_update(_update("/blacklist_market m-123"))
    assert reply == "blacklisted m-123"


async def test_commands_recorded():
    records = []
    tc = _controls({"status": _mk("ok")}, records=records)
    await tc.handle_update(_update("/status"))
    assert records and records[0]["command"] == "status"


# ---------------------------------------------------------------------------
# /start, /help, backlog/dedupe/throttle behavior
# ---------------------------------------------------------------------------

def _upd(update_id, text, chat=CHAT):
    return {"update_id": update_id, "message": {"chat": {"id": chat}, "text": text}}


async def test_start_returns_help_with_status():
    tc = _controls({"status": _mk("mode=shadow_live dry_run=True")})
    reply = await tc.process_update(_upd(1, "/start"))
    assert "/status" in reply
    assert "Protected" in reply
    assert "mode=shadow_live" in reply
    assert "Unknown" not in reply


async def test_help_returns_command_list():
    tc = _controls({})
    reply = await tc.process_update(_upd(1, "/help"))
    assert "/status" in reply and "/health" in reply
    assert "no manual trade approval" in reply.lower()


async def test_plain_text_ignored_silently_offset_advances():
    tc = _controls({"status": _mk("ok")})
    for i, text in enumerate(("test", "hello", "random words"), start=10):
        reply = await tc.process_update(_upd(i, text))
        assert reply is None
    assert tc._offset == 13  # every ignored update still advanced the offset


async def test_unknown_command_replies_once_then_throttled():
    from poly_alpha_sniper.core.clock import SimClock
    from poly_alpha_sniper.tests.helpers import NOW_MS
    clock = SimClock(NOW_MS)
    tc = _controls({}, clock=clock)
    first = await tc.process_update(_upd(1, "/abc"))
    assert first is not None and "Unknown command" in first
    second = await tc.process_update(_upd(2, "/abc"))
    assert second is None  # throttled — no spam
    assert tc._offset == 3  # offset still advanced
    clock.advance_ms(31_000)
    third = await tc.process_update(_upd(3, "/xyz"))
    assert third is not None and "Unknown command" in third


async def test_duplicate_update_id_processed_once():
    calls = []

    async def status():
        calls.append(1)
        return "ok"

    tc = _controls({"status": status})
    first = await tc.process_update(_upd(42, "/status"))
    assert first == "ok"
    dup = await tc.process_update(_upd(42, "/status"))
    assert dup is None
    assert len(calls) == 1
    assert tc._offset == 43


async def test_unauthorized_chat_offset_still_advances():
    tc = _controls({"status": _mk("ok")})
    reply = await tc.process_update(_upd(7, "/status", chat="999"))
    assert reply is None
    assert tc.ignored_chats == 1
    assert tc._offset == 8


async def test_offset_advances_after_failed_command():
    async def boom():
        raise RuntimeError("kaput")

    tc = _controls({"backup_now": boom})
    reply = await tc.process_update(_upd(5, "/backup_now"))
    assert "error" in reply.lower()
    assert tc._offset == 6


async def test_approve_trade_still_refused_via_process_update():
    tc = _controls({})
    reply = await tc.process_update(_upd(9, "/approve_trade 1"))
    assert "autonomous" in reply.lower()


async def test_dangerous_still_needs_confirm_via_process_update():
    executed = []

    async def close_all():
        executed.append(1)
        return "closed"

    tc = _controls({"close_all": close_all})
    r1 = await tc.process_update(_upd(11, "/close_all"))
    assert "confirm" in r1.lower() and not executed
    r2 = await tc.process_update(_upd(12, "/confirm close_all"))
    assert r2 == "closed" and executed
