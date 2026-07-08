import pytest

from poly_alpha_sniper.risk.kill_switch import KillSwitch
from poly_alpha_sniper.risk.panic_mode import PanicMode


def test_activate_records_triggers():
    p = PanicMode()
    p.activate("cex_stale")
    p.activate("balance_mismatch")
    assert p.is_active
    assert p.triggers == ["cex_stale", "balance_mismatch"]
    assert p.reason == "balance_mismatch"


def test_manual_clear_required():
    p = PanicMode()
    p.activate("x")
    with pytest.raises(PermissionError):
        p.clear(manual=False)
    p.clear(manual=True)
    assert not p.is_active
    assert p.triggers  # history preserved


async def test_callbacks_fire_on_first_activation():
    p = PanicMode()
    calls = []

    async def cb(trigger):
        calls.append(trigger)

    p.on_activate(cb)
    p.activate("api_errors")
    p.activate("second_trigger")  # no re-fire
    import asyncio
    await asyncio.sleep(0)
    assert calls == ["api_errors"]


async def test_pending_callbacks_drained_without_loop():
    p = PanicMode()
    calls = []

    async def cb(trigger):
        calls.append(trigger)

    p.on_activate(cb)
    # activate outside a running-loop context is simulated by direct append
    p.pending_callbacks.append((cb, "offline_trigger"))
    await p.drain_pending()
    assert calls == ["offline_trigger"]


def test_kill_switch_manual_clear():
    k = KillSwitch()
    k.activate("mismatch")
    assert k.is_active
    with pytest.raises(PermissionError):
        k.clear(manual=False)
    k.clear(manual=True)
    assert not k.is_active
    assert k.history == ["mismatch"]
