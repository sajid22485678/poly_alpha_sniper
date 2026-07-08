from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import RequestPriority
from poly_alpha_sniper.core.rate_limit_governor import RateLimitGovernor
from poly_alpha_sniper.tests.helpers import NOW_MS


def _gov(clock=None):
    return RateLimitGovernor(clock or SimClock(NOW_MS))


async def test_emergency_never_starved():
    g = _gov()
    # drain the clob_order bucket completely
    for _ in range(50):
        assert await g.acquire("clob_order", RequestPriority.EMERGENCY_EXIT)
    # still grants (bucket deeply negative)
    assert await g.acquire("clob_order", RequestPriority.EMERGENCY_EXIT)
    assert await g.acquire("clob_order", RequestPriority.CANCEL)


async def test_discovery_denied_when_empty():
    g = _gov()
    for _ in range(50):
        await g.acquire("gamma", RequestPriority.EMERGENCY_EXIT)  # drain
    assert not await g.acquire("gamma", RequestPriority.DISCOVERY)
    assert not await g.acquire("gamma", RequestPriority.ANALYTICS)


async def test_new_entry_waits_then_fails():
    clock = SimClock(NOW_MS)
    g = _gov(clock)
    for _ in range(50):
        await g.acquire("clob_order", RequestPriority.EMERGENCY_EXIT)
    ok = await g.acquire("clob_order", RequestPriority.NEW_ENTRY)
    # bucket at -N tokens; refill during 500ms wait insufficient -> denied
    assert not ok


async def test_429_penalty_and_recovery():
    clock = SimClock(NOW_MS)
    g = _gov(clock)
    g.report_429("gamma")
    assert not g.healthy
    clock.advance_ms(61_000)
    assert g.healthy


async def test_repeat_429_extends_penalty():
    clock = SimClock(NOW_MS)
    g = _gov(clock)
    g.report_429("gamma")
    clock.advance_ms(1000)
    g.report_429("gamma")
    clock.advance_ms(61_000)
    assert not g.healthy  # extended beyond 60s


async def test_usage_reporting():
    g = _gov()
    await g.acquire("telegram", RequestPriority.ANALYTICS)
    usage = g.usage()
    assert "telegram" in usage
    assert usage["telegram"]["used"] == 1
