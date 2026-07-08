from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.core.watchdog import Watchdog


class FakeWatchdog(Watchdog):
    """Watchdog with spawn/kill/heartbeat seams faked for tests."""

    def __init__(self, cfg, clock, alive=False, stale=False):
        super().__init__(cfg, "shadow_live", clock=clock)
        self._alive = alive
        self._stale = stale
        self.spawn_count = 0
        self.kill_count = 0

    def spawn(self):
        self.spawn_count += 1

    def child_alive(self):
        return self._alive

    def kill_child(self):
        self.kill_count += 1

    def heartbeat_stale(self):
        return self._stale


def test_dead_child_decides_restart():
    clock = SimClock(1_000_000)
    wd = FakeWatchdog(load_config(), clock, alive=False)
    status = wd.check_once()
    assert status["decision"] == "restart"


def test_restart_budget_exhausts():
    clock = SimClock(1_000_000)
    cfg = load_config()
    wd = FakeWatchdog(cfg, clock, alive=False)
    for _ in range(cfg.runtime.max_restarts_per_hour):
        assert wd.can_restart()
        wd.record_restart()
        clock.advance_ms(1000)
    assert not wd.can_restart()
    assert wd.check_once()["decision"] == "give_up"


def test_budget_window_slides():
    clock = SimClock(1_000_000)
    cfg = load_config()
    wd = FakeWatchdog(cfg, clock, alive=False)
    for _ in range(cfg.runtime.max_restarts_per_hour):
        wd.record_restart()
    clock.advance_ms(3_600_001)
    assert wd.can_restart()


def test_stale_heartbeat_decides_kill_and_restart():
    clock = SimClock(1_000_000)
    wd = FakeWatchdog(load_config(), clock, alive=True, stale=True)
    assert wd.check_once()["decision"] == "kill_and_restart"


def test_healthy_child_ok():
    clock = SimClock(1_000_000)
    wd = FakeWatchdog(load_config(), clock, alive=True, stale=False)
    assert wd.check_once()["decision"] == "ok"
