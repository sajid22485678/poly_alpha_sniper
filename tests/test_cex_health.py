"""CEX health verdicts: degraded Binance never kills shadow; live stays strict."""
import aiohttp
import pytest

from poly_alpha_sniper.core.startup_preflight import PreflightResult, build_cex_health
from poly_alpha_sniper.tests.helpers import cfg

B403 = (False, "status=403")
OK = (True, "ok")
DOWN = (False, "ClientConnectorError")


def _health(probe, mode="shadow_live", is_live=False, c=None):
    return build_cex_health(probe, mode, is_live, c or cfg())


def test_binance_403_bybit_ok_okx_ok_shadow_pass():
    h = _health({"binance": B403, "bybit": OK, "okx": OK})
    assert h["overall"] == "PASS"
    assert h["status"] == "HEALTHY_WITH_DEGRADED_BINANCE"
    assert h["reachable_count"] == 2
    assert h["sources"]["binance"] == {"ok": False, "state": "DEGRADED",
                                       "detail": "status=403", "status": 403}
    assert h["sources"]["bybit"]["state"] == "HEALTHY"
    assert "CEX_DEGRADED_BINANCE_403" in h["warnings"]


def test_binance_403_only_bybit_ok_shadow_pass_reduced():
    h = _health({"binance": B403, "bybit": OK, "okx": DOWN})
    assert h["overall"] == "PASS"
    assert h["status"] == "REDUCED_REDUNDANCY"
    assert "CEX_REDUCED_REDUNDANCY_SINGLE_SOURCE" in h["warnings"]
    assert "CEX_DEGRADED_BINANCE_403" in h["warnings"]


def test_binance_403_only_okx_ok_shadow_pass_reduced():
    h = _health({"binance": B403, "bybit": DOWN, "okx": OK})
    assert h["overall"] == "PASS"
    assert h["status"] == "REDUCED_REDUNDANCY"
    assert "CEX_REDUCED_REDUNDANCY_SINGLE_SOURCE" in h["warnings"]


def test_all_cex_down_shadow_fails():
    h = _health({"binance": B403, "bybit": DOWN, "okx": DOWN})
    assert h["overall"] == "FAIL"
    assert h["status"] == "ALL_DOWN"
    assert h["reachable_count"] == 0


def test_live_single_fallback_fails():
    h = _health({"binance": B403, "bybit": OK, "okx": DOWN},
                mode="live_micro", is_live=True)
    assert h["overall"] == "FAIL"
    assert h["status"] == "INSUFFICIENT_REDUNDANCY_FOR_LIVE"
    assert h["required_count"] == 2
    assert "CEX_INSUFFICIENT_REDUNDANCY_FOR_LIVE" in h["warnings"]


def test_live_two_sources_pass_with_redundancy_config():
    c = cfg()
    assert c.cex.require_multi_exchange_confirmation_live is True
    h = _health({"binance": B403, "bybit": OK, "okx": OK},
                mode="live_micro", is_live=True, c=c)
    assert h["overall"] == "PASS"
    assert h["status"] == "HEALTHY_WITH_DEGRADED_BINANCE"
    # and with redundancy requirement disabled, one source suffices for live
    c2 = cfg()
    c2.cex.require_multi_exchange_confirmation_live = False
    h2 = _health({"binance": B403, "bybit": OK, "okx": DOWN},
                 mode="live_micro", is_live=True, c=c2)
    assert h2["overall"] == "PASS"


def test_all_healthy():
    h = _health({"binance": OK, "bybit": OK, "okx": OK})
    assert h["overall"] == "PASS"
    assert h["status"] == "HEALTHY"
    assert h["warnings"] == []


def test_live_gates_not_weakened_all_down():
    h = _health({"binance": DOWN, "bybit": DOWN, "okx": DOWN},
                mode="live_micro", is_live=True)
    assert h["overall"] == "FAIL"
    assert h["status"] == "ALL_DOWN"


async def test_failed_startup_closes_sessions_and_lock(monkeypatch, tmp_path):
    """Failed preflight must close aiohttp sessions and release the lock."""
    from poly_alpha_sniper.core import process_lock as pl
    from poly_alpha_sniper.core import startup_preflight
    from poly_alpha_sniper.core.app import App
    from poly_alpha_sniper.core.config_loader import Secrets, load_config

    # hermetic lock: never touch runtime/live.lock (a real bot may be running)
    lock_file = tmp_path / "test.lock"
    orig_init = pl.ProcessLock.__init__
    monkeypatch.setattr(pl.ProcessLock, "__init__",
                        lambda self, path=None: orig_init(self, str(lock_file)))

    c = load_config()  # shadow_live / dry_run
    c.telegram.enabled = False
    c.dashboard.auth_enabled = False
    app = App(c, Secrets(env={"DASHBOARD_AUTH_ENABLED": "false"}))
    app.build()

    # give the telegram client a real (open) session to prove it gets closed
    session = aiohttp.ClientSession()
    app.telegram._session = session

    async def fake_preflight(cfg, secrets, offline_ok=False, cex_prober=None):
        return PreflightResult(ok=False, checks=[("cex_reachable", False, "ALL_DOWN")])

    monkeypatch.setattr(startup_preflight, "run_preflight", fake_preflight)
    with pytest.raises(SystemExit):
        await app.startup(offline_ok=True)
    assert session.closed, "aiohttp session leaked after failed preflight"
    # lock released: a fresh acquire must succeed
    lock = pl.ProcessLock()
    lock.acquire("shadow_live")
    lock.release()
