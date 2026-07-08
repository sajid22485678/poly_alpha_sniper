from poly_alpha_sniper.core.config_loader import Secrets, load_config
from poly_alpha_sniper.core.contracts import OrderRecord
from poly_alpha_sniper.core.live_readiness import check_live_readiness


class FakeClient:
    def __init__(self, balance=10.0, fail_balance=False):
        self._balance = balance
        self._fail = fail_balance

    async def place_order(self, req):
        return OrderRecord(order_id="x")

    async def cancel_order(self, order_id):
        return True

    async def cancel_all(self):
        return 0

    async def get_open_orders(self):
        return []

    async def get_balance_usd(self):
        if self._fail:
            raise RuntimeError("api down")
        return self._balance

    async def get_positions(self):
        return []


class Flag:
    def __init__(self, active=False):
        self.is_active = active


def _live_cfg():
    cfg = load_config()
    cfg.mode.trading_mode = "live_micro"
    cfg.mode.dry_run = False
    return cfg


def _live_secrets():
    return Secrets(env={
        "LIVE_TRADING_ENABLED": "true", "I_UNDERSTAND_REAL_MONEY_RISK": "true",
        "MAX_REAL_TRADE_USD": "1"})


async def test_all_good_passes():
    ok, failed = await check_live_readiness(
        _live_cfg(), _live_secrets(), FakeClient(), Flag(), Flag(),
        telegram_ok=True, reconciled=True)
    assert ok, failed


async def test_default_secrets_block_live():
    ok, failed = await check_live_readiness(
        _live_cfg(), Secrets(env={}), FakeClient(), Flag(), Flag(),
        telegram_ok=True, reconciled=True)
    assert not ok
    assert any("LIVE_TRADING_ENABLED" in f for f in failed)


async def test_each_gate_blocks():
    cfg, secrets = _live_cfg(), _live_secrets()
    ok, failed = await check_live_readiness(cfg, secrets, None, Flag(), Flag(), True, True)
    assert not ok and any("no trading client" in f for f in failed)

    ok, failed = await check_live_readiness(cfg, secrets, FakeClient(), Flag(True), Flag(), True, True)
    assert not ok and any("panic" in f for f in failed)

    ok, failed = await check_live_readiness(cfg, secrets, FakeClient(), Flag(), Flag(True), True, True)
    assert not ok and any("kill switch" in f for f in failed)

    ok, failed = await check_live_readiness(cfg, secrets, FakeClient(), Flag(), Flag(), True, False)
    assert not ok and any("reconciled" in f for f in failed)

    ok, failed = await check_live_readiness(cfg, secrets, FakeClient(), Flag(), Flag(), False, True)
    assert not ok and any("Telegram" in f for f in failed)

    ok, failed = await check_live_readiness(cfg, secrets, FakeClient(fail_balance=True),
                                            Flag(), Flag(), True, True)
    assert not ok and any("balance fetch failed" in f for f in failed)

    cfg2 = _live_cfg()
    cfg2.mode.dry_run = True
    ok, failed = await check_live_readiness(cfg2, secrets, FakeClient(), Flag(), Flag(), True, True)
    assert not ok and any("dry_run" in f for f in failed)


async def test_low_balance_blocks():
    ok, failed = await check_live_readiness(
        _live_cfg(), _live_secrets(), FakeClient(balance=0.2), Flag(), Flag(), True, True)
    assert not ok
    assert any("balance" in f for f in failed)
