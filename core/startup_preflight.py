"""Startup preflight — every check from the master spec.

Hard checks must pass before the app runs. Network checks become informational
when offline_ok=True (used by tests and the backtest path).
"""
from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from poly_alpha_sniper.core.config_loader import PROJECT_ROOT
from poly_alpha_sniper.core.config_validator import validate_config, validate_live_env
from poly_alpha_sniper.core.contracts import TradingMode
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("preflight")


@dataclass
class PreflightResult:
    ok: bool
    checks: list[tuple[str, bool, str]] = field(default_factory=list)
    cex_health: dict = field(default_factory=dict)

    def summary_text(self) -> str:
        lines = ["PREFLIGHT " + ("PASS" if self.ok else "FAIL")]
        for name, ok, detail in self.checks:
            lines.append(f"{'OK ' if ok else 'FAIL'} {name}" + (f" — {detail}" if detail else ""))
        return "\n".join(lines)


# CEX REST reachability probes. Bybit gets the bytick.com mirror fallback
# (bybit.com is geo-blocked on some networks); Binance 403 = geo-block.
CEX_PROBE_ENDPOINTS: dict[str, list[str]] = {
    "binance": ["https://api.binance.com/api/v3/ping"],
    "bybit": ["https://api.bybit.com/v5/market/time",
              "https://api.bytick.com/v5/market/time"],
    "okx": ["https://www.okx.com/api/v5/public/time"],
}


async def probe_cex_endpoints(session, cfg) -> dict[str, tuple[bool, str]]:
    """Probe each configured exchange; (reachable, detail) per exchange."""
    results: dict[str, tuple[bool, str]] = {}
    for name, urls in CEX_PROBE_ENDPOINTS.items():
        ok, detail = False, "not probed"
        for url in urls:
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        ok = True
                        detail = "ok" + (" (mirror)" if "bytick" in url else "")
                        break
                    detail = f"status={resp.status}"
            except Exception as exc:  # noqa: BLE001
                detail = type(exc).__name__
        results[name] = (ok, detail)
    return results


def build_cex_health(probe: dict[str, tuple[bool, str]], mode_value: str,
                     is_live: bool, cfg) -> dict:
    """Turn raw probe results into the mode-aware CEX health verdict.

    A single degraded exchange (e.g. Binance geo-blocked with 403) NEVER
    fails shadow mode while another source is reachable. Live mode requires
    the configured redundancy (2 sources when
    cex.require_multi_exchange_confirmation_live, else 1).
    """
    sources: dict[str, dict] = {}
    warnings: list[str] = []
    reachable = 0
    for name, (ok, detail) in probe.items():
        entry: dict = {"ok": ok, "state": "HEALTHY" if ok else "DEGRADED",
                       "detail": detail}
        if not ok and detail.startswith("status="):
            try:
                entry["status"] = int(detail.split("=", 1)[1].split()[0])
            except ValueError:
                pass
        sources[name] = entry
        if ok:
            reachable += 1
        else:
            code = entry.get("status")
            warnings.append(f"CEX_DEGRADED_{name.upper()}"
                            + (f"_{code}" if code else ""))

    required = 2 if (is_live and cfg.cex.require_multi_exchange_confirmation_live) else 1
    overall = "PASS" if reachable >= required else "FAIL"

    if overall == "FAIL":
        status = "ALL_DOWN" if reachable == 0 else "INSUFFICIENT_REDUNDANCY_FOR_LIVE"
        if reachable > 0:
            warnings.append("CEX_INSUFFICIENT_REDUNDANCY_FOR_LIVE")
    elif reachable == 1:
        status = "REDUCED_REDUNDANCY"
        warnings.append("CEX_REDUCED_REDUNDANCY_SINGLE_SOURCE")
    elif not sources.get("binance", {}).get("ok", True):
        status = "HEALTHY_WITH_DEGRADED_BINANCE"
    elif any(not s["ok"] for s in sources.values()):
        status = "HEALTHY_WITH_DEGRADED_SOURCE"
    else:
        status = "HEALTHY"

    return {"overall": overall, "status": status, "mode": mode_value,
            "reachable_count": reachable, "required_count": required,
            "sources": sources, "warnings": warnings}


def _writable_dir(path: Path) -> tuple[bool, str]:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return True, ""
    except OSError as exc:
        return False, repr(exc)


async def run_preflight(cfg, secrets, offline_ok: bool = False,
                        cex_prober=None) -> PreflightResult:
    """cex_prober: optional async callable(session) -> probe dict (tests)."""
    checks: list[tuple[str, bool, str]] = []
    cex_health: dict = {}
    hard_fail = False

    def add(name: str, ok: bool, detail: str = "", hard: bool = True) -> None:
        nonlocal hard_fail
        checks.append((name, ok, detail))
        if hard and not ok:
            hard_fail = True

    # config + profile + mode
    errors = validate_config(cfg)
    add("config_valid", not errors, "; ".join(errors[:5]))
    add("profile_valid", cfg.mode.profile in cfg.profiles or not cfg.mode.profile,
        f"profile={cfg.mode.profile}")
    try:
        mode = TradingMode(cfg.mode.trading_mode)
        add("mode_valid", True, mode.value)
    except ValueError:
        mode = None
        add("mode_valid", False, cfg.mode.trading_mode)

    # .env presence (warn-only for non-live)
    env_exists = (PROJECT_ROOT / ".env").exists()
    is_live = bool(mode and mode.is_live)
    add(".env_present", env_exists or not is_live,
        "" if env_exists else ".env missing (required for live, optional for shadow)",
        hard=is_live)

    # writable folders
    for name, p in (("logs_writable", PROJECT_ROOT / "logs"),
                    ("backup_writable", PROJECT_ROOT / "backups"),
                    ("runtime_writable", PROJECT_ROOT / "runtime")):
        ok, detail = _writable_dir(p)
        add(name, ok, detail)

    # database writable
    try:
        from poly_alpha_sniper.storage.sqlite_store import SqliteStore
        from poly_alpha_sniper.storage.migrations import run_migrations
        probe_path = Path(tempfile.gettempdir()) / f"pas_preflight_{os.getpid()}.db"
        s = SqliteStore(str(probe_path))
        run_migrations(s)
        s.insert("health_logs", {"ts_ms": 0, "report": "preflight_probe"})
        s.close()
        probe_path.unlink(missing_ok=True)
        add("database_writable", True)
    except Exception as exc:  # noqa: BLE001
        add("database_writable", False, repr(exc))

    # telegram config
    if cfg.telegram.enabled:
        tg_ok = secrets.has("TELEGRAM_BOT_TOKEN") and secrets.has("TELEGRAM_CHAT_ID")
        add("telegram_configured", tg_ok,
            "" if tg_ok else "TELEGRAM_BOT_TOKEN/CHAT_ID missing", hard=is_live)
    else:
        add("telegram_configured", True, "disabled", hard=False)

    # dashboard auth
    if cfg.dashboard.auth_enabled and secrets.dashboard_auth_enabled:
        auth_ok = secrets.has("DASHBOARD_USERNAME") and secrets.has("DASHBOARD_PASSWORD")
        add("dashboard_auth", auth_ok,
            "" if auth_ok else "auth enabled but DASHBOARD_USERNAME/PASSWORD unset")
    else:
        add("dashboard_auth", True, "auth disabled", hard=False)

    # duplicate process probe (non-acquiring)
    try:
        from poly_alpha_sniper.core.process_lock import ProcessLock, _pid_alive
        lock = ProcessLock()
        existing = lock._read()
        dup = bool(existing and int(existing.get("pid", -1)) != os.getpid()
                   and _pid_alive(int(existing.get("pid", -1))))
        add("no_duplicate_process", not dup,
            f"pid={existing.get('pid')}" if dup else "")
    except Exception as exc:  # noqa: BLE001
        add("no_duplicate_process", True, f"probe error {exc!r}", hard=False)

    # live gates (env/config level)
    if is_live:
        live_errors = validate_live_env(cfg, secrets)
        add("live_env_gates", not live_errors, "; ".join(live_errors[:6]))

    # network checks
    if offline_ok:
        add("polymarket_reachable", True, "SKIPPED (offline_ok)", hard=False)
        add("cex_reachable", True, "SKIPPED (offline_ok)", hard=False)
        add("time_sync", True, "SKIPPED (offline_ok)", hard=False)
        cex_health = {"overall": "SKIPPED", "status": "SKIPPED",
                      "mode": cfg.mode.trading_mode, "sources": {}, "warnings": []}
    else:
        import aiohttp
        import time as _time
        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5)) as session:
            try:
                async with session.get(cfg.polymarket.clob_base_url + "/ok") as r:
                    add("polymarket_reachable", r.status < 500, f"status={r.status}")
                    server_date = r.headers.get("Date", "")
                    if server_date:
                        from email.utils import parsedate_to_datetime
                        try:
                            drift = abs(parsedate_to_datetime(server_date).timestamp() - _time.time())
                            add("time_sync", drift < 10, f"drift={drift:.1f}s", hard=False)
                        except (TypeError, ValueError):
                            add("time_sync", True, "unparseable server date", hard=False)
            except Exception as exc:  # noqa: BLE001
                add("polymarket_reachable", False, repr(exc))

            # CEX: degraded sources never fail shadow while one source works.
            try:
                if cex_prober is not None:
                    probe = await cex_prober(session)
                else:
                    probe = await probe_cex_endpoints(session, cfg)
            except Exception as exc:  # noqa: BLE001
                probe = {name: (False, type(exc).__name__)
                         for name in CEX_PROBE_ENDPOINTS}
            cex_health = build_cex_health(probe, cfg.mode.trading_mode, is_live, cfg)
            for warning in cex_health["warnings"]:
                log.warning(warning, extra={"extra": {
                    "sources": {n: s["detail"] for n, s in cex_health["sources"].items()},
                    "mode": cex_health["mode"]}})
            detail = (f"{cex_health['status']} ({cex_health['reachable_count']}/"
                      f"{cex_health['required_count']} required): "
                      + ", ".join(f"{n}={s['detail']}"
                                  for n, s in cex_health["sources"].items()))
            add("cex_reachable", cex_health["overall"] == "PASS", detail)

    result = PreflightResult(ok=not hard_fail, checks=checks, cex_health=cex_health)
    log.info("preflight_done", extra={"extra": {
        "ok": result.ok, "failed": [c[0] for c in checks if not c[1]]}})
    return result
