"""poly_alpha_sniper entrypoint.

    python main.py --mode shadow_live
    python main.py --mode simulation
    python main.py --profile live_micro_safe
    python main.py --mode live_micro        (blocked unless ALL live gates pass)

Default mode is shadow_live with dry_run=true — no real orders are possible.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# make poly_alpha_sniper importable when run as `python main.py`
_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)


def main() -> None:
    parser = argparse.ArgumentParser(description="poly_alpha_sniper — Polymarket 5-min lag-arb bot")
    parser.add_argument("--mode", default="", choices=["", "simulation", "shadow_live",
                                                       "live_micro", "live_full"])
    parser.add_argument("--profile", default="")
    parser.add_argument("--config", default="")
    parser.add_argument("--offline-ok", action="store_true",
                        help="skip network preflight checks (dev only, non-live)")
    args = parser.parse_args()

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    from poly_alpha_sniper.core.config_loader import load_config, load_secrets
    from poly_alpha_sniper.core.app import App

    cfg = load_config(path=args.config or None,
                      mode_override=args.mode or None,
                      profile_override=args.profile or None)
    secrets = load_secrets()

    if args.offline_ok and cfg.trading_mode.is_live:
        raise SystemExit("--offline-ok is not allowed in live modes")

    app = App(cfg, secrets)
    app.build()
    try:
        asyncio.run(app.run(offline_ok=args.offline_ok))
    except KeyboardInterrupt:
        print("interrupted — shutdown complete")


if __name__ == "__main__":
    main()
