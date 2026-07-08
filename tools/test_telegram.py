"""Telegram connectivity check.

Run: python -m poly_alpha_sniper.tools.test_telegram
Sends: "Poly Alpha Sniper Telegram test OK."

Safe debug output only: token LENGTH and first 4 chars, chat id, env path and
bot identity (getMe). The full token is never printed, logged or stored.
"""
from __future__ import annotations

import asyncio
import sys


async def _bot_identity(token: str) -> dict:
    """getMe: which bot does this token belong to? (safe to display)"""
    import aiohttp
    try:
        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(f"https://api.telegram.org/bot{token}/getMe") as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("ok"):
                    result = data.get("result", {})
                    return {"ok": True, "username": result.get("username", "?"),
                            "bot_id": result.get("id")}
                return {"ok": False, "status": resp.status,
                        "description": str(data.get("description", ""))[:100]}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "description": type(exc).__name__}


async def _main() -> int:
    from poly_alpha_sniper.core.clock import WallClock
    from poly_alpha_sniper.core.config_loader import PROJECT_ROOT, load_config, load_secrets
    from poly_alpha_sniper.reporting.telegram import TelegramClient

    cfg = load_config()
    secrets = load_secrets()
    token = secrets.get("TELEGRAM_BOT_TOKEN")
    chat_id = secrets.get("TELEGRAM_CHAT_ID")

    # --- safe debug block (never the full token) ------------------------
    print(f"env file:   {PROJECT_ROOT / '.env'}")
    print(f"token_len:  {len(token)}")
    print(f"token_head: {token[:4] if token else '(unset)'}")
    print(f"chat_id:    {chat_id or '(unset)'}")

    if not token or not chat_id:
        print("FAIL: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set in .env")
        return 1

    identity = await _bot_identity(token)
    if identity.get("ok"):
        print(f"bot:        @{identity['username']} (id {identity['bot_id']})")
    else:
        print(f"FAIL: token rejected by Telegram getMe: {identity}")
        print("      -> the token in .env is not a valid bot token.")
        return 1

    # --- send through the SAME client the runtime uses -------------------
    client = TelegramClient(secrets, cfg, WallClock())
    ok = await client.send("Poly Alpha Sniper Telegram test OK.")
    await client.close()
    if ok:
        print("OK: test message sent.")
        return 0
    print("FAIL: sendMessage did not return 200.")
    print(f"      Most common cause of 'chat not found': the bot "
          f"@{identity.get('username')} has never received a message from "
          f"chat {chat_id}.")
    print(f"      -> Open Telegram, search @{identity.get('username')}, press "
          "START (or send any message), then re-run this tool.")
    print("      (Bots cannot message a user first. Also verify the chat id "
          "with that SAME bot via getUpdates.)")
    return 1


def main() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    raise SystemExit(asyncio.run(_main()))


if __name__ == "__main__":
    main()
