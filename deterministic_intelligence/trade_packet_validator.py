"""Completeness check on the execution 'trade packet' dict."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import RejectReason

REQUIRED_KEYS = ("signal_id", "market_id", "token_id", "side", "price",
                 "size_usd", "size_shares", "tier", "mode", "exit_plan")


def validate_trade_packet(packet: dict) -> tuple[bool, list[str], str]:
    missing = [k for k in REQUIRED_KEYS
               if k not in packet or packet[k] in (None, "")]
    if missing:
        return False, missing, RejectReason.INCOMPLETE_TRADE_PACKET
    if packet["price"] <= 0 or packet["size_usd"] <= 0 or packet["size_shares"] <= 0:
        return False, ["non_positive_values"], RejectReason.INCOMPLETE_TRADE_PACKET
    return True, [], ""
