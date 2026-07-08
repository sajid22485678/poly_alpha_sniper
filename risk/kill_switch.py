"""Kill switch: hard stop on new entries. Manual clear required."""
from __future__ import annotations

from poly_alpha_sniper.core.logger import get_logger

log = get_logger("kill_switch")


class KillSwitch:
    def __init__(self):
        self._active = False
        self._reason = ""
        self.history: list[str] = []

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def reason(self) -> str:
        return self._reason

    def activate(self, reason: str) -> None:
        self._active = True
        self._reason = reason
        self.history.append(reason)
        log.error("kill_switch_activated", extra={"extra": {"reason": reason}})

    def clear(self, manual: bool = True) -> None:
        if not manual:
            raise PermissionError("kill switch requires manual clear")
        log.warning("kill_switch_cleared", extra={"extra": {"was": self._reason}})
        self._active = False
        self._reason = ""
