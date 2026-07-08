"""Error-rate circuit breaker (repeated API failures -> open -> half-open)."""
from __future__ import annotations


class CircuitBreaker:
    def __init__(self, clock, max_errors: int = 5, window_s: float = 60.0,
                 cooldown_s: float = 120.0):
        self.clock = clock
        self.max_errors = max_errors
        self.window_ms = int(window_s * 1000)
        self.cooldown_ms = int(cooldown_s * 1000)
        self._errors_ms: list[int] = []
        self._opened_ms = 0

    def record_error(self) -> None:
        now = self.clock.now_ms()
        self._errors_ms = [t for t in self._errors_ms if now - t < self.window_ms]
        self._errors_ms.append(now)
        if len(self._errors_ms) >= self.max_errors and not self.open:
            self._opened_ms = now

    def record_success(self) -> None:
        if self.half_open:
            self._errors_ms = []
            self._opened_ms = 0

    @property
    def open(self) -> bool:
        if self._opened_ms == 0:
            return False
        return self.clock.now_ms() - self._opened_ms < self.cooldown_ms

    @property
    def half_open(self) -> bool:
        return self._opened_ms != 0 and not self.open

    @property
    def error_count(self) -> int:
        now = self.clock.now_ms()
        return len([t for t in self._errors_ms if now - t < self.window_ms])
