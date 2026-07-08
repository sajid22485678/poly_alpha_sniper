"""Retry policy: only idempotent operations retry.

A NEW ORDER submit after an AMBIGUOUS network error is NOT retried (the order
may exist on the exchange — reconciliation must decide). Cancels are always
retryable. Reads always retryable.
"""
from __future__ import annotations


class RetryPolicy:
    RETRYABLE_OPS = {"cancel", "read"}

    def __init__(self, max_retries: int = 2, base_delay_ms: int = 200):
        self.max_retries = max_retries
        self.base_delay_ms = base_delay_ms

    def should_retry(self, op: str, attempt: int, error_kind: str = "network") -> bool:
        if attempt >= self.max_retries:
            return False
        if op in self.RETRYABLE_OPS:
            return True
        if op == "submit":
            # retry only when we KNOW the order was rejected before acceptance
            return error_kind in ("rejected", "rate_limited", "validation")
        return False

    def backoff_ms(self, attempt: int) -> int:
        return self.base_delay_ms * (2 ** max(0, attempt))
