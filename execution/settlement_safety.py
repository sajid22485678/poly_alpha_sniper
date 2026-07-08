"""Settlement safety: reconciliation mismatch -> freeze everything.

Master rule: mismatch => kill switch + panic + no new entries + incident
report + manual reset required.
"""
from __future__ import annotations

from poly_alpha_sniper.core.logger import get_logger, redact_obj
from poly_alpha_sniper.execution.fill_reconciler import ReconcileResult

log = get_logger("settlement_safety")


class SettlementSafety:
    def __init__(self, panic_mode, kill_switch):
        self.panic = panic_mode
        self.kill = kill_switch

    def check(self, result: ReconcileResult) -> dict | None:
        if result.ok:
            return None
        self.kill.activate("reconciliation_mismatch")
        self.panic.activate("reconciliation_mismatch")
        incident = {
            "kind": "reconciliation_mismatch",
            "severity": "CRITICAL",
            "mismatches": list(result.mismatches),
            "details": redact_obj(dict(result.details)),
            "action": "kill switch + panic activated; manual reset required",
        }
        log.error("settlement_mismatch_freeze", extra={"extra": incident})
        return incident
