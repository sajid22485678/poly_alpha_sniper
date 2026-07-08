"""Tiny deterministic rule engine.

A Rule is a named predicate over an arbitrary subject. `evaluate` runs every
rule and reports which passed, which failed, and whether any FATAL rule failed.
No AI, no randomness, no I/O — pure functions so every gate decision is
reproducible and auditable.

ASSUMPTIONS:
- A predicate that raises is treated as FAILED (fail-safe): a rule that cannot
  be evaluated must never silently pass.
- Returned lists contain rule NAMES (strings), which is what callers log and
  store in the DB.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Sequence


@dataclass(frozen=True)
class Rule:
    """A named, deterministic predicate. fatal=True means a failure is a
    hard reject for whatever gate uses this rule set."""

    name: str
    predicate: Callable[[Any], bool]
    fatal: bool = False


def evaluate(rules: Sequence[Rule], subject: Any) -> tuple[list[str], list[str], bool]:
    """Run all rules against subject.

    Returns (passed_names, failed_names, fatal_failed). Rules are evaluated in
    order; evaluation never short-circuits so the full audit trail is always
    available.
    """
    passed: list[str] = []
    failed: list[str] = []
    fatal_failed = False
    for rule in rules:
        try:
            ok = bool(rule.predicate(subject))
        except Exception:  # noqa: BLE001 - fail-safe by design
            ok = False
        if ok:
            passed.append(rule.name)
        else:
            failed.append(rule.name)
            if rule.fatal:
                fatal_failed = True
    return passed, failed, fatal_failed
