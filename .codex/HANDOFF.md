# Poly Alpha handoff

Canonical evidence: `.codex/SUCCESSOR37_CAUSAL_FIX_AND_FINAL_STABILIZATION.json`.

Successor37 and every predecessor remain immutable and unavailable. The binding first failure is preserved, but causal forensics now prove it was a stale-publication guardian defect, not a real 16.2-second command acknowledgement. The exact commands committed in 147 ms and 93 ms; audit cleanup had occupied the state/heartbeat control lane.

The minimal fix and adjacent hardening are green without changing the deadline or strategy semantics. Broad affected validation is `584 / 584`; the final changed guardian/persistence/export/integrity tree is `162 / 162`. Safety remains shadow-only with no Phase Two/Three and no holdout.

Exact next action: prepare one new final source-seal identity and execute the full suite exactly once. Only a fully verified pass may authorize one final distinct materialization/runtime/guardian identity.
