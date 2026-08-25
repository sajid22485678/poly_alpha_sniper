# Poly Alpha durable status

State: `SUCCESSOR26_FOUR_HOUR_REVIEW_FAILED_RUNTIME_UNTOUCHED_CODEX_STOPPING`

Classification: `FOUR_HOUR_REVIEW_FAILED`

Canonical evidence: `.codex/SUCCESSOR26_FOUR_HOUR_HARD_REVIEW.json`
SHA-256: `4adb9dd7fe5f02c874faa1e0ad2d3b5e37bf6c64b26705c4dafbfd2dedb99f94`

Successor26 is still the original exact Phase-One identity: acquisition
`V4-PR-001-PROSPECTIVE-SUCCESSOR26-20260825T1537Z`, session
`94b7f805b73d4c82b851938e20570477`, nonce
`6e80d3ea67cc4d12994c1f20427de260`, and launcher/runtime PIDs
`8604 -> 6720`. Both runtime processes remain alive; no relaunch, fork, stop
request, or competing owner was found. The guardian PIDs `12992 -> 6048` are
absent because the guardian terminated on its first binding failure, not because
its configured four-hour duration completed.

The controlling immutable failure is
`guardian_failure_1787680938064.json`, SHA-256
`62901f4186f4ec344ed6a676fb21ea0da73be3db8e4c41bf09756fb2760639e7`.
After 220 observations it recorded `DEGRADED_PERSISTENCE` and a permanent
`V4PersistenceTimeout` latch. Two commands crossed the configured 15,000 ms
acknowledgement deadline at 15,133 ms and 15,221 ms. Both later committed
losslessly, but late commits cannot cure the guardian failure.

At census timestamp `1787698140893`, the database held 1,583 capsules, 3,154
predictions, 659 markets, 216 outcomes, and 59,679 committed journal commands,
with zero failed, unresolved, duplicate, or retried commands. Critical evidence
incomplete/lost, reconciliation mismatch/unexpected loss, queue overflow, and
source discard were all zero. The runtime remains alive but latched and no new
capsules or predictions had been produced since `1787681889501`.

Readiness is not complete. Ensemble is `216 / 216 / 117 / 99 / 0` and model is
`215 / 215 / 117 / 98 / 0` for rank-1 / unique / positive / negative /
unlabeled. Exact rank-1 and unique-market deficits are 84/84 and 85/85.
Lineage exclusions are zero. The frozen OOS endpoint `1787717005335` remains
in the future, but the earlier binding guardian failure already makes this
cohort permanently ineligible.

SQLite quick-check, FK, schema fingerprint, and the complete repository v6
graph validator passed. Source bytes still equal the sealed tree
`9a8ee9b4e6e3840775ab7e50576e545a3c67ef27c44233ef6edd2c2a21b25f21`.
Authoritative v5 remains byte-identical. Safety remains shadow-only with live,
real-order, signing, authenticated, Phase Two, holdout, and Phase Three paths
unavailable; the kill switch is engaged.

Next action requires distinct owner authority for exactly one create-once,
session/nonce/PID-bound graceful stop, followed only by terminal snapshot,
journal reconciliation, and causal forensic closure. Do not relaunch, repair,
create a successor, calibrate, register a tournament, enter a later phase,
consume holdout, enable live capabilities, or mutate authoritative v5.

Codex active work stops after bridge push verification.
