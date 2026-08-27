# Poly Alpha handoff

Canonical evidence: `.codex/SUCCESSOR37_MATERIALIZED_COLD_VERIFIED.json`.

Acquisition `V4-PR-001-PROSPECTIVE-SUCCESSOR37-20260827T1625Z` is materialized once and cold verified. Materialization nonce `233b148893cc461b8212d65069b41fdb` is consumed and must not be reused. Phase-One nonce `6f04157ef7c84d3db160c79c47510e63` is unconsumed; Phase-Two nonce `98b526db319043c7929f25db3ce88773` remains reserved and unauthorized.

Exact next action: prepare and cross-validate the Phase-One and guardian operators against the cold verification and final source authority. Consume Phase One once, then guardian once, and require strong bounded initial health before unattended OOS collection.
