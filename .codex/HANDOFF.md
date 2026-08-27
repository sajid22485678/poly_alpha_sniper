# Poly Alpha handoff

Canonical evidence: `.codex/SUCCESSOR37_BINDING_FAILURE_TERMINAL.json`.

Successor37 session `f1e3c4c3b8364996af6df77654848afa` and Phase-One nonce `6f04157ef7c84d3db160c79c47510e63` are terminal and `MUST_NOT_RELAUNCH`. Guardian `3336 → 14972` failed once and `MUST_NOT_RESTART`. First failure artifact SHA-256: `03540b96d32a9cbe8f4b3337345a5e1a9a62ee8516d647739ebb62a7d30221e7`.

Terminal state is lossless and safe: `26,650 / 26,650` journal commands committed, quick-check `ok`, FK `0`, reconciliation mismatch `0`, unexpected loss `0`, no trades, no calibration/tournament/holdout rows, and authoritative v5 unchanged.

Exact next action: stop under the current narrow authority. A distinct owner packet must authorize causal forensics, mandatory RED/minimal correction and verification, a fresh source seal, and any new successor. Never reuse Successor37 or consume reserved Phase-Two nonce `98b526db319043c7929f25db3ce88773`.
