# Successor27 handoff

Successor27 is terminal. Its clean four-hour close remains data-insufficient, not `OOS_PASS`.

- Correct guardian-cycle anchor: `1787704840042`.
- Correct +4h close: `1787719240042`.
- Final clean guardian snapshot: `guardian_snapshot_1787719255613.json`, SHA-256 `5c10900bd631742ce824efe40acb566914102fe10858bb709f8deb5234eada8f`.
- Guardian coverage: 471 observations, no failure artifacts, empty stderr.
- Final journal census: 41,427/41,427 committed; zero failed, unresolved, lost, mismatch, or unexpected loss.
- Blocking deficit: 15 unlabeled rank-1 rows for each target.

After the guardian ended, the still-running identity latched two persistence acknowledgement timeouts. The exact stop authority was consumed once; both runtime processes exited, the lease was released, and 92,125/92,125 commands reconciled without loss. The terminal closure SHA-256 is `0d156c6f05d63f9fd6118819c59a89d0f0d5b6de91ce048dae5205bd27faa47f`.

Do not relaunch or reuse Successor27. The protocol audit proved a structural 12-hour prediction-window versus 4-hour-close mismatch. The minimal preregistered correction is +5m guarded prediction start, +3h30m prediction cutoff, 30-minute official-resolution grace, and +4h immutable dataset close. The S27 corpus passes the numerical gate under those exact ex-ante bounds with 323/322 unique rows and zero unlabeled; this is feasibility evidence only.

The post-close timeouts were caused by unbounded strict-priority overtaking, not slow execution: the affected market commands executed in 0/1 ms after 23/24 newer fee commands and four newer execution commands jumped each queue. The scheduler now ages eligible heads after one second, retaining fresh priority and per-key FIFO. RED tests were observed and focused validation is green.

Focused and broader validation are green. Successor28 source qualification passed exactly once at `D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor28_20260826T1032Z`: 4,252 tests, zero failure/error/skip, exit 0, exact tree equality, v5 unchanged. Tested-tree SHA-256 is `fa4fcdbfa01eb42289f33c977e56901983f631ec531b1052a0f0e79c188051f7`; post-verification SHA-256 is `6f1ad1cd2eef72c15ed39606edb6137eb99b58e6884c9069dc1d7971d0c064ac`.

Successor28 materialization was consumed exactly once at `D:\poly_alpha_prospective_exact_v6_successor28_20260826T1056Z`; materialization nonce `8c22fe20ecd744bbacfbfd101f82b8f7` is permanently consumed. Independent cold verification passed. Its Phase-One launcher was invoked once but failed during authority preflight: mechanical replacement had produced `V4-PR-001-PROSPECTIVE-SUCCESSOR28-20260826T0024Z` in the launcher while the authority correctly required `...T1056Z`. The guard failed before writing launch outputs or creating any process/session/runtime root; the database nonce remains unconsumed. Do not retry or reuse the Successor28 Phase-One identity. Create a distinct Successor29 acquisition and require an explicit exact identity check across launcher, authority, materialization, and source seal before any launch. Calibration, tournament, Phase Two, holdout, Phase Three, v5 mutation, and all live capabilities remain forbidden.

Successor29 has completed that distinct recovery path. Its acquisition is `V4-PR-001-PROSPECTIVE-SUCCESSOR29-20260826T1110Z`; materialization nonce `870edc7d4b9646c6ac0d65d5ccf770e2`, Phase-One nonce `9b0e0721f98f4f83847ff854e336657f`, session `8b72cd4077494021abdc7aac33ff90ef`, runtime `13504 -> 3256`, guardian `14908 -> 16368`. Both cross-file validators passed before their exact-once launches. The first guardian snapshot preceded OOS collection by 13.686 seconds; the five-minute snapshot `guardian_snapshot_1787743942516.json` is clean with 148 capsules, 211 predictions, 56 markets, zero outcomes, and 1,903 committed-only journal rows. There are zero guardian artifacts, persistence failures, critical incomplete/lost rows, mismatch, unexpected loss, overflow, or source discard. The +2h review is `1787750554155`, prediction cutoff `1787755954155`, 30-minute grace follows, immutable dataset close/+4h review is `1787757754155`, and guardian coverage runs through about `1787758040469`. Leave this identity untouched until the bounded +2h review. Calibration, tournament, Phase Two, holdout, Phase Three, v5 mutation, and all live capabilities remain forbidden.
