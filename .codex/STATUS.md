# Poly Alpha status

Successor27 is terminal. Its immutable four-hour result remains `OOS_DATA_INSUFFICIENT`: the clean guardian-covered close had 15 unlabeled rows per target.

Readiness at cycle close:

- Ensemble: 372 rank-1, 372 unique, 180 positive, 177 negative, 15 unlabeled.
- Model: 369 rank-1, 369 unique, 180 positive, 174 negative, 15 unlabeled.

After the guardian-covered close, the still-running identity separately latched two `V4PersistenceTimeout` events. The owner-authorized exact graceful stop ended runtime `2312 -> 17176`, released its lease, and reconciled 92,125/92,125 committed commands with zero failed, unresolved, duplicate, retried, lost, or mismatched work. The post-close failure does not rewrite the earlier frozen cycle result.

Both causes are now proven and minimally corrected. The old protocol accepted predictions for twelve hours but the immutable review closed at four hours, so unlabeled zero depended on late-cycle luck. The corrected contract reserves five minutes for guarded startup, collects through +3h30m, grants a deterministic 30-minute official-resolution interval, and closes immutably at +4h. On the frozen S27 corpus those exact bounds would have yielded 323/322 unique rows and zero unlabeled; that feasibility result does not alter S27.

The timeout cause was strict priority starvation: the two market commands waited 15,931 and 16,323 ms while newer fee/execution commands overtook them. Eligible heads now receive monotonic age-bounded service after one second while fresh priority and per-key FIFO remain intact. Focused and broader validation are green.

Successor28 source qualification passed exactly once at `D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor28_20260826T1032Z`: 4,252/4,252, zero failure/error/skip, exit 0, exact post-tree equality, and authoritative v5 unchanged. Tested-tree SHA-256 is `fa4fcdbfa01eb42289f33c977e56901983f631ec531b1052a0f0e79c188051f7`; JUnit SHA-256 is `ff3cb71369dd3999903f68b05d8a356320b74b3204bf722b2f1e4f55f1ca13b4`. Prepare a distinct Successor28 materialization authority and consume it once. No later phase or live capability is authorized.
