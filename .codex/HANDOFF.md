# Poly Alpha durable handoff

## Decision

**GENUINE OOS_PASS ACHIEVED. CODEX ACTIVE WORK IS STOPPING.**

Successor32 acquisition `V4-PR-001-PROSPECTIVE-SUCCESSOR32-20260826T1428Z`, session `e70855a354954a61b8975c266c1704b0`, Phase-One nonce `3fbb80c98c9d49a7a349e3a460ca3cee`, runtime `3640 -> 17520`, and guardian `13036 -> 1388` are consumed, terminal, and must never be relaunched or restarted.

## OOS result

- Ensemble: rank-1 `317`; unique `317`; positive `156`; negative `161`; unlabeled `0`.
- Model: rank-1 `313`; unique `313`; positive `154`; negative `159`; unlabeled `0`.
- Temporal violations and invalid lineage/capsule exclusions: `0`.
- Late post-close outcomes affecting selected markets: `0`.

## Terminal integrity

- Guardian: `427` clean observations, `43` snapshots, `0` failures, empty stderr.
- Journal: `36,893 / 36,893` committed; unresolved/failed/retries/duplicates all `0`.
- Database: exact-v6 PASS; quick-check `ok`; FK `0`; schema `6`.
- Persistence: critical incomplete/lost/failed, mismatch/unexpected loss, deadline-expired, overflow/discard/shutdown-abandonment all `0`.
- Shutdown: exact graceful stop, terminal fence `1 / 1`, clean telemetry shutdown, lease/process lock released, processes absent.

## Safety and boundary

All live capabilities remain absent; kill switch remains engaged; no Phase Two/Three was entered; V4-HO-001 and holdout remain untouched; authoritative v5 remains byte-identical.

Use `.codex/SUCCESSOR32_OOS_PASS.json` as the canonical evidence index. No later phase is authorized. Do not relaunch/restart Successor32, rewrite OOS, finalize calibration, register/evaluate tournament, enter Phase Two, touch holdout, enter Phase Three, mutate/cut over v5, or enable live capability.
