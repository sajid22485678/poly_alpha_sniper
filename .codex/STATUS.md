# Poly Alpha durable status

Updated: 2026-08-24T18:11:18+07:00

## State

`SUCCESSOR20_CANONICAL_CLOSURE_BLOCKED`

Codex remains the sole repository writer. The main source tree is deliberately dirty and must be preserved exactly. Successor22 passed its one permitted source qualification and post-seal verification, but it has not been materialized or launched. No runtime or guardian is active.

## Binding blocker

Successor20 is permanently forensic-only after its first binding guardian failure. Its host processes disappeared after the power loss, while its database still contains one open runtime session. The owner conditionally authorized one canonical closure, but the required repository-defined mechanism does not exist. The normal graceful-stop script is live-PID-bound; for an absent PID it removes only stale control files and does not terminalize the database session. Directly calling `end_runtime_session` would be the forbidden manual session-status repair. Therefore no mutation was made and Successor22 materialization remains unauthorized.

## Immutable cohort history

- Successor20: session `27cad22fef8f4d43a68034c7e9f18af2`, phase-one nonce `13a4198c8f96427582a3e8c7e7b8bb4c`, former launcher/runtime PIDs `14236/11508`; never resume, repair, finalize, pool, or reuse.
- First binding failure: `guardian_failure_1787551905402.json`, SHA-256 `4848a897f4eff16181756dbefcfbbb6f0306093018d7102656772f7996ce22a7`.
- Failed command: `27cad22fef8f4d43a68034c7e9f18af2:000000053820:persist-evaluation-bundle`; three attempts; `V4EvidenceConflict`; stored monotonic `166059437000000`, offered `166059421000000`.
- The failed bundle represented 888 logical rows, not 888 independent failed commands. Final journal census: 55,498 committed, one failed, zero unresolved, zero duplicate command IDs; one open runtime session remains.
- Successor21: immutable failed exact-once source qualification; never rerun or materialize.

## Successor22 source qualification

- Root: `D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor22_20260824T1746Z`
- Tested/post tree SHA-256: `66f3739a96b29629aadfa25038d197c4b760e389823738a8a9129c7737bc938e`
- Manifest SHA-256: `b49653106a0d0f0a3d1b34b926f362ae9f019f8573c8aa596297bb67b9596bd4`
- Exact-once full suite: 4,175 passed, zero failed/error/skipped, exit 0.
- JUnit SHA-256: `7f7ffd6e195b4781ef5bee8e88e468c6ea5ac1fbceb46459abaebc44d15fc272`
- Post-verification SHA-256: `b83c537a89d203b3ded613143926478184109f1785941ea08985e9503746a1de`
- Exact post-tree equality and authoritative-v5 byte identity verified.

## Future collection constraint

After closure and every preceding gate authorize a fresh canonical Successor22 launch, strategy/execution collection is limited to a 2–4 hour active session. Use evidence quotas, not elapsed time alone. At about two hours classify `EARLY_STOP_SUFFICIENT` or `CONTINUE_TO_4H`; at about four hours stop automatic extension and classify sufficiency, low event density, coverage deficiency, or runtime/data-quality blockage. Software qualification and any separate reliability soak remain distinct gates. Successor20 cohort evidence is inadmissible.

## Safety

`LIVE_ENABLED=false`; `REAL_ORDERS_POSSIBLE=false`; wallet signing unavailable; authenticated trading unavailable; kill switch engaged; no Phase 3; `V4-HO-001` nonexistent/unconsumed; authoritative v5 immutable. No Successor22 materialization, runtime, guardian, nonce, session, or data collection exists.

## Exact next action

The owner must provide or separately authorize an implementation/specification boundary that creates a canonical, inspectable, exact-identity host-loss closure mechanism. The current conditional authorization permits only an already-existing repository-defined mechanism and expressly forbids improvisation, so it cannot be consumed. Do not materialize Successor22.
