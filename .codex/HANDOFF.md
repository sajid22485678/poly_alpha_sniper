# Poly Alpha continuation handoff

## Current boundary

Successor22 is source-qualified and post-seal verified but is not authorized for materialization. The owner conditionally authorized one canonical Successor20 host-loss closure, but disk inspection proved that no repository-defined host-loss closure mechanism exists. The authorization's precondition therefore fails. No mutation was made; no runtime or guardian is running; nothing should be relaunched.

## Source identity

- Repository: `D:\claude\poly_alpha_sniper`
- Branch/HEAD/index: `master` / `d3364f219feb37a09a547ff1daba6f0f96377fe4` / `700307bdbc9a4fdab7615d79eea83fe1bf6463cb`
- Deliberate source tree SHA-256: `66f3739a96b29629aadfa25038d197c4b760e389823738a8a9129c7737bc938e`
- Sealed Successor22 root: `D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor22_20260824T1746Z`
- Exact-once result: 4,175/4,175 pass, exit 0, exact post-tree equality.

## Successor20 forensic boundary

Successor20 is permanently ineligible. Its former session/nonce/PIDs are `27cad22fef8f4d43a68034c7e9f18af2` / `13a4198c8f96427582a3e8c7e7b8bb4c` / `14236,11508`. The processes are absent, but the database has one open runtime session. Full SQLite quick check, integrity check, foreign-key check, schema/migration checks passed; the cohort nevertheless failed semantically and cannot supply release evidence. The integrity report is `D:\pytest_tmp_v4\poly_alpha_successor20_full_db_integrity_20260824T1720Z\integrity_report.json`, SHA-256 `ce9393f6d210f9732b3b4521a7244b04621162f9d05368d161d26df1c8bc4431`.

No canonical host-loss closure exists. `scripts/stop_lite_frequency_v4_shadow.ps1` requires the original live PID for a graceful stop; when the PID is absent it only removes stale control files and leaves the open database session unchanged. `V4Store.end_runtime_session` is an internal persistence method, not a host-loss operator authority, and invoking it directly would violate the prohibition on manual session-status repair. Repository mission/release/handoff documents explicitly record that the mechanism is absent. The conditional authorization cannot be consumed, so do not materialize Successor22.

## Verified correction and qualification lineage

- First-causal book provenance is preserved across compact-equivalent rows without weakening collision checks.
- Telemetry JSON writes fsync the sibling temporary file before atomic replacement and do not reopen the destination after replacement.
- The CEX shutdown test now proves consumer start deterministically while retaining the late-callback regression.
- Affected union: 373 pass, zero failures/errors/skips; JUnit SHA-256 `cfdd1398496ab9d6db23227a6a00a5663727bd7fda491ec47a341e1f57f15d9f`.
- Successor21 exact-once qualification failed one nondeterministic test and remains immutable/ineligible.
- Successor22 exact-once qualification passed all 4,175 tests. JUnit SHA-256 `7f7ffd6e195b4781ef5bee8e88e468c6ea5ac1fbceb46459abaebc44d15fc272`; post-verification SHA-256 `b83c537a89d203b3ded613143926478184109f1785941ea08985e9503746a1de`.

## Authorized future sequence

1. Owner separately authorizes creation/specification of a canonical, exact-identity Successor20 host-loss closure mechanism; the present authorization does not permit that implementation work.
2. Apply it only under its exact conditions and preserve terminal evidence.
3. Re-run only the required read-only startup census/release checks.
4. If every gate passes, materialize/start Successor22 once through the repository-defined mechanism with a fresh identity, clean census, healthy detached guardian, shadow-only safety, and durable independence from Codex.
5. Perform only minimum launch-health verification, publish the running handoff, return `SUCCESSOR22_RUNNING_CODEX_STOP_BOUNDARY_REACHED`, and stop Codex while leaving runtime/guardian running.
6. Active strategy/execution collection uses a 2-hour evidence-density review and 4-hour hard review; no blind 8/12/24-hour extension. Historical/replay evidence should cover questions that do not require current live conditions.

## Forbidden

Do not resume Successor20; rerun/materialize Successor21; invent or apply an unratified closure; mutate the sealed source; duplicate materialization/session/nonce; reuse invalid cohort evidence; consume holdout; enter Phase Two or Phase 3; mutate/cut over v5; enable signing/authenticated/live trading; place or cancel real orders; lower filters to inflate samples; or treat time alone as evidence sufficiency.

## Safety tuple

`LIVE_ENABLED=false`, `REAL_ORDERS_POSSIBLE=false`, signing/authenticated trading unavailable, kill switch engaged, Phase 3 false, `V4-HO-001=NONEXISTENT_UNCONSUMED`, authoritative-v5 mutation forbidden.
