# Successor31 terminal handoff

Successor31 is `TERMINAL_OOS_FAILED_GUARDIAN_COVERAGE_GAP` and `MUST_NOT_RELAUNCH`.

- Source: Successor30 seal, tree `1e608dd9611d8ef0c0dfa3b00ee4ee3d1c5f152df5db3160c4c9d15e07d02556`, 4,252/4,252 PASS once.
- Acquisition: `V4-PR-001-PROSPECTIVE-SUCCESSOR31-20260826T1333Z`.
- Materialization nonce: `979a3e1f865e425baf956c012e4fa507`.
- Phase-One nonce: `1aeacf65f2224296b1056e7ff944d3e3`.
- Session: `626154fc536b4716919b7d58124b6f70`.
- Runtime: `15632 -> 18112`; guardian: `17404 -> 10332`; all terminal.
- First failure: `guardian_failure_1787752211715.json`, SHA-256 `2edf63fda793dcc9e0e3e1227d345afc0eff9c890c74d58a81df51aee7963a32`.
- Terminal closure: `successor31_terminal_closure.json`, SHA-256 `d0d39b7c9c0b910cfe5f05158f4be543fed86d589934793b97e68c0432a70a71`.

The binding defect is protocol timing, not persistence: the guardian started 54,517 ms after OOS prediction collection. The exact stop authority drained 1,223/1,223 commands with zero loss or mismatch and released the lease. Do not reuse any S31 identity.

Use fail-first tests to extend the guarded startup from five to ten minutes while retaining the +3h30 prediction cutoff, 30-minute resolution grace, and +4h immutable close. Immutable S27 evidence supports feasibility at those ex-ante bounds: ensemble 321 unique/162 positive/159 negative/0 unlabeled; model 320 unique/162 positive/158 negative/0 unlabeled. Fresh source qualification is required after the correction. No later phase or live capability is authorized.
