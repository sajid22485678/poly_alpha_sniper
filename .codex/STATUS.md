# Poly Alpha status

- State: `PHASE_TWO_STARTUP_FIX_QUALIFICATION_RUNNING`
- Tournament: empty expired parent preserved; child experiment `d8862bbd…` registered as conservative ordinal 2 with identical frozen semantics and no retuning.
- Failed launch: nonce `1fae05b6…` is terminal/MUST_NOT_RELAUNCH. Maintenance-store admission timed out before session admission; zero candidates were created and its lease released.
- Root cause: prospective maintenance-store open incorrectly used the 30-second maintenance command deadline rather than the 300-second exact-v6 startup admission deadline. Shutdown also attempted a terminal command despite no registered session.
- Fix: both causal paths are corrected; 90 focused engine/runtime/persistence tests pass.
- Qualification: exact-once replacement seal is running at `D:\pytest_tmp_v4\poly_alpha_combined_post_oos_source_freeze_final2_20260828T1210Z`, tested tree `4ab1be22…`.
- Next: if the seal passes, launch one distinct Phase-Two identity under the existing child experiment and never reuse `1fae05b6…`.
- Safety: shadow only; no admitted Phase Two/Phase Three; `V4-HO-001` nonexistent/unconsumed; v5 immutable.
