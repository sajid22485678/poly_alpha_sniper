# Poly Alpha status

- State: `FINAL31_RECOVERY1_ORDINAL6_REGISTRATION_FAILURE_CORRECTION_PENDING`.
- Recovery1 tree `faedc20c...c9c1b` remains exactly qualified once at 4,293/4,293; no source change or reseal occurred.
- Ordinal6 `358d2757...f6556` registered, but runtime nonce `44b8bed4...7c945` failed before lease/session creation because the first combined source authority contained one extra non-contract field. It is immutable and `MUST_NOT_RELAUNCH/MUST_NOT_EVALUATE`; zero candidates were collected and no guardian launched.
- First failure evidence: `final31_recovery1_phase_two_registration_failure_1787990429899.json`, SHA-256 `f2391659...fa5d8`.
- Next: create a non-overwriting exact-nine-field combined source authority bound to this pushed checkpoint, then use the repository's failed-registration continuation contract to preregister ordinal7 with fresh identities.
- Safety: shadow only; no live/signing/authenticated placement/cancellation; kill engaged; no Phase Three; `V4-HO-001` nonexistent/unconsumed; authoritative v5 immutable.
