# Poly Alpha handoff

Read `.codex/FINAL31_RECOVERY1_COMBINED_SOURCE_CORRECTION_AUTHORITY_20260829T0814Z.json` first.

Recovery1 remains source-qualified exactly once. Ordinal6 is a zero-candidate failed registration: its consumed runtime nonce exited before lease/session creation because its immutable combined source authority had one extra field. Never relaunch or evaluate it. Create the non-overwriting contract-exact combined authority, then register one parent-linked ordinal7 through the failed-registration contract with fresh runtime/guardian identities and unchanged research semantics.

Safety remains fail-closed: live and real orders disabled, signing/authenticated placement/cancellation unavailable, kill engaged, authoritative v5 immutable, `V4-HO-001` nonexistent/unconsumed, and no Phase Three.
