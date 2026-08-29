# Poly Alpha status

- State: `FINAL31_RECOVERY3_SOURCE_QUALIFIED_ORDINAL8_REGISTRATION_PENDING`.
- Ordinal7 `a12ffde5...24e28` is terminal before session creation after the 300-second persistence-startup bound expired; it collected zero candidates, launched no guardian, and must never relaunch/evaluate. Terminal SHA-256: `acab2df7...d3db`.
- Causal fix: exact-v6 prospective pre-session admission now uses the existing 900-second global cap. Command ACK deadlines, strategy semantics, evaluator/candidates, and production defaults are unchanged.
- Recovery3 tested tree `86730e8d...0d60d` passed 4,293/4,293 exactly once, zero failure/error/skip, exact post-tree, v5 equal. JUnit `e26cf543...63707`; manifest `c16626c7...7ba29`.
- Next: create the hash-bound Recovery3 combined source authority, then register one empty failed-registration ordinal8 continuation with a 45-minute not-before and fresh runtime/guardian identities.
- Safety: shadow only; no live/signing/authenticated placement/cancellation; kill engaged; no Phase Three; `V4-HO-001` nonexistent/unconsumed; authoritative v5 immutable.
