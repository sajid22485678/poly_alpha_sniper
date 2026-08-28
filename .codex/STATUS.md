# Poly Alpha status

- State: `COMBINED_POST_OOS_FINAL16_RUNTIME_SOURCE_AUTHORIZED`.
- Final14 remains terminal/ineligible and `MUST_NOT_RELAUNCH`; Final15 remains an immutable one-failure seal and `MUST_NOT_RERUN`.
- Final16 ran exactly once on tested tree `5dc8c6d416f5c6f5d4457f4d888b442d3f4dfb1bb1c4a4f600a3c65d9530432d`.
- Exact result: 4,286/4,286 passed, zero failures/errors/skips, exit 0, exact post-tree equality.
- JUnit SHA-256: `3124ad80c57cefedc9287ee970e11ba4c15d2388df22425d77d736b84afba953`.
- Post-verification SHA-256: `6ba5b1060c934b5370a5b912e871205c3a324d2cbc8f1bbfeb415066fd06fbab`.
- Authoritative v5 DB/WAL/SHM remain byte-identical; no v5 journal exists.
- Research equivalence: Final10-to-Final16 changed only `persistence.py` and two persistence tests; research/config/economics/tournament semantics are unchanged.
- Terminal Phase-Two nonce `95d21ed207ae488f8f59c022510f8461` has been added to the immutable failed-predecessor set.
- Next: write a non-overwriting external Final16 authority bound to this bridge artifact, then launch one distinct Phase-Two identity. Never reuse Final14.
- Safety: shadow only; no live/signing/authenticated/order capability; kill engaged; no Phase Three; `V4-HO-001` nonexistent/unconsumed; authoritative v5 immutable.
