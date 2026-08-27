# Poly Alpha status

Status: **SUCCESSOR33 HEALTHY OOS ACQUISITION**

Successor32's frozen OOS/calibration/tournament evidence remains immutable. Its consumed Phase-Two launch failed before session admission because the engine reused the 15-second command acknowledgement deadline for a measured 152.37-second exact-v6 startup admission. The nonce must never be relaunched; the database remained at 36,895 committed commands with no active/new session and no holdout.

The causal fix introduces a distinct bounded 300-second persistence startup deadline while preserving the 15-second post-admission acknowledgement deadline. The corrected Successor33 tree `7f34c14ef1a4016f576b517f28d912d569a76b98fd0a0f151932fee89bf3fb40` passed `4,254 / 4,254` exactly once with zero failures/errors/skips, exact post-tree equality, and unchanged authoritative v5.

Successor33 acquisition `V4-PR-001-PROSPECTIVE-SUCCESSOR33-20260827T0559Z`, session `2496e04267c7462bbe37c569bc01edcd`, Phase-One nonce `35b603969c314957855eb2f6ebceb9b1`, runtime `17424 -> 8696`, and guardian `10384 -> 4348` are consumed identities and must not be relaunched or restarted. Initial guardian observations are clean and acquisition is progressing.

Safety remains fail-closed. The next bounded review is at or after `1787818421267`; final frozen-OOS review is at or after `1787823821267`. Do not advance phases early.
