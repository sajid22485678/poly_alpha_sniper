# Successor36 interrupted source seal

The post-reboot classification is `OLD_SEAL_INTERRUPTED_AMBIGUOUS`.

The exact-once suite launched once against frozen tree `60adeb550751460589728336a8dd04d332467fc039edad7beb6a9226aa690086`, but the power loss left no JUnit, pytest exit capture, run-control record, or post-verification artifact. No old seal, runtime, guardian, materializer, or competing repository writer survived the reboot. The old seal root is retired permanently and must not be rerun.

The current repository is exactly equal to the 670-path frozen manifest, the index is empty, `git diff --check` has no errors, and authoritative v5 remains byte-identical with no journal. The stabilized source may therefore receive one distinct replacement exact source-seal identity under the owner recovery authority.

No prospective candidate was materialized or launched. Safety remains fail-closed.
