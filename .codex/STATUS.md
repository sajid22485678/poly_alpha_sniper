# Poly Alpha status

Successor37 is terminal and permanently inadmissible. Its guardian recorded the first binding failure at `1787858846844`: effective unacknowledged critical-command age `16,218 ms` exceeded the fixed `15,000 ms` deadline.

The exact nonce/PID-bound graceful stop was consumed once. Runtime `7448 → 16596` exited, the lease was released, and the terminal journal is `26,650 / 26,650` committed with zero failed, unresolved, lost, overflow, discard, reconciliation, or safety violations. Later lossless recovery does not cure the first guardian failure.

The causal distinction remains deliberately unresolved: the guardian projection combined a `171 ms` raw age with a `16,047 ms` stale metrics interval, while terminal metrics report maximum acknowledgement latency `3,813 ms` and timeout count zero. Distinct owner authority is required for forensics, RED/fix/verification, source qualification, or any replacement successor.
