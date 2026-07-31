# Poly Alpha Lite Frequency V4 Shadow

`lite_frequency_v4_shadow` is a fully isolated, deterministic research lane for
exact five-minute Polymarket crypto Up/Down markets. It does not import either
legacy Lite strategy or any authenticated trading surface.

## Permanent safety boundary

- Strategy: `lite_frequency_v4`
- Mode: `lite_frequency_v4_shadow`
- `dry_run=True`, `live_enabled=False`, `real_orders_possible=False`
- No live adapter, signer, wallet, authenticated client, order placement, or
  cancellation method
- Kill switch always engaged
- Exactly five simulated shares per entry
- At most one side and one entry per asset/window
- Positive fee-net expected value is a hard gate; the frequency target cannot
  override it

The account value of `$13` is used only for a read-only compounding and risk
preview. It never changes the fixed-share experiment.

## Isolation

| Resource | V4 owner |
|---|---|
| Database | `C:\poly_alpha_v4_db\poly_alpha_frequency_v4.db` (SSD; see `V4_DB_PATH`) |
| Audit snapshot | `data/integrity_audit/` (kept on the roomy volume, not the SSD) |
| Runtime | `runtime/lite_frequency_v4_shadow/` |
| Logs | `logs/lite_frequency_v4_shadow/` |
| Read-only export | `D:\claude\agent_readonly\poly_alpha_frequency_v4` |
| Dashboard | `http://127.0.0.1:8504` |

Runtime ownership is corroborated by the exact module, PID, launch nonce,
process lock, state, and heartbeat. SQLite reservations and unique constraints
provide persistent asset/window idempotency and exposure enforcement.

## Data and decision flow

Background discovery identifies and verifies current and upcoming exact
five-minute markets. The active path consumes public Polymarket CLOB and OKX
WebSockets; REST is limited to discovery, initial/reconnect hydration, bounded
same-market book recovery, and settlement verification. Provider timestamps
and local receipt times remain distinct. Future, regressed, duplicated, stale,
or mismatched evidence fails closed.

Seven auditable model families contribute to a regime-aware fair probability:
lead-lag impulse, trend continuation, sweep reversal, window-open displacement,
order-book microstructure, paired-book parity, and late-window dominance.
Correlated families share bounded weight rather than being counted repeatedly.
Every candidate stores the contribution and complete fee/buffer calculation for
both YES and NO.

Execution is simulated only. A net edge of at least `0.020` can cross the
five-share book immediately; `0.010` to `0.020` enters an event-driven
500–1500 ms maker observation; `0.005` to `0.010` observes for confirmation;
anything lower is rejected. Maker fills are never assumed. Every post-wait
cross requires a fresh full economic recomputation and chase-cap check.

## Operations

```powershell
scripts\start_lite_frequency_v4_shadow.ps1
scripts\status_lite_frequency_v4_shadow.ps1
scripts\restart_lite_frequency_v4_shadow.ps1
scripts\stop_lite_frequency_v4_shadow.ps1

scripts\start_frequency_v4_dashboard.ps1
scripts\status_frequency_v4_dashboard.ps1
scripts\stop_frequency_v4_dashboard.ps1
```

The dashboard must be built with `npm.cmd run build` in
`dashboard_frequency_v4` before its first start.

## Research acceptance

The 19–36 entries/hour range is a soft research objective. Capacity is computed
from verified eligible assets (12 five-minute windows per asset/hour), while
rolling 1/3/6/12-hour and full-session funnels distinguish raw events, unique
asset-windows, positive-edge opportunities, attempts, entries, and terminal
trades. V4 remains a research candidate until at least 300 verified terminal
trades have complete execution/fee evidence, no conflicts, no duplicates, no
unresolved finals, fee-net PF above 1, and positive fee-net expectancy. Live
trading is outside this lane by construction.
