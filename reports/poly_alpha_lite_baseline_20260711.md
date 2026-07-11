# Poly Alpha Lite immutable before-state

Snapshot: **2026-07-11 06:40:32.138 WIB** (`1783726832138` ms)
Starting commit: `db44a0cfcc4226bb9bb6bf8dfd7fef0f920bfabf` on `master`
Worktree: clean
Mode: `lite_shadow`; `dry_run=True`; `live_enabled=False`

Rollback database: `runtime/lite_audit/poly_alpha_lite_before_20260711_064032_WIB.db`
SHA-256: `F887994C6DA9F685C1E7D6656858886AB422EFEE612C111D1704C9920909054C`
SQLite integrity: `ok`; rows: 329

## Runtime and paths

- Lite worker: PID 22984, child of the venv redirector PID 21496; heartbeat healthy before the intentional audit stop.
- Advanced shadow worker remained separate: base interpreter PID 23256, parent redirector PID 22560.
- Dashboard returned HTTP 200 on `127.0.0.1:8503`; advanced exporter PID 3240 remained separate.
- Lite DB: `D:\claude\poly_alpha_sniper\data\poly_alpha_lite.db`.
- Lite export: `D:\claude\agent_readonly\poly_alpha_lite\lite_dashboard.json`.
- Lite runtime: `D:\claude\poly_alpha_sniper\runtime\lite_shadow`.
- DB + WAL + SHM at snapshot: 4,775,440 bytes; exported write rate: 49 writes/minute.
- Lite stdout/stderr logs were both zero bytes; exit/resolver attempt history was not reconstructable from logs.

## Exact baseline metrics

| Metric | Before |
|---|---:|
| Entries | 329 |
| Explicit terminal rows with non-null PnL | 266 |
| Open | 0 |
| Pending resolution | 0 |
| Unresolved final | 63 |
| Realized PnL shown | +$87.125 |
| Gross profit | $263.375 |
| Gross loss | $176.25 |
| Wins / losses / flats | 174 / 91 / 1 |
| Win rate | 65.4135338% |
| Profit factor | 1.4943262411 |
| Expectancy | +$0.327537594/trade |
| Average win | +$1.5136494253 |
| Average loss | -$1.9368131868 |
| Average hold | 207.396060 s |
| Max drawdown, timestamp-batched | $10.30 |
| Max drawdown, arbitrary `(exit_ts,id)` row order | $14.40 |
| Observed wall-clock span to snapshot | 5.129494 h |
| Entries/hour over that span | 64.138878 |
| Completed/hour over that span | 51.856965 |

Open/unrealized value was not represented in the schema. Pending and unresolved rows had null PnL and were excluded from the displayed realized metrics.

## Breakdowns

- Entries by asset: BTC 99, ETH 110, SOL 120.
- Terminal PnL by asset: BTC +$24.31, ETH +$44.035, SOL +$18.78.
- Entries by side: BUY_NO 166, BUY_YES 163.
- Terminal rows by side: BUY_NO 130, BUY_YES 136.
- Terminal PnL by side: BUY_NO +$19.70, BUY_YES +$67.425.
- Anchor/no-anchor: 0 / 329.
- Resolution source: 266 `book_exit`, 0 `official_outcome`, 63 `unresolved`.
- Same-side duplicate groups: 0.
- Exact opposite-side asset/window groups: **147** (BTC 41, ETH 48, SOL 58), covering 294 trades.
- Of those conflicts, 85 had two book exits and 62 had one book exit plus one unresolved final.
- Conflict appendix canonical-row SHA-256: `89eba9356c011f7ad34f91095666cd2685b6d4ff2df283c895ea9308bbe15f3a`.

The complete 147-window appendix and every unresolved identity are stored in
`reports/poly_alpha_lite_history_audit_read_only_20260711.json`.

## Baseline reject taxonomy

1. `no_momentum`: 11,007
2. `duplicate_position`: 4,391
3. `price_window`: 2,401
4. `opened`: 329 (incorrectly mixed into the reject table)
5. `expired_market`: 27
6. `no_book`: 7

## Last exported public feed/market evidence

Export timestamp: **2026-07-11 06:40:47.188 WIB**.

- OKX CEX receipt age was 268 ms for all assets: BTC 64,192.3; ETH 1,796.75; SOL 78.09.
- BTC exact market: `btc-updown-5m-1783726800`, market 2865149, event 685947.
- ETH exact market: `eth-updown-5m-1783726800`, market 2865148, event 685949.
- SOL exact market: `sol-updown-5m-1783726800`, market 2865150, event 685950.
- Each market had about 253.08 seconds to close and no optional anchor.

## Evidence limitations frozen with the baseline

The old schema did not store window-open timestamp, full entry/exit book levels,
top-level share depth, source timestamp/hash/condition identity, fees, slippage,
exit attempts, resolver attempt history, direction score, entry mode, or
pullback observations. Therefore the headline PnL was an accounting result, not
a verified executable-fill result. The immutable backup preserves that fact.
