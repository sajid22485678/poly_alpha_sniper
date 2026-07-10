# Poly Alpha Sniper — Dashboard v3

A separate, from-scratch React/Next.js/Tailwind rebuild of the Poly Alpha
Sniper control room, styled to match a hedge-fund-terminal reference
screenshot as closely as possible while staying strictly honest about our
own shadow-mode data. See `VISUAL_SPEC.md` for the layout/style spec and
`SCREENSHOT_COMPARISON_NOTES.md` for what matches the reference vs. what was
intentionally changed for safety/honesty reasons.

This is **v3**, not a replacement. The existing Streamlit dashboard
(`../dashboard/app.py`) keeps running unmodified on `:8501`. v3 is a
completely separate app in this folder, on `:8503`.

## Stack

- Next.js 16 (App Router), React 19, TypeScript, Tailwind CSS v4
- Recharts for charts
- No external UI kit — every card/pill/table is custom, hand-styled to the
  reference's palette (see `src/app/globals.css` for the design tokens)

## Data flow (strictly read-only)

```
poly_alpha_sniper bot (SQLite)
  → reporting/agent_export.py  (unchanged by this build; redacts secrets)
  → D:\claude\agent_readonly\poly_alpha_sniper\*.json
  → dashboard_v3/src/app/api/snapshot/route.ts  (GET-only, hardcoded file list)
  → dashboard_v3 React UI  (polls every 3s)
```

The API route (`src/app/api/snapshot/route.ts`) reads only fixed allow-listed
files, never a path built from a request. The four required advanced files are:

- `dashboard_snapshot.json`
- `latest_status.json`
- `trade_summary.json`
- `reject_breakdown.json`

It also reads two optional status exports: `auto_export_status.json` and the
separate Lite file at
`D:\claude\agent_readonly\poly_alpha_lite\lite_dashboard.json`. Missing Lite
data never changes the advanced dashboard's missing-data banner, baseline PnL,
KPIs, or live-readiness calculations.

It never imports `.env`, never touches `process.env`, and exposes no
POST/PUT/DELETE handler anywhere in the app — there is no write path from
the browser to anything. `tests/test_dashboard_v3_safety.py` (in the main
project) enforces this with static source scans, run as part of the normal
pytest suite.

## Running it

**One-click**: double-click `..\scripts\start_dashboard_v3.bat` (from the
project root) or `D:\TradingVault\Open Poly Dashboard V3.bat` to just open
the browser tab if it's already running.

**Manually**:
```
cd dashboard_v3
npm install    # first time only
npm run dev    # http://127.0.0.1:8503
```

`npm run build && npm run start` runs the production build on the same
port. Both scripts bind to `127.0.0.1` only (loopback), not `0.0.0.0` — v3
is not exposed to the LAN by default.

## If you see "No data yet"

The exporter hasn't run yet (or its output directory doesn't exist). Run
`..\scripts\export_agent_readonly.bat` from the project root, or the bot's
normal export cadence, then the dashboard will pick it up on its next 3s
poll — no restart needed.

## What's NOT implemented (shown honestly in the UI, not hidden)

- Continuation/fade classification (`classification_framework:
  "not_implemented"` from the exporter — surfaced as a "NOT IMPLEMENTED"
  pill in the Signal Engine panel)
- Markov-chain state transitions / Kelly-criterion sizing (not part of this
  bot's strategy — the Signal & Sizing Logic card says so explicitly and
  shows the bot's real edge-gate/min-order formulas instead)
- Per-asset (BTC/ETH/SOL) live freshness grid (the export contract only
  carries the single latest evaluated market + aggregate book-freshness
  counts, not a per-asset breakdown)
- Calmar ratio, infra metrics (uptime/CPU/tokens-per-sec/alerts-sent) — not
  computed anywhere in the codebase, so omitted rather than invented

## Safety invariants this app depends on and never changes

`mode=shadow_live`, `dry_run=True`, `LIVE_TRADING_ENABLED=false`. This app
has no code path that could place, cancel, or modify an order, or change
those flags — it is a pure read-only viewer over already-exported,
already-redacted JSON files.
