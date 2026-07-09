# Live Activation Checklist (LOCKED — read-only reference)

> ⚠️ **DO NOT go live unless the live-readiness scorecard shows `LIVE_READY`
> AND every item below is satisfied.** This file is documentation only. It
> contains **no** script that enables live trading, changes `.env`, or stores
> secrets. Activating live is a deliberate, manual act performed by *you*.

As of the last audit (see `ultra_final_shadow_stabilization_and_live_readiness`)
the verdict is **`LIVE_NOT_READY`** — the system is running `shadow_live`,
`dry_run=True`, `live_enabled=False`, and must stay there until the criteria
below are met.

---

## 1. Exact criteria required before live

The bot enforces live-gating in **two independent layers**. Both must pass.

### Layer A — Statistical / shadow readiness (dashboard scorecard)
Source: `reporting/agent_export.py::build_live_readiness`, shown on Dashboard V3
and in `dashboard_snapshot.json → live_readiness`. Verdict must read
**`LIVE_READY`** with every requirement `ok=true`:

- **50+ completed standard shadow trades** *after* the oracle-aware fixes
  (current: **8** — not met).
- **Profit factor ≥ 1.3** (configured threshold).
- **Positive expectancy** (USD per trade > 0).
- **Max drawdown ≤ 25%**.
- **Max loss streak ≤ 4**.
- **No panic active**, **no kill switch active**.
- **No stuck / unresolved positions**.
- **Zero errors in the last hour.**

### Additional readiness conditions to confirm by eye before flipping live
(these are the "production-grade" bar — verify them manually even once the
scorecard is green):

- **A / A+ tier trades are profitable on their own** (not carried by B-tier).
- **Calibration acceptable** — realized win-rate ≈ modeled probability.
- **Oracle anchor (`price_to_beat`) is available for the candidates you intend
  to trade.** Note: Polymarket publishes `priceToBeat` per-market with a delay
  after each 5-minute window opens, and leaves `eventMetadata=null` the rest of
  the time. Fail-closed means *no anchor → no trade*; this is expected and must
  not be bypassed.
- **CEX freshness / direct book refresh working** (no chronic
  `no_fresh_cex_price` or `book_fetch_failed` spikes caused by a bug rather than
  market conditions).
- **Exporter fresh** — `auto_export_status.json` heartbeat is recent.
- **Dashboard fresh** — Dashboard V3 shows no `STALE` banner.
- **Full test suite passes** and **dashboard `npm run build` passes**.
- **No open blocker is caused by a bug** (only by real market conditions).

### Layer B — Operational live gate (enforced automatically at startup)
Source: `core/live_readiness.py::check_live_readiness`. When you launch a live
mode, the bot **refuses to start (`SystemExit`)** unless ALL of these hold. You
cannot skip this layer:

- `trading_mode` is a live mode (`live_micro` / `live_full`).
- `dry_run` is **false**.
- `.env` `LIVE_TRADING_ENABLED=true`.
- `.env` `I_UNDERSTAND_REAL_MONEY_RISK=true`.
- `.env` `MAX_REAL_TRADE_USD > 0`, and config `max_trade_usd ≤ MAX_REAL_TRADE_USD`.
- Panic mode not active; kill switch not active.
- Telegram critical alerts working (if Telegram enabled).
- Account **reconciled** (balance / positions / open orders fetched cleanly).
- A real trading client is constructed and exposes `place_order`,
  `cancel_order`, `cancel_all`, `get_open_orders`, `get_balance_usd`,
  `get_positions` (so emergency close is possible).
- Wallet balance ≥ the minimum tradable amount.

---

## 2. Exact commands YOU run manually to go live

> These are the only steps. **Claude/automation will not perform them.** You
> edit `.env` yourself; secrets never pass through tooling.

1. **Confirm shadow readiness is green.** Open Dashboard V3 (or read
   `D:\claude\agent_readonly\poly_alpha_sniper\dashboard_snapshot.json`) and
   verify `live_readiness.verdict == "LIVE_READY"`.

2. **Stop the shadow bot** (close its window, or Ctrl-C in the shadow console).

3. **Edit `.env` by hand** (never committed, never shared) and set:
   ```
   LIVE_TRADING_ENABLED=true
   I_UNDERSTAND_REAL_MONEY_RISK=true
   MAX_REAL_TRADE_USD=<your hard per-trade cap, e.g. 1>
   ```
   Leave every other risk cap as-is. Do **not** raise exposure/loss caps to
   "get more trades."

4. **Set `dry_run: false`** in `config.yaml` (the live modes also require this;
   the startup gate checks it).

5. **Start live micro (smallest size) — gates enforced:**
   ```
   powershell -ExecutionPolicy Bypass -File scripts\run_live_micro_windows.ps1
   ```
   or
   ```
   .venv\Scripts\python.exe main.py --mode live_micro
   ```
   If any Layer B gate fails, the bot prints the failed gates and exits. Fix the
   cause — **do not** weaken the gate.

6. **Watch the first live window end-to-end** before walking away: one small
   trade, correct anchor, correct exit, Telegram alerts firing.

---

## 3. Exact rollback plan (return to safe shadow)

1. Stop the live bot (Ctrl-C / close window).
2. In `.env` set:
   ```
   LIVE_TRADING_ENABLED=false
   ```
   (Leaving `I_UNDERSTAND_REAL_MONEY_RISK` either way is harmless once
   `LIVE_TRADING_ENABLED=false`, since Layer B requires both.)
3. In `config.yaml` set `dry_run: true`.
4. Restart shadow:
   ```
   scripts\start_shadow.bat
   ```
   or
   ```
   powershell -ExecutionPolicy Bypass -File scripts\run_shadow_windows.ps1
   ```
5. Confirm `latest_status.json` shows `mode=shadow_live`, `dry_run=true`,
   `live_enabled=false`.

Rollback is always safe: flipping `LIVE_TRADING_ENABLED=false` makes Layer B
refuse to start any live mode.

---

## 4. Exact emergency stop plan (while live, something is wrong)

1. **Immediate:** close the bot console window / Ctrl-C. No new orders can be
   placed once the process is down.
2. **If positions are open**, restart in the SAME live mode briefly so the bot
   reconciles and can run its emergency-close path, OR close positions manually
   in the Polymarket UI. Do not leave naked exposure.
3. **Trip the kill switch / panic** (whichever your runbook uses) so that even a
   restart will not place new orders — Layer B blocks live start while kill or
   panic is active.
4. **Then roll back** using Section 3.
5. Only investigate root cause *after* exposure is flat and the bot is back in
   shadow.

---

## 5. Standing warnings

- **Never** enable live to "get more trades." Low trade frequency here is
  dominated by real market conditions (`no_shock` is by design; `price_to_beat`
  is intermittently published by Polymarket). Going live does not change that —
  it only puts real money behind the same rare, gated signals.
- **Never** bypass: oracle anchor, book freshness, spread/depth, EV, or
  risk/exposure/cash gates. **Never** use Martingale, averaging-down, or revenge
  scaling.
- **Never** commit `.env` or any secret. This checklist and all tooling operate
  without ever reading or writing secrets.
- The dashboard is and stays **read-only**.

> Bottom line: **Live stays OFF until `LIVE_READY` + every box above is ticked,
> and turning it on is a manual `.env` edit you perform deliberately.**
