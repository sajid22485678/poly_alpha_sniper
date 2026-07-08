# poly_alpha_sniper

Autonomous Polymarket **5-minute crypto lag-arbitrage** bot. Deterministic,
auditable, fail-safe. **No runtime AI. Live trading is OFF by default.**

## What it does

1. Watches BTC/ETH/SOL in real time on Binance + Bybit (+ optional OKX) WebSockets.
2. When a sharp CEX move ("shock") happens, Polymarket's 5-minute up/down and
   above/below odds often reprice with a delay of a second or more.
3. The bot computes a deterministic fair probability (distance-to-strike /
   ensemble logistic model), compares it with the executable Polymarket ask,
   and only acts when the edge **survives spread, slippage, latency, liquidity,
   expiry risk, market quality, tier gating and every risk check**.
4. Entries and exits are fully autonomous: take-profit, stop-loss, edge decay,
   opposite shock, orderbook flip, momentum fade, max hold, profit lock, and a
   hard force-exit before expiry.
5. Everything is recorded to SQLite: predictions, rejects, signals, orders,
   fills, positions, exits, PnL, latency, incidents, calibration.

The same engines run backtest, simulation, shadow, live_micro and live_full —
there is no separate "backtest logic" that could lie to you.

## Modes

| mode | data | orders | purpose |
|---|---|---|---|
| `simulation` | replay/synthetic | simulated | safe preview |
| `shadow_live` (default) | real | **none** — hypothetical fills logged | measure the edge |
| `live_micro` | real | real, ~$1 | tiny-bankroll live |
| `live_full` | real | real, config-bounded | after live_micro proves out |

## Install (Windows)

```powershell
powershell -ExecutionPolicy Bypass -File scripts/install_windows.ps1
powershell -ExecutionPolicy Bypass -File scripts/setup_env.ps1   # creates .env
```

Then edit `.env`:

- **Telegram**: create a bot with [@BotFather](https://t.me/BotFather) → token.
  Message your bot once, then get your chat id from
  `https://api.telegram.org/bot<TOKEN>/getUpdates`. Fill `TELEGRAM_BOT_TOKEN`
  and `TELEGRAM_CHAT_ID`. Test: `python -m poly_alpha_sniper.tools.test_telegram`
- **Dashboard**: set `DASHBOARD_USERNAME` / `DASHBOARD_PASSWORD`.
- **Polymarket (live only)**: export your wallet private key from your
  Polymarket account (Settings → Export private key) into
  `POLYMARKET_PRIVATE_KEY`, set `POLYMARKET_FUNDER_ADDRESS` to your deposit
  address, and set `POLYMARKET_SIGNATURE_TYPE` (0 = raw EOA wallet, 1 =
  email/Magic login, 2 = browser wallet proxy — check current Polymarket docs).
  Then derive API creds:
  `python -m poly_alpha_sniper.tools.create_polymarket_api_credentials --write-env`
  (backs up `.env` to `.env.backup` first). Verify read-only:
  `python -m poly_alpha_sniper.tools.test_polymarket_auth`
  Live also needs `pip install py-clob-client`.

**Never commit `.env`.** The bot never logs, stores, displays or sends secrets.

## Run

```powershell
python main.py --mode shadow_live        # default, no orders — START HERE
python main.py --mode simulation
python main.py --profile live_micro_safe # profile-driven
python main.py --mode live_micro         # blocked unless ALL live gates pass
python -m poly_alpha_sniper.core.watchdog --mode shadow_live   # supervised
streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port 8501
pytest -q                                 # test suite
python -m poly_alpha_sniper.reporting.stats_report
```

or double-click `scripts\start_shadow.bat`, `scripts\start_dashboard.bat`,
`scripts\start_watchdog.bat`.

## Live gates (why live is impossible by default)

Live orders require **all** of: `LIVE_TRADING_ENABLED=true`,
`I_UNDERSTAND_REAL_MONEY_RISK=true`, `dry_run=false`, a live trading mode,
`MAX_REAL_TRADE_USD > 0` (and >= config max trade), valid config, passing
preflight (config/db/logs/telegram/dashboard-auth/duplicate-process/network),
reconciled balance + positions + open orders, working Telegram critical
alerts, no panic, no kill switch, fresh CEX + book data, healthy rate limiter,
and a constructible authenticated client. Any failure = the process refuses to
start live. A Telegram command can never bypass any of this.

## Risk caps (config.yaml `risk:`)

$10 starting bankroll defaults: $1 trades (min = max = $1), max 2 open
positions, max daily loss $2 (and 20% of equity), stop after 2 consecutive
losses, 30% total / 10% per-market exposure caps, one position per market, no
martingale, no averaging down, **compounding uses realized PnL only**.
Capital scaling ladder exists but `auto_increase_size: false` — the cap stays
$1 until you change it yourself.

## Balanced alpha gate & tiers

Signals are tiered deterministically: **A_PLUS** (edge ≥ 10%, high confidence
and quality) always tradable; **A** tradable in NORMAL/AGGRESSIVE; **B**
shadow-only in NORMAL, tradable only in AGGRESSIVE; **C** rejected. Hard
failures (stale data, unclear token mapping, wide spread, panic, caps...)
always reject; soft weaknesses only subtract score. Borderline-edge signals
get a WAIT → book/CEX refresh → single re-evaluation.

## Adaptive aggression

DEFENSIVE / NORMAL / AGGRESSIVE from rolling performance (last 20 trades):
loss streak ≥ 2, drawdown ≥ 15%, PF < 0.9 or bad fills → DEFENSIVE (30-min
cooldown). AGGRESSIVE needs ≥ 20 trades, PF ≥ 1.25, winrate ≥ 52%, edge
realization ≥ 0.6, low drawdown, good fills. Mode changes are Telegram-alerted.

## Compounding with $10

Equity = $10 + **realized** PnL. Position size = 10% of equity clamped to
[$1, $1]. Unrealized gains never increase size; daily PnL resets at UTC
midnight; the daily-loss cap uses min($2, 20% of equity).

## Backtest

```powershell
python -m poly_alpha_sniper.backtest.download_cex_data --days 3
python -m poly_alpha_sniper.backtest.replay --cex data/cex.csv
python -m poly_alpha_sniper.backtest.walk_forward --cex data/cex.csv
python -m poly_alpha_sniper.backtest.monte_carlo --cex data/cex.csv
python -m poly_alpha_sniper.strategy.calibration_report
python -m poly_alpha_sniper.backtest.download_polymarket_snapshots --minutes 30  # capture real books
```

The replay engine drives the exact production pipeline on historical ticks
with a simulated clock. Without real Polymarket book history it synthesizes
**delayed** odds (the lag is configurable) and labels every result
**RESEARCH ONLY** — treat those numbers as an upper bound on the edge. Output
includes fixed-size vs compounded results (with a sizing-dependence warning),
full tier/aggression breakdowns, B-taken-vs-skipped counterfactual, reject
counts, slippage, edge realization, per-hour/per-market PnL and data-driven
answers to: does B-tier pay, which aggression mode wins, best trade rate, best
edge floor, which tiers to live-enable. Walk-forward rejects configs that are
train-good/test-bad, one-lucky-day, sizing-dependent, too-few-trades or
PF-unstable. Monte Carlo stresses shuffle/slippage/missed fills/latency/API
errors and reports probability of profit, median & 5th-percentile ending
equity, worst drawdown and risk of ruin.

## Dashboard (read-only)

`scripts\start_dashboard.bat` → http://localhost:8501. Login uses
`DASHBOARD_USERNAME`/`DASHBOARD_PASSWORD` from `.env`. Shows status/gates/
panic/kill, equity + ROI + drawdown, trades, winrate/PF/expectancy, edges,
fill quality, latency percentiles, YES/NO positions, order lifecycle, rejects
and near misses, tier distribution + per-tier PnL + B allowed/skipped,
frequency vs target with over/under-trade warnings, calibration plot,
incidents/reconciliation/watchdog/backups/rate limits. With no database yet it
shows demo rows labeled **DEMO DATA — NOT REAL BOT DATA**. There are **no
trading buttons** anywhere.

**Phone access (same Wi-Fi):** run `ipconfig`, note the IPv4 address, allow
the port once (admin PowerShell):
`netsh advfirewall firewall add rule name="PAS Dashboard" dir=in action=allow protocol=TCP localport=8501`
then open `http://LOCAL_PC_IP:8501` in Safari/Chrome. For remote access
install [Tailscale](https://tailscale.com) on PC + phone and use the
Tailscale IP. **Do not port-forward the dashboard to the public internet.**

## Telegram controls (monitoring/admin only — never trade approval)

`/status /health /daily /positions /open_orders /pnl /mode /latency /budget`
(info), `/pause /panic /cancel_orders /disable_live /mode_shadow
/blacklist_market /whitelist_market /disable_asset /enable_asset /backup_now`
(admin), and `/resume /clear_panic /close_all /mode_live_micro` which require
a second `/confirm <cmd>` within 60 s. Only your `TELEGRAM_CHAT_ID` is
honored. There is no `/approve_trade` — trading is autonomous, and commands
route through the same risk APIs as everything else (no bypass possible).

## Watchdog, crash safety, backups

The watchdog supervises the bot process, watches `runtime/heartbeat.json`, and
restarts on crash or stale heartbeat up to 5×/hour (then writes an incident
and gives up). State persists in `runtime/state.json` (atomic writes); a lock
file prevents duplicate live instances; on live restart the account is
reconciled before trading resumes — mismatch = frozen in panic. SQLite is
backed up every 30 min to `backups/` (keeps 20);
`scripts\backup_database.ps1` / `scripts\restore_database.ps1 -Backup <file>`
do it manually (restore requires confirmation and makes a safety copy).

## Panic mode

Triggers: stale feeds, repeated API/order failures, unknown orders, balance or
position mismatch, daily loss cap, drawdown breach, failed emergency exit.
Actions: block entries → cancel all orders → emergency-close positions →
freeze live → Telegram critical alert → incident report → **manual reset
required** (`/clear_panic`, then `/resume`).

## Troubleshooting

- **Bybit unreachable**: some networks block bybit.com; the connector
  automatically falls back to the bytick.com mirror (see `cex.bybit_ws_url`).
- **`python` opens the Microsoft Store**: use the full path
  `%LOCALAPPDATA%\Programs\Python\Python312\python.exe` or the venv python.
- **Dashboard empty**: no data yet — run shadow mode first (demo data shown).
- **live_micro refuses to start**: read the printed gate list; every line is a
  specific unmet requirement.
- **No trades in shadow**: normal in quiet markets; check `/status`, the
  near-miss panel and reject breakdown to see what the gate is rejecting.

## Known limitations

- Synthetic-odds backtests are RESEARCH ONLY; real captured book history
  (`download_polymarket_snapshots`) gives honest numbers.
- The simulator/shadow fill model is taker-only (no maker-fill credit) —
  conservative by design.
- Polymarket 5-min series discovery depends on current Gamma metadata
  conventions; if Polymarket renames series, update
  `discovery/market_universe_expander.py` query plans.
- `py-clob-client` is only imported for live trading; its API can change —
  the wrapper is isolated in `connectors/polymarket_clob_private.py`.
- Round-trip fees/gas are not modeled beyond spread+slippage (Polymarket CLOB
  is currently fee-free for takers; revisit if that changes).

## Architecture guarantee

One pipeline, five modes. `docs/BUILD_SPEC.md` documents the binding internal
interfaces. Every trade decision writes its full audit trail (gate output,
risk decision, validation, final decision) to SQLite.
