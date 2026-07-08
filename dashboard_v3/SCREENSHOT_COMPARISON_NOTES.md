# Dashboard v3 — Screenshot Comparison Notes

Reference: the X/Twitter marketing screenshot the user pasted directly into
chat for a third-party "Claude x Hermes BTC 5-MIN Polymarket agent" product
(handle `antpalkin`). The tweet URL itself was never fetched — `WebFetch`
returned `402 Payment Required` on `x.com`, and no Chrome browser extension
was connected (`list_connected_browsers` → `[]`). Everything below is based
on the pasted screenshot image only.

## What matches the reference

- **Overall gestalt**: black/graphite page background, dark card surfaces
  with thin low-opacity gold-gray borders, no drop shadows, small-caps
  muted-gray labels with letter-spacing, gold as the single accent color,
  green/red reserved for positive/negative values.
- **Header layout**: square logo badge + two-line title block on the left,
  a status pill with a colored dot on the right — same structure as the
  reference's "H / Claude x Hermes / BTC 5-MIN..." + "● 23:00 ET" pill.
- **Hero PnL banner**: same three-zone layout (label+status left, big
  centered number, secondary stat right) and the same serif display font
  treatment for the headline number with a gold `$`.
- **KPI card row**: identical card anatomy (small-caps label → bold number
  → muted subtext), just 9 cards instead of 4 to fit the required metric
  list.
- **"Signal engine" card pair**: mirrors the reference's two-card layout
  (a state/decision card with boxed chips + a probability pill, next to a
  monospace "formula log" card with divider-separated formula blocks).
- **Chart styling**: gold line/area fill, no gridlines on the vertical axis,
  right-side-only price axis, sparse time ticks, stat row in the chart
  header ending in a colored pill — same visual language as the reference's
  BTC/USD chart, applied to our cumulative-PnL chart instead.
- **Trade tape / reject taxonomy / Hermes panel**: same card surface,
  divider-only rows (no heavy grid lines), pill-shaped status tags, and the
  reference's two-column "process + activity feed" layout for the
  self-learning-loop / Hermes-report section.
- **Footer ticker**: same thin-bar, label-value-pairs-in-a-row treatment as
  the reference's bottom market-data ticker.

## What is intentionally different (because of our bot's real data)

This is the important section. The reference is a marketing image for a
**different, real-money live product**. Dashboard v3 is a read-only view of
**our own shadow-mode bot**, and several pieces of the reference's content
would be dishonest or unsafe to copy literally:

- **"Anonymous Whale · Verified on-chain · $881,418"** → replaced with
  "Poly Alpha Sniper · shadow_live · dry_run=true · not real funds" and the
  bot's real `all_time_pnl_usd` (currently a few dollars of simulated PnL).
  We never claim on-chain verification or real money — the label reads
  "Shadow — Simulated, Not Live" instead of "Verified on-chain", and it is
  never styled green (which would wrongly read as "verified good").
- **"Markov State Transition" + Kelly-criterion formula card** → this bot
  does **not** implement Markov-chain state transitions or Kelly-criterion
  position sizing (confirmed by grepping the strategy code — no matches).
  Inventing that math would violate the no-fake-data rule. Instead, the
  "Signal & Sizing Logic" card shows the bot's **actual** formulas, pulled
  from real source: the edge gate (`edge = fair_probability −
  executable_price`, from `strategy/edge_engine.py`) and the min-order
  sizing check (`min_required_usd = min_shares × ask_price`, from
  `execution/order_validator.py`), each substituted with real current
  numbers. The card explicitly states Markov/Kelly are NOT IMPLEMENTED.
- **"Calmar 6.14"** → not computed anywhere in this codebase; omitted rather
  than fabricated. The equivalent right-hand slot shows the real
  `sample_size_note` (e.g. "6/30 minimum trades") instead.
- **"THE STACK" (Claude Opus 4.7 / Hermes Agent / Hetzner VPS / Telegram
  Bot cards with TOK/S 42,966, uptime, CPU%, SENT 1,919)** → **omitted
  entirely**. These are real-looking but unverifiable infra metrics; our
  4-file read-only export contract (`dashboard_snapshot.json`,
  `latest_status.json`, `trade_summary.json`, `reject_breakdown.json`)
  doesn't carry token throughput, uptime, CPU%, or a Telegram send-count,
  and inventing plausible-looking numbers for them would be exactly the
  "fake demo data" the safety rules forbid. If this is wanted later, it
  needs new real fields added to `reporting/agent_export.py` first.
- **"SELF-LEARNING NIGHTLY LOOP" step 3: "STRATEGY UPDATE — Rewrites
  MIN_PROB, MIN_EDGE, Kelly. Next run smarter."** → this describes an
  agent that **autonomously rewrites live strategy parameters**. That is
  explicitly forbidden for Hermes in this project ("Hermes must remain
  read-only... no execution capability of any kind"). Our step 3 is
  relabeled "Hermes Advisory Review" and its description states plainly
  that Hermes is advisory-only and never auto-applies strategy or
  threshold changes.
- **"Hermes is typing…"** live-chat-style footer → omitted. It implies a
  live conversational agent; ours shows a real "brief generated `<real
  timestamp>`" line instead, which is honest about being a periodic
  export snapshot, not a live chat session.
- **Per-asset BTC/ETH/SOL market rows** (source, age, freshness per asset)
  → the reference implies a live 3-row grid. Our 4-file export contract
  only carries a single `latest_market_state` (the most recently evaluated
  market, whichever asset that happens to be) plus **aggregate**
  fresh-book counts — there is no per-asset breakdown in the current
  exporter output. The Market Intelligence panel shows the one real
  evaluated-market card plus the real aggregate stats, with an explicit
  note that a true per-asset grid isn't available from this read-only
  contract rather than showing three rows where two would be invented.

## What could not be matched / verified

- **Exact reference layout at the pixel level**: the reference tweet page
  itself was never rendered (see access note above), so this spec and the
  resulting build are based on one static screenshot at whatever resolution
  it was posted at — exact spacing/font-size ratios are a close read, not a
  measured trace.
- **Screenshot-based visual QA in this session was unreliable**: the
  `Claude_Preview` screenshot tool renders this page's full scroll height
  into a fixed-size thumbnail, which visually compresses a tall page (this
  dashboard is ~2900px tall) into a narrow-looking column in the returned
  image — that is a tool rendering artifact, not a real layout bug. It was
  independently confirmed to be a rendering-pipeline issue, not our CSS, by
  inspecting computed styles and bounding boxes directly (`main` measured
  1216.8px wide inside a 1280px-wide viewport with `body` at 1424.8px
  inside a 1440px-wide viewport — both correctly full-width). Verification
  was therefore done via DOM/accessibility-tree inspection
  (`preview_inspect`, `preview_snapshot`, `preview_eval`) rather than pixel
  screenshots. **Recommend opening http://localhost:8503 directly in a
  real browser to do a final visual pass against the reference image** —
  that was not possible from this environment.
- **Font choices**: the reference's exact typeface is unknown (no font
  metadata available from a screenshot); we approximated with Georgia for
  the serif hero number and the system UI font stack elsewhere, per
  `VISUAL_SPEC.md` §11, rather than guessing a specific commercial font.
