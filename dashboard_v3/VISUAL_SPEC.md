# Dashboard v3 — Visual Spec

Derived from the reference screenshot the user posted in-chat (an X/Twitter
marketing image for a third-party "Claude x Hermes BTC 5-min Polymarket
agent" product). WebFetch on the tweet URL returned `402 Payment Required`
and no Chrome browser extension was connected, so the tweet page itself was
never rendered — this spec is built entirely from the screenshot image the
user pasted directly into chat, not from guessing.

This file is the layout/style source of truth for every component in
`dashboard_v3/`. Content honesty rules (no fake data, no invented metrics,
"not available"/"NOT IMPLEMENTED" where we lack a real equivalent) are
enforced separately in `SCREENSHOT_COMPARISON_NOTES.md` and in the
components themselves — this file only describes the *visual* target.

## 0. Desktop viewport target

Designed at **1280–1440px** wide, dense information layout, dark mode only
(no light theme — the reference has no light variant). Minimum usable width
~1100px before panels should start stacking.

## 1. Overall page layout

Single-column vertical stack of full-width "sections," each section itself
often split into a horizontal row of 2+ cards. No sidebar, no left nav. A
sticky-feeling top hero bar, a bottom ticker footer. Everything else scrolls
between those two anchors. Consistent horizontal page margin (~24px desktop
gutters), consistent vertical rhythm (~16–20px gap between sections).

## 2. Header / hero bar (top)

Single row, three zones:
- **Left**: square badge with a single glyph ("H" in the reference) inside a
  thin-bordered rounded box, then a two-line title block: bold title line
  ("Claude x Hermes"), smaller uppercase gold subtitle line ("BTC 5-MIN
  POLYMARKET AGENT").
- **Center**: a thin row of small-caps nav-style labels separated by
  mid-dots ("Markov · Kelly · Self-learn") — muted gray, no active/hover
  chrome visible, reads as static capability labels rather than clickable
  nav.
- **Right**: a pill-shaped badge, dark fill, thin border, a small colored
  status dot + text ("● 23:00 ET").

## 3. PnL hero banner (below header)

Centered, three-zone horizontal band:
- **Left column**: two stacked small-caps labels — muted gray top line
  ("ALL-TIME · PNL"), green line below ("Verified on-chain").
- **Center**: an italic/serif accent line above the number ("Anonymous
  Whale"), a monospace gray line below that (wallet address · chain), then
  a very large bold display number with a gold `$` glyph ("$881,418") —
  this is the single largest text element on the page.
- **Right column**: two stacked small-caps labels, right-aligned — gray top
  line ("14 MONTHS · LIVE"), gold line below ("Calmar 6.14").

## 4. KPI card row

Row of equal-width cards (4 in the reference; we use more to fit 9 metrics
— see comparison notes). Each card: dark card surface, thin 1px border,
generous internal padding, no shadow. Content stacked top-to-bottom inside:
small-caps muted label, large bold number (white), small muted subtext line
underneath giving context. No icons in these cards in the reference.

## 5. Signal/state panel (two cards side by side)

**Left card** — "state transition" style:
- Header row: small colored dot + small-caps title
  ("MARKOV STATE TRANSITION · BTC 5-MIN").
- Two boxed state chips side by side with an arrow between them: each chip
  has a bold state label ("UP") and a small-caps sublabel below ("S0 ·
  CURRENT" / "S4 · NEXT"). A small gold pill sits between/above the arrow
  showing a probability ("p=0.89").
- A row of 3 compact metric columns beneath (label + value), no card
  border between them, just spacing.
- A full-width pill-shaped button/badge below that, dark fill, centered
  text ("SIGNAL · ENTER").
- A final row of 3 small stat columns at the card bottom (label above,
  number below), separated by a thin top border from the button above.

**Right card** — "formula log" style:
- Stack of 2–3 formula blocks, each block = one bold monospace formula line
  followed by one smaller/muted monospace line showing the formula with
  real numbers substituted and a result. Thin horizontal divider lines
  between blocks. Entirely monospace font in this card.

## 6. Primary chart panel

Full-width card:
- Header row: left = small-caps chart title ("BTC / USD · 5-MIN"), right =
  a run of compact stat labels (H/L/VOL/last price) ending in a small
  rounded pill badge colored green/red showing a percent change.
- Chart body: a combo chart — thin light line/area for price across the
  top ~70% of the chart height, low-opacity vertical bars (volume-style)
  filling the bottom ~30%, sharing the same x-axis. Sparse floating
  annotation labels sit directly on/above the line at specific points
  (small pill or plain text, e.g. a dollar delta), plus one emphasized
  circular marker with a callout line to a highlighted price label pinned
  to the right edge of the chart (the "current value" callout). X-axis:
  sparse time ticks along the bottom, muted gray monospace. Y-axis: sparse
  price ticks on the right edge only (no left axis).
- No gridlines, or extremely faint gridlines only.

## 7. "Stack" info-card row

Header row above the cards: small-caps title + cost/setup-time subtext on
the left, small-caps muted note on the right.
Row of 4 equal cards, each:
- Small pill tag top-left, colored fill (dark gold/tan bg, dark text),
  short uppercase word ("BRAIN", "BODY", "RUNNER", "ALERTS").
- Bold title line below the tag ("Claude Opus 4.7").
- 1–2 lines of small muted description text.
- At the card bottom: a small-caps metric label + value pair, then a thin
  (2–3px) horizontal progress/level bar beneath it, gold fill on a dark
  track, no percentage text needed — reads as a decorative activity meter.

## 8. Two-column "process" section

**Left card** — numbered step list ("SELF-LEARNING NIGHTLY LOOP"):
- Header row with title + a pill badge top-right showing a countdown/time
  ("NEXT REVIEW 04:22:41").
- 3 rows, each: a two-digit index ("01"), a small line icon, a bold
  uppercase step title, and a muted description line — all left-aligned in
  a loose grid. One row (the currently-relevant one) has a visually
  distinct card-within-card treatment: its own thin border/background
  wrapping just that row, setting it apart from the other two plain rows.

**Right card** — activity/report feed style:
- Header row: small icon + bold name ("Hermes Trading Bot"), small-caps
  status line below with a colored dot ("● ONLINE · @handle").
- Sub-header: small-caps section label + date ("NIGHTLY REPORT · MAY 17").
- Row of 3 compact metric columns (label + value, one value colored gold,
  one colored green).
- A single highlighted pill/row showing a before→after config change.
- A tagged activity row: small colored tag ("FILL") + one line of compact
  monospace-flavored trade detail + a small timestamp with a checkmark at
  the right edge.
- Footer line: muted italic "is typing…" style status text.

## 9. Bottom ticker footer

Full-width thin bar, top border only (no card look), dark background.
Single row of short `LABEL VALUE` pairs separated by generous whitespace,
small uppercase text, muted gray with occasional gold emphasis on values.
Reads like a market-data ticker tape.

## 10. Color palette (sampled from the reference)

| Token | Approx hex | Use |
|---|---|---|
| `--v3-bg` | `#000000`–`#060605` | page background |
| `--v3-card` | `#0c0b09` | card surfaces |
| `--v3-card-border` | `#2a2620` (≈12% opacity gold-gray) | 1px card borders |
| `--v3-gold` | `#c9a35a` | primary accent, labels, dividers |
| `--v3-gold-bright` | `#e8c876` | emphasized gold text/numbers |
| `--v3-cream` | `#f3ead9` | hero display number, high-emphasis white-ish text |
| `--v3-green` | `#5fd98a` | positive values, online/verified state |
| `--v3-red` | `#e5615a` | negative values, alert dot |
| `--v3-muted` | `#8b8578` | small-caps labels, secondary text |
| `--v3-muted-2` | `#5c584e` | tertiary/disabled text, dividers |

## 11. Typography

- **Display/hero numbers** (the big PnL figure, KPI card numbers): a serif
  or high-contrast display face for the hero number specifically (the `$`
  glyph reads distinctly serif); KPI card numbers use a bold sans.
  We use `Georgia, 'Times New Roman', serif` for the one hero PnL figure
  and system sans (`-apple-system, Segoe UI, Inter, sans-serif`) everywhere
  else for parity without a font-loading dependency.
- **UI text / labels**: clean geometric sans, small sizes (11–13px) with
  wide letter-spacing (~0.08em) and uppercase for all small-caps labels.
- **Data / formulas / addresses / tickers**: monospace
  (`'SF Mono', 'Cascadia Code', Consolas, monospace`).

## 12. Spacing, radius, borders, shadows

- Card border radius: **10–12px**. Pills/badges: **fully rounded** (999px).
- Card padding: **16–20px**. Section gaps: **16–20px**. Grid gutters: **12–16px**.
- Borders: 1px, low-opacity gold-gray — never a bright/high-contrast border.
- No drop shadows. Flat surfaces; the only "glow" is color emphasis on gold
  text, not a blur/box-shadow effect.

## 13. Tables

Not heavily featured in the reference (the closest is the compact fill row
in the activity feed), so the trade-tape table follows the same visual
language as the cards: no heavy grid lines, thin 1px row dividers only,
small-caps column headers, monospace numeric cells, right-aligned numbers,
generous row padding, alternating-row background is *not* used in the
reference (flat, divider-only).

## 14. Icons / badges

Minimal single-color line icons only (play, circle, pencil, plane) sized
~14–16px, no icon fills, no multi-color icon sets. Status dots are solid
filled circles, 6–8px, colored by state (green/red/gold/gray).

## 15. Animation / refresh behavior

Nothing overtly animated in a static screenshot; the "typing…" footer text
implies a subtle live/pulsing feel. Dashboard v3 auto-refreshes its data
every 3s (per spec) — we reflect *data* freshness via the heartbeat dot and
a "last updated" timestamp rather than full-page flicker/reload.
