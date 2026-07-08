"""Dashboard v2: premium black/gold 'hedge-fund control room' visual theme.
CSS/markup only -- no data logic, no network calls, nothing that could touch
secrets or place orders.

Palette:
  background   near-black (#050505 -> #0c0c0c gradient)
  cards        dark charcoal with a thin gold hairline border
  accent       gold (#c9a227 / #e0b93a) -- dividers, hero border, headings
  healthy      neon green (#39ff88) -- ONLY for healthy/safe status badges
  danger       red (#ff3b3b) -- ONLY for danger/off/live-disabled badges
  muted text   warm grey (#9a9488)
"""
from __future__ import annotations

PREMIUM_CSS = """
<style>
:root {
  --pas-bg: #050505;
  --pas-card: #111008;
  --pas-border: rgba(201,162,39,0.22);
  --pas-gold: #c9a227;
  --pas-gold-bright: #e6c65c;
  --pas-green: #39ff88;
  --pas-red: #ff3b3b;
  --pas-muted: #9a9488;
}
.stApp { background: linear-gradient(180deg, #050505 0%, #0c0c0a 100%); }
h1, h2, h3 { letter-spacing: 0.03em; color: #f2ead9; }
.pas-hero {
  background: linear-gradient(135deg, rgba(201,162,39,0.10), rgba(5,5,5,0.6));
  border: 1px solid var(--pas-gold);
  box-shadow: 0 0 0 1px rgba(201,162,39,0.08), 0 8px 24px rgba(0,0,0,0.5);
  border-radius: 14px;
  padding: 1.2rem 1.5rem;
  margin-bottom: 0.9rem;
}
.pas-hero-title {
  font-size: 1.6rem; font-weight: 800; margin: 0; color: var(--pas-gold-bright);
  text-shadow: 0 0 18px rgba(201,162,39,0.25);
}
.pas-hero-sub { color: var(--pas-muted); font-size: 0.85rem; margin-top: 0.3rem; }
div[data-testid="stMetric"] {
  background: var(--pas-card) !important;
  border: 1px solid var(--pas-border) !important;
  border-radius: 10px !important;
  padding: 0.7rem 0.8rem !important;
}
div[data-testid="stMetricLabel"] { color: var(--pas-muted) !important; font-size: 0.76rem !important;
  text-transform: uppercase; letter-spacing: 0.04em; }
div[data-testid="stMetricValue"] { font-size: 1.25rem !important; font-weight: 700 !important;
  color: #f2ead9 !important; }
div[data-testid="stExpander"] {
  background: var(--pas-card);
  border: 1px solid var(--pas-border);
  border-radius: 10px;
}
.pas-section-title {
  font-size: 1.02rem; font-weight: 700; color: var(--pas-gold-bright);
  margin: 0.7rem 0 0.35rem 0; padding-bottom: 0.25rem;
  border-bottom: 1px solid var(--pas-border);
  text-transform: uppercase; letter-spacing: 0.05em;
}
.pas-disclaimer {
  color: var(--pas-gold-bright); font-size: 0.8rem; font-weight: 600;
  border-left: 3px solid var(--pas-red); padding-left: 0.7rem; margin: 0.5rem 0;
  background: rgba(255,59,59,0.06); border-radius: 0 6px 6px 0; padding-top: 0.3rem; padding-bottom: 0.3rem;
}
.pas-not-implemented, .pas-not-available {
  color: var(--pas-muted); font-style: italic; font-size: 0.85rem;
}
.pas-placeholder-card {
  background: var(--pas-card); border: 1px dashed var(--pas-border);
  border-radius: 10px; padding: 0.6rem 0.8rem; color: var(--pas-muted);
  font-size: 0.85rem;
}
.pas-badge-ok { color: var(--pas-green); font-weight: 700; }
.pas-badge-off { color: var(--pas-red); font-weight: 700; }
.pas-badge-neutral { color: var(--pas-gold-bright); font-weight: 600; }
.pas-hermes-brief {
  background: var(--pas-card); border: 1px solid var(--pas-gold);
  border-radius: 10px; padding: 0.8rem 1rem; margin: 0.4rem 0;
}
.scroll-table table { min-width: 640px; }
</style>
"""


def get_premium_css() -> str:
    return PREMIUM_CSS


def inject_premium_theme() -> None:
    import streamlit as st
    st.markdown(PREMIUM_CSS, unsafe_allow_html=True)
