"""Premium dark 'control room' visual theme. CSS/markup only -- no data
logic, no network calls, nothing that could touch secrets or place orders."""
from __future__ import annotations

PREMIUM_CSS = """
<style>
:root {
  --pas-bg: #0b0e14;
  --pas-card: #121826;
  --pas-border: rgba(255,255,255,0.08);
  --pas-accent: #3b82f6;
  --pas-green: #22c55e;
  --pas-red: #ef4444;
  --pas-amber: #f59e0b;
  --pas-muted: #8b95a8;
}
.stApp { background: linear-gradient(180deg, var(--pas-bg) 0%, #0e1220 100%); }
h1, h2, h3 { letter-spacing: 0.02em; }
.pas-hero {
  background: linear-gradient(135deg, rgba(59,130,246,0.14), rgba(18,24,38,0.5));
  border: 1px solid var(--pas-border);
  border-radius: 16px;
  padding: 1.1rem 1.4rem;
  margin-bottom: 0.8rem;
}
.pas-hero-title { font-size: 1.5rem; font-weight: 700; margin: 0; color: #f5f7fa; }
.pas-hero-sub { color: var(--pas-muted); font-size: 0.85rem; margin-top: 0.25rem; }
div[data-testid="stMetric"] {
  background: var(--pas-card) !important;
  border: 1px solid var(--pas-border) !important;
  border-radius: 12px !important;
  padding: 0.7rem 0.8rem !important;
}
div[data-testid="stMetricLabel"] { color: var(--pas-muted) !important; font-size: 0.78rem !important; }
div[data-testid="stMetricValue"] { font-size: 1.25rem !important; font-weight: 700 !important; }
div[data-testid="stExpander"] {
  background: var(--pas-card);
  border: 1px solid var(--pas-border);
  border-radius: 12px;
}
.pas-section-title {
  font-size: 1.0rem; font-weight: 700; color: #f5f7fa;
  margin: 0.6rem 0 0.3rem 0; padding-bottom: 0.2rem;
  border-bottom: 1px solid var(--pas-border);
}
.pas-disclaimer {
  color: var(--pas-amber); font-size: 0.78rem; font-style: italic;
  border-left: 3px solid var(--pas-amber); padding-left: 0.6rem; margin: 0.4rem 0;
}
.pas-not-implemented, .pas-not-available {
  color: var(--pas-muted); font-style: italic; font-size: 0.85rem;
}
.pas-placeholder-card {
  background: var(--pas-card); border: 1px dashed var(--pas-border);
  border-radius: 10px; padding: 0.6rem 0.8rem; color: var(--pas-muted);
  font-size: 0.85rem;
}
.scroll-table table { min-width: 640px; }
</style>
"""


def get_premium_css() -> str:
    return PREMIUM_CSS


def inject_premium_theme() -> None:
    import streamlit as st
    st.markdown(PREMIUM_CSS, unsafe_allow_html=True)
