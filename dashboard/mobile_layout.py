"""Mobile-friendly layout helpers (pure functions testable without streamlit)."""
from __future__ import annotations

MOBILE_CSS = """
<style>
/* viewport handled by streamlit; these rules make phone Safari/Chrome usable */
@media (max-width: 640px) {
  .block-container { padding: 0.6rem 0.5rem !important; }
  div[data-testid="stMetric"] { padding: 0.3rem !important; }
  div[data-testid="stMetricValue"] { font-size: 1.1rem !important; }
  h1 { font-size: 1.3rem !important; }
  h2, h3 { font-size: 1.05rem !important; }
}
div[data-testid="stMetric"] {
  background: rgba(128,128,128,0.08);
  border: 1px solid rgba(128,128,128,0.18);
  border-radius: 10px;
  padding: 0.5rem;
}
.scroll-table { overflow-x: auto; -webkit-overflow-scrolling: touch; }
.scroll-table table { min-width: 640px; }
.demo-banner {
  background: #7a5c00; color: #fff; font-weight: 700;
  padding: 0.6rem 1rem; border-radius: 8px; text-align: center;
}
</style>
"""


def get_mobile_css() -> str:
    return MOBILE_CSS


def layout_columns(n_desktop: int, is_mobile: bool) -> int:
    """Column count for metric card rows: collapse on phones."""
    if is_mobile:
        return min(2, max(1, n_desktop))
    return max(1, n_desktop)


def inject_mobile_css() -> None:
    import streamlit as st
    st.markdown(MOBILE_CSS, unsafe_allow_html=True)
