"""Streamlit render helpers (imported lazily; read-only widgets only —
NO buy/sell/order controls exist anywhere in the dashboard)."""
from __future__ import annotations

from typing import Any


def metric_card_row(items: list[tuple[str, Any, Any]], is_mobile: bool = False) -> None:
    import streamlit as st

    from poly_alpha_sniper.dashboard.mobile_layout import layout_columns
    per_row = layout_columns(min(len(items), 4), is_mobile)
    for start in range(0, len(items), per_row):
        cols = st.columns(per_row)
        for col, (label, value, delta) in zip(cols, items[start:start + per_row]):
            with col:
                st.metric(label, value, delta)


def status_badges(states: dict[str, bool | str]) -> None:
    import streamlit as st
    parts = []
    for name, val in states.items():
        if isinstance(val, bool):
            icon = "🟢" if val else "🔴"
            parts.append(f"{icon} {name}")
        else:
            parts.append(f"🔹 {name}: {val}")
    st.markdown(" &nbsp; ".join(parts))


def scrollable_table(rows: list[dict], title: str = "", limit: int = 50) -> None:
    import streamlit as st
    if title:
        st.caption(title)
    if not rows:
        st.caption("_no data_")
        return
    import pandas as pd
    df = pd.DataFrame(rows[:limit])
    st.markdown('<div class="scroll-table">', unsafe_allow_html=True)
    st.dataframe(df, width="stretch", height=min(400, 60 + 35 * min(len(df), 10)))
    st.markdown("</div>", unsafe_allow_html=True)


def warning_banner(text: str) -> None:
    import streamlit as st
    st.markdown(f'<div class="demo-banner">{text}</div>', unsafe_allow_html=True)


def section_title(text: str) -> None:
    import streamlit as st
    st.markdown(f'<div class="pas-section-title">{text}</div>', unsafe_allow_html=True)


def hero_header(title: str, subtitle: str) -> None:
    import streamlit as st
    st.markdown(f'<div class="pas-hero"><p class="pas-hero-title">{title}</p>'
               f'<p class="pas-hero-sub">{subtitle}</p></div>', unsafe_allow_html=True)


def not_implemented(label: str) -> None:
    """Honest placeholder for a feature that doesn't exist yet -- never
    fabricate a value in its place."""
    import streamlit as st
    st.markdown(f'<span class="pas-not-implemented">{label}: not implemented</span>',
               unsafe_allow_html=True)


def not_available(label: str, reason: str = "") -> None:
    """Honest placeholder for data that isn't present right now (as opposed
    to a feature that doesn't exist -- see not_implemented)."""
    import streamlit as st
    suffix = f" ({reason})" if reason else ""
    st.markdown(f'<span class="pas-not-available">{label}: not available{suffix}</span>',
               unsafe_allow_html=True)


def disclaimer(text: str) -> None:
    import streamlit as st
    st.markdown(f'<div class="pas-disclaimer">{text}</div>', unsafe_allow_html=True)
