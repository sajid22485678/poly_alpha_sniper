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
    """Standard semantics: True (safe/healthy) -> green, False -> red. For the
    live-trading toggle specifically -- where False is the safe state but
    must still read as visually alarming/prominent -- use live_status_badge()
    instead, not this generic helper."""
    import streamlit as st
    parts = []
    for name, val in states.items():
        if isinstance(val, bool):
            cls = "pas-badge-ok" if val else "pas-badge-off"
            icon = "🟢" if val else "🔴"
            parts.append(f'<span class="{cls}">{icon} {name}</span>')
        else:
            parts.append(f'<span class="pas-badge-neutral">🔹 {name}: {val}</span>')
    st.markdown(" &nbsp; ".join(parts), unsafe_allow_html=True)


def live_status_badge(live_enabled: bool, real_orders_disabled: bool = True) -> None:
    """Prominent, unmissable LIVE indicator. Always renders in red when live
    is off (the expected, safe state) -- matches a studio on-air light
    convention: red = not broadcasting live. If live_enabled is ever True
    this renders a loud red ALERT rather than green, since that state should
    never be reached in this project and must never look reassuring."""
    import streamlit as st
    if not live_enabled:
        st.markdown(
            '<span class="pas-badge-off">🔴 LIVE: OFF</span> &nbsp; '
            '<span class="pas-badge-off">🔴 REAL ORDERS: DISABLED</span>',
            unsafe_allow_html=True)
    else:
        st.markdown(
            '<span class="pas-badge-off">🔴 LIVE: ON — VERIFY IMMEDIATELY</span> &nbsp; '
            f'<span class="pas-badge-off">🔴 REAL ORDERS: '
            f'{"DISABLED" if real_orders_disabled else "ENABLED"}</span>',
            unsafe_allow_html=True)


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


def hermes_brief_card(brief: dict) -> None:
    """Renders the Hermes Brief panel from a structured dict (see
    reporting.agent_export.build_hermes_brief). Never fabricates a value --
    an unavailable brief renders as an honest placeholder, not blank fields."""
    import streamlit as st
    if not brief.get("available"):
        st.markdown(f'<div class="pas-placeholder-card">Hermes Brief: not available '
                   f'({brief.get("reason", "no exported report found")})</div>',
                   unsafe_allow_html=True)
        return
    st.markdown(
        '<div class="pas-hermes-brief">'
        f'<b>Verdict:</b> {brief["verdict"]}<br>'
        f'<b>Top blocker:</b> {brief["top_blocker"]}<br>'
        f'<b>Anomaly:</b> {brief["anomaly"]}<br>'
        f'<b>Next action:</b> {brief["next_action"]}<br>'
        f'<b>Live readiness:</b> {brief["live_readiness_status"]}'
        '</div>', unsafe_allow_html=True)
