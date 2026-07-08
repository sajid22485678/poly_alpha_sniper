"""Dashboard auth gate (username/password from .env, constant-time compare).

Streamlit is imported inside functions so tests can import this module
without a streamlit runtime. Secrets are never rendered.
"""
from __future__ import annotations

import hmac


def credentials_ok(username: str, password: str, secrets) -> bool:
    expected_user = secrets.get("DASHBOARD_USERNAME")
    expected_pass = secrets.get("DASHBOARD_PASSWORD")
    if not expected_user or not expected_pass:
        return False
    return (hmac.compare_digest(username.encode(), expected_user.encode())
            and hmac.compare_digest(password.encode(), expected_pass.encode()))


def check_auth() -> bool:
    """Returns True when the current streamlit session is authenticated."""
    import streamlit as st

    from poly_alpha_sniper.core.config_loader import load_secrets

    secrets = load_secrets()
    if not secrets.dashboard_auth_enabled:
        return True
    if not secrets.has("DASHBOARD_USERNAME") or not secrets.has("DASHBOARD_PASSWORD"):
        st.error("Dashboard auth is enabled but DASHBOARD_USERNAME / "
                 "DASHBOARD_PASSWORD are not set in .env. Access denied.")
        return False
    if st.session_state.get("pas_authed") is True:
        return True
    st.markdown("### 🔒 Poly Alpha Sniper — login")
    with st.form("login"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Log in")
    if submitted:
        if credentials_ok(username, password, secrets):
            st.session_state["pas_authed"] = True
            st.rerun()
        else:
            st.error("Invalid credentials.")
    return False
