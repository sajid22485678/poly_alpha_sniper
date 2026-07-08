from poly_alpha_sniper.core.config_loader import Secrets
from poly_alpha_sniper.dashboard.auth import credentials_ok
from poly_alpha_sniper.dashboard.db_reader import DEMO_LABEL, DashboardData, demo_data
from poly_alpha_sniper.dashboard.mobile_layout import get_mobile_css, layout_columns


def test_mobile_css_has_required_rules():
    css = get_mobile_css()
    assert "overflow-x: auto" in css
    assert "@media (max-width: 640px)" in css
    assert ".scroll-table" in css
    assert ".demo-banner" in css


def test_layout_columns_collapse_on_mobile():
    assert layout_columns(4, is_mobile=True) == 2
    assert layout_columns(4, is_mobile=False) == 4
    assert layout_columns(1, is_mobile=True) == 1


def test_demo_data_clearly_labeled():
    d = demo_data()
    assert d["label"][0]["label"] == "DEMO DATA — NOT REAL BOT DATA"
    assert all(row.get("source") == "DEMO" for row in d["predictions"])
    assert all("[DEMO]" in row["market_title"] for row in d["predictions"])


def test_db_reader_missing_file_safe(tmp_path):
    data = DashboardData(str(tmp_path / "nope.db"))
    assert data.has_data is False
    assert data.safe_query("SELECT * FROM predictions") == []
    assert data.predictions() == []
    assert data.recent("not_a_table") == []


def test_db_reader_reads_real_db(tmp_path):
    from poly_alpha_sniper.storage.migrations import run_migrations
    from poly_alpha_sniper.storage.sqlite_store import SqliteStore
    path = str(tmp_path / "real.db")
    s = SqliteStore(path)
    run_migrations(s)
    s.insert("predictions", {"ts_ms": 1, "asset": "BTC", "tier": "A"})
    s.close()
    data = DashboardData(path)
    assert data.has_data is True
    assert data.predictions()[0]["asset"] == "BTC"


def test_auth_constant_time_compare():
    secrets = Secrets(env={"DASHBOARD_USERNAME": "admin", "DASHBOARD_PASSWORD": "pw123"})
    assert credentials_ok("admin", "pw123", secrets)
    assert not credentials_ok("admin", "wrong", secrets)
    assert not credentials_ok("", "", secrets)
    empty = Secrets(env={})
    assert not credentials_ok("admin", "pw123", empty)  # unset creds deny all


def test_dashboard_has_no_trading_controls():
    from pathlib import Path
    app_source = (Path(__file__).resolve().parent.parent / "dashboard" / "app.py"
                  ).read_text(encoding="utf-8")
    for forbidden in ("place_order", "cancel_order", "buy_button", "sell_button",
                      "st.button(\"Buy", "st.button(\"Sell"):
        assert forbidden not in app_source
