"""Plotly figure builders (pure functions returning go.Figure)."""
from __future__ import annotations

import plotly.graph_objects as go

_LAYOUT = dict(margin=dict(l=30, r=15, t=35, b=30), height=280,
               paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
               font=dict(size=11))


def equity_curve_fig(points: list[dict]) -> go.Figure:
    fig = go.Figure()
    if points:
        fig.add_trace(go.Scatter(
            x=[p["ts_ms"] for p in points], y=[p["equity"] for p in points],
            mode="lines", name="equity", line=dict(width=2)))
    fig.update_layout(title="Equity ($, realized)", **_LAYOUT)
    return fig


def pnl_by_tier_fig(pnl_map: dict[str, float]) -> go.Figure:
    tiers = list(pnl_map.keys())
    fig = go.Figure(go.Bar(x=tiers, y=[pnl_map[t] for t in tiers],
                           marker_color=["#2e7d32" if pnl_map[t] >= 0 else "#c62828"
                                         for t in tiers]))
    fig.update_layout(title="PnL by tier ($)", **_LAYOUT)
    return fig


def tier_distribution_fig(dist: dict[str, int]) -> go.Figure:
    fig = go.Figure(go.Pie(labels=list(dist.keys()), values=list(dist.values()),
                           hole=0.5))
    fig.update_layout(title="Tier distribution", **_LAYOUT)
    return fig


def latency_fig(percentiles: dict[str, dict]) -> go.Figure:
    stages = list(percentiles.keys())
    fig = go.Figure()
    for q in ("p50", "p95", "p99"):
        fig.add_trace(go.Bar(name=q, x=stages,
                             y=[percentiles[s].get(q, 0) for s in stages]))
    fig.update_layout(title="Latency (ms)", barmode="group", **_LAYOUT)
    return fig


def calibration_fig(buckets: list[dict]) -> go.Figure:
    fig = go.Figure()
    populated = [b for b in buckets if b.get("n")]
    fig.add_trace(go.Scatter(x=[0, 1], y=[0, 1], mode="lines", name="perfect",
                             line=dict(dash="dash", width=1)))
    if populated:
        fig.add_trace(go.Scatter(
            x=[b["mean_pred"] for b in populated],
            y=[b["winrate"] for b in populated],
            mode="markers+lines", name="model",
            marker=dict(size=[max(6, min(20, b["n"])) for b in populated])))
    fig.update_layout(title="Calibration (predicted vs realized)", **_LAYOUT)
    return fig


def frequency_fig(actual: float, target: float, maximum: float) -> go.Figure:
    fig = go.Figure(go.Bar(x=["actual", "target", "max"],
                           y=[actual, target, maximum],
                           marker_color=["#1565c0", "#2e7d32", "#c62828"]))
    fig.update_layout(title="Trades/hour vs target", **_LAYOUT)
    return fig


def drawdown_fig(points: list[dict]) -> go.Figure:
    fig = go.Figure()
    if points:
        peak = points[0]["equity"]
        dd = []
        for p in points:
            peak = max(peak, p["equity"])
            dd.append((p["equity"] - peak) / peak * 100 if peak > 0 else 0)
        fig.add_trace(go.Scatter(x=[p["ts_ms"] for p in points], y=dd,
                                 fill="tozeroy", name="drawdown %"))
    fig.update_layout(title="Drawdown from peak (%)", **_LAYOUT)
    return fig
