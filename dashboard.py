"""
Duval Triangle Dynamic Dashboard — Light Theme
===============================================
Real-time web dashboard built with Plotly Dash.

Run:
    python dashboard.py

Then open http://127.0.0.1:8050 in your browser.
"""

import datetime
import numpy as np
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc

from duval_engine import (
    ZONE_BOUNDARIES_TRI,
    ZONE_DESCRIPTIONS,
    tri_to_cartesian,
    build_cartesian_zone,
    batch_diagnose,
    DGASample,
    DiagnosisResult,
)
from db_connector import MockAdapter, SQLiteAdapter, BaseDBAdapter


# ---------------------------------------------------------------------------
# Configuration — swap this adapter for your real DB connection
# ---------------------------------------------------------------------------

def get_adapter() -> BaseDBAdapter:
    """
    Edit this function to connect to your real database.
        return SQLiteAdapter("transformer_dga.db")
        return PostgreSQLAdapter(host=..., port=5432, dbname=..., user=..., password=...)
        return MySQLAdapter(host=..., port=3306, database=..., user=..., password=...)
        return CSVAdapter("dga_data.csv")
    """
    return MockAdapter(n_transformers=4, n_readings=300)


REFRESH_INTERVAL_MS = 5_000
MAX_POINTS = 200


# ---------------------------------------------------------------------------
# Light-mode color palette
# ---------------------------------------------------------------------------

# Zone fill colors — distinct and readable on white background
# PD was #1a1a2e (near-black) — replaced with purple
ZONE_COLORS_LIGHT = {
    "PD": "#9b59b6",
    "D1": "#e67e22",
    "D2": "#e74c3c",
    "DT": "#3498db",
    "T1": "#1abc9c",
    "T2": "#f39c12",
    "T3": "#27ae60",
}

SEVERITY_COLOR = {
    "LOW":      "#27ae60",
    "MEDIUM":   "#f39c12",
    "HIGH":     "#e67e22",
    "CRITICAL": "#e74c3c",
}

BG_PAGE   = "#f4f6f9"
BG_WHITE  = "#ffffff"
BORDER    = "#dee2e6"
TEXT_DARK = "#212529"
TEXT_MID  = "#495057"
TEXT_LITE = "#6c757d"
ACCENT    = "#c0392b"


# ---------------------------------------------------------------------------
# Duval Triangle figure builders
# ---------------------------------------------------------------------------

def build_triangle_background() -> list:
    traces = []

    for zone, tri_pts in ZONE_BOUNDARIES_TRI.items():
        cart = build_cartesian_zone(tri_pts)
        xs = [p[0] for p in cart] + [cart[0][0]]
        ys = [p[1] for p in cart] + [cart[0][1]]
        cx = np.mean([p[0] for p in cart])
        cy = np.mean([p[1] for p in cart])
        color = ZONE_COLORS_LIGHT[zone]

        traces.append(go.Scatter(
            x=xs, y=ys,
            fill="toself",
            fillcolor=color,
            opacity=0.35,
            line=dict(color=color, width=1.5),
            name=zone,
            text=ZONE_DESCRIPTIONS[zone],
            hovertemplate=f"<b>{zone}</b><br>%{{text}}<extra></extra>",
            mode="lines",
            legendgroup=zone,
        ))
        traces.append(go.Scatter(
            x=[cx], y=[cy],
            mode="text",
            text=[f"<b>{zone}</b>"],
            textfont=dict(size=13, color=TEXT_DARK),
            showlegend=False,
            hoverinfo="skip",
        ))

    # Triangle outline
    traces.append(go.Scatter(
        x=[0, 50, 100, 0],
        y=[0, 100 * np.sin(np.radians(60)), 0, 0],
        mode="lines",
        line=dict(color="#444444", width=2),
        showlegend=False,
        hoverinfo="skip",
    ))

    # Tick labels
    for pct in range(0, 110, 10):
        x1, y1 = tri_to_cartesian(pct, 0, 100 - pct)
        traces.append(go.Scatter(x=[x1], y=[y1], mode="text", text=[str(pct)],
            textfont=dict(size=8, color=TEXT_MID), textposition="middle left",
            showlegend=False, hoverinfo="skip"))
        x2, y2 = tri_to_cartesian(0, pct, 100 - pct)
        traces.append(go.Scatter(x=[x2], y=[y2], mode="text", text=[str(pct)],
            textfont=dict(size=8, color=TEXT_MID), textposition="middle right",
            showlegend=False, hoverinfo="skip"))
        x3, y3 = tri_to_cartesian(0, 100 - pct, pct)
        traces.append(go.Scatter(x=[x3], y=[y3], mode="text", text=[str(pct)],
            textfont=dict(size=8, color=TEXT_MID), textposition="bottom center",
            showlegend=False, hoverinfo="skip"))

    # Axis labels
    for x, y, txt, pos in [
        (25, -7,  "← %C₂H₂", "bottom center"),
        (75, -7,  "%C₂H₄ →", "bottom center"),
        (8,  46,  "%CH₄ →",  "middle left"),
    ]:
        traces.append(go.Scatter(x=[x], y=[y], mode="text", text=[txt],
            textfont=dict(size=11, color=TEXT_DARK),
            textposition=pos, showlegend=False, hoverinfo="skip"))

    return traces


TRIANGLE_BG = build_triangle_background()


def make_triangle_figure(results: list) -> go.Figure:
    fig = go.Figure(data=TRIANGLE_BG)

    if results:
        for r in results[:-1]:
            fig.add_trace(go.Scatter(
                x=[r.x], y=[r.y], mode="markers",
                marker=dict(size=8, color=SEVERITY_COLOR.get(r.severity, "#888"),
                            opacity=0.65, line=dict(color=BG_WHITE, width=0.8)),
                name=r.sample.transformer_id,
                text=[f"<b>{r.sample.transformer_id}</b><br>"
                      f"Zone: {r.fault_zone}<br>"
                      f"CH₄={r.sample.ch4_ppm:.1f} | C₂H₄={r.sample.c2h4_ppm:.1f} | C₂H₂={r.sample.c2h2_ppm:.1f} ppm<br>"
                      f"Time: {r.sample.timestamp}"],
                hovertemplate="%{text}<extra></extra>",
                showlegend=False,
            ))

        latest = results[-1]
        fig.add_trace(go.Scatter(
            x=[latest.x], y=[latest.y], mode="markers+text",
            marker=dict(size=18, color=SEVERITY_COLOR.get(latest.severity, ACCENT),
                        symbol="star", line=dict(color=BG_WHITE, width=2)),
            text=["◀ Latest"], textposition="middle right",
            textfont=dict(size=11, color=TEXT_DARK),
            name="Latest",
            hovertemplate=(
                f"<b>LATEST — {latest.sample.transformer_id}</b><br>"
                f"Zone: {latest.fault_zone}<br>Severity: {latest.severity}<br>"
                f"CH₄={latest.sample.ch4_ppm:.1f} ppm<br>"
                f"C₂H₄={latest.sample.c2h4_ppm:.1f} ppm<br>"
                f"C₂H₂={latest.sample.c2h2_ppm:.1f} ppm<br>"
                f"Time: {latest.sample.timestamp}<extra></extra>"
            ),
            showlegend=True,
        ))

    fig.update_layout(
        title=dict(text="Duval Triangle — Live Fault Diagnosis",
                   font=dict(size=15, color=TEXT_DARK)),
        xaxis=dict(range=[-8, 112], visible=False),
        yaxis=dict(range=[-14, 100], visible=False, scaleanchor="x", scaleratio=1),
        plot_bgcolor=BG_WHITE,
        paper_bgcolor=BG_WHITE,
        font=dict(color=TEXT_DARK, family="Segoe UI, Arial, sans-serif"),
        margin=dict(l=10, r=10, t=50, b=10),
        legend=dict(bgcolor=BG_WHITE, bordercolor=BORDER, borderwidth=1,
                    font=dict(color=TEXT_DARK)),
        height=520,
    )
    return fig


def make_trend_figure(samples: list, transformer_id: str) -> go.Figure:
    filtered = [s for s in samples if s.transformer_id == transformer_id]
    if not filtered:
        return go.Figure()

    ts   = [s.timestamp  for s in filtered]
    ch4  = [s.ch4_ppm    for s in filtered]
    c2h4 = [s.c2h4_ppm   for s in filtered]
    c2h2 = [s.c2h2_ppm   for s in filtered]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ts, y=ch4,  mode="lines+markers", name="CH₄",
                             line=dict(color="#e67e22", width=2), marker=dict(size=4)))
    fig.add_trace(go.Scatter(x=ts, y=c2h4, mode="lines+markers", name="C₂H₄",
                             line=dict(color="#2980b9", width=2), marker=dict(size=4)))
    fig.add_trace(go.Scatter(x=ts, y=c2h2, mode="lines+markers", name="C₂H₂",
                             line=dict(color="#c0392b", width=2), marker=dict(size=4)))

    fig.update_layout(
        title=dict(text=f"Gas Concentration Trend — {transformer_id}",
                   font=dict(size=13, color=TEXT_DARK)),
        xaxis=dict(title="Time", color=TEXT_MID, showgrid=True,
                   gridcolor="#e9ecef", linecolor=BORDER, tickfont=dict(color=TEXT_MID)),
        yaxis=dict(title="Concentration (ppm)", color=TEXT_MID, showgrid=True,
                   gridcolor="#e9ecef", linecolor=BORDER, tickfont=dict(color=TEXT_MID)),
        plot_bgcolor=BG_WHITE,
        paper_bgcolor=BG_WHITE,
        font=dict(color=TEXT_DARK, family="Segoe UI, Arial, sans-serif"),
        margin=dict(l=50, r=20, t=50, b=50),
        height=300,
        legend=dict(bgcolor=BG_WHITE, bordercolor=BORDER, borderwidth=1,
                    font=dict(color=TEXT_DARK)),
    )
    return fig


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
app.title = "Duval Triangle — DGA Monitor"

# Inject global CSS to force entire page white/light
app.index_string = (
    "<!DOCTYPE html><html><head>{%metas%}<title>{%title%}</title>"
    "{%favicon%}{%css%}"
    "<style>"
    "html,body,.dash-app-layout,.container-fluid{"
    f"  background-color:{BG_PAGE} !important;"
    f"  color:{TEXT_DARK} !important;"
    "  font-family:'Segoe UI',Arial,sans-serif;"
    "}"
    # Dash dropdown light override
    ".Select-control,.Select-menu-outer,.VirtualizedSelectOption{"
    f"  background-color:{BG_WHITE} !important;"
    f"  color:{TEXT_DARK} !important;"
    "}"
    f".Select-value-label,.Select-placeholder{{color:{TEXT_MID} !important;}}"
    ".dash-dropdown .Select-control{"
    f"  border:1px solid {BORDER} !important;"
    "}"
    # DataTable pagination
    ".previous-next-container,.page-number{"
    f"  background-color:{BG_WHITE} !important;"
    f"  color:{TEXT_DARK} !important;"
    "}"
    "</style>"
    "</head><body>{%app_entry%}"
    "<footer>{%config%}{%scripts%}{%renderer%}</footer></body></html>"
)

CARD = {
    "backgroundColor": BG_WHITE,
    "border": f"1px solid {BORDER}",
    "borderRadius": "8px",
    "padding": "16px",
    "marginBottom": "14px",
    "color": TEXT_DARK,
    "boxShadow": "0 1px 4px rgba(0,0,0,0.06)",
}

app.layout = dbc.Container(
    fluid=True,
    style={"backgroundColor": BG_PAGE, "minHeight": "100vh", "padding": "0"},
    children=[

    # Header
    dbc.Row(dbc.Col(html.Div([
        html.H2("⚡ Duval Triangle — Dynamic DGA Fault Monitor",
                style={"color": ACCENT, "marginBottom": "2px", "fontWeight": "700"}),
        html.P("Real-time transformer fault diagnosis via Dissolved Gas Analysis (IEC 60599)",
               style={"color": TEXT_LITE, "marginTop": "0", "fontSize": "13px"}),
    ]), style={
        "backgroundColor": BG_WHITE,
        "padding": "18px 28px 12px",
        "marginBottom": "14px",
        "borderBottom": f"3px solid {ACCENT}",
        "boxShadow": "0 2px 6px rgba(0,0,0,0.05)",
    })),

    # Controls
    dbc.Row([
        dbc.Col([
            html.Label("Filter by Transformer",
                       style={"color": TEXT_MID, "fontSize": "12px", "fontWeight": "600"}),
            dcc.Dropdown(id="transformer-select", options=[], value=None,
                         placeholder="All transformers", clearable=True,
                         style={"fontSize": "13px", "backgroundColor": BG_WHITE,
                                "color": TEXT_DARK}),
        ], width=3),
        dbc.Col([
            html.Label("Auto-refresh",
                       style={"color": TEXT_MID, "fontSize": "12px", "fontWeight": "600"}),
            dcc.Checklist(id="auto-refresh-toggle",
                          options=[{"label": "  Enabled", "value": "on"}],
                          value=["on"],
                          style={"color": TEXT_DARK, "marginTop": "8px", "fontSize": "13px"}),
        ], width=2),
        dbc.Col([
            dbc.Button("🔄 Refresh Now", id="refresh-btn", color="danger",
                       size="sm", style={"marginTop": "22px"}),
        ], width=2),
        dbc.Col([
            html.Div(id="last-update-label",
                     style={"color": TEXT_LITE, "fontSize": "12px", "marginTop": "26px"}),
        ], width=5),
    ], style={"padding": "0 24px 12px", "backgroundColor": BG_PAGE}),

    # Triangle + right panel
    dbc.Row([
        dbc.Col([
            html.Div(dcc.Graph(id="duval-triangle-plot",
                               config={"displayModeBar": False}),
                     style={**CARD, "padding": "8px"}),
        ], width=7),
        dbc.Col([
            html.Div(id="latest-card", style=CARD),
            html.Div([
                html.H6("🗺 Zone Legend",
                        style={"color": ACCENT, "marginBottom": "10px",
                               "fontWeight": "700"}),
                *[html.Div([
                    html.Span("█ ", style={"color": ZONE_COLORS_LIGHT[z],
                                           "fontSize": "16px", "marginRight": "4px"}),
                    html.Span(f"{z} — ",
                              style={"fontWeight": "700", "color": TEXT_DARK,
                                     "fontSize": "12px"}),
                    html.Span(ZONE_DESCRIPTIONS[z][:52] + "…",
                              style={"color": TEXT_LITE, "fontSize": "11px"}),
                ], style={"marginBottom": "5px"}) for z in ZONE_COLORS_LIGHT],
            ], style=CARD),
        ], width=5),
    ], style={"padding": "0 16px"}),

    # Trend chart
    dbc.Row([
        dbc.Col(html.Div(dcc.Graph(id="trend-chart",
                                   config={"displayModeBar": False}),
                         style={**CARD, "padding": "8px"}),
                width=12),
    ], style={"padding": "0 16px"}),

    # Diagnosis table
    dbc.Row([
        dbc.Col([
            html.H5("📋 Recent Diagnoses",
                    style={"color": ACCENT, "marginBottom": "10px",
                           "fontWeight": "700"}),
            dash_table.DataTable(
                id="diagnosis-table",
                columns=[
                    {"name": "Transformer", "id": "transformer_id"},
                    {"name": "Timestamp",   "id": "timestamp"},
                    {"name": "CH₄ (ppm)",   "id": "ch4"},
                    {"name": "C₂H₄ (ppm)",  "id": "c2h4"},
                    {"name": "C₂H₂ (ppm)",  "id": "c2h2"},
                    {"name": "Zone",         "id": "zone"},
                    {"name": "Severity",     "id": "severity"},
                ],
                data=[],
                page_size=12,
                style_table={
                    "overflowX": "auto",
                    "borderRadius": "6px",
                    "border": f"1px solid {BORDER}",
                },
                style_header={
                    "backgroundColor": "#f1f3f5",
                    "color": TEXT_DARK,
                    "fontWeight": "700",
                    "border": f"1px solid {BORDER}",
                    "fontSize": "12px",
                    "padding": "8px 10px",
                },
                style_cell={
                    "backgroundColor": BG_WHITE,
                    "color": TEXT_DARK,
                    "border": f"1px solid {BORDER}",
                    "fontSize": "12px",
                    "padding": "7px 10px",
                    "fontFamily": "Segoe UI, Arial, sans-serif",
                },
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#fafbfc"},
                    {"if": {"filter_query": '{severity} = "CRITICAL"'},
                     "backgroundColor": "#fde8e8", "color": "#c0392b",
                     "fontWeight": "600"},
                    {"if": {"filter_query": '{severity} = "HIGH"'},
                     "backgroundColor": "#fef3e2", "color": "#d35400",
                     "fontWeight": "600"},
                    {"if": {"filter_query": '{severity} = "MEDIUM"'},
                     "backgroundColor": "#eafaf1", "color": "#1e8449"},
                    {"if": {"filter_query": '{severity} = "LOW"'},
                     "backgroundColor": "#f0faf4", "color": "#27ae60"},
                ],
            ),
        ], width=12),
    ], style={"padding": "0 16px 28px"}),

    dcc.Interval(id="interval", interval=REFRESH_INTERVAL_MS,
                 n_intervals=0, disabled=False),
    dcc.Store(id="store"),
])


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

_adapter = get_adapter()


@app.callback(
    Output("interval", "disabled"),
    Input("auto-refresh-toggle", "value"),
)
def toggle_interval(value):
    return "on" not in (value or [])


@app.callback(
    Output("store", "data"),
    Output("transformer-select", "options"),
    Output("last-update-label", "children"),
    Input("interval", "n_intervals"),
    Input("refresh-btn", "n_clicks"),
    Input("transformer-select", "value"),
)
def refresh_data(n_intervals, n_clicks, selected_tx):
    samples = _adapter.fetch_latest(MAX_POINTS)
    if selected_tx:
        samples = [s for s in samples if s.transformer_id == selected_tx]

    results = batch_diagnose(samples)
    rows = [{
        "transformer_id": r.sample.transformer_id,
        "timestamp":      r.sample.timestamp,
        "ch4":            round(r.sample.ch4_ppm, 2),
        "c2h4":           round(r.sample.c2h4_ppm, 2),
        "c2h2":           round(r.sample.c2h2_ppm, 2),
        "zone":           r.fault_zone,
        "severity":       r.severity,
        "x":              r.x,
        "y":              r.y,
    } for r in results]

    all_samples = _adapter.fetch_latest(MAX_POINTS)
    tx_ids = sorted({s.transformer_id for s in all_samples})
    options = [{"label": t, "value": t} for t in tx_ids]
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return rows, options, f"Last updated: {ts}  |  {len(rows)} readings loaded"


@app.callback(
    Output("duval-triangle-plot", "figure"),
    Output("latest-card", "children"),
    Output("diagnosis-table", "data"),
    Input("store", "data"),
)
def update_ui(rows):
    blank_layout = go.Layout(plot_bgcolor=BG_WHITE, paper_bgcolor=BG_WHITE,
                             font=dict(color=TEXT_DARK))
    if not rows:
        return go.Figure(layout=blank_layout), \
               [html.P("No data available.", style={"color": TEXT_LITE})], []

    class _R:
        def __init__(self, row):
            self.x = row["x"]; self.y = row["y"]
            self.fault_zone = row["zone"]; self.severity = row["severity"]
            self.sample = type("S", (), {
                "transformer_id": row["transformer_id"],
                "timestamp":      row["timestamp"],
                "ch4_ppm":        row["ch4"],
                "c2h4_ppm":       row["c2h4"],
                "c2h2_ppm":       row["c2h2"],
            })()

    fig = make_triangle_figure([_R(r) for r in rows])

    latest   = rows[-1]
    sev_col  = SEVERITY_COLOR.get(latest["severity"], "#888")
    zone_col = ZONE_COLORS_LIGHT.get(latest["zone"].rstrip("*"), "#888")

    card = [
        html.H5("🔬 Latest Reading",
                style={"color": ACCENT, "marginBottom": "12px",
                       "fontWeight": "700", "fontSize": "15px"}),
        html.Table([
            html.Tr([
                html.Td(lbl, style={"color": TEXT_LITE, "fontSize": "12px",
                                    "paddingRight": "14px", "paddingBottom": "4px",
                                    "fontWeight": "600", "whiteSpace": "nowrap"}),
                html.Td(val, style={"color": TEXT_DARK, "fontSize": "13px",
                                    "paddingBottom": "4px"}),
            ]) for lbl, val in [
                ("Transformer", latest["transformer_id"]),
                ("Timestamp",   latest["timestamp"]),
                ("CH₄",         f"{latest['ch4']} ppm"),
                ("C₂H₄",        f"{latest['c2h4']} ppm"),
                ("C₂H₂",        f"{latest['c2h2']} ppm"),
            ]
        ], style={"borderCollapse": "collapse", "width": "100%",
                  "marginBottom": "12px"}),

        html.Div([
            html.Span("Zone  ", style={"color": TEXT_MID, "fontSize": "12px",
                                       "fontWeight": "600"}),
            html.Span(latest["zone"], style={
                "backgroundColor": zone_col, "color": BG_WHITE,
                "padding": "3px 14px", "borderRadius": "20px",
                "fontWeight": "700", "fontSize": "13px",
            }),
        ], style={"marginBottom": "8px"}),

        html.Div([
            html.Span("Severity  ", style={"color": TEXT_MID, "fontSize": "12px",
                                           "fontWeight": "600"}),
            html.Span(latest["severity"], style={
                "backgroundColor": sev_col, "color": BG_WHITE,
                "padding": "3px 14px", "borderRadius": "20px",
                "fontWeight": "700", "fontSize": "13px",
            }),
        ], style={"marginBottom": "10px"}),

        html.Hr(style={"borderColor": BORDER, "margin": "10px 0"}),
        html.P(ZONE_DESCRIPTIONS.get(latest["zone"].rstrip("*"), ""),
               style={"fontSize": "11px", "color": TEXT_LITE,
                      "fontStyle": "italic", "marginBottom": "0"}),
    ]

    return fig, card, rows


@app.callback(
    Output("trend-chart", "figure"),
    Input("store", "data"),
    Input("transformer-select", "value"),
)
def update_trend(rows, selected_tx):
    blank = go.Figure(layout=go.Layout(plot_bgcolor=BG_WHITE,
                                       paper_bgcolor=BG_WHITE))
    if not rows:
        return blank
    samples = [DGASample(
        transformer_id=r["transformer_id"], timestamp=r["timestamp"],
        ch4_ppm=r["ch4"], c2h4_ppm=r["c2h4"], c2h2_ppm=r["c2h2"],
        source="store",
    ) for r in rows]
    tx = selected_tx or (samples[-1].transformer_id if samples else None)
    return make_trend_figure(samples, tx) if tx else blank


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("  Duval Triangle Dynamic Dashboard  (Light Theme)")
    print("  Open http://127.0.0.1:8050 in your browser")
    print("  Press Ctrl+C to stop")
    print("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=8050)