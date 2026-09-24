"""Report look and feel: Power BI-style cards on a light canvas, and color rules.

Colors follow the dataviz method: pressure levels are a *status* (fixed status
palette, always shown with their label); rainfall is a *magnitude* (one blue
hue, light -> dark); a single series is slot-1 blue, context is gray.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

HANOI = ZoneInfo("Asia/Ho_Chi_Minh")

ACCENT = "#2a78d6"  # categorical slot 1
CONTEXT = "#c8c6c4"  # de-emphasised marks
INK = "#252423"
MUTED = "#605e5c"

LEVELS = ["HIGH", "ELEVATED", "WATCH", "NORMAL", "UNKNOWN"]
LEVEL_COLORS = {  # status palette (critical, serious, warning, good) + neutral
    "HIGH": "#d03b3b",
    "ELEVATED": "#ec835a",
    "WATCH": "#fab219",
    "NORMAL": "#0ca30c",
    "UNKNOWN": "#a19f9d",
}
LEVEL_LABELS = {
    "HIGH": "Cao",
    "ELEVATED": "Tăng",
    "WATCH": "Theo dõi",
    "NORMAL": "Bình thường",
    "UNKNOWN": "Chưa đủ dữ liệu",
}

# Sequential blue ramp for rainfall bins (steps 100 -> 650 of the blue ramp).
RAIN_BINS = [0.1, 1, 5, 10, 25, 50]  # mm; below the first bin counts as "no rain"
RAIN_COLORS = [
    "#f3f2f1",
    "#cde2fb",
    "#9ec5f4",
    "#6da7ec",
    "#3987e5",
    "#256abf",
    "#104281",
]

MAP_METRICS = {
    "Áp lực mưa": "pressure_level",
    "Mưa trong giờ": "precipitation_mm",
    "Mưa 24 giờ tới": "forecast_next_24h_mm",
}

CSS = f"""
<style>
[data-testid="stHeader"] {{ display: none; }}
.block-container {{ padding-top: 1rem; max-width: 1500px; }}
[class*="st-key-card"] {{
    background: #ffffff;
    border: 1px solid #e1dfdd;
    border-radius: 4px;
    box-shadow: 0 1.6px 3.6px rgba(0,0,0,.08), 0 .3px .9px rgba(0,0,0,.06);
    padding: 12px 16px;
}}
.tile-label {{ color: {MUTED}; font-size: .8rem; }}
.tile-value {{ color: {INK}; font-size: 2rem; font-weight: 600; line-height: 1.25; }}
.tile-note {{ color: {MUTED}; font-size: .78rem; }}
.visual-title {{ color: {INK}; font-size: .95rem; font-weight: 600; margin-bottom: 4px; }}
.report-title {{ color: {INK}; font-size: 1.35rem; font-weight: 600; }}
.report-meta {{ color: {MUTED}; font-size: .85rem; }}
</style>
"""


def local(value: datetime, pattern: str = "%H:%M %d/%m") -> str:
    return value.astimezone(HANOI).strftime(pattern)


def mm(value: float | None) -> str:
    return "–" if pd.isna(value) else f"{value:.1f} mm"


def card(key: str):
    """A white report card; `key` must be unique on the page."""
    return st.container(key=f"card_{key}")


def visual_title(text: str) -> None:
    st.markdown(f'<div class="visual-title">{text}</div>', unsafe_allow_html=True)


def tile(key: str, label: str, value: str, note: str = "") -> None:
    with card(key):
        st.markdown(
            f'<div class="tile-label">{label}</div><div class="tile-value">{value}</div>'
            f'<div class="tile-note">{note or "&nbsp;"}</div>',
            unsafe_allow_html=True,
        )


def rain_color(value: float | None) -> str:
    if pd.isna(value):
        return LEVEL_COLORS["UNKNOWN"]
    index = sum(value >= edge for edge in RAIN_BINS)
    return RAIN_COLORS[index]


def fill_color(metric: str, row: dict) -> list[int]:
    hex_color = (
        LEVEL_COLORS.get(row["pressure_level"] or "UNKNOWN")
        if metric == "pressure_level"
        else rain_color(row[metric])
    )
    return [int(hex_color[i : i + 2], 16) for i in (1, 3, 5)] + [210]


def style(chart):
    """White plot area and recessive hairline axes, to sit inside a report card."""
    return (
        chart.configure(background="#ffffff")
        .configure_view(strokeWidth=0)
        .configure_axis(
            gridColor="#edebe9",
            domainColor="#c8c6c4",
            tickColor="#c8c6c4",
            labelColor=MUTED,
            titleColor=MUTED,
            labelLimit=180,
        )
    )
