"""Design system dùng chung cho dashboard Streamlit."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pydeck as pdk
import streamlit as st

HANOI_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

SCENARIO_LABELS = {
    "below_50": "Dưới 50 mm/giờ",
    "from_50_to_under_70": "Từ 50 đến dưới 70 mm/giờ",
    "from_70_to_100": "Từ 70 đến 100 mm/giờ",
    "over_100": "Trên 100 mm/giờ",
}

SCENARIO_SHORT_LABELS = {
    "below_50": "Dưới 50",
    "from_50_to_under_70": "50–<70",
    "from_70_to_100": "70–100",
    "over_100": ">100",
}

SCENARIO_COLORS = {
    "below_50": [75, 85, 99, 100],
    "from_50_to_under_70": [250, 204, 21, 190],
    "from_70_to_100": [249, 115, 22, 215],
    "over_100": [239, 68, 68, 240],
}

MAP_STYLES: dict[str, str] = {
    "Tối (Dark Matter)": "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    "Sáng chi tiết (Positron)": "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    "Địa hình & Đường sá (Voyager)": "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
}

# Thứ tự là thứ tự selectbox; 24h mặc định để bắt cả mưa kéo dài.
RAIN_INDICATORS: dict[str, dict] = {
    "Mưa tích lũy 24 giờ": {
        "metric": "rain_24h_mm",
        "band": "vn_rain_band_24h",
        "forecast_band": "forecast_next_24h_band",
        "window_label": "24h",
        "unit": "mm trong 24 giờ",
        "threshold": 50.0,
        "max_key": "max_rain_24h_mm",
        "summary_key": "elevated_ward_count_24h",
        "colors": {
            "below_50": [75, 85, 99, 100],
            "from_50_to_under_100": [56, 189, 248, 170],
            "from_100_to_under_150": [250, 204, 21, 190],
            "from_150_to_under_200": [249, 115, 22, 215],
            "from_200_to_300": [239, 68, 68, 235],
            "over_300": [168, 85, 247, 245],
        },
        "labels": {
            "below_50": "Dưới 50 mm/24 giờ",
            "from_50_to_under_100": "Từ 50 đến dưới 100 mm/24 giờ",
            "from_100_to_under_150": "Từ 100 đến dưới 150 mm/24 giờ",
            "from_150_to_under_200": "Từ 150 đến dưới 200 mm/24 giờ",
            "from_200_to_300": "Từ 200 đến 300 mm/24 giờ",
            "over_300": "Trên 300 mm/24 giờ",
        },
        "legend": (
            ("rgba(75, 85, 99, 0.7)", "Dưới 50", "mm / 24h"),
            ("#38BDF8", "50–<100", "mm / 24h"),
            ("#FACC15", "100–<150", "mm / 24h"),
            ("#F97316", "150–<200", "mm / 24h"),
            ("#EF4444", "200–300", "mm / 24h"),
            ("#A855F7", "Trên 300", "mm / 24h"),
        ),
    },
    "Mưa tích lũy 12 giờ": {
        "metric": "rain_12h_mm",
        "band": "vn_rain_band_12h",
        "forecast_band": "forecast_next_12h_band",
        "window_label": "12h",
        "unit": "mm trong 12 giờ",
        "threshold": 30.0,
        "max_key": "max_rain_12h_mm",
        "summary_key": "elevated_ward_count_12h",
        "colors": {
            "below_30": [75, 85, 99, 100],
            "from_30_to_under_50": [56, 189, 248, 170],
            "from_50_to_under_70": [250, 204, 21, 190],
            "from_70_to_100": [249, 115, 22, 215],
            "over_100": [239, 68, 68, 240],
        },
        "labels": {
            "below_30": "Dưới 30 mm/12 giờ",
            "from_30_to_under_50": "Từ 30 đến dưới 50 mm/12 giờ",
            "from_50_to_under_70": "Từ 50 đến dưới 70 mm/12 giờ",
            "from_70_to_100": "Từ 70 đến 100 mm/12 giờ",
            "over_100": "Trên 100 mm/12 giờ",
        },
        "legend": (
            ("rgba(75, 85, 99, 0.7)", "Dưới 30", "mm / 12h"),
            ("#38BDF8", "30–<50", "mm / 12h"),
            ("#FACC15", "50–<70", "mm / 12h"),
            ("#F97316", "70–100", "mm / 12h"),
            ("#EF4444", "Trên 100", "mm / 12h"),
        ),
    },
    "Kịch bản QĐ 2280 · mưa 1 giờ": {
        "metric": "rain_1h_mm",
        "band": "hanoi_rain_scenario_band",
        "forecast_band": "forecast_next_1h_band",
        "window_label": "1h",
        "unit": "mm trong 1 giờ",
        "threshold": 50.0,
        "max_key": "max_rain_1h_mm",
        "summary_key": "elevated_ward_count",
        "colors": SCENARIO_COLORS,
        "labels": SCENARIO_LABELS,
        "legend": (
            ("rgba(75, 85, 99, 0.7)", "Dưới 50", "mm trong 1 giờ"),
            ("#FACC15", "50–<70", "mm trong 1 giờ"),
            ("#F97316", "70–100", "mm trong 1 giờ"),
            ("#EF4444", "Trên 100", "mm trong 1 giờ"),
        ),
    },
}


def number(value: object) -> float:
    """Ép về float hiển thị được; NaN/None thành 0.0 để format không vỡ."""
    try:
        result = float(value)
        return result if math.isfinite(result) else 0.0
    except (TypeError, ValueError):
        return 0.0


def rain_label(value: object) -> str:
    """Nhãn mm cho một giá trị mưa; NULL cửa sổ thiếu giờ KHÔNG hiện thành 0."""
    try:
        result = float(value)
        return f"{result:.1f} mm" if math.isfinite(result) else "Không có dữ liệu"
    except (TypeError, ValueError):
        return "Không có dữ liệu"


def pressure_score_label(value: object, *, digits: int = 0) -> str:
    """Hiển thị pressure score mà không biến NULL thành 0/100."""
    try:
        result = float(value)
        return (
            f"{result:.{digits}f}/100" if math.isfinite(result) else "Không đủ dữ liệu"
        )
    except (TypeError, ValueError):
        return "Không đủ dữ liệu"


def legend_row(legend: tuple[tuple[str, str, str], ...]) -> None:
    items = "".join(
        '<div class="legend-item">'
        f'<div class="legend-swatch" style="background:{color}"></div>'
        f"<strong>{label}</strong><span>{unit}</span></div>"
        for color, label, unit in legend
    )
    st.markdown(f'<div class="legend-row">{items}</div>', unsafe_allow_html=True)


def configure_page(title: str, icon: str) -> None:
    st.set_page_config(
        page_title=title,
        page_icon=icon,
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    st.markdown(_GLOBAL_CSS, unsafe_allow_html=True)


def navigation(active: str) -> None:
    items = (
        ("home", "/", "Tổng quan"),
        ("map", "/forecast_map", "Bản đồ"),
        ("archive", "/archive_replay", "Quá khứ"),
        ("ward", "/ward_drilldown", "Phường/Xã"),
    )
    desktop_links = "".join(
        f'<a href="{href}" class="{"active" if key == active else ""}" target="_self">{label}</a>'
        for key, href, label in items
    )
    mobile_links = "".join(
        f'<a href="{href}" target="_self">{label}</a>' for _, href, label in items
    )
    st.markdown(
        '<header class="app-nav">'
        '<a class="brand" href="/" target="_self"><i></i><span>HANOI CLIMATE</span></a>'
        f'<nav class="desktop-nav">{desktop_links}</nav>'
        '<span class="nav-source">OPEN-METEO · UTC+7</span>'
        '<details class="mobile-nav"><summary aria-label="Mở menu">☰</summary>'
        f"<div>{mobile_links}</div></details>"
        "</header>",
        unsafe_allow_html=True,
    )


def page_header(eyebrow: str, title: str, subtitle: str, meta: str = "") -> None:
    meta_markup = f'<span class="page-meta">{meta}</span>' if meta else ""
    st.markdown(
        '<section class="page-heading">'
        f'<div><span class="section-kicker">{eyebrow}</span><h1>{title}</h1><p>{subtitle}</p></div>'
        f"{meta_markup}</section>",
        unsafe_allow_html=True,
    )


def metric_strip(items: list[tuple[str, str, str]]) -> None:
    cards = "".join(
        '<div class="metric-card">'
        f"<span>{label}</span><strong>{value}</strong><small>{meta}</small>"
        "</div>"
        for label, value, meta in items
    )
    st.markdown(f'<div class="metric-strip">{cards}</div>', unsafe_allow_html=True)


def local_time(value: object, pattern: str = "%H:%M · %d/%m/%Y") -> str:
    if value is None or pd.isna(value):
        return "—"
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize("UTC")
    return parsed.tz_convert(HANOI_TZ).strftime(pattern)


def warn_if_stale(metadata: Mapping[str, object], *, threshold_minutes: int = 90) -> None:
    freshness_minutes = int(metadata.get("freshness_minutes") or 0)
    if freshness_minutes <= threshold_minutes:
        return
    st.warning(
        f"Snapshot đã chậm khoảng {freshness_minutes // 60} giờ "
        f"{freshness_minutes % 60} phút. Kiểm tra pipeline trước khi dùng cho vận hành."
    )


def to_local_naive(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.tz_convert(HANOI_TZ).dt.tz_localize(None)


def default_hour_index(hours: list[datetime]) -> int:
    if not hours:
        return 0
    now = pd.Timestamp.now(tz="UTC")
    for index, hour in enumerate(hours):
        stamp = pd.Timestamp(hour)
        stamp = (
            stamp.tz_localize("UTC")
            if stamp.tzinfo is None
            else stamp.tz_convert("UTC")
        )
        if stamp >= now.floor("h"):
            return index
    return len(hours) - 1


def scenario_label(value: object, *, short: bool = False) -> str:
    labels = SCENARIO_SHORT_LABELS if short else SCENARIO_LABELS
    return labels.get(str(value), "Chưa phân loại")


def ward_polygon_layer(geojson_data: dict) -> pdk.Layer:
    return pdk.Layer(
        "GeoJsonLayer",
        geojson_data,
        opacity=0.62,
        stroked=True,
        filled=True,
        get_fill_color="properties.fill_color",
        get_line_color=[226, 232, 240, 110],
        line_width_min_pixels=1.2,
        pickable=True,
        auto_highlight=True,
        highlight_color=[255, 255, 255, 70],
    )


def hero(
    eyebrow: str,
    title: str,
    subtitle: str,
    rotating_words: tuple[str, ...] | None = None,
) -> None:
    word_stage = ""
    if rotating_words:
        words = "".join(
            f'<span style="animation-delay:{index * 2}s">{word}</span>'
            for index, word in enumerate(rotating_words[:4])
        )
        word_stage = f'<div class="word-stage">{words}</div>'
    # Keep raw HTML contiguous or CommonMark may render the tail as code.
    markup = (
        '<section class="climate-hero">'
        '<div class="hero-grid" aria-hidden="true"></div>'
        '<div class="hero-orb hero-orb-left" aria-hidden="true"></div>'
        '<div class="hero-orb hero-orb-right" aria-hidden="true"></div>'
        '<div class="hero-content">'
        f'<div class="hero-eyebrow"><span></span>{eyebrow}</div>'
        f"{word_stage}<h1>{title}</h1><p>{subtitle}</p>"
        "</div></section>"
    )
    st.markdown(markup, unsafe_allow_html=True)


_GLOBAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Zen+Dots&display=swap');

:root {
  --page: #14151A;
  --card: #181A1E;
  --surface: #1E2026;
  --divider: #373943;
  --accent: #FFE900;
  --brand: #F0B90B;
  --text: #FFFFFF;
  --muted: #8C8F9B;
  --soft: #C4C5CB;
}

html, body, [class*="css"], [data-testid="stAppViewContainer"] {
  font-family: "Space Grotesk", -apple-system, ".SFNSText-Regular", "San Francisco",
    BlinkMacSystemFont, ".PingFang-SC-Regular", "Microsoft YaHei", "Segoe UI",
    "Helvetica Neue", Helvetica, Arial, sans-serif;
}

[data-testid="stAppViewContainer"] { background: var(--page); }
[data-testid="stHeader"],
[data-testid="stSidebar"],
[data-testid="stSidebarCollapsedControl"],
[data-testid="stSkillsNudgeAnchor"],
[data-testid="stSkillsNudge"] { display:none !important; }

.stApp { color: var(--text); }
.block-container { max-width: 1320px; padding-top: 0; padding-bottom: 4rem; }
h1, h2, h3 { color: var(--text); letter-spacing: -.035em; }
p, label, .stCaption { color: var(--soft); }

.app-nav { position:sticky; top:0; z-index:999; height:64px; display:flex; align-items:center; gap:32px; margin:0 -1rem 28px; padding:0 16px; border-bottom:1px solid rgba(255,255,255,.06); background:rgba(20,21,26,.96); backdrop-filter:blur(14px); }
.brand { display:flex; align-items:center; gap:10px; color:var(--brand) !important; text-decoration:none !important; font-size:18px; font-weight:700; letter-spacing:-.02em; white-space:nowrap; }
.brand i { width:18px; height:18px; display:block; transform:rotate(45deg); border:4px solid var(--brand); box-shadow:inset 0 0 0 2px var(--page); }
.desktop-nav { display:flex; align-items:center; gap:6px; margin:auto; }
.desktop-nav a { padding:9px 14px; border-radius:8px; color:var(--soft) !important; text-decoration:none !important; font-size:13px; font-weight:600; transition:background .18s,color .18s; }
.desktop-nav a:hover { background:rgba(255,255,255,.08); color:var(--text) !important; }
.desktop-nav a.active { background:rgba(255,233,0,.1); color:var(--accent) !important; }
.nav-source { color:var(--muted); font-size:11px; font-weight:600; letter-spacing:.08em; white-space:nowrap; }
.mobile-nav { display:none; position:relative; margin-left:auto; }
.mobile-nav summary { list-style:none; cursor:pointer; color:var(--accent); font-size:22px; }
.mobile-nav summary::-webkit-details-marker { display:none; }
.mobile-nav div { position:absolute; right:0; top:36px; width:210px; padding:8px; border:1px solid rgba(255,255,255,.1); border-radius:12px; background:var(--surface); box-shadow:0 24px 64px rgba(0,0,0,.48); }
.mobile-nav a { display:block; padding:12px; border-radius:8px; color:var(--soft) !important; text-decoration:none !important; font-size:14px; font-weight:600; }
.mobile-nav a:hover { background:rgba(255,255,255,.1); color:var(--text) !important; }

.page-heading { display:flex; align-items:flex-end; justify-content:space-between; gap:32px; margin:12px 0 24px; padding-bottom:24px; border-bottom:1px solid var(--divider); }
.page-heading h1 { margin:8px 0 8px; font-size:40px; line-height:48px; font-weight:700; }
.page-heading p { max-width:760px; margin:0; font-size:15px; line-height:24px; color:var(--soft); }
.page-meta { flex:0 0 auto; padding:8px 12px; border:1px solid rgba(255,233,0,.35); border-radius:999px; color:var(--accent); background:rgba(255,233,0,.08); font-size:12px; font-weight:600; }

.climate-hero {
  position: relative;
  min-height: 330px;
  overflow: hidden;
  display: flex;
  align-items: center;
  justify-content: center;
  margin: -1rem 0 2rem;
  padding: 4rem 1.5rem;
  border: 1px solid rgba(255,255,255,.07);
  border-radius: 24px;
  background: #14151A;
  isolation: isolate;
}
.hero-content { position: relative; z-index: 3; max-width: 900px; text-align: center; }
.hero-eyebrow { display:flex; gap:.55rem; justify-content:center; align-items:center; color:var(--soft); font-size:12px; font-weight:700; letter-spacing:.16em; text-transform:uppercase; }
.hero-eyebrow span { width:8px; height:8px; border-radius:50%; background:var(--accent); box-shadow:0 0 20px rgba(255,233,0,.8); animation:pulse 2s ease-in-out infinite; }
.word-stage { position:relative; height:1.28em; margin:.75rem 0 -.7rem; perspective:900px; overflow:hidden; color:var(--accent); font-size:72px; line-height:92px; font-weight:700; letter-spacing:-.035em; }
.word-stage span { position:absolute; inset:0; display:flex; justify-content:center; opacity:0; transform:rotateX(55deg) translateY(70%); animation:word-cycle 8s ease-in-out infinite; }
.climate-hero h1 { margin:.8rem 0 .65rem; font-size:72px; line-height:92px; font-weight:700; }
.climate-hero h1 em { color:var(--accent); font-style:normal; }
.climate-hero p { max-width:680px; margin:0 auto; font-size:17px; line-height:1.65; color:var(--soft); }
.hero-grid { position:absolute; inset:-50%; z-index:0; opacity:.2; transform:perspective(700px) rotateX(64deg) translateY(20%); background-image:linear-gradient(rgba(255,233,0,.18) 1px,transparent 1px),linear-gradient(90deg,rgba(255,233,0,.18) 1px,transparent 1px); background-size:48px 48px; mask-image:radial-gradient(circle,black 0%,transparent 68%); }
.hero-orb { position:absolute; width:330px; height:330px; border-radius:50%; filter:blur(75px); opacity:.18; background:var(--brand); animation:float 9s ease-in-out infinite; }
.hero-orb-left { left:-130px; bottom:-150px; }
.hero-orb-right { right:-150px; top:-160px; animation-delay:-4s; }

.metric-strip { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:16px; margin:20px 0 32px; }
.metric-card { position:relative; min-height:128px; overflow:hidden; padding:22px; border:1px solid rgba(255,255,255,.08); border-radius:12px; background:var(--card); transform-style:preserve-3d; transition:transform .18s ease,box-shadow .18s ease,border-color .18s ease; }
.metric-card::before { content:""; position:absolute; inset:-45%; opacity:0; pointer-events:none; background:radial-gradient(circle at 20% 10%,rgba(255,233,0,.22),transparent 36%); transition:opacity .18s; }
.metric-card:hover { transform:translateY(-4px) scale(1.01) rotateX(1.5deg); border-color:rgba(255,233,0,.42); box-shadow:0 26px 72px rgba(0,0,0,.42); }
.metric-card:hover::before { opacity:1; }
.metric-card span,.metric-card strong,.metric-card small { position:relative; z-index:1; display:block; }
.metric-card span { color:var(--muted); font-size:13px; font-weight:600; }
.metric-card strong { margin:8px 0 5px; color:var(--text); font-size:28px; line-height:34px; font-weight:600; }
.metric-card small { color:var(--soft); font-size:12px; }

[data-testid="stVerticalBlockBorderWrapper"] { border-color:rgba(255,255,255,.08) !important; background:var(--card); border-radius:16px; }
[data-testid="stDataFrame"], [data-testid="stTable"] { border:1px solid rgba(255,255,255,.08); border-radius:12px; overflow:hidden; }
[data-testid="stExpander"] { border-color:rgba(255,255,255,.09); background:var(--card); border-radius:12px; }

.stButton > button, .stLinkButton > a { min-height:44px; border-radius:8px; border:1px solid var(--accent); background:var(--accent); color:#181A1E; font-weight:700; }
.stButton > button:hover, .stLinkButton > a:hover { border-color:#fff36b; background:#fff36b; color:#181A1E; transform:translateY(-1px); }
.stSelectbox [data-baseweb="select"] > div, .stMultiSelect [data-baseweb="select"] > div { background:var(--surface); border-color:var(--divider); }
.stSelectbox [data-baseweb="select"] input, .stMultiSelect [data-baseweb="select"] input { color:var(--text); }
.stRadio [role="radiogroup"] { gap:8px; }
.stRadio label { padding:7px 11px; border:1px solid var(--divider); border-radius:999px; background:var(--surface); }
.stSlider [role="slider"] { background:var(--accent) !important; border-color:var(--accent) !important; }
.stSlider [data-baseweb="slider"] div { color:var(--soft); }
div[data-testid="stNotification"],
div[data-testid="stAlertContainer"],
div[data-baseweb="notification"] {
  background:var(--surface) !important;
  border-color:rgba(255,233,0,.28) !important;
  color:var(--soft) !important;
}
div[data-testid="stAlertContainer"] p { color:var(--soft) !important; }
div[data-testid="stAlertContainer"] svg { fill:var(--accent) !important; color:var(--accent) !important; }
hr { border-color:var(--divider); }
[data-testid="stDeckGlJsonChart"], [data-testid="stVegaLiteChart"] { overflow:hidden; border:1px solid rgba(255,255,255,.08); border-radius:12px; background:var(--card); }

.status-pill { display:inline-flex; align-items:center; gap:.4rem; padding:.35rem .7rem; border-radius:999px; background:rgba(255,233,0,.1); border:1px solid rgba(255,233,0,.25); color:var(--accent); font-size:12px; font-weight:700; }
.section-kicker { color:var(--accent); font-size:12px; font-weight:700; letter-spacing:.13em; text-transform:uppercase; }
.legend-row { display:grid; grid-template-columns:repeat(auto-fit, minmax(180px, 1fr)); gap:10px; margin:.5rem 0 1.25rem; }
.legend-item { padding:12px; border-radius:10px; background:var(--surface); border:1px solid rgba(255,255,255,.07); }
.legend-item strong { display:block; color:var(--text); font-size:13px; }
.legend-item span { color:var(--muted); font-size:11px; }
.legend-swatch { width:24px; height:5px; margin-bottom:8px; border-radius:3px; }

@keyframes float { 0%,100%{transform:translate3d(0,0,0) scale(1)} 50%{transform:translate3d(20px,-15px,0) scale(1.08)} }
@keyframes pulse { 0%,100%{opacity:.65;transform:scale(.9)} 50%{opacity:1;transform:scale(1.18)} }
@keyframes word-cycle {
  0%,2%,25%,100% { opacity:0; transform:rotateX(55deg) translateY(70%); }
  4%,22% { opacity:1; transform:rotateX(0) translateY(0); }
  24.9% { opacity:0; transform:rotateX(-55deg) translateY(-70%); }
}
@media (max-width: 768px) {
  .block-container { padding-left:1rem; padding-right:1rem; }
  .app-nav { margin:0 0 22px; padding:0; }
  .desktop-nav,.nav-source { display:none; }
  .mobile-nav { display:block; }
  .page-heading { display:block; margin-top:0; }
  .page-heading h1 { font-size:32px; line-height:40px; }
  .page-meta { display:inline-block; margin-top:16px; }
  .climate-hero { min-height:280px; padding:3rem 1rem; border-radius:18px; }
  .climate-hero h1 { font-size:36px; }
  .word-stage { font-size:36px; }
  .climate-hero p { font-size:15px; }
  .metric-strip { grid-template-columns:repeat(2,minmax(0,1fr)); gap:12px; }
  .metric-card { min-height:112px; padding:16px; }
  .metric-card strong { font-size:24px; line-height:30px; }
  .legend-row { grid-template-columns:repeat(2,minmax(0,1fr)); }
  [data-testid="stDeckGlJsonChart"] { min-height:440px; }
}
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { animation-duration:.01ms !important; animation-iteration-count:1 !important; scroll-behavior:auto !important; }
  .word-stage span { display:none; }
  .word-stage span:first-child { display:flex; opacity:1; transform:none; }
}
</style>
"""
