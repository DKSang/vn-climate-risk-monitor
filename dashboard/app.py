"""Hanoi rain-pressure report: one report, three pages, cross-filtered by ward.

Every number comes from the latest published DuckLake snapshot (dashboard.queries).
Click a ward on the map or in the ranking to filter the other visuals to it.
"""

from __future__ import annotations

import copy

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

from dashboard import queries, ui
from dashboard.geography import ward_boundaries

st.set_page_config(page_title="Hanoi Rain Pressure", page_icon="🌧️", layout="wide")
st.markdown(ui.CSS, unsafe_allow_html=True)

snapshot = queries.published_snapshot()
if snapshot is None:
    st.info(
        "Chưa có bản publish nào. Chạy DAG `open_meteo_forecast_hourly` để có dữ liệu."
    )
    st.stop()
info = queries.overview(snapshot)
if not info["hours"]:
    st.info("Bản publish mới nhất không còn giờ dự báo nào từ giờ hiện tại trở đi.")
    st.stop()


# ── Cross-filter: a click on the map or the ranking selects a ward ──────────
ALL = ""  # the ward slicer's "whole city" option


def _pick(ward_code: str | None) -> None:
    st.session_state.ward = ward_code or ALL


def pick_from_map() -> None:
    objects = st.session_state.map.selection.get("objects", {}).get("wards", [])
    if objects:
        _pick(objects[0].get("properties", objects[0]).get("ward_code"))


def pick_from_ranking() -> None:
    points = st.session_state.ranking.selection.get("pick", [])
    _pick(points[0]["ward_code"] if points else None)


# ── Header and slicers ──────────────────────────────────────────────────────
head, pages = st.columns([3, 2], vertical_alignment="bottom")
with head:
    st.markdown(
        '<div class="report-title">Áp lực mưa Hà Nội · 72 giờ tới</div>'
        f'<div class="report-meta">Forecast run {ui.local(info["forecast_run"])} · '
        f"publish {ui.local(info['published_at'])} (giờ Hà Nội) · snapshot {snapshot}</div>",
        unsafe_allow_html=True,
    )
with pages:
    page = st.segmented_control(
        "Trang",
        ["Tổng quan", "Chi tiết phường", "Bảng dữ liệu"],
        default="Tổng quan",
        key="page",
        label_visibility="collapsed",
    )

hours = info["hours"]
wards_now = queries.ward_hour(snapshot, hours[0])
names = dict(zip(wards_now["ward_code"], wards_now["ward_name"], strict=True))
st.session_state.setdefault("ward", ALL)

with ui.card("slicers"):
    hour_col, metric_col, ward_col, clear_col = st.columns(
        [3, 3, 2, 1], vertical_alignment="bottom"
    )
    hour = hour_col.select_slider(
        "Giờ", options=hours, format_func=ui.local, key="hour"
    )
    metric_name = metric_col.segmented_control(
        "Chỉ số bản đồ", list(ui.MAP_METRICS), default="Áp lực mưa", key="metric"
    )
    ward_col.selectbox(
        "Phường/xã",
        [ALL, *sorted(names, key=names.get)],
        format_func=lambda code: names.get(code, "Toàn thành phố"),
        key="ward",
    )
    clear_col.button("Bỏ lọc", on_click=_pick, args=(None,), width="stretch")

metric = ui.MAP_METRICS[metric_name or "Áp lực mưa"]
ward = st.session_state.ward or None
wards = queries.ward_hour(snapshot, hour)


# ── Visuals ─────────────────────────────────────────────────────────────────
def kpi_row() -> None:
    cols = st.columns(4)
    if ward:
        row = wards[wards["ward_code"] == ward].iloc[0]
        level = row["pressure_level"] or "UNKNOWN"
        values = [
            ("Mức áp lực", ui.LEVEL_LABELS[level], names[ward]),
            ("Điểm áp lực", f"{row['pressure_score'] or 0:.0f} / 100", ui.local(hour)),
            ("Mưa trong giờ", ui.mm(row["precipitation_mm"]), ui.local(hour)),
            ("Mưa 24 giờ tới", ui.mm(row["forecast_next_24h_mm"]), "từ giờ đang chọn"),
        ]
    else:
        watched = wards["pressure_level"].isin(["WATCH", "ELEVATED", "HIGH"]).sum()
        wettest = wards.loc[wards["precipitation_mm"].idxmax()]
        wettest_24h = wards.loc[wards["forecast_next_24h_mm"].fillna(-1).idxmax()]
        values = [
            ("Phường từ mức Theo dõi", f"{watched} / {len(wards)}", ui.local(hour)),
            (
                "Mưa trong giờ lớn nhất",
                ui.mm(wettest["precipitation_mm"]),
                wettest["ward_name"],
            ),
            (
                "Mưa 24 giờ tới lớn nhất",
                ui.mm(wettest_24h["forecast_next_24h_mm"]),
                wettest_24h["ward_name"],
            ),
            (
                "Điểm áp lực cao nhất",
                f"{wards['pressure_score'].max() or 0:.0f} / 100",
                "thang 0–100",
            ),
        ]
    for col, (label, value, note) in zip(cols, values, strict=True):
        with col:
            ui.tile(label, label, value, note)


@st.cache_data
def _map_features(snapshot: int, hour: pd.Timestamp, metric: str) -> dict:
    rows = queries.ward_hour(snapshot, hour).set_index("ward_code").to_dict("index")
    geo = copy.deepcopy(ward_boundaries())
    for feature in geo["features"]:
        props = feature["properties"]
        row = rows.get(props["ward_code"])
        if row is None:
            props.update(fill=[243, 242, 241, 120], label="Không có dữ liệu")
            continue
        level = row["pressure_level"] or "UNKNOWN"
        props.update(
            fill=ui.fill_color(metric, row),
            label=(
                f"{ui.LEVEL_LABELS[level]} · điểm {row['pressure_score'] or 0:.0f}"
                if metric == "pressure_level"
                else ui.mm(row[metric])
            ),
        )
    return geo


def ward_map() -> None:
    geo = _map_features(snapshot, hour, metric)
    layers = [
        pdk.Layer(
            "GeoJsonLayer",
            data=geo,
            id="wards",
            pickable=True,
            auto_highlight=True,
            stroked=True,
            get_fill_color="properties.fill",
            get_line_color=[255, 255, 255],
            line_width_min_pixels=0.6,
        )
    ]
    if ward:
        chosen = [f for f in geo["features"] if f["properties"]["ward_code"] == ward]
        layers.append(
            pdk.Layer(
                "GeoJsonLayer",
                data={"type": "FeatureCollection", "features": chosen},
                id="chosen",
                filled=False,
                get_line_color=[37, 36, 35],
                line_width_min_pixels=3,
            )
        )
    deck = pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=21.02, longitude=105.75, zoom=8.8),
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        tooltip={"html": "<b>{fullName}</b><br/>{label}"},
    )
    st.pydeck_chart(
        deck,
        on_select=pick_from_map,
        selection_mode="single-object",
        key="map",
        height=470,
    )
    if metric == "pressure_level":
        chips = [(ui.LEVEL_COLORS[lv], ui.LEVEL_LABELS[lv]) for lv in ui.LEVELS]
    else:
        edges = ["0", *[f"{e:g}" for e in ui.RAIN_BINS]]
        chips = [
            (color, f"≥ {edge} mm")
            for color, edge in zip(ui.RAIN_COLORS[1:], edges[1:], strict=True)
        ]
    st.markdown(
        " ".join(
            f'<span class="tile-note" style="margin-right:12px">'
            f'<span style="display:inline-block;width:10px;height:10px;'
            f'background:{color};border-radius:2px"></span> {label}</span>'
            for color, label in chips
        ),
        unsafe_allow_html=True,
    )


def ranking() -> None:
    value = "pressure_score" if metric == "pressure_level" else metric
    top = wards.nlargest(10, value, keep="first")[["ward_code", "ward_name", value]]
    pick = alt.selection_point(fields=["ward_code"], name="pick")
    color = (
        alt.condition(
            alt.datum.ward_code == ward, alt.value(ui.ACCENT), alt.value(ui.CONTEXT)
        )
        if ward in set(top["ward_code"])
        else alt.value(ui.ACCENT)
    )
    chart = (
        alt.Chart(top)
        .mark_bar(cornerRadiusEnd=4, height=14)
        .encode(
            y=alt.Y(
                "ward_name:N", sort="-x", title=None, axis=alt.Axis(labelOverlap=False)
            ),
            x=alt.X(
                f"{value}:Q",
                title="điểm (0–100)" if value == "pressure_score" else "mm",
                # Scores keep their full 0-100 scale so small gaps do not look large.
                scale=alt.Scale(domain=(0, 100))
                if value == "pressure_score"
                else alt.Undefined,
            ),
            color=color,
            tooltip=[
                alt.Tooltip("ward_name:N", title="Phường/xã"),
                alt.Tooltip(f"{value}:Q", title=metric_name, format=".1f"),
            ],
        )
        .add_params(pick)
        .properties(height=320)
    )
    st.altair_chart(
        ui.style(chart), on_select=pick_from_ranking, key="ranking", width="stretch"
    )


def level_mix() -> None:
    counts = (
        wards["pressure_level"]
        .fillna("UNKNOWN")
        .value_counts()
        .reindex(ui.LEVELS, fill_value=0)
        .rename_axis("level")
        .reset_index(name="wards")
    )
    counts["label"] = counts["level"].map(ui.LEVEL_LABELS)
    bars = alt.Chart(counts).encode(
        y=alt.Y(
            "label:N",
            sort=[ui.LEVEL_LABELS[lv] for lv in ui.LEVELS],
            title=None,
            axis=alt.Axis(labelOverlap=False),
        ),
        x=alt.X(
            "wards:Q",
            title="số phường/xã",
            scale=alt.Scale(domainMax=len(wards) * 1.12),
        ),
    )
    chart = bars.mark_bar(cornerRadiusEnd=4, height=14).encode(
        color=alt.Color(
            "level:N",
            scale=alt.Scale(
                domain=ui.LEVELS, range=[ui.LEVEL_COLORS[lv] for lv in ui.LEVELS]
            ),
            legend=None,
        ),
        tooltip=[
            alt.Tooltip("label:N", title="Mức"),
            alt.Tooltip("wards:Q", title="Số phường"),
        ],
    ) + bars.mark_text(align="left", dx=4, color=ui.MUTED).encode(text="wards:Q")
    st.altair_chart(ui.style(chart.properties(height=170)), width="stretch")


def hourly(
    series: str,
    title: str,
    y_title: str,
    *,
    as_bars: bool = False,
    domain: tuple[float, float] | None = None,
) -> None:
    ts = queries.timeseries(snapshot, ward)
    ts["time"] = ts["valid_at"].dt.tz_convert(ui.HANOI).dt.tz_localize(None)
    base = alt.Chart(ts).encode(
        x=alt.X("time:T", title=None, axis=alt.Axis(format="%Hh %d/%m", tickCount=12)),
        y=alt.Y(
            f"{series}:Q",
            title=y_title,
            scale=alt.Scale(domain=domain) if domain else alt.Undefined,
        ),
        tooltip=[
            alt.Tooltip("time:T", title="Giờ", format="%H:%M %d/%m"),
            alt.Tooltip(f"{series}:Q", title=title, format=".1f"),
        ],
    )
    marks = (
        base.mark_bar(color=ui.ACCENT, cornerRadiusEnd=2)
        if as_bars
        else base.mark_area(color=ui.ACCENT, opacity=0.18)
        + base.mark_line(color=ui.ACCENT, strokeWidth=2)
        + base.mark_point(opacity=0, size=80)
    )
    now = pd.DataFrame({"time": [hour.tz_convert(ui.HANOI).tz_localize(None)]})
    rule = alt.Chart(now).mark_rule(color=ui.INK, strokeWidth=1).encode(x="time:T")
    st.altair_chart(ui.style((marks + rule).properties(height=240)), width="stretch")


# ── Pages ───────────────────────────────────────────────────────────────────
where = names[ward] if ward else "toàn thành phố"

if page in (None, "Tổng quan"):
    kpi_row()
    left, right = st.columns([3, 2])
    with left, ui.card("map"):
        ui.visual_title(f"{metric_name} theo phường/xã · {ui.local(hour)}")
        ward_map()
    with right:
        with ui.card("ranking"):
            ui.visual_title(f"Top 10 phường/xã · {metric_name.lower()}")
            ranking()
        with ui.card("levels"):
            ui.visual_title("Số phường/xã theo mức áp lực")
            level_mix()
    with ui.card("trend"):
        ui.visual_title(f"Mưa theo giờ (mm) · {where}")
        hourly("precipitation_mm", "Mưa", "mm")

elif page == "Chi tiết phường":
    if not ward:
        st.info(
            "Chọn một phường/xã ở thanh lọc, hoặc click trên bản đồ ở trang Tổng quan."
        )
    else:
        kpi_row()
        rain, pressure = st.columns(2)
        with rain, ui.card("ward_rain"):
            ui.visual_title(f"Mưa từng giờ (mm) · {where}")
            hourly("precipitation_mm", "Mưa", "mm", as_bars=True)
        with pressure, ui.card("ward_pressure"):
            ui.visual_title(f"Điểm áp lực mưa (0–100) · {where}")
            hourly("pressure_score", "Điểm áp lực", "điểm", domain=(0, 100))
        row = wards[wards["ward_code"] == ward].iloc[0]
        with ui.card("ward_reasons"):
            ui.visual_title(f"Lý do ở giờ {ui.local(hour)}")
            st.write(row["trigger_reasons"] or "Không có ngưỡng nào bị vượt.")

else:
    with ui.card("table"):
        ui.visual_title(f"Tất cả phường/xã · {ui.local(hour)}")
        table = wards.assign(
            pressure_level=wards["pressure_level"].map(ui.LEVEL_LABELS),
            trigger_reasons=wards["trigger_reasons"].fillna(""),
        )[
            [
                "ward_name",
                "pressure_level",
                "pressure_score",
                "precipitation_mm",
                "forecast_next_1h_mm",
                "forecast_next_6h_mm",
                "forecast_next_24h_mm",
                "trigger_reasons",
            ]
        ]
        st.dataframe(
            table,
            hide_index=True,
            height=560,
            column_config={
                "ward_name": "Phường/xã",
                "pressure_level": "Mức áp lực",
                "pressure_score": st.column_config.ProgressColumn(
                    "Điểm áp lực", min_value=0, max_value=100, format="%.0f"
                ),
                "precipitation_mm": st.column_config.NumberColumn(
                    "Mưa trong giờ", format="%.1f mm"
                ),
                "forecast_next_1h_mm": st.column_config.NumberColumn(
                    "1 giờ tới", format="%.1f mm"
                ),
                "forecast_next_6h_mm": st.column_config.NumberColumn(
                    "6 giờ tới", format="%.1f mm"
                ),
                "forecast_next_24h_mm": st.column_config.NumberColumn(
                    "24 giờ tới", format="%.1f mm"
                ),
                "trigger_reasons": "Lý do",
            },
        )
        st.download_button(
            "Tải CSV",
            table.to_csv(index=False).encode("utf-8"),
            file_name=f"hanoi_rain_pressure_{hour:%Y%m%dT%H}.csv",
            mime="text/csv",
        )

st.caption(
    "Nguồn: Open-Meteo (ECMWF IFS). Áp lực mưa là tín hiệu ưu tiên theo dõi, "
    "không phải xác suất hay độ sâu ngập. Giờ hiển thị theo Asia/Ho_Chi_Minh."
)
