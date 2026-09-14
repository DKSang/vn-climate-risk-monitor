"""Tra cứu forecast và điểm úng ngập theo phường/xã."""
# ruff: noqa: N999

from __future__ import annotations

import altair as alt
import pydeck as pdk
import streamlit as st

from serving.dashboard.common import load_all_wards, load_serving_snapshot
from serving.dashboard.forecast import (
    load_forecast_metadata,
    load_ward_flood_context,
    load_ward_forecast_summary,
    load_ward_forecast_timeseries,
)
from serving.dashboard.ui import (
    MAP_STYLES,
    POINT_INACTIVE_COLOR,
    POINT_SCENARIO_LABELS,
    POINT_TRIGGERED_COLOR,
    configure_page,
    local_time,
    metric_strip,
    navigation,
    number,
    page_header,
    pressure_score_label,
    to_local_naive,
    warn_if_stale,
)

configure_page("Chi tiết phường/xã", "⌖")
navigation("ward")

published = load_serving_snapshot()
if not published:
    st.error("Không resolve được snapshot DuckLake hiện hành từ metadata PostgreSQL.")
    st.stop()
snapshot_version = int(published["snapshot_id"])
table_snapshot_version = int(published["table_snapshot_id"])
wards = load_all_wards(snapshot_version)
metadata = load_forecast_metadata(snapshot_version)
if not wards:
    st.warning("Chưa có danh mục phường/xã hiện hành trong Lakehouse.")
    st.stop()

page_header(
    "Ward intelligence",
    "Chi tiết phường/xã",
    "Chọn một địa bàn để đọc lượng mưa dự kiến phía trước, tín hiệu áp lực "
    "và điều kiện kích hoạt của các điểm úng ngập liên quan.",
    f"Forecast S{table_snapshot_version} · {local_time(metadata.get('updated_at_utc'), '%H:%M · %d/%m')}",
)

warn_if_stale(metadata)

ward_options = {f"{ward['ward_name']} · {ward['ward_code']}": ward for ward in wards}
with st.container(border=True):
    selected_label = st.selectbox(
        "Chọn phường/xã",
        options=list(ward_options),
        help="Tìm theo tên hoặc mã hành chính 5 ký tự.",
    )
selected_ward = ward_options[selected_label]
ward_code = str(selected_ward["ward_code"])
ward_name = str(selected_ward["ward_name"])

summary = load_ward_forecast_summary(ward_code, snapshot_version)
df_ts = load_ward_forecast_timeseries(ward_code, snapshot_version)
df_points = load_ward_flood_context(ward_code, snapshot_version)

available_hours = int(summary.get("available_hours") or 0)
metric_strip(
    [
        (
            "Tổng mưa 24 giờ đầu",
            f"{number(summary.get('next_24h_rain_mm')):.1f} mm",
            ward_name,
        ),
        (
            "Tổng mưa horizon",
            f"{number(summary.get('horizon_rain_mm')):.1f} mm",
            f"{available_hours} giờ",
        ),
        (
            "Đỉnh mưa 1 giờ",
            f"{number(summary.get('peak_1h_mm')):.1f} mm",
            local_time(summary.get("peak_time_utc"), "%H:%M · %d/%m"),
        ),
        (
            "Áp lực mưa đầu horizon",
            str(summary.get("pressure_level") or "Chưa có"),
            pressure_score_label(summary.get("pressure_score")),
        ),
    ]
)

if df_ts.empty:
    st.info("Phường/xã này chưa có dữ liệu trong forecast horizon hiện hành.")
    st.stop()

df_ts = df_ts.copy()
df_ts["Giờ Hà Nội"] = to_local_naive(df_ts["valid_time_utc"])

chart_col, detail_col = st.columns([2, 1])
with chart_col:
    st.markdown(f"### Diễn biến mưa · {ward_name}")
    metric_choice = st.radio(
        "Chỉ số hiển thị",
        ("Mưa từng giờ", "Mưa dự kiến 6 giờ tới", "Mưa dự kiến 24 giờ tới"),
        horizontal=True,
        label_visibility="collapsed",
    )
    field, mark, label = {
        "Mưa từng giờ": ("precipitation_mm", "bar", "Mưa trong giờ (mm)"),
        "Mưa dự kiến 6 giờ tới": (
            "forecast_next_6h_mm",
            "line",
            "Mưa dự kiến 6 giờ tới (mm)",
        ),
        "Mưa dự kiến 24 giờ tới": (
            "forecast_next_24h_mm",
            "line",
            "Mưa dự kiến 24 giờ tới (mm)",
        ),
    }[metric_choice]

    base = alt.Chart(df_ts).encode(
        x=alt.X(
            "Giờ Hà Nội:T", title="Giờ Hà Nội", axis=alt.Axis(format="%H:%M\n%d/%m")
        ),
        y=alt.Y(f"{field}:Q", title=label),
        tooltip=[
            alt.Tooltip("Giờ Hà Nội:T", title="Thời gian", format="%H:%M · %d/%m/%Y"),
            alt.Tooltip(f"{field}:Q", title=label, format=".1f"),
            alt.Tooltip(
                "precipitation_probability_pct:Q", title="Xác suất mưa", format=".0f"
            ),
        ],
    )
    if mark == "bar":
        chart = base.mark_bar(
            color="#FFE900", cornerRadiusTopLeft=3, cornerRadiusTopRight=3
        )
    else:
        chart = base.mark_line(
            color="#FFE900",
            strokeWidth=3,
            point=alt.OverlayMarkDef(color="#F0B90B", size=35),
        )
    chart = (
        chart.configure(background="#181A1E")
        .configure_view(strokeOpacity=0)
        .configure_axis(
            domainColor="#373943",
            gridColor="#373943",
            labelColor="#C4C5CB",
            labelFont="Space Grotesk",
            titleColor="#C4C5CB",
            titleFont="Space Grotesk",
        )
        .configure_legend(
            labelColor="#C4C5CB",
            labelFont="Space Grotesk",
            titleColor="#FFFFFF",
            titleFont="Space Grotesk",
        )
    )
    st.altair_chart(chart.properties(height=410), width="stretch")
    st.caption(
        "Mưa từng giờ là lượng forecast tại timestamp. Cửa sổ 6/24 giờ là "
        "tổng sau timestamp hiện tại đến H giờ kế tiếp; NULL ở đuôi horizon nghĩa "
        "là chưa đủ dữ liệu forecast."
    )

with detail_col:
    st.markdown("### Điểm úng ngập trong danh mục")
    if df_points.empty:
        st.info(
            "Không có điểm thuộc danh mục nguồn tại địa bàn này. Điều này không đồng nghĩa không thể úng ngập."
        )
    else:
        reached = int(summary.get("triggered_point_count") or 0)
        st.markdown(
            f'<span class="status-pill">{reached}/{len(df_points)} điểm đạt ngưỡng</span>',
            unsafe_allow_html=True,
        )
        st.write("")
        point_table = df_points[
            ["point_name", "rain_scenario", "threshold_reached"]
        ].copy()
        point_table["rain_scenario"] = [
            POINT_SCENARIO_LABELS.get(str(value), "Không rõ")
            for value in point_table["rain_scenario"]
        ]
        point_table.columns = ["Điểm", "Ngưỡng danh mục", "Đạt ngưỡng"]
        st.dataframe(
            point_table,
            hide_index=True,
            width="stretch",
            column_config={"Đạt ngưỡng": st.column_config.CheckboxColumn()},
        )

        map_points = df_points.copy()
        map_points["color"] = [
            POINT_TRIGGERED_COLOR if bool(value) else POINT_INACTIVE_COLOR
            for value in map_points["threshold_reached"]
        ]
        map_points["line_color"] = [
            [255, 255, 255, 240] if bool(value) else [20, 21, 26, 200]
            for value in map_points["threshold_reached"]
        ]
        st.pydeck_chart(
            pdk.Deck(
                layers=[
                    pdk.Layer(
                        "ScatterplotLayer",
                        map_points,
                        get_position=["longitude", "latitude"],
                        get_fill_color="color",
                        get_line_color="line_color",
                        stroked=True,
                        line_width_min_pixels=2,
                        get_radius=120,
                        radius_min_pixels=7,
                        radius_max_pixels=14,
                        pickable=True,
                    )
                ],
                initial_view_state=pdk.ViewState(
                    latitude=number(selected_ward["ward_latitude"]),
                    longitude=number(selected_ward["ward_longitude"]),
                    zoom=13,
                ),
                tooltip={
                    "html": "<b>{point_name}</b><br/>Đỉnh mưa 1h: {peak_1h_mm} mm",
                    "style": {"backgroundColor": "#181A1E", "color": "#FFFFFF"},
                },
                map_style=MAP_STYLES["Tối (Dark Matter)"],
            ),
            width="stretch",
            height=320,
        )

with st.expander("Bảng dữ liệu forecast theo giờ"):
    table = df_ts[
        [
            "Giờ Hà Nội",
            "precipitation_mm",
            "precipitation_probability_pct",
            "forecast_next_6h_mm",
            "forecast_next_24h_mm",
        ]
    ].copy()
    table.columns = ["Giờ Hà Nội", "Mưa giờ", "Xác suất", "Mưa tới 6h", "Mưa tới 24h"]
    st.dataframe(
        table,
        hide_index=True,
        width="stretch",
        column_config={
            "Giờ Hà Nội": st.column_config.DatetimeColumn(format="HH:mm · DD/MM/YYYY"),
            "Mưa giờ": st.column_config.NumberColumn(format="%.1f mm"),
            "Xác suất": st.column_config.ProgressColumn(
                min_value=0, max_value=100, format="%.0f%%"
            ),
            "Mưa tới 6h": st.column_config.NumberColumn(format="%.1f mm"),
            "Mưa tới 24h": st.column_config.NumberColumn(format="%.1f mm"),
        },
    )

st.caption(
    "Các KPI tổng hợp phía trên được tính trực tiếp trong DuckDB. Pandas chỉ chuyển bảng kết quả "
    "sang định dạng mà Streamlit, Altair và PyDeck yêu cầu để render."
)
