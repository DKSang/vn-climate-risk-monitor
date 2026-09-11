"""Bản đồ forecast horizon hiện hành."""
# ruff: noqa: N999

from __future__ import annotations

import copy
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pydeck as pdk
import streamlit as st

from serving.dashboard.geography import load_hanoi_geojson
from serving.dashboard.queries import (
    load_flood_points_for_hour,
    load_forecast_by_hour,
    load_forecast_hour_summary,
    load_forecast_hours,
    load_forecast_metadata,
    load_forecast_pressure_ranking,
    load_serving_snapshot,
)
from serving.dashboard.ui import (
    MAP_STYLES,
    RAIN_INDICATORS,
    classify_rain_band,
    configure_page,
    default_hour_index,
    flood_point_layer,
    legend_row,
    local_time,
    metric_strip,
    navigation,
    number,
    page_header,
    pressure_score_label,
    rain_label,
    ward_polygon_layer,
    warn_if_stale,
)

configure_page("Bản đồ dự báo mưa", "◈")
navigation("map")

DEFAULT_COLOR = [75, 85, 99, 80]

published = load_serving_snapshot()
if not published:
    st.error("Không resolve được snapshot DuckLake hiện hành từ metadata PostgreSQL.")
    st.stop()
snapshot_version = int(published["snapshot_id"])
table_snapshot_version = int(published["table_snapshot_id"])
metadata = load_forecast_metadata(snapshot_version)
hours = load_forecast_hours(snapshot_version)
if not hours:
    st.warning("Chưa có forecast horizon hợp lệ trong Lakehouse.")
    st.stop()

page_header(
    "Operational rainfall map",
    "Bản đồ áp lực mưa",
    "Theo dõi 126 phường/xã theo từng giờ, ưu tiên lượng mưa dự kiến sắp tới "
    "và độ bền/tăng giảm của các forecast run. Đây là tín hiệu vận hành, không "
    "phải cảnh báo thiên tai chính thức.",
    f"Forecast S{table_snapshot_version} · {local_time(metadata.get('updated_at_utc'), '%H:%M · %d/%m')}",
)

warn_if_stale(metadata)

with st.container(border=True):
    control_left, indicator_col, control_right, style_col = st.columns(
        [2.0, 1.4, 1.2, 1.1]
    )
    with control_left:
        selected_index = st.select_slider(
            "Thời điểm dự báo · giờ Hà Nội (UTC+7)",
            options=range(len(hours)),
            value=default_hour_index(hours),
            format_func=lambda index: local_time(hours[index], "%H:%M · %d/%m"),
        )
        selected_hour = hours[selected_index]
    with indicator_col:
        selected_indicator = st.selectbox(
            "Chỉ báo trên bản đồ",
            tuple(RAIN_INDICATORS),
            help=(
                "Mặc định dùng tích lũy 24 giờ để không bỏ sót mưa dai nhiều giờ. "
                "Kịch bản QĐ 2280 theo 1 giờ vẫn được giữ để đối chiếu đúng nguồn."
            ),
        )
    with control_right:
        point_mode = st.selectbox(
            "Điểm úng ngập",
            ("Chỉ điểm đạt ngưỡng 1h", "Toàn bộ danh mục", "Không hiển thị"),
            help=(
                "Điểm QĐ 2280 chỉ được đối chiếu với kịch bản mưa 1 giờ; "
                "không suy trạng thái điểm từ tổng mưa 12/24 giờ."
            ),
        )
    with style_col:
        selected_style = st.selectbox(
            "Nền bản đồ",
            tuple(MAP_STYLES),
            index=0,
            help="Chuyển đổi kiểu bản đồ nền để xem rõ mạng lưới đường sá, sông ngòi.",
        )

indicator = RAIN_INDICATORS[selected_indicator]
# Forecast dùng cửa sổ nhìn về tương lai; archive replay vẫn dùng metric trailing
# trong RAIN_INDICATORS.
forecast_metric = {
    "Mưa tích lũy 24 giờ": "forecast_next_24h_mm",
    "Mưa tích lũy 12 giờ": "forecast_next_12h_mm",
    "Kịch bản QĐ 2280 · mưa 1 giờ": "forecast_next_1h_mm",
}[selected_indicator]
forecast_max_key = {
    "Mưa tích lũy 24 giờ": "max_forecast_next_24h_mm",
    "Mưa tích lũy 12 giờ": "max_forecast_next_12h_mm",
    "Kịch bản QĐ 2280 · mưa 1 giờ": "max_forecast_next_1h_mm",
}[selected_indicator]
forecast_summary_key = {
    "Mưa tích lũy 24 giờ": "forecast_elevated_ward_count_24h",
    "Mưa tích lũy 12 giờ": "forecast_elevated_ward_count_12h",
    "Kịch bản QĐ 2280 · mưa 1 giờ": "forecast_elevated_ward_count_1h",
}[selected_indicator]
window_label = str(indicator["window_label"])

df_forecast = load_forecast_by_hour(
    selected_hour,
    snapshot_version,
    sort_metric=forecast_metric,
)
summary = load_forecast_hour_summary(selected_hour, snapshot_version)
df_points = load_flood_points_for_hour(selected_hour, snapshot_version)
df_pressure = load_forecast_pressure_ranking(selected_hour, snapshot_version)

ward_count = int(summary.get("ward_count") or 0)
pressure_unknown_count = int(summary.get("pressure_unknown_ward_count") or 0)
max_pressure_label = pressure_score_label(summary.get("max_pressure_score"))
metric_strip(
    [
        (
            "Độ phủ phường/xã",
            f"{ward_count}/126",
            f"{int(summary.get('grid_count') or 0)} ô lưới",
        ),
        (
            f"Mưa dự kiến {window_label} tới lớn nhất",
            f"{number(summary.get(forecast_max_key)):.1f} mm",
            local_time(selected_hour, "%H:%M · %d/%m"),
        ),
        (
            "Phường vượt dải nền",
            f"{int(summary.get(forecast_summary_key) or 0)}",
            f"≥ ngưỡng {indicator['threshold']:.0f} mm",
        ),
        (
            "Phường áp lực cao",
            f"{int(summary.get('pressure_alert_ward_count') or 0)}",
            f"cao/vừa · đỉnh {max_pressure_label}",
        ),
    ]
)

if ward_count < 126:
    st.warning(
        f"Mốc giờ này chỉ phủ {ward_count}/126 phường/xã; phần thiếu không được nội suy."
    )
if pressure_unknown_count:
    st.warning(
        f"{pressure_unknown_count} phường chưa đủ forecast coverage tại giờ này; "
        "UNKNOWN không được tính là NORMAL."
    )

forecast_lookup: dict[str, dict] = {}
for row in df_forecast.to_dict(orient="records"):
    code = str(row.get("ward_code", "")).zfill(5)
    raw_metric_val = row.get(forecast_metric)
    # Không fallback sang trailing band: forecast map phải thể hiện đúng
    # completeness của cửa sổ nhìn về tương lai.
    band_val = classify_rain_band(raw_metric_val, selected_indicator)
    forecast_lookup[code] = {
        "metric_label": rain_label(raw_metric_val),
        "rain_1h_label": rain_label(row.get("rain_1h_mm")),
        "pressure_label": (
            f"{row.get('pressure_level') or 'Chưa có'} · "
            f"{pressure_score_label(row.get('pressure_score'), digits=1)}"
        ),
        "pressure_reason": str(row.get("trigger_reasons") or "Không có trigger"),
        "band": band_val,
    }

geojson_data = copy.deepcopy(load_hanoi_geojson())
boundary_codes: set[str] = set()
for feature in geojson_data.get("features", []):
    props = feature["properties"]
    code = str(props.get("ward_code", "")).zfill(5)
    boundary_codes.add(code)
    data = forecast_lookup.get(code)
    if data:
        props["fill_color"] = indicator["colors"].get(data["band"], DEFAULT_COLOR)
        props["metric_label"] = data["metric_label"]
        props["rain_1h_label"] = data["rain_1h_label"]
        props["scenario_label"] = indicator["labels"].get(
            data["band"], "Chưa phân loại"
        )
        props["pressure_label"] = data["pressure_label"]
        props["pressure_reason"] = data["pressure_reason"]
    else:
        props["fill_color"] = DEFAULT_COLOR
        props["metric_label"] = "Không có dữ liệu"
        props["rain_1h_label"] = "Không có dữ liệu"
        props["scenario_label"] = "Chưa phân loại"
        props["pressure_label"] = "Chưa có"
        props["pressure_reason"] = "Không có dữ liệu"
    props["tooltip_name"] = props["name"]
    props["tooltip_primary_label"] = selected_indicator
    props["tooltip_primary_value"] = props["metric_label"]
    props["tooltip_rain_1h"] = props["rain_1h_label"]
    props["tooltip_status"] = props["scenario_label"]
    props["tooltip_pressure"] = props["pressure_label"]
    props["tooltip_pressure_reason"] = props["pressure_reason"]

if len(boundary_codes) != 126:
    st.error(
        f"Bộ ranh giới S13 không đầy đủ: {len(boundary_codes)}/126 polygon. "
        "Bản đồ đã dừng để tránh hiển thị sai."
    )
    st.stop()

layers: list[pdk.Layer] = [ward_polygon_layer(geojson_data)]
pressure_by_ward = {
    str(row["ward_code"]).zfill(5): (
        f"{row.get('pressure_level') or 'Chưa có'} · "
        f"{pressure_score_label(row.get('pressure_score'), digits=1)}"
    )
    for row in df_forecast.to_dict(orient="records")
}
point_layer = flood_point_layer(
    df_points, point_mode, pressure_by_ward=pressure_by_ward
)
if point_layer is not None:
    layers.append(point_layer)

map_legend = tuple(indicator["legend"])
if point_mode != "Không hiển thị":
    map_legend += (("#06B6D4", "Điểm ngập đạt ngưỡng", "QĐ 2280 · mưa 1h"),)
legend_row(map_legend)

map_col, rank_col = st.columns([3.2, 1])
with map_col:
    deck = pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=20.975,
            longitude=105.653,
            zoom=9.15,
            pitch=0,
        ),
        map_style=MAP_STYLES.get(selected_style, MAP_STYLES["Tối (Dark Matter)"]),
        tooltip={
            "html": "<b>{tooltip_name}</b><br/>"
            "{tooltip_primary_label}: <b>{tooltip_primary_value}</b><br/>"
            "Mưa 1h cùng thời điểm: {tooltip_rain_1h}<br/>"
            "Áp lực mưa: {tooltip_pressure}<br/>"
            "Lý do: {tooltip_pressure_reason}<br/>"
            "Trạng thái: {tooltip_status}",
            "style": {
                "backgroundColor": "#181A1E",
                "color": "#FFFFFF",
                "fontSize": "12px",
            },
        },
    )
    st.pydeck_chart(deck, width="stretch", height=650)
    st.caption(
        "126/126 ranh giới hành chính S13 · Phiên bản NQ 1656/2025 · Không nội suy polygon."
    )

with rank_col, st.container(border=True):
    pressure_tab, rain_tab = st.tabs(["Áp lực mưa", f"Mưa tới {window_label}"])
    with pressure_tab:
        if df_pressure.empty:
            st.info("Pressure alert mart chưa có dữ liệu tại mốc giờ này.")
        else:
            pressure_table = df_pressure[
                [
                    "ward_name",
                    "pressure_level",
                    "pressure_score",
                    "coverage_status",
                    "forecast_next_24h_mm",
                    "persistence_runs",
                    "revision_direction",
                    "trigger_reasons",
                ]
            ].copy()
            pressure_table.columns = [
                "Phường/Xã",
                "Mức áp lực",
                "Điểm",
                "Coverage",
                "Mưa tới 24h",
                "Số run bền",
                "Xu hướng",
                "Lý do",
            ]
            st.dataframe(
                pressure_table,
                hide_index=True,
                width="stretch",
                height=400,
                column_config={
                    "Điểm": st.column_config.NumberColumn(format="%.1f"),
                    "Mưa tới 24h": st.column_config.NumberColumn(format="%.1f mm"),
                    "Số run bền": st.column_config.NumberColumn(format="%d"),
                },
            )
        st.caption("Điểm 0–100 để ưu tiên vận hành; không phải % xác suất ngập.")
    with rain_tab:
        if df_forecast.empty:
            st.info("Không có dữ liệu tại mốc giờ này.")
        else:
            ranking_fields = ["ward_name", forecast_metric]
            if forecast_metric != "forecast_next_1h_mm":
                ranking_fields.append("rain_1h_mm")
            ranking = df_forecast.head(10)[ranking_fields].copy()
            ranking.columns = (
                ["Phường/Xã", f"Mưa tới {window_label}", "Mưa 1h cùng giờ"]
                if forecast_metric != "forecast_next_1h_mm"
                else ["Phường/Xã", "Mưa tới 1h"]
            )
            st.dataframe(
                ranking,
                hide_index=True,
                width="stretch",
                height=400,
                column_config={
                    column: st.column_config.NumberColumn(format="%.1f")
                    for column in ranking.columns
                    if column != "Phường/Xã"
                },
            )
        st.caption("Đơn vị mm. Phường chung ô lưới có thể nhận cùng giá trị.")

st.caption(
    f"Horizon: {local_time(metadata.get('starts_at_utc'))} → "
    f"{local_time(metadata.get('ends_at_utc'))}. Dải màu là lượng mưa dự kiến "
    "từ mốc đang chọn về phía trước; điểm áp lực là heuristic có thể giải thích."
)
