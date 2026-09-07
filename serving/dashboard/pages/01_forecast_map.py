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
    load_forecast_risk_ranking,
    load_serving_snapshot,
)
from serving.dashboard.ui import (
    MAP_STYLES,
    POINT_INACTIVE_COLOR,
    POINT_SCENARIO_LABELS,
    POINT_TRIGGERED_COLOR,
    RAIN_INDICATORS,
    classify_rain_band,
    configure_page,
    default_hour_index,
    legend_row,
    local_time,
    metric_strip,
    navigation,
    number,
    page_header,
    rain_label,
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
    "Theo dõi 126 phường/xã theo từng giờ, kết hợp mưa nhiều cửa sổ với bằng "
    "chứng ngập lịch sử. Risk index là baseline thử nghiệm, không phải xác suất.",
    f"Forecast S{table_snapshot_version} · {local_time(metadata.get('updated_at_utc'), '%H:%M · %d/%m')}",
)

freshness_minutes = int(metadata.get("freshness_minutes") or 0)
if freshness_minutes > 90:
    st.warning(
        f"Snapshot đã chậm khoảng {freshness_minutes // 60} giờ "
        f"{freshness_minutes % 60} phút. Không dùng cho quyết định vận hành "
        "trước khi pipeline được kiểm tra."
    )

with st.container(border=True):
    control_left, indicator_col, control_right, style_col = st.columns([2.0, 1.4, 1.2, 1.1])
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
rain_metric = str(indicator["metric"])
window_label = str(indicator["window_label"])

df_forecast = load_forecast_by_hour(
    selected_hour,
    snapshot_version,
    sort_metric=rain_metric,
)
summary = load_forecast_hour_summary(selected_hour, snapshot_version)
df_points = load_flood_points_for_hour(selected_hour, snapshot_version)
df_risk = load_forecast_risk_ranking(selected_hour, snapshot_version)

ward_count = int(summary.get("ward_count") or 0)
metric_strip(
    [
        (
            "Độ phủ phường/xã",
            f"{ward_count}/126",
            f"{int(summary.get('grid_count') or 0)} ô lưới",
        ),
        (
            f"Mưa {window_label} lớn nhất",
            f"{number(summary.get(str(indicator['max_key']))):.1f} mm",
            local_time(selected_hour, "%H:%M · %d/%m"),
        ),
        (
            "Phường vượt dải nền",
            f"{int(summary.get(str(indicator['summary_key'])) or 0)}",
            indicator["unit"],
        ),
        (
            "Phường Risk index ≥ 50",
            f"{int(summary.get('experimental_risk_50_count') or 0)}",
            "Heuristic · chưa hiệu chỉnh",
        ),
    ]
)

if ward_count < 126:
    st.warning(
        f"Mốc giờ này chỉ phủ {ward_count}/126 phường/xã; phần thiếu không được nội suy."
    )

forecast_lookup: dict[str, dict] = {}
for row in df_forecast.to_dict(orient="records"):
    code = str(row.get("ward_code", "")).zfill(5)
    raw_metric_val = row.get(rain_metric)
    band_val = classify_rain_band(raw_metric_val, selected_indicator) or str(
        row.get(str(indicator["band"])) or ""
    )
    forecast_lookup[code] = {
        "metric_label": rain_label(raw_metric_val),
        "rain_1h_label": rain_label(row.get("rain_1h_mm")),
        "risk_label": f"{number(row.get('risk_score')):.1f}/100",
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
        props["risk_label"] = data["risk_label"]
    else:
        props["fill_color"] = DEFAULT_COLOR
        props["metric_label"] = "Không có dữ liệu"
        props["rain_1h_label"] = "Không có dữ liệu"
        props["scenario_label"] = "Chưa phân loại"
        props["risk_label"] = "Chưa có"
    props["tooltip_name"] = props["name"]
    props["tooltip_primary_label"] = selected_indicator
    props["tooltip_primary_value"] = props["metric_label"]
    props["tooltip_rain_1h"] = props["rain_1h_label"]
    props["tooltip_status"] = props["scenario_label"]
    props["tooltip_risk"] = props["risk_label"]

if len(boundary_codes) != 126:
    st.error(
        f"Bộ ranh giới S13 không đầy đủ: {len(boundary_codes)}/126 polygon. "
        "Bản đồ đã dừng để tránh hiển thị sai."
    )
    st.stop()

layers: list[pdk.Layer] = [
    pdk.Layer(
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
]

visible_points = df_points
if point_mode == "Chỉ điểm đạt ngưỡng 1h":
    visible_points = df_points.loc[df_points["is_triggered"].fillna(False)]
elif point_mode == "Không hiển thị":
    visible_points = df_points.iloc[0:0]

if not visible_points.empty:
    visible_points = visible_points.copy()
    visible_points["color"] = [
        POINT_TRIGGERED_COLOR if bool(value) else POINT_INACTIVE_COLOR
        for value in visible_points["is_triggered"]
    ]
    visible_points["line_color"] = [
        [255, 255, 255, 240] if bool(value) else [20, 21, 26, 200]
        for value in visible_points["is_triggered"]
    ]
    visible_points["threshold_label"] = [
        POINT_SCENARIO_LABELS.get(str(value), "Không rõ")
        for value in visible_points["rain_scenario"]
    ]
    visible_points["tooltip_name"] = visible_points["point_name"]
    visible_points["tooltip_rain_1h"] = [
        rain_label(value) for value in visible_points["rain_1h_mm"]
    ]
    visible_points["tooltip_primary_label"] = "Kịch bản danh mục"
    visible_points["tooltip_primary_value"] = visible_points["threshold_label"]
    risk_by_ward = {
        str(row["ward_code"]).zfill(5): f"{number(row.get('risk_score')):.1f}/100"
        for row in df_forecast.to_dict(orient="records")
    }
    visible_points["tooltip_risk"] = [
        risk_by_ward.get(str(code).zfill(5), "Chưa có")
        for code in visible_points["ward_code"]
    ]
    visible_points["tooltip_status"] = [
        "Đã đạt ngưỡng" if bool(value) else f"Chưa đạt · {label}"
        for value, label in zip(
            visible_points["is_triggered"],
            visible_points["threshold_label"],
            strict=True,
        )
    ]
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            visible_points,
            get_position=["longitude", "latitude"],
            get_fill_color="color",
            get_line_color="line_color",
            stroked=True,
            line_width_min_pixels=1.5,
            get_radius=160,
            radius_min_pixels=6,
            radius_max_pixels=12,
            pickable=True,
        )
    )

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
            "Risk index: {tooltip_risk}<br/>"
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
    risk_tab, rain_tab = st.tabs(["Risk thử nghiệm", f"Mưa {window_label}"])
    with risk_tab:
        if df_risk.empty:
            st.info("Risk mart chưa có dữ liệu tại mốc giờ này.")
        else:
            risk_table = df_risk[
                ["ward_name", "risk_score", "hazard_index", "vulnerability_index"]
            ].copy()
            risk_table.columns = ["Phường/Xã", "Risk", "Mưa H", "Tổn thương V"]
            st.dataframe(
                risk_table,
                hide_index=True,
                width="stretch",
                height=400,
                column_config={
                    "Risk": st.column_config.NumberColumn(format="%.1f"),
                    "Mưa H": st.column_config.NumberColumn(format="%.2f"),
                    "Tổn thương V": st.column_config.NumberColumn(format="%.2f"),
                },
            )
        st.caption("0–100 để xếp hạng tương đối; không phải % xác suất ngập.")
    with rain_tab:
        if df_forecast.empty:
            st.info("Không có dữ liệu tại mốc giờ này.")
        else:
            ranking_fields = ["ward_name", rain_metric]
            if rain_metric != "rain_1h_mm":
                ranking_fields.append("rain_1h_mm")
            ranking = df_forecast.head(10)[ranking_fields].copy()
            ranking.columns = (
                ["Phường/Xã", f"Mưa {window_label}", "Mưa 1h"]
                if rain_metric != "rain_1h_mm"
                else ["Phường/Xã", "Mưa 1h"]
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
    f"{local_time(metadata.get('ends_at_utc'))}. Dải màu là áp lực khí tượng, "
    "Risk index kết hợp H×V nhưng chưa được hiệu chỉnh thành xác suất hoặc độ sâu ngập."
)
