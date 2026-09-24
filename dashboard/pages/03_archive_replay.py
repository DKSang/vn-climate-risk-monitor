"""Phát lại weather archive để kiểm chứng giao diện theo ngày và giờ."""
# ruff: noqa: N999

from __future__ import annotations

import copy
from datetime import date

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

from dashboard.archive import (
    load_archive_by_hour,
    load_archive_event_timeseries,
    load_archive_hour_summary,
    load_archive_hours_for_local_date,
    load_archive_models,
    load_archive_peak_hour_for_local_date,
    load_archive_top_events,
)
from dashboard.common import (
    load_serving_snapshot,
)
from dashboard.geography import load_hanoi_geojson
from dashboard.ui import (
    MAP_STYLES,
    RAIN_INDICATORS,
    configure_page,
    legend_row,
    local_time,
    metric_strip,
    navigation,
    number,
    page_header,
    rain_label,
    to_local_naive,
    ward_polygon_layer,
)

configure_page("Phát lại mưa quá khứ", "◷")
navigation("archive")

DEFAULT_COLOR = [75, 85, 99, 80]
MODEL_LABELS = {
    "ecmwf_ifs": "ECMWF IFS archive",
    "era5": "ERA5 reanalysis",
}


def _as_local_date(value: object) -> date:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    return stamp.tz_convert("Asia/Ho_Chi_Minh").date()


@st.cache_data(show_spinner=False)
def _cached_archive_models(snapshot_version: int) -> list[dict]:
    return load_archive_models(snapshot_version)


@st.cache_data(show_spinner=False)
def _cached_top_events(
    weather_model: str,
    rain_metric: str,
    alert_threshold_mm: float,
    snapshot_version: int,
) -> list[dict]:
    return load_archive_top_events(
        weather_model,
        snapshot_version,
        rain_metric=rain_metric,
        alert_threshold_mm=alert_threshold_mm,
    )


@st.cache_data(show_spinner=False)
def _cached_hours(
    weather_model: str,
    local_date: date,
    snapshot_version: int,
) -> list:
    return load_archive_hours_for_local_date(
        weather_model,
        local_date,
        snapshot_version,
    )


@st.cache_data(show_spinner=False)
def _cached_peak_hour(
    weather_model: str,
    local_date: date,
    rain_metric: str,
    snapshot_version: int,
):
    return load_archive_peak_hour_for_local_date(
        weather_model,
        local_date,
        rain_metric,
        snapshot_version,
    )


published = load_serving_snapshot(table_name="fct_rain_archive_hourly")
if not published:
    st.error("Không resolve được snapshot của bảng weather archive từ PostgreSQL.")
    st.stop()

snapshot_version = int(published["snapshot_id"])
table_snapshot_version = int(published["table_snapshot_id"])
models = _cached_archive_models(snapshot_version)
if not models:
    st.warning("Bảng weather archive chưa có dữ liệu để phát lại.")
    st.stop()
page_header(
    "Historical weather replay",
    "Phát lại mưa quá khứ",
    "Dùng các giờ mưa đã có trong weather_archive để kiểm tra ranh giới, màu ngưỡng "
    "và logic ngưỡng mưa trước khi forecast xuất hiện mưa lớn.",
    f"Archive S{table_snapshot_version}",
)

st.info(
    "Đây là dữ liệu archive/reanalysis theo ô lưới từ Open-Meteo, không phải số đo "
    "quan trắc tại trạm. Trang này kiểm chứng cách UI diễn giải dữ liệu theo ô lưới."
)

model_lookup = {str(item["weather_model"]): item for item in models}
model_options = list(model_lookup)
default_model_index = (
    model_options.index("ecmwf_ifs") if "ecmwf_ifs" in model_options else 0
)
with st.container(border=True):
    model_col, indicator_col, style_col = st.columns([1.1, 1.3, 1.1])
    with model_col:
        selected_model = st.selectbox(
            "Nguồn archive",
            model_options,
            index=default_model_index,
            format_func=lambda value: (
                f"{MODEL_LABELS.get(value, value)} · "
                f"{int(model_lookup[value].get('grid_count') or 0)} ô"
            ),
        )
    with indicator_col:
        selected_indicator = st.selectbox(
            "Chỉ báo trên bản đồ",
            tuple(RAIN_INDICATORS),
            help=(
                "Mặc định dùng tích lũy 24 giờ để không bỏ sót sự kiện mưa kéo dài. "
                "Kịch bản QĐ 2280 theo 1 giờ vẫn được giữ để đối chiếu đúng nguồn."
            ),
        )
    with style_col:
        selected_style = st.selectbox(
            "Nền bản đồ",
            tuple(MAP_STYLES),
            index=0,
            help="Chuyển đổi kiểu bản đồ nền để xem rõ mạng lưới đường sá, sông ngòi.",
        )

    selection_options = ["Giờ mưa nổi bật", "Chọn ngày/giờ"]
    selection_mode = st.radio(
        "Cách chọn thời điểm",
        selection_options,
        horizontal=True,
    )
    indicator = RAIN_INDICATORS[selected_indicator]
    rain_metric = str(indicator["metric"])
    window_label = str(indicator["window_label"])
    selected_metadata = model_lookup[selected_model]
    top_events = _cached_top_events(
        selected_model,
        rain_metric,
        float(indicator["threshold"]),
        snapshot_version,
    )
    if selection_mode == "Giờ mưa nổi bật":
        if not top_events:
            st.warning("Không tìm thấy sự kiện mưa trong nguồn archive đã chọn.")
            st.stop()
        event_options = list(range(len(top_events)))
        selected_event_index = st.selectbox(
            "Sự kiện để kiểm tra",
            event_options,
            format_func=lambda index: (
                f"{local_time(top_events[index]['valid_time_utc'])} · "
                f"đỉnh {float(top_events[index]['max_metric_mm'] or 0):.1f} mm · "
                f"{int(top_events[index]['elevated_grid_count'] or 0)} ô vượt dải nền"
            ),
        )
        selected_hour = top_events[selected_event_index]["valid_time_utc"]
    else:
        default_date = (
            _as_local_date(top_events[0]["valid_time_utc"])
            if top_events
            else _as_local_date(selected_metadata["ends_at_utc"])
        )
        selected_date = st.date_input(
            "Ngày Hà Nội (UTC+7)",
            value=default_date,
            min_value=_as_local_date(selected_metadata["starts_at_utc"]),
            max_value=_as_local_date(selected_metadata["ends_at_utc"]),
            format="DD/MM/YYYY",
        )
        archive_hours = _cached_hours(
            selected_model,
            selected_date,
            snapshot_version,
        )
        if not archive_hours:
            st.warning("Ngày đã chọn không có giờ dữ liệu trong archive.")
            st.stop()
        peak_hour = _cached_peak_hour(
            selected_model,
            selected_date,
            rain_metric,
            snapshot_version,
        )
        default_hour = peak_hour if peak_hour in archive_hours else archive_hours[0]
        selected_hour = st.select_slider(
            "Giờ Hà Nội (UTC+7)",
            options=archive_hours,
            value=default_hour,
            format_func=lambda value: local_time(value, "%H:%M"),
            key=f"archive-hour-{selected_model}-{rain_metric}-{selected_date.isoformat()}",
        )

df_archive = load_archive_by_hour(
    selected_model,
    selected_hour,
    snapshot_version,
    sort_metric=rain_metric,
)
summary = load_archive_hour_summary(selected_model, selected_hour, snapshot_version)
df_event = load_archive_event_timeseries(
    selected_model,
    selected_hour,
    snapshot_version,
)
ward_count = int(summary.get("ward_count") or 0)
elevated_count = int(summary.get(str(indicator["summary_key"])) or 0)
metric_strip(
    [
        (
            "Thời điểm phát lại",
            local_time(selected_hour, "%H:%M"),
            local_time(selected_hour, "%d/%m/%Y"),
        ),
        (
            f"Mưa {window_label} lớn nhất",
            f"{number(summary.get(str(indicator['max_key']))):.1f} mm",
            f"{int(summary.get('grid_count') or 0)} ô lưới",
        ),
        (
            "Phường vượt dải nền",
            f"{elevated_count}",
            f"Độ phủ {ward_count}/126",
        ),
    ]
)

if ward_count < 126:
    st.warning(
        f"Mốc giờ này chỉ ánh xạ được {ward_count}/126 phường/xã; phần thiếu không được nội suy."
    )

archive_lookup: dict[str, dict] = {}
for row in df_archive.to_dict(orient="records"):
    code = str(row.get("ward_code", "")).zfill(5)
    raw_metric_val = row.get(rain_metric)
    band_val = str(row.get(str(indicator["band"])) or "")
    archive_lookup[code] = {
        # `rain_*h_mm` NULL nghĩa là cửa sổ thiếu giờ, KHÔNG phải mưa 0 mm.
        "metric_label": rain_label(raw_metric_val),
        "rain_1h_label": rain_label(row.get("rain_1h_mm")),
        "band": band_val,
    }

geojson_data = copy.deepcopy(load_hanoi_geojson())
boundary_codes: set[str] = set()
for feature in geojson_data.get("features", []):
    props = feature["properties"]
    code = str(props.get("ward_code", "")).zfill(5)
    boundary_codes.add(code)
    data = archive_lookup.get(code)
    if data:
        props["fill_color"] = indicator["colors"].get(data["band"], DEFAULT_COLOR)
        props["metric_label"] = data["metric_label"]
        props["rain_1h_label"] = data["rain_1h_label"]
        props["scenario_label"] = indicator["labels"].get(
            data["band"], "Chưa phân loại"
        )
    else:
        props["fill_color"] = DEFAULT_COLOR
        props["metric_label"] = "Không có dữ liệu"
        props["rain_1h_label"] = "Không có dữ liệu"
        props["scenario_label"] = "Chưa phân loại"
    props["tooltip_name"] = props["name"]
    props["tooltip_primary_label"] = selected_indicator
    props["tooltip_primary_value"] = props["metric_label"]
    props["tooltip_rain_1h"] = props["rain_1h_label"]
    props["tooltip_status"] = props["scenario_label"]

if len(boundary_codes) != 126:
    st.error(
        f"Bộ ranh giới S13 không đầy đủ: {len(boundary_codes)}/126 polygon. "
        "Bản đồ đã dừng để tránh hiển thị sai."
    )
    st.stop()

layers: list[pdk.Layer] = [ward_polygon_layer(geojson_data)]
map_legend = tuple(indicator["legend"])
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
            "Trạng thái: {tooltip_status}",
            "style": {
                "backgroundColor": "#181A1E",
                "color": "#FFFFFF",
                "fontSize": "12px",
            },
        },
    )
    st.pydeck_chart(deck, width="stretch", height=620)
    st.caption(
        "126/126 ranh giới hành chính S13 · Giá trị phường lấy từ ô lưới được ánh xạ, không nội suy polygon."
    )

with rank_col, st.container(border=True):
    st.markdown(f"#### Top 10 · {window_label}")
    if df_archive.empty:
        st.info("Không có dữ liệu tại mốc giờ này.")
    else:
        ranking_fields = ["ward_name", rain_metric]
        if rain_metric != "rain_1h_mm":
            ranking_fields.append("rain_1h_mm")
        ranking = df_archive.head(10)[ranking_fields].copy()
        ranking.columns = (
            ["Phường/Xã", f"Mưa {window_label}", "Mưa 1h"]
            if rain_metric != "rain_1h_mm"
            else ["Phường/Xã", "Mưa 1h"]
        )
        number_columns = {
            column: st.column_config.NumberColumn(format="%.1f")
            for column in ranking.columns
            if column != "Phường/Xã"
        }
        st.dataframe(
            ranking,
            hide_index=True,
            width="stretch",
            height=410,
            column_config=number_columns,
        )
    st.caption("Đơn vị mm. Phường chung ô lưới có thể nhận cùng giá trị.")

st.markdown("### Diễn biến quanh thời điểm đã chọn")
if df_event.empty:
    st.info("Không có chuỗi thời gian quanh sự kiện.")
else:
    df_event = df_event.copy()
    df_event["Giờ Hà Nội"] = to_local_naive(df_event["valid_time_utc"])
    selected_local = to_local_naive(pd.Series([selected_hour])).iloc[0]
    max_series = f"max_{rain_metric}"
    avg_series = f"avg_{rain_metric}"
    line_chart = (
        alt.Chart(df_event)
        .transform_fold(
            [max_series, avg_series],
            as_=["series", "rain_mm"],
        )
        .transform_calculate(
            label=f"datum.series === '{max_series}' ? 'Cao nhất theo ô' : 'Trung bình các ô'"
        )
        .mark_line(strokeWidth=3, point=True)
        .encode(
            x=alt.X(
                "Giờ Hà Nội:T",
                title="Giờ Hà Nội",
                axis=alt.Axis(format="%H:%M\n%d/%m"),
            ),
            y=alt.Y("rain_mm:Q", title=f"Mưa {window_label} (mm)"),
            color=alt.Color(
                "label:N",
                title=None,
                scale=alt.Scale(
                    domain=["Cao nhất theo ô", "Trung bình các ô"],
                    range=["#FFE900", "#8C8F9B"],
                ),
            ),
            tooltip=[
                alt.Tooltip(
                    "Giờ Hà Nội:T", title="Thời gian", format="%H:%M · %d/%m/%Y"
                ),
                alt.Tooltip("label:N", title="Chuỗi"),
                alt.Tooltip("rain_mm:Q", title=f"Mưa {window_label}", format=".1f"),
            ],
        )
    )
    selected_rule = (
        alt.Chart(pd.DataFrame({"Giờ Hà Nội": [selected_local]}))
        .mark_rule(color="#F0B90B", strokeDash=[5, 4], strokeWidth=2)
        .encode(x="Giờ Hà Nội:T")
    )
    chart = (
        alt.layer(line_chart, selected_rule)
        .properties(height=320)
        .configure(background="#181A1E")
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
    st.altair_chart(chart, width="stretch")

with st.expander("Bảng dữ liệu 126 phường/xã tại giờ đã chọn"):
    table = df_archive[
        ["ward_code", "ward_name", "rain_1h_mm", "rain_6h_mm", "rain_24h_mm"]
    ].copy()
    table.columns = ["Mã", "Phường/Xã", "Mưa 1h", "Trượt 6h", "Trượt 24h"]
    st.dataframe(
        table,
        hide_index=True,
        width="stretch",
        column_config={
            "Mưa 1h": st.column_config.NumberColumn(format="%.1f mm"),
            "Trượt 6h": st.column_config.NumberColumn(format="%.1f mm"),
            "Trượt 24h": st.column_config.NumberColumn(format="%.1f mm"),
        },
    )

st.caption(
    f"Nguồn {MODEL_LABELS.get(selected_model, selected_model)}: "
    f"{local_time(selected_metadata.get('starts_at_utc'), '%d/%m/%Y')} → "
    f"{local_time(selected_metadata.get('ends_at_utc'), '%d/%m/%Y')} · "
    "KPI được tính trực tiếp trong DuckDB; DataFrame chỉ dùng cho component hiển thị."
)
