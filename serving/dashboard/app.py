"""Hanoi Flood & Climate Risk Monitor — trang tổng quan."""

from __future__ import annotations

import streamlit as st

from serving.dashboard.common import load_all_wards, load_serving_snapshot
from serving.dashboard.forecast import load_forecast_metadata
from serving.dashboard.ui import (
    configure_page,
    hero,
    local_time,
    metric_strip,
    navigation,
    warn_if_stale,
)

configure_page("Hanoi Climate Monitor", "◈")
navigation("home")

published = load_serving_snapshot()
if not published:
    st.error("Không resolve được snapshot DuckLake hiện hành từ metadata PostgreSQL.")
    st.stop()
snapshot_version = int(published["snapshot_id"])
table_snapshot_version = int(published["table_snapshot_id"])
metadata = load_forecast_metadata(snapshot_version)
wards = load_all_wards(snapshot_version)

hero(
    "Hanoi Climate Intelligence",
    "Hà Nội. <em>Trong một nhịp.</em>",
    "Theo dõi áp lực mưa tối đa 72 giờ trên 126 phường/xã, đối chiếu ngưỡng vận hành "
    "và danh mục điểm úng ngập trong một giao diện thống nhất.",
    rotating_words=(
        "Đúng snapshot.",
        "72 giờ tới.",
        "126 phường/xã.",
        "Theo ngưỡng mưa.",
    ),
)

updated = local_time(metadata.get("updated_at_utc"))
horizon = int(metadata.get("horizon_hours") or 0)
grid_count = int(metadata.get("grid_count") or 0)
ward_count = int(metadata.get("ward_count") or 0)

st.markdown(
    f'<span class="status-pill">● Forecast S{table_snapshot_version} · dữ liệu {updated}</span>',
    unsafe_allow_html=True,
)
st.write("")

metric_strip(
    [
        ("Phường/xã hiện hành", f"{len(wards):,}", "Hà Nội"),
        ("Phường có dự báo", f"{ward_count:,}", f"{grid_count} ô lưới"),
        ("Horizon còn hiệu lực", f"{horizon} giờ", "Snapshot mới nhất"),
        (
            "Điểm trong danh mục",
            f"{int(metadata.get('flood_point_count') or 0):,}",
            "Có nguồn và tọa độ",
        ),
    ]
)

warn_if_stale(metadata)

if ward_count and ward_count < len(wards):
    st.warning(
        f"Snapshot hiện chỉ phủ {ward_count}/{len(wards)} phường/xã. "
        "Dashboard không nội suy các phường còn thiếu."
    )

st.write("")
st.markdown(
    '<div class="section-kicker">Khám phá dữ liệu</div>', unsafe_allow_html=True
)
st.subheader("Từ toàn cảnh đến từng phường")

left, middle, right = st.columns(3)
with left, st.container(border=True):
    st.markdown("### Bản đồ dự báo")
    st.write(
        "Xem phân bố mưa theo giờ, độ phủ dữ liệu, các dải ngưỡng và điểm "
        "úng ngập thực sự đạt điều kiện kích hoạt."
    )
    st.page_link("pages/01_forecast_map.py", label="Mở bản đồ  →", icon="🗺️")

with middle, st.container(border=True):
    st.markdown("### Phát lại quá khứ")
    st.write(
        "Mở các giờ mưa lớn trong archive hoặc chọn ngày/giờ để kiểm tra màu ngưỡng, "
        "ranh giới và điểm úng ngập."
    )
    st.page_link(
        "pages/03_archive_replay.py",
        label="Mở dữ liệu archive  →",
        icon="⏱️",
    )

with right, st.container(border=True):
    st.markdown("### Chi tiết phường/xã")
    st.write(
        "Phân tích mưa từng giờ, tổng mưa trong horizon, đỉnh mưa và danh "
        "mục điểm úng ngập của một địa bàn."
    )
    st.page_link(
        "pages/02_ward_drilldown.py",
        label="Tra cứu địa bàn  →",
        icon="🔎",
    )

st.write("")
st.markdown(
    '<div class="section-kicker">Phạm vi diễn giải</div>', unsafe_allow_html=True
)
st.subheader("Số liệu nói điều gì — và không nói điều gì")

col_a, col_b, col_c = st.columns(3)
with col_a, st.container(border=True):
    st.markdown("#### Áp lực khí tượng")
    st.write(
        "Lượng mưa theo ô lưới là forcing khí tượng, không phải xác suất hay độ sâu ngập."
    )
with col_b, st.container(border=True):
    st.markdown("#### Ngưỡng vận hành")
    st.write(
        "Các dải 50 / 70 / 100 mm mỗi giờ là đối chiếu kịch bản, không khẳng định mọi điểm sẽ ngập."
    )
with col_c, st.container(border=True):
    st.markdown("#### Độ phân giải")
    st.write(
        "Nhiều phường có thể dùng chung một ô lưới; giá trị giống nhau không phải các dự báo độc lập."
    )

st.caption(
    "Nguồn thời tiết: Open-Meteo · Danh mục điểm: dữ liệu seed trích từ nguồn tham chiếu dự án · "
    "Mốc thời gian trên giao diện dùng Asia/Ho_Chi_Minh (UTC+7)."
)
