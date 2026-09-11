# Bước 8: Serving & Dashboard

**Hanoi Flood & Climate Risk Monitor** · v2.1 · 2026-09-08

Tài liệu này hướng dẫn kiến trúc lớp phục vụ (Serving Layer) và giao diện trực quan hóa thông tin (Dashboard) dành cho người dân và cán bộ vận hành.

---

## 1. Mục tiêu & Đối tượng

- **Đối tượng phục vụ**:
  - **Người dân & tài xế**: Tra cứu diễn biến mưa theo giờ cho phường/xã mình sinh sống hoặc di chuyển; tránh các khu vực có nguy cơ úng ngập.
  - **Cán bộ UBND phường/xã & HSDC**: Nhận diện sớm các ô lưới có mưa vượt ngưỡng vận hành để kích hoạt kịch bản ứng phó trước 6–12 giờ.
- **Nguyên tắc sản phẩm**:
  - Minh bạch giới hạn độ phân giải: hiển thị số ô lưới thực có trong snapshot
    (snapshot hiện hành có **48 ô**) và độ phủ trên 126 phường/xã.
  - Tuyệt đối không công bố "xác suất ngập" hay "độ sâu ngập" khi chưa có mô hình thủy lực và số liệu quan trắc cống rãnh thực tế. Forecast serving công bố `fct_rain_pressure_alert` với level/score/reason; đây là tín hiệu ưu tiên vận hành, không phải cảnh báo pháp lý.

---

## 2. Kiến trúc Serving Layer

```text
               ┌────────────────────────────────────────────────────────┐
               │              Gold Marts (DuckLake / S3)                │
               │  - fct_rain_forecast_hourly (history)                  │
               │  - fct_rain_forecast_current_hourly (serving view)     │
               │  - fct_rain_pressure_alert (operational signal)        │
               │  - dim_ward / dim_flood_point / bridge_ward_grid       │
               └──────────────────────────┬─────────────────────────────┘
                                          │
                                          ▼
                      get_connection(read_only=True)
                      + SNAPSHOT_VERSION từ PostgreSQL
                                          │
                     ┌────────────────────┴────────────────────┐
                     │                                         │
                     ▼                                         ▼
         [Operational REST API]                     [Streamlit Dashboard]
          serving/api/app/main.py                    serving/dashboard/app.py
          - /health/live                             - Trang 1: Bản đồ dự báo tối đa 72h
          - /health/ready                            - Trang 2: Drill-down phường
          - /ops (HTML Dashboard)                    - PyDeck (deck.gl GPU maps)
          (Port 8000)                                (Port 8501)
```

---

## 3. Các chức năng của Dashboard

### 3.1 Trang 1: Bản đồ dự báo & Kịch bản mưa tối đa 72h (`pages/01_forecast_map.py`)
- **Choropleth phường/xã (GeoJsonLayer)**:
  - Tô màu theo lượng mưa forecast phía trước (1/12/24 giờ tùy lựa chọn),
    không dùng rolling trailing để cảnh báo tương lai.
- **Điểm úng ngập QĐ 2280 (ScatterplotLayer)**:
  - Overlay đúng **15 điểm đã có nguồn và tọa độ trong seed hiện hành**.
    Dashboard không suy diễn con số 220+ từ nhãn kịch bản và không tự tạo thêm điểm.
- **Slider thời gian**:
  - Cho phép người dùng tua forecast horizon hiện hành theo giờ Hà Nội (UTC+7),
    bước nhảy một giờ.
- **Thống kê Top áp lực mưa**:
  - Bảng xếp hạng 10 phường theo `pressure_score`, kèm level, lý do, persistence
    và revision của forecast.
  - `UNKNOWN` biểu thị horizon thiếu coverage. UI không hiển thị score NULL thành
    `0/100` và không tính `UNKNOWN` như `NORMAL`.

### 3.2 Trang 2: Tra cứu chi tiết theo Phường/Xã (`pages/02_ward_drilldown.py`)
- **Bộ lọc Dropdown**: Cho phép chọn bất kỳ phường nào trong 126 phường/xã.
- **Chuỗi thời gian forecast**:
  - Biểu đồ Altair cho phép chuyển giữa mưa từng giờ, tổng mưa 6 giờ tới và tổng
    mưa 24 giờ tới; cửa sổ tương lai bắt đầu sau timestamp hiện tại.
- **Danh sách điểm ngập nội bộ**:
  - Lọc ra các điểm ngập thuộc địa bàn phường được chọn kèm mini-map tập trung vào khu vực đó.

### 3.3 Trang phát lại dữ liệu quá khứ (`pages/03_archive_replay.py`)
- Mặc định tô bản đồ theo `rain_24h_mm` và `vn_rain_band_24h`; người dùng có thể
  chuyển sang `rain_12h_mm` hoặc kịch bản mưa một giờ QĐ 2280.
- Không hạ hoặc đổi nghĩa các ngưỡng 50/70/100 mm/h của QĐ 2280. Chúng được giữ
  thành một lớp tham chiếu riêng, không dùng để phủ định ngập do mưa tích lũy.
- Khi chọn ngày, dashboard tự chuyển tới giờ có cực trị lớn nhất của chỉ báo đang
  xem và tạo widget state riêng theo model × chỉ báo × ngày, tránh giữ timestamp
  của ngày trước.
- Điểm QĐ 2280 vẫn chỉ được đánh giá bằng kịch bản mưa một giờ. Tổng mưa 12/24 giờ
  không được dùng để suy diễn một điểm chắc chắn ngập.

---

## 4. Dữ liệu địa lý GeoJSON (`hanoi_wards.geojson`)

File GeoJSON được đồng bộ và chuẩn hóa bằng script:
```bash
uv run python scripts/fetch_hanoi_geojson.py
```
- Đây là thao tác phát triển trên host vì script cập nhật file versioned trong
  repo; cần Python/`uv`.
- Script tải đúng bộ S13 từ `vietnamese-provinces-database` tại commit đã ghim,
  sau đó kiểm tra tập mã với `commune_code` trong
  [transform/seeds/ward_coordinates_seed.csv](../transform/seeds/ward_coordinates_seed.csv).
- Chỉ ghi output khi có đủ đúng 126 mã phường/xã; không dùng OSM hay polygon
  xấp xỉ từ centroid làm fallback.

---

## 5. Hướng dẫn chạy và Vận hành

### 5.1 Chạy qua Docker Compose
Dashboard là một phần của stack mặc định:
```bash
docker compose up -d --build

docker compose logs -f dashboard
```

Compose build image bất biến từ `uv.lock`; container không bind-mount
source và không cài dependency lại khi khởi động.

### 5.2 Chạy trên host khi phát triển

```bash
uv run streamlit run serving/dashboard/app.py --server.port 8501 --server.address 0.0.0.0
uv run uvicorn serving.api.app.main:app --host 0.0.0.0 --port 8000
```

Các lệnh này cần Python/`uv` và cấu hình kết nối runtime trên host.

---

## 6. Ghi chú thiết kế kỹ thuật
- **Snapshot-consistent serving**: runner lưu `published_snapshot_id` khi Gold
  build và tests cùng thành công. `serving/dashboard/queries.py` lấy snapshot từ
  Gold run `SUCCEEDED` gần nhất rồi pin connection DuckLake bằng
  `SNAPSHOT_VERSION`; dashboard không đọc catalog HEAD trong lúc build và fact,
  dimension, bridge không đổi version giữa các query của cùng một trang.
- **Fail closed**: nếu chưa có publication hợp lệ, query layer trả trạng thái
  không có snapshot thay vì tự ý đọc phiên bản mới nhất chưa qua quality gate.
- **DuckDB-first**: KPI và metadata dùng `fetchone()`/`fetchall()` trực tiếp.
  DataFrame chỉ được tạo ở ranh giới render cho Streamlit, Altair và PyDeck.
- **Render**: Dashboard dùng PyDeck cho bản đồ và Altair cho biểu đồ.
