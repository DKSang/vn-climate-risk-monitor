# Bước 4 — Ingest dữ liệu (dlt)

**Hanoi Flood & Climate Risk Monitor** · v0.1 · 2026-08-20 · *Trạng thái: CHỜ DUYỆT*

> Mục tiêu: đưa **S1 Forecast** (theo giờ) và **S2 Archive** (theo ngày) vào **Bronze** trên
> lakehouse (MinIO/DuckLake — hoặc DuckDB file nếu chọn v0.2), **giữ nguyên vẹn response** như
> nguồn trả về (A13, A17). Tài liệu này là **nghiên cứu API + quickstart**, bạn tự code.

---

## 1. Nghiên cứu API — Open-Meteo

Cả hai API đều **không cần key** cho mục đích phi thương mại. Hỗ trợ **multi-point**: truyền
danh sách `latitude,longitude` cách nhau dấu phẩy trong **một request** → response là **mảng JSON,
đúng thứ tự** danh sách tọa độ gửi lên.

### 1.1 S1 — Forecast API (dự báo theo giờ)

- **Endpoint:** `https://api.open-meteo.com/v1/forecast`
- **Tham số quan trọng:**

| Param | Giá trị ta dùng | Ghi chú |
|---|---|---|
| `latitude`, `longitude` | danh sách 126 centroid phường-xã (hoặc 49 ô sau khi dedup) | bắt buộc; comma-separated |
| `hourly` | `precipitation` | mm, **tổng của giờ trước đó** (preceding hour sum) — đúng khái niệm mm/h của QĐ 2280 |
| `models` | `best_match` | đã đo (Bước 2): cho độ phân giải hiệu dụng tốt nhất, Hà Nội = 49 ô |
| `forecast_days` | `2` | 48h tới, đủ cho hỏi "6–24h tới" |
| `past_days` | `0` | quá khứ lấy từ Archive, không lấy ở đây |
| `timezone` | `GMT` (mặc định) | timestamp UTC, thống nhất toàn pipeline |
| `timeformat` | `iso8601` (mặc định) | `2026-08-20T00:00` |
| `cell_selection` | `land` (mặc định) | lấy ô trên đất liền, tránh ô trên sông/biển |

- **Response** (mỗi phần tử trong mảng multi-point):
  ```json
  {
    "latitude": 21.05,          // tâm Ô LƯỚI model thực sự dùng (cách tọa độ gửi đi vài km)
    "longitude": 105.8,         // → đây là mấu chốt để detect 49 ô (xem §3.1)
    "elevation": 14.0,
    "generationtime_ms": 1.2,
    "utc_offset_seconds": 0,
    "hourly": {
      "time": ["2026-08-20T00:00", "..."],
      "precipitation": [0.0, 0.0, ...]
    },
    "hourly_units": { "precipitation": "mm" }
  }
  ```
- **Lưu ý:** `precipitation` = mm tích lũy trong 1 giờ trước timestamp đó (không phải tức thời).
- 15-minutely KHÔNG dùng: ngoài châu Âu/Bắc Mỹ chỉ là nội suy từ hourly.

### 1.2 S2 — Archive API (lịch sử / chuẩn khí hậu)

- **Endpoint:** `https://archive-api.open-meteo.com/v1/archive`
- **Tham số quan trọng:**

| Param | Giá trị ta dùng | Ghi chú |
|---|---|---|
| `latitude`, `longitude` | danh sách tọa độ cần truy vấn | bắt buộc; multi-point như Forecast |
| `start_date`, `end_date` | `yyyy-mm-dd` | **bắt buộc**; ERA5 trễ ~5 ngày so với thực tế |
| `daily` | `precipitation_sum` | mm/ngày — đủ cho chuẩn khí hậu + hạn hán + proxy lũ |
| `hourly` | `precipitation` (tùy) | nếu cần mm/h lịch sử |
| `model` | **`ERA5`** | 0.25° (~25 km), 1940–nay, cố định — **đúng cho chuẩn khí hậu 1991–2020** (A16) |
| `cell_selection` | `land` (mặc định) | — |

- **Tại sao `ERA5` chứ không `best_match`:** `best_match` trộn IFS (chỉ có từ 2017) + ERA5 —
  chuẩn khí hậu dài hạn phải dùng **một model duy nhất** để không bị đứt đoạn do đổi model.
  ERA5-Land (0.1°) cũng được nhưng nặng hơn; ERA5 0.25° đủ cho mưa theo ô ~25 km.
- **Độ trễ:** ERA5 cập nhật **mỗi ngày, trễ 5 ngày** (R9) → fetch ngày `T-5` là điểm cân bằng
  giữa "sớm nhất có" và "đầy đủ nhất".

### 1.3 Giới hạn & lịch sử đã kiểm chứng (Bước 2)

- 126 centroid phường-xã → **49 ô lưới phân biệt** với `best_match` (đã đo, R1).
- Archive lịch sử từ **1940**, dùng chuẩn khí hậu **1991–2020** (A4).

## 2. Thiết kế ingest (quyết định trước khi code)

| Quyết định | Giá trị | Lý do |
|---|---|---|
| **Bronze = as-is** | yield **toàn bộ object response** làm 1 dòng, `max_table_nesting=0` (A17) | Giữ nguyên vẹn JSON nguồn; unnest/split để ở Silver (Bước 5) |
| **Định dạng Bronze** | Parquet trên MinIO (`s3://vn-climate/bronze/`) | A13; filesystem destination của dlt |
| **Partition** | Theo ngày: `year=YYYY/month=MM/day=DD` | Idempotent, backfill nhanh, đúng nguyên tắc §2 Bước 3 |
| **Forecast: write_disposition** | `append` + cột `__fetched_at` | Mỗi giờ chạy là một **lần dự báo mới** — giữ đủ lịch sử dự báo (đúng bản chất, không ghi đè) |
| **Archive: write_disposition** | `append` (như Forecast) | **Filesystem KHÔNG hỗ trợ merge** (R17) → dùng append; ERA5 1 ngày bất biến nên trùng lặp nếu có, **dedup ở Silver** (Bước 5, đúng tinh thần medallion: Bronze giữ hết, Silver làm sạch) |
| **Mapping phường → ô** | Chỉ chạy 1 lần, lưu `reference/` (Bước 3a) | Các bước sau (Silver) dùng file này, không gọi lại API |
| **Chunk request** | Nếu multi-point 126 tọa độ bị chặn/timeout → chia theo chunk 50 tọa độ (R15) | Phòng rate-limit |

## 3. Quickstart — bạn tự code

### 3.1 Cài đặt

```bash
uv add dlt[filesystem] pyarrow duckdb pendulum
```

Cấu hình MinIO trong `.dlt/secrets.toml` (đã có `.env.example` → tạo `.env`):

```toml
[destination.filesystem]
bucket_url = "s3://vn-climate/bronze"

[destination.filesystem.credentials]
aws_access_key_id = "minioadmin"
aws_secret_access_key = "minioadmin"
endpoint_url = "http://localhost:9000"   # MinIO local; bỏ nếu dùng AWS S3 thật

[destination.filesystem.kwargs]
use_ssl = false
auto_mkdir = true
```

```toml
# .dlt/config.toml — layout file theo ngày (partition)
[destination.filesystem]
layout = "{table_name}/year={YYYY}/month={MM}/day={DD}/{load_id}.{ext}"
```

> Bỏ `endpoint_url` + `use_ssl=false` nếu bạn chọn phương án v0.2 (DuckDB file): đổi
> `destination="duckdb"` và `dataset_name="bronze"` là xong, source giữ nguyên.

### 3.2 Source chung — `ingest/dlt_pipelines/sources/open_meteo.py`

```python
"""Nguồn chung cho cả Forecast và Archive.
Mỗi dòng = toàn bộ response của 1 tọa độ (Bronze as-is, A17)."""
import pendulum
import dlt
from dlt.sources.helpers.rest_client import RESTClient

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def _rows(url: str, lats: list[float], lons: list[float], params: dict, fetched_at: str):
    payload = {
        "latitude": ",".join(f"{x:.5f}" for x in lats),
        "longitude": ",".join(f"{x:.5f}" for x in lons),
        "timeformat": "iso8601",
        "timezone": "GMT",
        "cell_selection": "land",
        **params,
    }
    resp = RESTClient(base_url=url).get("", params=payload)
    resp.raise_for_status()
    cells = resp.json()  # mảng, đúng thứ tự tọa độ gửi lên
    for i, cell in enumerate(cells):
        cell["__query_lat"] = lats[i]      # tọa độ ta gửi
        cell["__query_lon"] = lons[i]
        cell["__fetched_at"] = fetched_at  # nhãn lần chạy (lineage)
        yield cell


@dlt.resource(
    name="raw_forecast",
    write_disposition="append",
    max_table_nesting=0,          # giữ nguyên object, không tách bảng con
)
def open_meteo_forecast(lats: list[float], lons: list[float], forecast_days: int = 2):
    params = {"hourly": "precipitation", "models": "best_match", "forecast_days": forecast_days}
    yield from _rows(FORECAST_URL, lats, lons, params, fetched_at=pendulum.now().isoformat())


@dlt.resource(
    name="raw_archive",
    write_disposition="append",   # filesystem không hỗ trợ merge (R17); dedup ở Silver
    max_table_nesting=0,
)
def open_meteo_archive(lats: list[float], lons: list[float], start_date: str, end_date: str):
    params = {"daily": "precipitation_sum", "model": "ERA5",
              "start_date": start_date, "end_date": end_date}
    yield from _rows(ARCHIVE_URL, lats, lons, params, fetched_at=pendulum.now().isoformat())
```

### 3.3 Pipeline — `ingest/dlt_pipelines/pipelines/forecast_pipeline.py`

```python
"""Chạy mỗi giờ (phút :05). Lấy 49 ô lưới → fetch → Bronze."""
import dlt
from dlt.destinations import filesystem
from dlt_pipelines.sources.open_meteo import open_meteo_forecast
from reference.scripts.build_ward_grid_mapping import load_grid_cells  # từ §3.4


def main() -> None:
    grid = load_grid_cells()                     # 49 dòng: lat, lon
    pipeline = dlt.pipeline(
        pipeline_name="open_meteo_forecast",
        destination=filesystem(),
        dataset_name="bronze",                   # s3://vn-climate/bronze/raw_forecast/...
    )
    info = pipeline.run(open_meteo_forecast(grid["lat"], grid["lon"]))
    print(info)


if __name__ == "__main__":
    main()
```

Pipeline Archive (chạy 1 lần/ngày, fetch `T-5`; backfill = gọi nhiều khoảng ngày):

```python
"""Chạy mỗi ngày. Fetch T-5 (lag ERA5, R9). Backfill: gọi nhiều khoảng ngày."""
import pendulum
import dlt
from dlt.destinations import filesystem
from dlt_pipelines.sources.open_meteo import open_meteo_archive
from reference.scripts.build_ward_grid_mapping import load_grid_cells


def main(start: str | None = None, end: str | None = None) -> None:
    grid = load_grid_cells()
    start = start or (pendulum.today().subtract(days=5).to_date_string())
    end = end or start
    pipeline = dlt.pipeline(
        pipeline_name="open_meteo_archive",
        destination=filesystem(),
        dataset_name="bronze",
    )
    info = pipeline.run(open_meteo_archive(grid["lat"], grid["lon"], start, end))
    print(info)


if __name__ == "__main__":
    main()
```

### 3.4 Ánh xạ 49 ô lưới — `reference/scripts/build_ward_grid_mapping.py`

Mấu chốt của R1: dùng **tọa độ ô lưới model trả về** (không phải tọa độ gửi đi) để dedup.

```python
"""Centroid 126 phường → 49 ô lưới duy nhất (gọi Forecast 1 lần).
Lưu reference/grid_cells.csv + reference/ward_to_grid.csv."""
import pandas as pd
from dlt.sources.helpers.rest_client import RESTClient

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
WARDS = pd.read_csv("reference/ward_centroids.csv")   # id, name, lat, lon (từ S13)


def main() -> None:
    payload = {
        "latitude": ",".join(f"{x:.5f}" for x in WARDS["lat"]),
        "longitude": ",".join(f"{x:.5f}" for x in WARDS["lon"]),
        "hourly": "precipitation",
        "models": "best_match",
        "forecast_days": 1,
    }
    cells = RESTClient(base_url=FORECAST_URL).get("", params=payload).json()

    # Ô lưới = tọa độ model trả về; dedup theo (lat, lon) làm tròn 3 chữ số
    grid = pd.DataFrame(cells)[["latitude", "longitude"]].drop_duplicates()
    grid = grid.rename(columns={"latitude": "grid_lat", "longitude": "grid_lon"})
    grid.to_csv("reference/grid_cells.csv", index=False)

    mapping = WARDS.copy()
    cell_by_query = {(round(r["latitude"], 3), round(r["longitude"], 3)): i
                     for i, r in enumerate(cells)}
    mapping["grid_id"] = [cell_by_query[(round(clat, 3), round(clon, 3))]
                          for clat, clon in zip(WARDS["lat"], WARDS["lon"])]
    mapping.to_csv("reference/ward_to_grid.csv", index=False)
    print(f"{len(grid)} ô lưới duy nhất từ {len(WARDS)} phường-xã")


if __name__ == "__main__":
    main()
```

### 3.5 Chạy & kiểm chứng (theo nguyên tắc dự án: gọi thật, không tin doc)

```bash
make up                 # dựng MinIO (+ Postgres, Airflow...)
python reference/scripts/build_ward_grid_mapping.py   # → phải in "49 ô lưới duy nhất"
python ingest/dlt_pipelines/pipelines/forecast_pipeline.py
dlt pipeline open_meteo_forecast show    # mở browser xem dữ liệu
```

**Checklist kiểm chứng:**
- [ ] `build_ward_grid_mapping` ra **đúng 49 ô** (khớp kết quả đo Bước 2).
- [ ] `raw_forecast` có **126 dòng**/lần chạy (49 ô × ~2.5 phường trung bình), mỗi dòng đủ
  `hourly.time` 48 giờ + `precipitation` (đơn vị mm).
- [ ] `__fetched_at` phân biệt từng lần chạy; chạy 2 lần → không mất dữ liệu lần 1 (append).
- [ ] Trên MinIO: đường dẫn `bronze/raw_forecast/year=.../month=.../day=.../...parquet` đúng.
- [ ] Archive: chạy 2 lần cùng ngày → **số dòng tăng** (append) — không lo: dedup sẽ xử lý ở Silver (R17).
- [ ] Tọa độ trong response ≠ tọa độ gửi đi (chênh vài km) — đó là tâm ô lưới, đúng thiết kế R1.

## 4. Giả định mới

- **A15** — Dùng `cell_selection=land` (mặc định) cho cả Forecast và Archive.
- **A16** — Chuẩn khí hậu/hạn hán dùng **Archive model `ERA5` (0.25°)** và **ánh xạ lại** phường→
  ô ERA5 theo centroid phường (không dùng lại 49 ô của Forecast, vì lưới ERA5 thô hơn). Lưới
  Forecast chỉ dùng cho cảnh báo theo giờ.
- **A17** — Bronze giữ nguyên response qua `max_table_nesting=0`; mọi unnest/split (mảng
  `time`/`precipitation` thành dòng) nằm ở Silver (Bước 5), không làm ở ingest.

## 5. Rủi ro mới

| # | Rủi ro | Mức | Xử lý |
|---|---|---|---|
| **R15** | Multi-point 126 tọa độ/request có thể bị chặn hoặc timeout do kích thước URL. | Trung bình | Chia chunk ~50 tọa độ/request; test ở §3.5 trước khi cam kết. |
| **R16** | Lưới ERA5 0.25° (~25 km) thô hơn lưới Forecast (9 km) → chuẩn khí hậu theo ô to hơn. | Thấp | Chấp nhận + ghi chú rõ trong dashboard; đúng bản chất nguồn dữ liệu (A16). |

## 6. Việc thủ công liên quan (từ Bước 2 §3)

- [ ] Tải 126 GeoJSON S13, tính centroid → `reference/ward_centroids.csv` (ghim commit SHA).
- [ ] Nhập 4 ngưỡng QĐ 2280 vào `reference/qd2280/` (chưa cần cho ingest, cần cho Bước 5).
- [ ] Tạo bucket `vn-climate` trên MinIO (`make bootstrap`).