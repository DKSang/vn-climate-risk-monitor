# Bước 7: Data Quality & Observability

**Hanoi Flood & Climate Risk Monitor** · v2.0 · 2026-09-06

Tài liệu này mô tả toàn bộ kiến trúc kiểm tra chất lượng dữ liệu (Data Quality Gate) và giám sát vận hành (Observability & Alerting) của lakehouse.

---

## 1. Tổng quan Quality Gates trong Pipeline

Kiểm tra chất lượng được đặt làm các **cổng chặn (hard gates)** ở mọi bước chuyển tiếp giữa các lớp medallion:

```text
               ┌────────────────────────────────────────────────────────┐
               │                  Fetch (HTTP → MinIO)                  │
               └──────────────────────────┬─────────────────────────────┘
                                          │
                                          ▼
               ┌────────────────────────────────────────────────────────┐
               │          Autoloader (MinIO → Silver Staging)           │
               └──────────────────────────┬─────────────────────────────┘
                                          │
                     [GATE 1] Provero Data Quality Check
                     ├── quality/provero.yaml         (Forecast)
                     └── quality/provero_archive.yaml (Archive)
                                          │
                                          ▼
               ┌────────────────────────────────────────────────────────┐
               │          dbt Seeds (dim_ward, ward_grid_map)           │
               └──────────────────────────┬─────────────────────────────┘
                                          │
                                          ▼
               ┌────────────────────────────────────────────────────────┐
               │    Silver Curated (int_weather_*) — Change-aware MERGE │
               └──────────────────────────┬─────────────────────────────┘
                                          │
                                          ▼
               ┌────────────────────────────────────────────────────────┐
               │             Gold Marts (dim_*, bridge_*, fct_*)        │
               └──────────────────────────┬─────────────────────────────┘
                                          │
                     [GATE 2] dbt Tests
                     ├── Schema tests (unique, not_null, accepted_values, relationships)
                     └── Singular tests (grain, monotonic, ward coverage, daily=hourly)
                                          │
                     [GATE 3] Healthcheck Collector (scripts/healthcheck.py)
                     ├── Lakehouse query & coverage
                     ├── PostgreSQL control plane backlog
                     └── Host free disk space
                                          │
                                          ▼
               ┌────────────────────────────────────────────────────────┐
               │               Serving & Ops Dashboard                  │
               └────────────────────────────────────────────────────────┘
```

Mọi gate đều trả **exit code non-zero** khi phát hiện vi phạm. Trong Airflow DAG hoặc Makefile, bước tiếp theo sẽ bị **chặn hoàn toàn**, ngăn chặn dữ liệu bẩn lan truyền lên các tầng phục vụ.

---

## 2. Gate 1: Provero — Kiểm tra chất lượng Staging

### 2.1 Vì sao dùng Provero và connector DuckLake riêng?
Autoloader nạp dữ liệu trực tiếp vào DuckLake ngoài đồ thị dbt. dbt không thể chạy test trên các bảng staging này trước khi transform bắt đầu.

Provero được cấu hình với connector riêng `autoloader.provero_ducklake:DuckLakeConnector`:
- Mở DuckDB, tự động `LOAD httpfs`, `LOAD ducklake`.
- Tạo secret S3 kết nối MinIO và `ATTACH` catalog metadata qua PostgreSQL.
- **Không glob file Parquet trực tiếp**: glob Parquet sẽ đọc cả dữ liệu đã xóa và file của snapshot cũ (đã ghi nhận lệch 82 dòng do DuckLake ghi xoá vào catalog metadata).

### 2.2 Các bộ check hiện hành

#### a. Forecast (`quality/provero.yaml`)
- **Bảng mục tiêu**: `catalog1.silver.stg_weather_forecast`
- **Các rule**:
  - `row_count >= 1`: đảm bảo autoloader đã nạp dữ liệu.
  - `not_null: [grid_latitude, grid_longitude, valid_time_utc]`: khóa không gian - thời gian bắt buộc.
  - `range: precipitation_mm ∈ [0, 500]`: giá trị lượng mưa vật lý hợp lệ.
  - `freshness: _ingested_at <= 24h`: đảm bảo dữ liệu forecast luôn được cập nhật mỗi ngày.

#### b. Archive (`quality/provero_archive.yaml`)
- **Bảng mục tiêu**: `catalog1.silver.stg_weather_archive_hourly`
- **Các rule**:
  - `row_count >= 1`: bảng không được rỗng.
  - `not_null: [grid_latitude, grid_longitude, valid_time_utc, weather_model]`: khóa bắt buộc kèm model.
  - `range: precipitation_mm ∈ [0, 500]`: dải mưa vật lý.
  - `accepted_values: weather_model ∈ ['era5', 'ecmwf_ifs']`: chỉ chấp nhận 2 model đã được kiểm định.

### 2.3 Tham số chạy bắt buộc
```bash
# --no-store: BẮT BUỘC. Tránh bug datetime serialization trong Provero v0.2.1 khi range check fail.
# --no-optimize: chạy từng check tuần tự để bảo toàn trace lỗi.
uv run provero run -c quality/provero.yaml --no-optimize --no-store
uv run provero run -c quality/provero_archive.yaml --no-optimize --no-store
```

---

## 3. Gate 2: dbt Tests & Source Freshness

### 3.1 Schema tests & Relationships
Được định nghĩa trong các file `schema.yml`:
- [transform/models/staging/schema.yml](transform/models/staging/schema.yml): test view mỏng staging.
- [transform/models/intermediate/schema.yml](transform/models/intermediate/schema.yml):
  - `weather_archive_hourly_key` & `weather_forecast_hourly_key`: `unique`, `not_null`.
  - `_row_hash`, `_updated_at`, `is_active`: `not_null`.
- [transform/models/marts/schema.yml](transform/models/marts/schema.yml):
  - `dim_grid`: `grid_cell_id` unique/not_null, accepted_values model.
  - `dim_ward`: `ward_code` unique/not_null.
  - `dim_flood_point`: `point_id` unique, relationship tới `dim_ward`.
  - `bridge_ward_grid`: `ward_grid_key` unique, relationships tới cả `dim_ward` và `dim_grid`.
  - `fct_rain_*`: unique key, relationships tới dimensions, accepted_values cho kịch bản mưa (`hanoi_rain_scenario_band`, `vn_rain_band_24h`).

### 3.2 Singular Tests (Kiểm thử quan hệ logic & tính đúng đắn dữ liệu)
Nằm tại thư mục [transform/tests/](transform/tests/):

1. **`assert_weather_archive_hourly_grain.sql`**:
   - Đảm bảo grain `(grid_cell_id, valid_time_utc)` trong `int_weather_archive_hourly` không bị trùng lặp sau dedup.
2. **`assert_forecast_grain.sql`**:
   - Đảm bảo grain của `int_weather_forecast_hourly` luôn duy nhất.
3. **`assert_rolling_windows_are_monotonic.sql`**:
   - Đảm bảo cửa sổ rộng hơn luôn chứa cửa sổ hẹp hơn: `rain_1h <= rain_3h <= rain_6h <= rain_12h <= rain_24h`. Phát hiện lỗi PARTITION/ROWS sai trong SQL windowing.
4. **`assert_forecast_rolling_monotonic.sql`**:
   - Kiểm tra tính monotonic tương tự cho bảng dự báo `fct_rain_forecast_hourly`.
5. **`assert_ward_grid_covers_every_ward.sql`**:
   - Đảm bảo mỗi phường trong 126 phường Hà Nội đều được map chính xác vào đúng 1 ô lưới cho mỗi model thời tiết.
6. **`assert_daily_total_matches_hourly.sql`**:
   - Đối soát `fct_rain_archive_daily.rain_total_mm` phải bằng tổng của 24 giờ trong `fct_rain_archive_hourly` (sai số <= 0.01mm).

### 3.3 Source Freshness
Khai báo tại [transform/models/sources.yml](transform/models/sources.yml) trên trường `_ingested_at`:
- `stg_weather_forecast`: cảnh báo sau 12h, lỗi sau 26h.
- `stg_weather_archive_hourly`: cảnh báo sau 96h, lỗi sau 168h.

Chạy kiểm tra:
```bash
make freshness
# Tương đương: cd transform && uv run dbt source freshness --profiles-dir .
```

---

## 4. Gate 3: Healthcheck Collector & Auditing

File thực thi: [src/vn_climate_risk_monitor/health.py](src/vn_climate_risk_monitor/health.py), CLI: `scripts/healthcheck.py`.

### 4.1 Các khía cạnh kiểm tra
| Check | Mục tiêu | Tiêu chuẩn PASS |
|---|---|---|
| `host.free_disk` | Không gian đĩa cứng | Còn trống >= 5 GiB |
| `ingestion.backlog` | Tồn đọng file tại MinIO | Số file PENDING/PROCESSING <= ngưỡng quy định |
| `ingestion.failed_files` | File lỗi autoloader | Không có file nào trạng thái FAILED |
| `stg_*.required_keys` | Khóa tọa độ/thời gian staging | 0 dòng bị NULL grid_lat/lon/valid_time |
| `stg_*.rescued_data` | Dữ liệu không parse được | 0 dòng có `_rescued_data` |
| `stg_*.precipitation_range` | Lượng mưa vật lý | 0 dòng ngoài khoảng [0, 500] mm |
| `forecast.complete_run` | Độ phủ forecast run | Mọi run đều đủ 126 phường ở tất cả các giờ |
| `archive.monthly_coverage` | Tính trọn vẹn tháng lịch sử | Mọi tháng trong quá khứ đủ 100% số giờ |
| `archive.silver_grain` | Grain sau dedup Silver | Không có dòng trùng |
| `archive.ward_mapping` | Ánh xạ phường - ô lưới | Đủ 126 phường × số lượng model |
| `gold.rain_hourly.rows/grain` | Fact Gold | Bảng có dòng và key không trùng lặp |

### 4.2 Định dạng kết quả JSON
Lệnh `make health` (hoặc `scripts/healthcheck.py --output logs/health.json`) sinh file audit:
```json
{
  "status": "HEALTHY",
  "checked_at_utc": "2026-09-06T10:00:00.000000+00:00",
  "scope": "all",
  "checks": [
    {
      "name": "host.free_disk",
      "status": "PASS",
      "message": "Còn 42.15 GiB tại /home/dksan/vn-climate-risk-monitor",
      "metrics": { "free_gib": 42.15, "minimum_gib": 5.0 }
    }
  ]
}
```

---

## 5. Alerting & Tích hợp Airflow

### 5.1 Cấu hình Webhook
Thêm URL webhook vào `.env` (hỗ trợ Discord, Slack, Telegram, Custom HTTP endpoint):
```env
ALERT_WEBHOOK_URL=https://webhook.site/your-custom-uuid
```
Biến này được tự động forward vào container Airflow thông qua `docker-compose.yml`.

### 5.2 Airflow `on_failure_callback`
Trong [orchestration/dags/common.py](orchestration/dags/common.py), callback `on_failure_alert` được gắn vào `DEFAULT_ARGS`:
- Khi bất kỳ task nào trong DAG fail (fetch, autoloader, provero, dbt, healthcheck), callback sẽ:
  1. Ghi log cảnh báo với thông tin `dag_id` và `task_id`.
  2. Kích hoạt `scripts/alert_health.py --scope all`.
  3. Gửi payload HTTP POST chứa danh sách các failed checks tới `ALERT_WEBHOOK_URL`.

---

## 6. Sổ tay vận hành (Runbook)

### 6.1 Khi Provero báo FAIL
- **Hiện tượng**: `make quality-forecast` hoặc `make quality-archive` trả mã lỗi 1.
- **Nguyên nhân phổ biến**:
  - `freshness` fail: fetch cron bị chết, không có file mới nạp trong 24h.
  - `range` fail: API Open-Meteo trả giá trị bất thường (ví dụ < 0 hoặc > 500mm).
- **Cách xử lý**:
  1. Kiểm tra log autoloader: `docker compose logs airflow` hoặc xem `logs/health.json`.
  2. Kiểm tra file lỗi trong Postgres:
     ```sql
     SELECT * FROM ingestion.ingestion_files WHERE status = 'FAILED';
     ```
  3. Re-run riêng lẻ để kiểm tra:
     ```bash
     make quality-forecast
     ```

### 6.2 Khi dbt tests báo FAIL
- **Hiện tượng**: `make dbt-test` fail hoặc bước `transform` dừng lại.
- **Nguyên nhân**:
  - Grain trùng: lỗi ở logic dedup tại `int_weather_*`.
  - Monotonic fail: lỗi tính toán cửa sổ trượt trong macro `rolling_rain_sums`.
  - Ward mapping fail: seed `ward_grid_map_seed.csv` thiếu phường hoặc model.
- **Cách xử lý**:
  1. Đọc query fail trong `transform/target/run_results.json` hoặc `transform/logs/dbt.log`.
  2. Chạy thử singular test cụ thể bằng:
     ```bash
     cd transform && uv run dbt test --select assert_weather_archive_hourly_grain
     ```

### 6.3 Khi healthcheck báo DEGRADED hoặc UNHEALTHY
- **`host.free_disk` FAIL**: Chạy `make clean-lake` để giải phóng snapshot DuckLake cũ và dọn dẹp thư mục tạm.
- **`ingestion.backlog` WARN**: Autoloader chưa kịp xử lý lượng file landing lớn; chạy `make load` thủ công để tiêu thụ backlog.
- **`archive.monthly_coverage` FAIL**: Có tháng bị thiếu giờ; kiểm tra lại tham số START/END và chạy `make fetch-archive START=... END=... EXEC=1`.

---

## 7. Giới hạn hiện tại & Lộ trình tương lai

- **Hiện tại**:
  - Single-node zero-cost: Không sử dụng SaaS APM đắt tiền (Datadog, Monte Carlo).
  - Snapshot DuckLake được kiểm tra snapshot-aware thông qua catalog metadata.
  - Webhook POST đơn giản qua standard library `urllib.request`.
- **Tương lai (sau MVP)**:
  - Tích hợp Prometheus exporter cho metric lat/lon processing times.
  - OpenTelemetry distributed tracing cho API serving ở Bước 8.
  - Tự động re-fetch các partition archive bị incomplete.
