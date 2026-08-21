# Runbook ingestion Open-Meteo

**Phạm vi:** hourly forecast · single-node · cron + Python · 2026-08-21

## Chạy thủ công

```bash
make up
make bootstrap
make run-weather-plan
make run-weather
make weather-healthcheck
```

`run-weather-plan` chỉ đọc Gold dimension và in logical schedule slot. Lệnh
`run-weather` dùng slot gần nhất tại phút 15 UTC, collect đủ 126 location rồi
drain loader. Chạy lại trong cùng slot là idempotent.

Canary tách checkpoint khỏi production:

```bash
make run-weather-canary
uv run observe-open-meteo-ingestion --scope canary_1
```

## Lịch chạy zero-cost

Không cần Airflow cho một pipeline hourly. Cài cron template sau khi thay đường
dẫn tuyệt đối:

```bash
crontab -e
# copy orchestration/cron/open_meteo_forecast.cron.example
```

Cron gọi `scripts/run_weather_pipeline.sh`; script đổi về project directory và
dùng `flock --nonblock` trên `/tmp/vn-climate-risk-monitor-weather.lock` để từ
chối invocation chạy chồng.

## Health và metrics

```bash
make weather-status
uv run observe-open-meteo-ingestion --scope production --json
make weather-healthcheck
```

| Health | Điều kiện chính | Hành động |
|---|---|---|
| `HEALTHY` | latest success không quá 120 phút, không backlog/error | không cần can thiệp |
| `DEGRADED` | chưa có success, stale, hoặc còn pending/processing/failed file | kiểm tra scheduler và chạy lại pipeline |
| `CRITICAL` | latest run failed hoặc file hết retry | đọc `error_type/error_message`, sửa nguyên nhân rồi retry |

Metrics 24 giờ gồm số run success/failed, file committed, rows parsed/inserted và
rescued rows. Đây là query trực tiếp PostgreSQL, không có metrics state thứ hai.

## Recovery

### Collector bị dừng giữa run

Attempt `RUNNING` mới hơn 1.800 giây được coi là đang hoạt động và invocation
trùng slot thất bại. Sau timeout, lần chạy kế tiếp tự đóng attempt cũ bằng
`CollectorTimeout`, tạo attempt number mới và object prefix mới.

### Loader chết sau khi claim

File ở `PROCESSING` đến khi lease 300 giây hết hạn. Lần loader kế tiếp chuyển nó
về `FAILED`, tăng `retry_count`, xác minh lại checksum và `MERGE` deterministic.

### Bronze commit nhưng PostgreSQL checkpoint chưa commit

Chạy lại pipeline. `bronze_row_id` giữ nguyên theo attempt/file/location/hour;
`MERGE` trả `rows_inserted=0`, sau đó checkpoint chuyển `COMMITTED`.

### File hết retry

Không reset hàng loạt. Sau khi sửa root cause, cho phép đúng một attempt bổ sung:

```bash
uv run load-open-meteo-forecast --scope production --max-retries 4
make weather-healthcheck
```

Tăng dần từ giá trị hiện tại và kiểm tra `error_type`; không đặt một giới hạn rất
lớn vì sẽ tạo retry loop cho source hỏng thực sự.

### Dữ liệu rescued tăng

Lấy mẫu `_rescued_data` theo parser version, phân loại source field mới rồi cập
nhật explicit Arrow schema. Replay từ immutable response JSON; không sửa JSON
nguồn. Chỉ coi run usable khi rescued fields đã được review.

## Truy vấn điều tra

```sql
SELECT attempt_id, attempt_number, scheduled_at_utc, status,
       error_type, error_message
FROM ingestion.ingestion_runs
WHERE pipeline_name = 'open_meteo_forecast'
ORDER BY started_at_utc DESC
LIMIT 20;

SELECT file_id, attempt_id, batch_index, object_key, status, retry_count,
       rows_parsed, rows_inserted, rescued_rows, error_type, error_message
FROM ingestion.ingestion_files
WHERE status <> 'COMMITTED'
ORDER BY updated_at_utc;
```

Không xóa source object chỉ vì một run thất bại. Orphan cleanup phải resolve exact
attempt/prefix và đối chiếu PostgreSQL trước khi xóa.
