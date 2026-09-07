# Hanoi Flood & Climate Risk Monitor

Lakehouse theo dõi rủi ro mưa lớn, ngập úng và proxy lũ cho Hà Nội.

Đây là portfolio project vận hành production-like trên single-node với mục tiêu
chi phí bắt buộc 0 đồng/tháng. Open-Meteo Free API chỉ được dùng cho mục đích
non-commercial và dữ liệu công bố phải có attribution.

```text
MinIO + PostgreSQL/DuckLake + DuckDB/dbt
Bronze → Silver → Gold
```

Luồng ingestion tách hai bước: `fetch-open-meteo` lập danh sách URL còn thiếu rồi
GET + PUT JSON lên `bronze/files` trên MinIO (`fetch.land`, song song qua
`fetch.pool` với `OPEN_METEO_FETCH_WORKERS` luồng, không pacing chủ động — dựa
vào retry phản ứng của `land()` khi gặp 429); `autoloader` liệt kê storage, checkpoint Postgres
(một discovery run / nguồn, lease khi load), micro-batch, nạp staging DuckLake
(`catalog1.silver.stg_*`) bằng SQL trong `sources/*.yml` + `*.sql`. Staging là
append-only; dedup theo grain nghiệp vụ và MERGE
change-aware thực hiện ở intermediate. Forecast chạy
production hằng giờ cho đủ 126 phường/xã (9.072 dòng staging hourly, không
rescued row). Silver/Gold giữ từng logical run hoàn chỉnh để audit/backtest;
view Gold `fct_rain_forecast_current_hourly` phục vụ riêng run mới nhất.
Archive: ERA5 trước 2017 (12 ô) + ECMWF IFS từ 2017 (48 ô);
năm 2000 đã land đủ 1.106.784 ward-hour; backfill 2001–nay vận hành dần theo
quota Free API, không chạy burst cả lịch sử.

```bash
make up                    # MinIO + PostgreSQL + pgAdmin
make bootstrap             # bucket, DuckLake catalog, control plane schema
make bootstrap-geography   # seed/build graph tối thiểu cho gold.dim_ward
make forecast-pipeline     # fetch -> load -> quality gate -> dbt build
make fetch-forecast                    # dry-run; EXEC=1 để chạy thật
make fetch-archive START=… END=…       # dry-run; EXEC=1 để chạy thật
make load SOURCE="open_meteo_archive open_meteo_ifs"
make quality               # Provero quét silver staging (exit 1 khi có check fail)
```

`bootstrap-geography` chạy sau `bootstrap`. Target này chỉ seed tổ tiên cần
thiết (danh mục phường đến từ `transform/seeds/*.csv`, không từ PostgreSQL) và
build graph `+dim_ward`, không kéo các fact thời tiết vào bootstrap.

Backup PostgreSQL dùng custom archive + SHA-256; thông tin kết nối và mật khẩu
chỉ nhận qua biến môi trường, không ghi vào artifact:

```bash
BACKUP_DIR=/secure/backups make backup-metadata
BACKUP_FILE=/secure/backups/vnclimate_metadata_….dump \
  RESTORE_CONFIRM=vnclimate make restore-metadata
```

Restore có tính phá huỷ đối với object hiện tại trong database đích. Hãy dừng
cron/writer trước khi chạy; xem quy trình đầy đủ trong runbook ingestion.

Xem [tài liệu kiến trúc](docs/03-architecture.md),
[cấu trúc repository](docs/03a-repo-structure.md),
[thiết kế ingestion](docs/04-ingestion.md),
[runbook ingestion](docs/04b-ingestion-runbook.md) và
[Archive ingestion](docs/04c-open-meteo-archive.md),
[phương pháp KPI mưa/ngập](docs/05-kpi-methodology.md).
