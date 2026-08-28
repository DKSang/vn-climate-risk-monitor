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
GET + PUT JSON lên `bronze/files` trên MinIO (`fetch.land`, giãn nhịp theo
`OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR`); `autoloader` liệt kê storage, checkpoint Postgres
(một discovery run / nguồn, lease khi load), micro-batch, nạp Bronze DuckLake
bằng SQL trong `sources/*.yml` + `*.sql`. Bronze là INSERT — mọi vintage
được giữ nguyên; dedup theo (ô lưới, giờ) thực hiện ở Silver. Forecast chạy
production hằng giờ cho đủ 126 phường/xã (9.072 dòng Bronze hourly, không
rescued row). Archive: năm 2000 đã land đủ 1.106.784 ward-hour; backfill
2001–nay vận hành dần theo quota Free API, không chạy burst cả lịch sử.

```bash
make up            # MinIO + PostgreSQL + pgAdmin
make bootstrap     # bucket, DuckLake catalog, control plane schema
make transform     # dbt build Silver/Gold
make fetch-forecast                # dry-run; EXEC=1 để chạy thật
make fetch-archive START=… END=…   # dry-run; EXEC=1 để chạy thật
make load SOURCE=open_meteo_forecast   # autoloader nạp file mới vào Bronze
make quality       # Provero quét Bronze (exit 1 khi có check fail)
```

Xem [tài liệu kiến trúc](docs/03-architecture.md),
[cấu trúc repository](docs/03a-repo-structure.md),
[thiết kế ingestion](docs/04-ingestion.md),
[runbook ingestion](docs/04b-ingestion-runbook.md) và
[Archive ingestion](docs/04c-open-meteo-archive.md),
[phương pháp KPI mưa/ngập](docs/05-kpi-methodology.md).
