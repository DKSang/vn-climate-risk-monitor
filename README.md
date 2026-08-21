# Hanoi Flood & Climate Risk Monitor

Lakehouse theo dõi rủi ro mưa lớn, ngập úng và proxy lũ cho Hà Nội.

Đây là portfolio project vận hành production-like trên single-node với mục tiêu
chi phí bắt buộc 0 đồng/tháng. Open-Meteo Free API chỉ được dùng cho mục đích
non-commercial và dữ liệu công bố phải có attribution.

```text
MinIO + PostgreSQL/DuckLake + DuckDB/dbt
Bronze → Silver → Gold
```

Open-Meteo forecast collector ghi immutable response JSON vào `bronze/files`;
PostgreSQL là generic control plane cho logical run, attempt và file checkpoint;
parser cùng Bronze schema vẫn source-specific. Forecast Phase 1–5 đã hoàn thành;
production run đã tạo 9.072 dòng Bronze hourly cho đủ 126 phường/xã, không có
rescued row. Archive ingestion cũng đã đủ planner, immutable collector, strict
parser, monthly checkpoint, Bronze `MERGE` partition theo năm và daily tail;
production năm 2000 đã commit đủ 1.106.784 ward-hour và Bronze có cùng số khóa
duy nhất. Backfill 2001–nay được vận hành dần theo quota Free API với
minute/hour pacing, không chạy burst cả lịch sử.

```bash
make up
make bootstrap
make transform
make run-weather-plan
make run-weather
make weather-status
make plan-historical
make run-historical-backfill # tối đa một monthly checkpoint mới
make run-historical-tail
```

Xem [tài liệu kiến trúc](docs/03-architecture.md),
[cấu trúc repository](docs/03a-repo-structure.md),
[thiết kế ingestion](docs/04-ingestion.md),
[runbook ingestion](docs/04b-ingestion-runbook.md) và
[Archive ingestion](docs/04c-open-meteo-archive.md),
[phương pháp KPI mưa/ngập](docs/05-kpi-methodology.md).
