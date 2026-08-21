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
PostgreSQL là control plane cho logical run, attempt và file checkpoint. Phase
1–5 đã hoàn thành; production run đã tạo 9.072 dòng Bronze hourly cho đủ 126
phường/xã, không có rescued row.

```bash
make up
make bootstrap
make transform
make run-weather-plan
make run-weather
make weather-status
```

Xem [tài liệu kiến trúc](docs/03-architecture.md),
[cấu trúc repository](docs/03a-repo-structure.md),
[thiết kế ingestion](docs/04-ingestion.md),
[runbook ingestion](docs/04b-ingestion-runbook.md) và
[phương pháp KPI mưa/ngập](docs/05-kpi-methodology.md).
