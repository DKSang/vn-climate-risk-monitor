# Hanoi Flood & Climate Risk Monitor — Tài liệu dự án

Theo dõi **mưa lớn và áp lực mưa** cho Hà Nội, chi tiết đến **126 phường-xã**.

## Quy trình 9 bước

| # | Bước | File | Trạng thái |
|---|---|---|---|
| 1 | Bắt đầu từ bài toán nghiệp vụ | [01-business-problem.md](01-business-problem.md) | ✅ Xong |
| 2 | Xác định & đánh giá nguồn dữ liệu | [02-data-sources.md](02-data-sources.md) | ✅ Xong |
| 3 | Thiết kế kiến trúc | [03-architecture.md](03-architecture.md) | ✅ **Đã triển khai & kiểm chứng** |
| 3a | Cấu trúc repository | [03a-repo-structure.md](03a-repo-structure.md) | ✅ Xong |
| 4a | Setup Lakehouse (DuckLake+MinIO+Postgres) | [04a-lakehouse-setup.md](04a-lakehouse-setup.md) | ✅ Xong |
| 4b | Ingestion runbook | [04b-ingestion-runbook.md](04b-ingestion-runbook.md) | ✅ Cron, health, recovery |
| 5 | Clean, Transform & KPI | [05-kpi-methodology.md](05-kpi-methodology.md) | ✅ Rainfall windows + pressure alert + archive replay |
| 6 | Lưu trữ — single source of truth | [06-storage-modeling.md](06-storage-modeling.md) | ✅ Gold SSOT và processing checkpoint |
| 7 | Data Quality & Observability | [07-data-quality.md](07-data-quality.md) | ✅ Provero (Forecast + Archive), dbt tests, Healthcheck collector, Airflow callback alerting |
| 8 | Serving & BI | [08-serving-bi.md](08-serving-bi.md) | ✅ Streamlit Dashboard (bản đồ pressure + drill-down phường/điểm ngập) |
| 9 | Governance & Continuous Improvement | [09-governance.md](09-governance.md) | ✅ Policy, contract, SLO, risk và improvement loop |

## Trạng thái hệ thống

```
PostgreSQL → ingestion_runs + ingestion_files      (file checkpoint)
             processing_state + processing_runs    (processing checkpoint)
DuckLake   → MỘT catalog `catalog1`
MinIO      → bronze/files (raw)  ·  silver/  ·  gold/
```

| Layer / Lớp dbt | Bảng / Model |
|---|---|
| landing | `bronze/files/**.json` *(raw bất biến, ngoài catalog)* |
| seed | `ward_coordinates_seed` · `ward_grid_map_seed` *(input artifact)* |
| silver staging | `stg_weather_archive_hourly` · `stg_weather_forecast` và các staging view |
| silver intermediate | `int_weather_archive_hourly` · `int_weather_forecast_hourly` |
| gold dimensions | `dim_grid` · `dim_ward` · `dim_flood_point` · `bridge_ward_grid` |
| gold archive | `fct_rain_archive_hourly` · `fct_flood_event_observation` |
| gold forecast | `fct_rain_forecast_hourly` · `fct_rain_forecast_current_hourly` · `fct_rain_pressure_alert` |

Không hard-code row count của current view/pressure vào tài liệu vì chúng thay
đổi theo giờ. Lấy trạng thái trực tiếp từ runtime:

```bash
docker compose exec -T airflow uv run python scripts/healthcheck.py --scope all --require-gold
```

Staging có thể giữ nhiều bản ghi cùng grain một cách CÓ CHỦ Ý: đó là
change log của các lần fetch. Dedup xảy ra ở Silver intermediate.

## Lệnh thường dùng

```bash
docker compose up -d --build
docker compose exec airflow airflow dags unpause open_meteo_forecast_hourly
docker compose exec airflow airflow dags trigger open_meteo_forecast_hourly
docker compose exec -T airflow uv run python scripts/healthcheck.py --scope all --require-gold
docker compose exec -T airflow uv run python scripts/run_processing.py status rain_gold
docker compose exec -T airflow uv run python scripts/maintain_lake.py --snapshot-retention-days 7 --file-grace-days 2
```

Các lệnh data-plane chạy trong Airflow container. Ruff, docs link checker và dbt
docs là tác vụ phát triển trên host nên cần Python/`uv`.

## Cổng vào bước 9 — đã đạt

Code và runtime đã đáp ứng cổng kỹ thuật để bắt đầu Governance:

- Gold inventory chỉ còn các mart có consumer; selectors tách forecast/archive.
- Forecast forward windows và pressure semantics có dbt contract tests.
- Gold publication được pin vào snapshot của run `SUCCEEDED`, không đọc HEAD.
- Ingestion/processing có checkpoint, audit, single-writer và recovery path.
- Secrets không còn nằm trong container environment; runtime images immutable,
  pinned và application containers chạy non-root.
- CI, unit tests, dbt tests, Provero, healthcheck và dashboard health đều là gate
  thực thi được.

Một điều kiện hạ tầng không thể hoàn thành bên trong repository: chạy ít nhất
một backup/restore drill trên filesystem **độc lập** và database/bucket test,
ghi lại RPO/RTO thực đo. Script backup, verifier và restore đã sẵn sàng; không
được restore thử vào dữ liệu production hiện hành. Kết quả drill là đầu vào đầu
tiên của bước 9, không phải lý do thêm một hệ thống backup phức tạp vào MVP.

Governance hiện hành, risk acceptance và promotion gates được chốt tại
[09-governance.md](09-governance.md). Phase 9 hoàn tất cho scope portfolio
single-node; restore drill độc lập và least-privilege IAM vẫn là điều kiện trước
khi tuyên bố production/public deployment.

## Nguyên tắc làm việc

- **Không sang bước sau khi bước trước chưa được duyệt.**
- Mọi khẳng định về nguồn dữ liệu phải được **kiểm chứng bằng cách gọi thật**, không đọc doc rồi tin.
- **Test dựa trên metadata không chứng minh được dữ liệu tồn tại** — bài học từ sự cố bảng ma
  20/08/2026. Test quan trọng phải buộc engine đọc file thật (xem `assert_gold_is_readable`).
- Mọi giả định ghi rõ dạng `A1`, `A2`… và rủi ro dạng `R1`, `R2`… để trace ngược.
- Bronze CHỈ là landing zone raw file. Bảng append-only đầu tiên là
  `silver.stg_*`; dedup nằm ở `silver.int_weather_*`.
