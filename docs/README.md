# Hanoi Flood & Climate Risk Monitor — Tài liệu dự án

Theo dõi rủi ro **ngập úng / lũ / hạn hán** cho Hà Nội, chi tiết đến **126 phường-xã**.

## Quy trình 9 bước

| # | Bước | File | Trạng thái |
|---|---|---|---|
| 1 | Bắt đầu từ bài toán nghiệp vụ | [01-business-problem.md](01-business-problem.md) | ✅ Xong |
| 2 | Xác định & đánh giá nguồn dữ liệu | [02-data-sources.md](02-data-sources.md) | ✅ Xong |
| 3 | Thiết kế kiến trúc | [03-architecture.md](03-architecture.md) | ✅ **Đã triển khai & kiểm chứng** |
| 3a | Cấu trúc repository | [03a-repo-structure.md](03a-repo-structure.md) | ✅ Xong |
| 4 | Ingest dữ liệu | [04-ingestion.md](04-ingestion.md) | ✅ Forecast + Archive code/canary xong · backfill đang vận hành theo quota |
| 4a | Setup Lakehouse (DuckLake+MinIO+Postgres) | [04a-lakehouse-setup.md](04a-lakehouse-setup.md) | ✅ Xong |
| 4b | Ingestion runbook | [04b-ingestion-runbook.md](04b-ingestion-runbook.md) | ✅ Cron, health, recovery |
| 4c | Open-Meteo Archive | [04c-open-meteo-archive.md](04c-open-meteo-archive.md) | ✅ Monthly incremental + year partition + tail |
| 5 | Clean, Transform & KPI | [05-kpi-methodology.md](05-kpi-methodology.md) | 🟡 MVP forecast đã triển khai/test · baseline lịch sử và hiệu chỉnh chưa làm |
| 6 | Lưu trữ — single source of truth | `06-storage-modeling.md` | ⬜ |
| 7 | Data Quality & Observability | `07-data-quality.md` | 🟡 ingestion health có · model observability chưa làm |
| 8 | Make it accessible | `08-serving-bi.md` | ⬜ |
| 9 | Governance & Continuous Improvement | `09-governance.md` | ⬜ |

## Trạng thái hệ thống (2026-08-22)

```
dbt build  → PASS=112  WARN=0  ERROR=0
MinIO      → 257 object / 153.9 MiB
PostgreSQL → ingestion_runs + ingestion_files
```

| Layer | Bảng | Dòng |
|---|---|---|
| seed | `ward_coordinates_seed` *(input artifact, ngoài medallion)* | 3.321 |
| bronze | `gso_provinces` · `gso_wards` · `gso_administrative_units` · `gso_administrative_regions` · `ward_coordinates` | 34 · 3.321 · 5 · 8 · 3.321 |
| silver | `wards` · `ward_centroids` · `ward_locations` | 3.321 mỗi bảng (view) |
| gold | `dim_hanoi_ward` | **126** |
| bronze weather | `open_meteo_forecast` | **11.016 rows** *(nhiều retrieval slot; Silver chỉ chọn slot hoàn chỉnh mới nhất)* |
| bronze archive | `open_meteo_archive` (ERA5, trước 2017) · `open_meteo_ifs` (IFS, từ 2017) | backfill đang vận hành theo quota |
| silver weather | `forecast_hourly` | **3.456 rows** *(48 grid × 72 giờ, snapshot `2026-08-21 11:00 UTC`)* |
| silver bridge | `bridge_hanoi_ward_forecast_grid` | **126** |
| gold forecast | `fct_rainfall_forecast_hourly` · `fct_rainfall_forecast_summary` | **3.456** · **48** |
| gold ward forecast | `fct_ward_rainfall_forecast_hourly` · `fct_ward_rainfall_forecast_summary` | **9.072** · **126** |

## Lệnh thường dùng

```bash
make up            # bật Postgres + MinIO + pgAdmin
make transform     # dbt build (run + test + tự dọn file cũ)
make fetch-forecast EXEC=1   # land dự báo slot giờ hiện tại
make load                    # autoloader nạp file mới vào Bronze
make quality                 # Provero quét Bronze
make backfill-archive        # fetch+load archive theo năm
make clean-lake              # squash lakehouse, bỏ lịch sử snapshot
make dbt-docs                # sinh và mở dbt docs
```

## Nguyên tắc làm việc

- **Không sang bước sau khi bước trước chưa được duyệt.**
- Mọi khẳng định về nguồn dữ liệu phải được **kiểm chứng bằng cách gọi thật**, không đọc doc rồi tin.
- **Test dựa trên metadata không chứng minh được dữ liệu tồn tại** — bài học từ sự cố bảng ma
  20/08/2026. Test quan trọng phải buộc engine đọc file thật (xem `assert_gold_is_readable`).
- Mọi giả định ghi rõ dạng `A1`, `A2`… và rủi ro dạng `R1`, `R2`… để trace ngược.
- Medallion dùng ba layer; Bronze chứa source object và source-faithful table — xem [03-architecture.md](03-architecture.md).
