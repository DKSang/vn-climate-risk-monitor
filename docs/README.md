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
| 5 | Clean, Transform & KPI | [05-kpi-methodology.md](05-kpi-methodology.md) | ✅ Gold forecast + baseline lịch sử + event + replay · không công bố xác suất ngập (K5) |
| 6 | Lưu trữ — single source of truth | [06-storage-modeling.md](06-storage-modeling.md) | ✅ Gold SSOT, `dim_grid`, watermark control, SCD2 từ 2025-07-01 |
| 7 | Data Quality & Observability | `07-data-quality.md` | 🟡 ingestion health có · model observability chưa làm |
| 8 | Make it accessible | `08-serving-bi.md` | ⬜ |
| 9 | Governance & Continuous Improvement | `09-governance.md` | ⬜ |

## Trạng thái hệ thống (2026-09-03)

```
PostgreSQL → ingestion_runs + ingestion_files      (file checkpoint)
             processing_state + processing_runs    (processing checkpoint)
DuckLake   → MỘT catalog `catalog1`
MinIO      → bronze/files (raw)  ·  silver/  ·  gold/
```

Medallion đã dựng lại gọn (xem
[plan lean medallion](superpowers/plans/2026-09-03-lean-medallion.md) và
[plan Silver Layer Flow](superpowers/plans/2026-09-03-silver-layer-flow.md)).

| Layer / Lớp dbt | Bảng / Model | Dòng |
|---|---|---|
| landing | `bronze/files/**.json` *(raw bất biến, ngoài catalog)* | 1.302 file · 790 MB |
| seed | `ward_coordinates_seed` · `ward_grid_map_seed` *(input artifact)* | 3.321 · 252 |
| silver staging | `stg_weather_archive_hourly` (bảng) · `stg_open_meteo__weather_archive_hourly` (view) · `stg_seed__ward` (view) · `stg_seed__ward_grid` (view) | **19.895.304** · 126 · 252 |
| silver intermediate | `int_weather_archive_hourly` (curated: dedup + MERGE change-aware) | **5.818.584** |
| gold marts (dim) | `dim_grid` · `dim_ward` · `bridge_ward_grid` | 60 · 126 · 252 |
| gold marts (fact) | `fct_rain_archive_hourly` · `fct_rain_archive_daily` · `fct_ward_rain_archive_daily` | **5.818.584** · **242.441** · **1.223.303** |

Staging giữ 70% dòng "trùng" một cách CÓ CHỦ Ý: đó là change log của mọi lần
fetch. Dedup xảy ra ở `silver.int_weather_archive_hourly`.

## Lệnh thường dùng

```bash
make up                      # bật Postgres + MinIO + pgAdmin
make fetch-forecast EXEC=1   # land dự báo slot giờ hiện tại
make load                    # autoloader nạp file raw vào silver.stg_*
make transform               # dbt build qua processing framework
make processing-status       # checkpoint + lịch sử run
make quality                 # Provero quét silver staging
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
- Bronze CHỈ là landing zone raw file. Bảng append-only đầu tiên là `silver.stg_*`
  (staging), dedup ở `silver.int_weather_archive_hourly` (curated) — xem
  [plan Silver Layer Flow](superpowers/plans/2026-09-03-silver-layer-flow.md).
