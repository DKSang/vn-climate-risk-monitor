# Hanoi Flood & Climate Risk Monitor — Tài liệu dự án

Theo dõi rủi ro **ngập úng / lũ / hạn hán** cho Hà Nội, chi tiết đến **126 phường-xã**.

## Quy trình 9 bước

| # | Bước | File | Trạng thái |
|---|---|---|---|
| 1 | Bắt đầu từ bài toán nghiệp vụ | [01-business-problem.md](01-business-problem.md) | ✅ Xong |
| 2 | Xác định & đánh giá nguồn dữ liệu | [02-data-sources.md](02-data-sources.md) | ✅ Xong |
| 3 | Thiết kế kiến trúc | [03-architecture.md](03-architecture.md) | ✅ **Đã triển khai & kiểm chứng** |
| 3a | Cấu trúc repository | [03a-repo-structure.md](03a-repo-structure.md) | ✅ Xong |
| 4 | Ingest dữ liệu | [04-ingestion.md](04-ingestion.md) | 🟡 Địa lý xong · **Open-Meteo chưa làm** |
| 4a | Setup Lakehouse (DuckLake+MinIO+Postgres) | [04a-lakehouse-setup.md](04a-lakehouse-setup.md) | ✅ Xong |
| 5 | Clean & Transform | `05-transformation.md` | 🟡 Silver/Gold địa lý xong · fact chưa có |
| 6 | Lưu trữ — single source of truth | `06-storage-modeling.md` | ⬜ |
| 7 | Data Quality & Observability | `07-data-quality.md` | 🟡 `dbt test` 29 test · chưa có observability |
| 8 | Make it accessible | `08-serving-bi.md` | ⬜ |
| 9 | Governance & Continuous Improvement | `09-governance.md` | ⬜ |

## Trạng thái hệ thống (2026-08-20)

```
dbt build → PASS=49  ERROR=0
MinIO     → 10 object / 2.6 MiB
```

| Layer | Bảng | Dòng |
|---|---|---|
| seed | `ward_coordinates_seed` *(input artifact, ngoài medallion)* | 3.321 |
| bronze | `provinces_raw` · `wards_raw` · `administrative_units_raw` · `administrative_regions_raw` · `ward_coordinates_raw` | 34 · 3.321 · 5 · 8 · 3.321 |
| silver | `wards_cleaned` · `ward_coordinates_cleaned` · `ward_locations` | 3.321 mỗi bảng (view) |
| gold | `dim_hanoi_ward` | **126** |

## Lệnh thường dùng

```bash
make up            # bật Postgres + MinIO + pgAdmin
make transform     # dbt build (run + test + tự dọn file cũ)
make clean-lake    # squash lakehouse, bỏ lịch sử snapshot
make dbt-docs      # sinh và mở dbt docs
```

## Nguyên tắc làm việc

- **Không sang bước sau khi bước trước chưa được duyệt.**
- Mọi khẳng định về nguồn dữ liệu phải được **kiểm chứng bằng cách gọi thật**, không đọc doc rồi tin.
- **Test dựa trên metadata không chứng minh được dữ liệu tồn tại** — bài học từ sự cố bảng ma
  20/08/2026. Test quan trọng phải buộc engine đọc file thật (xem `assert_gold_is_readable`).
- Mọi giả định ghi rõ dạng `A1`, `A2`… và rủi ro dạng `R1`, `R2`… để trace ngược.
- Chuẩn medallion bám tài liệu Microsoft — xem [03-architecture.md §4](03-architecture.md).
