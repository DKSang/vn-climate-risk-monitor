# Bước 3 — Thiết kế kiến trúc

**Hanoi Flood & Climate Risk Monitor** · v1.0 · 2026-08-20 · *Trạng thái: ĐÃ TRIỂN KHAI & KIỂM CHỨNG*

> v1.0 viết lại toàn bộ theo **kiến trúc đã chạy thật**, thay cho v0.3 vốn còn để mở lựa chọn
> stack. Mọi con số trong tài liệu này đều lấy từ hệ thống đang chạy, không phải ước lượng.

---

## 1. Ràng buộc đầu vào (từ Bước 1, 2)

| Ràng buộc | Giá trị | Hệ quả thiết kế |
|---|---|---|
| Nguồn | Open-Meteo Forecast + Archive, QĐ 2280, danh mục hành chính | 2 API ngoài + 2 nguồn tĩnh |
| Độ phân giải mưa | **49 ô lưới cho 126 phường/xã** (R1) | Mưa theo ô, phường gán vào ô |
| Nhịp | Hourly (forecast) + Daily (archive) | 2 pipeline riêng |
| Chi phí | 0đ, chạy local | 100% open-source, Docker Compose |
| DQ | Fail → chặn publish | `dbt test` là cổng chặn |

## 2. Stack đã chốt

```
Docker Compose (local, 0đ)
├── Postgres 17.5   → DuckLake catalog metadata (schema `ducklake`)
│                     + nguồn danh mục hành chính (schema `public`)
├── MinIO           → object storage, Parquet cho bronze/silver/gold
├── pgAdmin         → xem catalog
└── DuckDB 1.5.5    → compute engine (embedded, không phải service)
    └── dbt 1.12.2 + dbt-duckdb 1.11.0 → toàn bộ transform
```

**Chưa có:** orchestration (Bước 8), serving/API/dashboard (Bước 8), ingest Open-Meteo (Bước 4).

## 3. Sơ đồ luồng dữ liệu

```
┌─ NGUỒN ────────────────────────────────────────────────────────┐
│  Postgres public.*          MinIO raw/*.csv       Open-Meteo    │
│  (danh mục hành chính)      (toạ độ centroid)     (CHƯA LÀM)    │
└──────────┬────────────────────────┬────────────────────────────┘
           │  ATTACH postgres        │  read_csv_auto('s3://...')
           │  (read_only)            │
           └───────────┬─────────────┘
                       │   dbt + DuckDB
        ┌──────────────▼──────────────────────────────────────┐
        │  DuckLake  (catalog: Postgres · data: MinIO Parquet) │
        │                                                       │
        │  bronze.*_raw      as-is, cấm mọi biến đổi           │
        │        ↓                                              │
        │  silver.*_cleaned  làm sạch, ép kiểu, dedup, JOIN     │
        │        ↓           (view — không tốn dung lượng)      │
        │  gold.dim_* fct_*  dimensional model, lọc phạm vi     │
        └──────────────┬───────────────────────────────────────┘
                       │ dbt test = cổng chặn publish
                       ▼
              Serving / Dashboard  (Bước 8, chưa làm)
```

## 4. Chuẩn medallion — bám tài liệu Microsoft

Nguồn: [Implement Medallion Lakehouse Architecture in Fabric](https://learn.microsoft.com/en-us/fabric/onelake/onelake-medallion-lakehouse-architecture)
· [What is the medallion lakehouse architecture? (Azure Databricks)](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)

### 4.1 Nhiệm vụ từng lớp

| | **Bronze** | **Silver** | **Gold** |
|---|---|---|---|
| Microsoft gọi là | Raw data ingestion | Data cleaning and validation | Dimensional modeling and aggregation |
| Được làm | Không sửa gì. Thêm cột provenance | Schema enforcement · null handling · **dedup** · type casting · **JOIN** · schema evolution | Dim/fact · aggregate · lọc theo vùng hoặc thời gian |
| Cấm | Ép kiểu, đổi tên, lọc, JOIN | — | Chạm `source()` trực tiếp |
| Người dùng | Data engineer, audit | Data engineer, analyst, data scientist | Business analyst, BI, lãnh đạo |
| Materialization | `table` | `view` | `table` |

> **Đính chính so với tài liệu nội bộ trước đây:** từng có quy tắc *"silver cấm JOIN"*.
> Đó là convention **staging của dbt**, không phải chuẩn Microsoft. Microsoft liệt kê `Joins`
> là thao tác hợp lệ của silver và lấy `customer_transactions` (một bảng join) làm ví dụ.
> Yêu cầu thật của silver là: phải có ít nhất một bản **đã validate, CHƯA aggregate** cho mỗi record.

### 4.2 Quy ước đặt tên

| Layer | Mẫu | Ví dụ của Microsoft | Bảng trong dự án |
|---|---|---|---|
| bronze | `<entity>_raw` — **hậu tố** | `leads_raw` | `wards_raw`, `provinces_raw` |
| silver | `<entity>_cleaned` | `leads_cleaned` | `wards_cleaned` |
| silver | tên thực thể đã join | `customer_transactions` | `ward_locations` |
| gold | `dim_<entity>` / `fct_<process>` | *(MS dùng tên nghiệp vụ)* | `dim_hanoi_ward` |
| gold | `<business>_summary` | `business_summary` | *(chưa có)* |

Schema đặt đúng theo mẫu `ops.bronze` / `ops.silver` / `ops.gold` của Microsoft →
`catalog1.bronze` / `catalog1.silver` / `catalog1.gold`.

**Lệch có chủ ý:** giữ tiền tố `dim_`/`fct_` ở gold. Ví dụ gold của Microsoft toàn bảng tổng hợp
nên không có tiền tố, nhưng chính họ định nghĩa gold là *"dimensional model"* — `dim_`/`fct_`
là chuẩn Kimball, bổ sung chứ không mâu thuẫn.

## 5. Các bảng hiện có

| Bảng | Dòng | Vai trò |
|---|---|---|
| `bronze.provinces_raw` | 34 | 34 tỉnh/thành sau sáp nhập 2025 |
| `bronze.wards_raw` | 3.321 | Danh mục phường/xã GSO toàn quốc |
| `bronze.administrative_units_raw` | 5 | Loại đơn vị hành chính |
| `bronze.administrative_regions_raw` | 8 | Vùng địa lý |
| `bronze.ward_coordinates_raw` | 3.321 | Toạ độ centroid phường/xã |
| `silver.wards_cleaned` | 3.321 | Làm sạch danh mục GSO |
| `silver.ward_coordinates_cleaned` | 3.321 | Làm sạch + ép kiểu toạ độ |
| `silver.ward_locations` | 3.321 | **Ward master toàn quốc** — đã JOIN, chưa aggregate |
| `gold.dim_hanoi_ward` | **126** | Chiều phường/xã Hà Nội (NQ 1656/NQ-UBTVQH15) |

`dbt build` hiện: **40 PASS / 0 ERROR** (6 table model, 3 view model, 29 test, 2 hook).

## 6. Quyết định kiến trúc (ADR)

### ADR-1 — Bỏ dlt khỏi luồng dữ liệu địa lý

**Bối cảnh:** luồng ban đầu là `Postgres/CSV → dlt → Parquet trên MinIO → Python CREATE TABLE → DuckLake`.
Bước cuối là **transform viết bằng Python**, trái nguyên tắc "transform thuộc về dbt".

**Quyết định:** dbt đọc thẳng nguồn.
- Postgres: `ATTACH ... (TYPE postgres, READ_ONLY)` trong `profiles.yml`
- CSV: `read_csv_auto('s3://...')` qua `meta.external_location` của source

**Lý do:** với bảng quan hệ tĩnh, dlt không mang lại gì — không cần incremental state, không có
JSON lồng nhau, và DuckLake đã có snapshot/time-travel riêng. Bỏ được 2 hop.

**Hệ quả:** 4 hop → 1 hop. dlt **vẫn giữ trong dự án** cho Open-Meteo (Bước 4), nơi cần HTTP
retry, state để không fetch lại 1981–2020 mỗi lần chạy, và unnest mảng `hourly` lồng nhau.

### ADR-2 — Materialization `table` tuỳ biến cho DuckLake

**Vấn đề:** materialization mặc định của dbt-duckdb dùng create-then-swap:

```
1. CREATE TABLE <model>__dbt_tmp AS (...)
2. RENAME <model>        -> <model>__dbt_backup
3. RENAME <model>__dbt_tmp -> <model>
4. DROP <model>__dbt_backup
```

Với DuckLake, đường dẫn Parquet được quyết định ở **bước 1** theo tên lúc tạo. Bước 3 chỉ đổi tên
trong catalog Postgres, **không di dời file**. Kết quả: dữ liệu bảng `wards_raw` nằm vĩnh viễn ở
`s3://vn-climate/bronze/wards_raw__dbt_tmp/`.

**Đã thử và loại:**
- `ducklake_rewrite_data_files()` — chạy OK nhưng không đổi đường dẫn
- `ducklake_merge_adjacent_files()` — tương tự
- `adapter.use_ducklake_table_workarounds()` của dbt-duckdb — chỉ xử lý `persist_docs` cho
  DuckLake < 1.5.3, không liên quan đường dẫn

**Quyết định:** override materialization `table`, dùng `CREATE OR REPLACE TABLE` thẳng vào tên đích.

**Cảnh báo quan trọng — bài học đã trả giá:** một bản override trước đây làm đúng ý tưởng này
nhưng **bỏ `adapter.commit()`** và toàn bộ hooks. Hậu quả đo được: DuckLake ghi metadata vào
Postgres nhưng Parquet không finalize → **bảng ma** (`COUNT(*)` trả 126 từ metadata, `SELECT *`
lỗi HTTP 404), và `dbt test` cho **green giả** vì `not_null` cũng đọc từ thống kê.

Bản hiện tại giữ đầy đủ vòng đời: pre/post hooks, grants, `persist_docs`, và `adapter.commit()`.
Chỉ hỗ trợ SQL — model Python sẽ báo lỗi rõ ràng thay vì hỏng ngầm.
Xem `transform/macros/materializations.sql`.

### ADR-3 — Silver materialize thành `view`

Silver chỉ làm sạch cơ học và join, không aggregate. Dùng `view` → **0 byte trên MinIO**, không
tốn thời gian build, luôn đồng bộ với bronze. Kiểm chứng: prefix `silver/` trên MinIO rỗng.

Khi nào đổi sang `table`: nếu silver có phép tính nặng bị lặp lại nhiều lần bởi gold.

### ADR-4 — Bảo trì lakehouse tự động

dbt materialize theo kiểu ghi bản mới, nên mỗi lần build để lại Parquet phiên bản cũ.
`on-run-end` trong `dbt_project.yml`:

```sql
CALL ducklake_expire_snapshots('catalog1', older_than => now() - INTERVAL 7 DAY);
CALL ducklake_cleanup_old_files('catalog1', cleanup_all => true);
```

Giữ 7 ngày để **vẫn time-travel được** — file của các build trong 7 ngày còn nằm đó là **chủ ý**,
không phải rác. Khi cần squash sạch: `make clean-lake` (mất time-travel, giữ bản hiện tại).

### ADR-5 — Bảng nhỏ được DuckLake inline

`administrative_units_raw` (5 dòng) và `administrative_regions_raw` (8 dòng) **không có file
Parquet nào** trên MinIO nhưng đọc bình thường — DuckLake inline dữ liệu nhỏ thẳng vào catalog.
Không phải lỗi. Cần biết điều này khi đối chiếu danh sách file với danh sách bảng.

## 7. Data quality — cổng chặn

`dbt test` chạy trong `dbt build`, fail thì model downstream bị SKIP.

**Test đặc biệt `assert_gold_is_readable`:** dùng `COUNT(DISTINCT <cột VARCHAR>)` để **buộc engine
đọc Parquet thật**. Sinh ra sau sự cố bảng ma — khi đó `COUNT(*)` và `not_null` đều PASS vì trả lời
từ thống kê metadata mà không chạm file. Bài học: **test dựa trên metadata không chứng minh được
dữ liệu tồn tại.**

## 8. Còn thiếu

| Hạng mục | Bước |
|---|---|
| Ingest Open-Meteo (S1 Forecast, S2 Archive) bằng dlt | 4 |
| Bảng fact: `fct_rainfall_hourly`, `fct_flood_risk_hourly` | 5–6 |
| Chiều `dim_grid_cell` (49 ô) + ánh xạ phường → ô lưới | 5–6 |
| Seed ngưỡng QĐ 2280 (50/70/100 mm/h) | 5 |
| Orchestration | 8 |
| API + dashboard | 8 |
| Governance, CI/CD | 9 |
