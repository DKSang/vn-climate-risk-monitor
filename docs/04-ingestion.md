# Thiết kế ingestion

**Hanoi Flood & Climate Risk Monitor** · v1.0 · 2026-08-21 · *Chưa ingest Open-Meteo*

## Nguyên tắc

Hệ thống chỉ có ba data layer. Bronze đồng thời chứa payload nguồn nguyên bản và
bảng ingestion đã parse:

```text
Source API
    ↓ collector
bronze/files/.../response.json
    ↓ incremental loader
bronze_store.tables.open_meteo_*_hourly
    ↓ dbt
silver → gold
```

Ranh giới module:

- `collectors`: chỉ giao tiếp source và ghi immutable source run.
- `loaders`: discover source file hoàn chỉnh, parse và ghi Bronze table.
- `state`: file-level checkpoint và pipeline run.
- `transform`: validation, dedup, conformance và business logic.

Không có module nào vừa gọi API vừa tính KPI.

## Bronze files contract

```text
bronze/files/<source_system>/<dataset>/<load_type>/
  YYYY/MM/DD/HH/<run_id>/
    request_000.json
    response_000.json
    _manifest.json
    _SUCCESS
```

- Thời gian trên path là ingestion time UTC và luôn zero-pad.
- File không overwrite; mỗi logical run có `run_id` riêng.
- `_manifest.json` lưu checksum, size, request metadata và code version.
- `_SUCCESS` được ghi cuối cùng và là commit marker ở cấp source run.
- JSON gốc không được thêm lineage field; lineage nằm trong manifest/Bronze table.

Contract path được triển khai ở
`vn_climate_risk_monitor.ingestion.layout.BronzeFilesLayout`.

## Bronze table contract cho Open-Meteo

Tên dự kiến:

```text
bronze_store.tables.open_meteo_forecast_hourly
bronze_store.tables.open_meteo_archive_hourly
```

Grain:

```text
forecast: source file × location_index × hourly_index
archive:  source file × location_index × hourly_index
```

Bronze loader được phép ghép `hourly.time[i]` với
`hourly.precipitation[i]` và explode thành dòng. Loader không được:

- lọc record;
- loại duplicate;
- chọn forecast mới nhất;
- gán grid cell hoặc ward;
- tính rolling rainfall hay risk level.

Deterministic id dự kiến:

```text
bronze_row_id = sha256(source_file_sha256 || location_index || hourly_index)
```

Nếu array không cùng độ dài, loader phải giữ phần tử thiếu dưới dạng `NULL` và
ghi chi tiết vào `_rescued_data`, không dùng `zip()` làm mất record.

## Incremental file discovery

Trạng thái nằm trong DuckLake:

```text
ops.pipeline_runs
ops.ingestion_files
```

`ops.ingestion_files` dùng khóa:

```text
(pipeline_name, source_file_path)
```

Không dùng duy nhất `last_modified > watermark`, vì file đến muộn có thể bị bỏ
qua. Mỗi loader scan các run có `_SUCCESS`, đối chiếu file path với ledger và chỉ
xử lý file chưa `COMMITTED`.

Micro-batch:

```text
discover pending files
    → parse staging
    → đánh dấu PROCESSING trong ops
    → MERGE staging vào Bronze theo bronze_row_id
    → COMMIT Bronze
    → cập nhật ops.ingestion_files = COMMITTED
```

Không giả định transaction phân tán giữa `catalog1.ops` và
`bronze_store.tables`. Nếu process chết sau Bronze commit nhưng trước checkpoint,
retry chạy lại cùng deterministic id nên không tạo duplicate. Đây là pattern
tương đương file checkpoint của Auto Loader trong stack MinIO/DuckLake.

## Schema evolution

- Field đã biết có contract rõ ràng.
- Field mới hoặc type mismatch không bị drop; lưu vào `_rescued_data`.
- Parser có `parser_version` trong file ledger.
- Schema change không tương thích phải fail batch trước commit.
- Có thể replay từ `bronze/files` sau khi nâng parser.

## Nhịp dự kiến

| Pipeline | Collector | Loader |
|---|---:|---:|
| Forecast | mỗi giờ | Available-now ngay sau `_SUCCESS` |
| Archive daily | mỗi ngày | Available-now |
| Archive backfill | chunk theo năm/toạ độ | Resume theo file ledger |

Đây mới là contract kiến trúc. Chưa có source, loader, bảng hoặc request
Open-Meteo nào được tạo trong code ở bước này.
