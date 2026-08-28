# Ingestion Open-Meteo Historical Archive

> ## ⚠️ TÀI LIỆU CŨ — kiến trúc đã thay đổi 2026-08-21
>
> Mọi lệnh `collect-open-meteo-*`, `load-open-meteo-*`, `run-open-meteo-*`,
> `observe-*` trong file này **KHÔNG CÒN TỒN TẠI**. Tầng collector/loader/pipeline
> viết bằng Python đã được gộp về package generic `autoloader` (discovery +
> checkpoint) cộng với YAML + SQL cho từng nguồn.
>
> **Lệnh hiện hành và cách chạy backfill: xem [04b-ingestion-runbook.md](04b-ingestion-runbook.md).**
>
> Giữ file này làm ghi chép thiết kế và kết quả khảo sát API — phần đó vẫn đúng.
>
> **Thêm nữa (2026-08-22):** Bronze loader giờ là **INSERT**, không còn
> `bronze_row_id` + `MERGE INTO` như mô tả ở §6 — bảng đích là
> `bronze_store.tables.open_meteo_archive` và dedup theo (ô lưới, giờ) làm ở
> Silver (`ROW_NUMBER() ... rn = 1`). Lý do và đánh đổi: xem ADR cuối
> [04b-ingestion-runbook.md](04b-ingestion-runbook.md).
>
> **2026-08-28:** nhịp API chỉ còn `OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR`.
> Fetch = urllib + MinIO (`open_meteo.land`).


**Trạng thái code:** hoàn thành collector, parser, Bronze loader, backfill và
daily tail · **Trạng thái dữ liệu:** năm 2000 đã commit đủ 1.106.784 ward-hour;
backfill 2001–nay là workload vận hành dài hạn theo giới hạn Free API.

## 1. Mục tiêu và ranh giới

Pipeline tải ERA5 hourly cho 126 phường/xã Hà Nội từ năm 2000 đến ngày Archive
thực sự cung cấp, giữ JSON nguồn để replay và tạo bảng:

```text
bronze_store.tables.open_meteo_archive_hourly
```

Bronze chỉ parse payload theo source contract. Dedup semantic, hợp nhất Archive
với Forecast và công thức KPI mưa/ngập thuộc Silver/Gold, không nằm trong
ingestion.

## 2. Luồng dữ liệu

```text
Open-Meteo Archive
       │ monthly window × tối đa 25 locations
       ▼
ArchiveCollector
       ├── PostgreSQL: run/file/checksum/status
       └── MinIO: immutable response JSON
                         │
                         ▼ claim + verify SHA-256
                 ArchiveHourlyLoader
                         │ strict Arrow parser
                         ▼
          DuckLake Bronze MERGE theo business key
                         │
                         ▼
       Parquet partition year(observed_time_utc)
```

PostgreSQL là control plane duy nhất. MinIO không có `_manifest.json`,
`_SUCCESS` hay `_FAILED.json`.

## 3. Granularity và partition

Ba khái niệm thời gian không được trộn lẫn:

| Khái niệm | Granularity | Lý do |
|---|---|---|
| HTTP request/source file | 1 tháng × 1 location batch | response vừa phải, xác định được file lỗi |
| Logical run/checkpoint | 1 tháng | lỗi cuối năm không bắt chạy lại các tháng trước |
| Bronze physical partition | `year(observed_time_utc)` | query lịch sử theo năm, tránh quá nhiều partition nhỏ |
| Bronze row grain | model × ward × observed hour | khóa ổn định giữa các lần recollect |

Thiết kế ban đầu dùng một logical run cho cả năm. Stress run 126 locations gọi
được 48 request đến hết tháng 08/2000 rồi API trả `HTTP 429` tại tháng 09. Vì run chỉ
được `SUCCEEDED` khi đủ 72 file, toàn bộ run phải `FAILED`. Kết quả này dẫn đến
quyết định dùng **monthly checkpoint nhưng year partition**. Công thức quota
cũng được sửa để nhân số locations. Đây là tách biệt giữa đơn vị recovery và
cách tổ chức Parquet.

Raw error body xác định chính xác nguyên nhân là `Minutely API request limit
exceeded`, không phải daily quota. Gọi lại đúng batch 25 locations sau cooldown
trả HTTP 200, đủ 25 × 720 giờ và không null precipitation.

Lệnh collector cấp thấp vì vậy chỉ cho phép `--execute` khi có `--month`. Lệnh
pipeline `run-open-meteo-archive --year 2000 --execute` vẫn an toàn: nó lập 12
monthly run và mặc định chỉ nhận thêm một period mới mỗi invocation.

## 4. Source contract

Model mặc định được pin bằng:

```dotenv
OPEN_METEO_ARCHIVE_MODEL=era5
```

Biến hourly:

```text
precipitation
rain
weather_code
soil_moisture_0_to_7cm
soil_moisture_7_to_28cm
```

Request dùng `timeformat=unixtime`, `timezone=GMT`, `precipitation_unit=mm` và
`cell_selection=land`. Theo tài liệu Open-Meteo, ERA5 có dữ liệu từ năm 1940 và
độ trễ khoảng 5 ngày; project bắt đầu ở 2000 theo phạm vi nghiệp vụ. Biên
`today - 5 days` chỉ là **candidate availability**, không phải cam kết payload
đã có dữ liệu. Parser vẫn là quality gate cuối cùng.

ERA5 được chọn sau canary: ERA5-Land trả HTTP 200 nhưng `precipitation`, `rain`
và `weather_code` đều null cho 744 giờ tháng 01/2000. Raw payload được giữ và
run bị đánh dấu lỗi; project không impute dữ liệu ở Bronze.

## 5. MinIO source layout

JSON được lưu nguyên byte, append-only:

```text
bronze/files/open_meteo/historical_weather_hourly/backfill/
└── year=2000/
    └── month=01/
        └── archive_2000_20000131_a1_<attempt>/
            ├── response_000.json
            ├── response_001.json
            └── ...
```

Object key chứa source year/month; PostgreSQL giữ `size_bytes`, SHA-256, ETag,
HTTP status, số HTTP attempt và typed source context. Object lỗi, payload null
và object của attempt không hoàn chỉnh không bị xóa tự động vì cần cho audit.

## 6. Parser và Bronze table

Parser dùng explicit Arrow schema và từ chối file khi:

- root không phải object/list đúng theo số location;
- `hourly.time` không phủ đủ request window hoặc không liên tục từng giờ;
- thiếu requested variable, sai chiều dài array hoặc cả array là null;
- response không phải GMT/UTC;
- metadata location/date trong PostgreSQL không khớp payload.

Field mới hoặc value sai type có thể đi vào `_rescued_data`; metric
`rescued_rows` buộc người vận hành review schema drift.

`bronze_row_id` là SHA-256 của:

```text
model + ward_key + observed_time_utc
```

Khóa không chứa attempt/file, vì vậy recollect cùng model/ward/hour sẽ `UPDATE`
lineage và giá trị mới thay vì tạo duplicate. Loader chạy transaction:

```sql
MERGE INTO bronze_store.tables.open_meteo_archive_hourly AS target
USING staging AS source
ON target.bronze_row_id = source.bronze_row_id
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT BY NAME;
```

DuckLake table được cấu hình một lần bằng:

```sql
ALTER TABLE bronze_store.tables.open_meteo_archive_hourly
SET PARTITIONED BY (year(observed_time_utc));
```

Partition setting áp dụng cho file mới. Không dùng `source_year` làm partition
expression vì `observed_time_utc` là source-of-truth ở grain hourly.

## 7. Incremental và idempotency

Một monthly run đi qua:

```text
RUNNING → SUCCEEDED → files được loader claim
   └──────────────→ FAILED

PENDING → PROCESSING → COMMITTED
              └────→ FAILED → retry
```

Các bảo đảm chính:

1. `logical_run_id` xác định bởi pipeline + dataset + scope + period/model;
2. logical run đã `SUCCEEDED` không gọi API lần nữa;
3. loader chỉ claim file của run `SUCCEEDED` bằng `SKIP LOCKED` và lease;
4. object được xác minh size/SHA-256 trước parse;
5. Bronze commit xảy ra trước PostgreSQL `COMMITTED`;
6. crash giữa hai commit được replay bằng deterministic `bronze_row_id`;
7. canary, backfill và tail dùng scope riêng nên không ăn checkpoint của nhau.

Backfill không dùng “high watermark lớn nhất” vì một period ở giữa có thể lỗi.
Ledger theo từng logical period cho phép phát hiện gap và chạy bù đúng period.

## 8. Lệnh vận hành

Dry-run không ghi dữ liệu:

```bash
make plan-historical
uv run run-open-meteo-archive --year 2000
uv run run-open-meteo-archive --year 2000 --month 1 --limit 1
```

Plan dài mặc định chỉ in ba period đầu, period cuối và số period đã lược bỏ.
Thêm `--verbose-plan` khi thực sự cần xem toàn bộ.

Canary và backfill:

```bash
make run-historical-canary
make run-historical-year HISTORICAL_YEAR=2000
make run-historical-backfill
```

`run-historical-year` lập mọi tháng trong năm và cho phép tối đa 12 monthly
checkpoint mới trong một invocation. CLI thuần mặc định vẫn chỉ nhận một tháng;
muốn cho phép nhiều period phải chỉ định rõ và vẫn chịu daily guardrail:

```bash
uv run run-open-meteo-archive --year 2000 --execute --max-periods 2
```

Không khuyến nghị tăng số này trên Free API. Chạy lại cùng lệnh sẽ bỏ qua các
period đã `SUCCEEDED` và tiến tới period chưa hoàn thành tiếp theo.

Daily tail:

```bash
make run-historical-tail
uv run run-open-meteo-archive --tail --limit 1       # dry-run canary
```

Cron mẫu ở `orchestration/cron/open_meteo_archive.cron.example`. Không bật
backfill cron và tail cron cùng lúc nếu tổng budget dự phòng vượt giới hạn ngày.
Hai job dùng chung `flock` để không chạy chồng trên single-node.

Health cho workload daily dùng freshness 36 giờ thay vì ngưỡng forecast 120
phút:

```bash
make historical-status
make historical-tail-status
```

## 9. Quota guardrail

Collector tính worst-case reserve trước request bằng **location count × time
factor × variable factor × `OPEN_METEO_MAX_ATTEMPTS`**. Mặc định:

```dotenv
OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_MINUTE=500
OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR=4500
OPEN_METEO_CONCURRENCY=1
```

Các giá trị này giữ headroom dưới giới hạn Free API công bố tương ứng
600/phút và 5.000/giờ. Không có daily admission guardrail nội bộ.
`EffectiveCallPacer` dùng hai token bucket
minute/hour: cho phép burst còn nằm trong budget, sau đó chỉ sleep đúng thời
gian cần để bucket thiếu token được refill. Một tháng 31 ngày cho toàn bộ 126
locations dùng khoảng 279 units nên vẫn nằm trong burst budget 500/phút.

Một tháng 31 ngày cho 126 locations tốn khoảng 279 effective call units và dự
phòng 1.396 units với năm HTTP attempts. Một năm có 12 monthly period.
Ước lượng cũ không nhân locations đã đánh giá thấp quota; unit test hiện khóa
công thức đúng. Runtime admission mặc định là một tháng. Guardrail nội bộ không
thay thế rate limit phía Open-Meteo.

HTTP retry luôn ưu tiên `Retry-After`. Nếu response `429` không có header như
sự cố đã quan sát, client dùng fallback cooldown 60 giây thay vì exponential
backoff ngắn rồi gửi lại liên tục.

Khi vẫn gặp `429`:

1. dừng invocation, không loop retry toàn năm;
2. giữ run/file/object lỗi;
3. client tự chờ `Retry-After` hoặc fallback 60 giây;
4. chạy lại cùng command; monthly logical period tạo attempt mới;
5. không sửa hoặc xóa JSON cũ để “làm xanh” state.

## 10. Kiểm chứng đã chạy

Canary ERA5 tháng 01/2000:

```text
source:       1 location × 744 contiguous hours
Bronze:       744 rows / 744 distinct bronze_row_id
time range:   2000-01-01T00:00Z .. 2000-01-31T23:00Z
required null rows: 0
rescued rows: 0
partition metadata: year(observed_time_utc)
physical path: bronze/tables/open_meteo_archive_hourly/year=2000/*.parquet
```

Canary sau khi triển khai pacing chạy lại period 01–14/01/2000:

```text
collection=SUCCEEDED; file=COMMITTED; health=HEALTHY
rows_parsed=336; rows_inserted=0; retry_count=0
Bronze vẫn 744 rows / 744 distinct keys / 0 rescued rows
```

`rows_inserted=0` là kết quả mong đợi: deterministic key khiến `MERGE` cập nhật
lineage của 336 giờ đã có thay vì tạo duplicate.

Production tháng 01/2000 sau khi bật token-bucket pacing:

```text
locations=126; source_files=6; HTTP 429=0
collection=SUCCEEDED; files_committed=6; health=HEALTHY
request_attempt_count=1 cho cả 6 files
rows_parsed=93.744; rows_inserted=51.000; failures=0
Bronze total=93.744 rows / 93.744 distinct keys / 0 required-null/rescued rows
```

Kết quả này kiểm chứng pacing trên đúng multi-location workload từng gặp
minutely limit, không chỉ trên unit test hoặc request một location. Số inserted
thấp hơn parsed vì `MERGE` cập nhật các giờ đã có từ hai canary trước đó.

Backfill năm 2000 hoàn chỉnh:

```text
monthly logical runs=12; committed source files=72
wards=126; months=12; observed hours=8.784
Bronze rows=1.106.784; distinct bronze_row_id=1.106.784
time range=2000-01-01T00:00Z..2000-12-31T23:00Z
required-null rows=0; rescued rows=0; health=HEALTHY
```

Stress/canary partial runs vẫn được giữ trong control plane và raw storage để
audit. Stable `bronze_row_id` khiến chúng không làm tăng row count cuối cùng.

Tail canary tại candidate date 2026-08-16 trả requested weather arrays toàn
null. Loader từ chối cả file bằng `ArchiveParseError`; Bronze không tăng dòng.
Điều này xác nhận fail-closed hoạt động. Khi system clock đi trước data
availability thực, tạm dừng tail và dùng một `end-date` backfill đã được xác
minh; không giảm quality rule để nhận null payload.

Full-year stress run 2000 dừng ở request tháng 09 do minutely `HTTP 429`. Run ở
PostgreSQL là `FAILED`, các raw response trước lỗi được giữ, và loader không
claim partial run. Sự cố này là bằng chứng để chuyển runtime checkpoint sang
monthly run và thêm effective-call pacing. Gọi lại nguyên batch sau
đó trả HTTP 200, chứng minh source contract vẫn hợp lệ.

## 11. Definition of Done

- [x] deterministic planner từ 2000 đến candidate availability date;
- [x] monthly run/request checkpoint và location batching;
- [x] immutable source JSON trên `bronze/files`;
- [x] generic PostgreSQL control plane, retry và lease;
- [x] strict Archive parser với explicit schema;
- [x] idempotent Bronze `MERGE`;
- [x] DuckLake physical partition theo năm;
- [x] one-command collect → load pipeline;
- [x] daily tail command và cron template;
- [x] free-tier admission guardrail;
- [x] minute/hour effective-call pacing và 60-second `429` cooldown;
- [x] unit test và live canary;
- [ ] vận hành lặp đến khi toàn bộ 2000–nay được backfill;
- [ ] Silver conformance và KPI (phase tiếp theo).

## Tham khảo

- [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
- [Open-Meteo pricing](https://open-meteo.com/en/pricing)
- [DuckLake partitioning](https://ducklake.select/docs/stable/duckdb/advanced_features/partitioning)
- [DuckDB `MERGE INTO`](https://duckdb.org/docs/stable/sql/statements/merge_into.html)
- [PostgreSQL `SKIP LOCKED`](https://www.postgresql.org/docs/current/sql-select.html)
