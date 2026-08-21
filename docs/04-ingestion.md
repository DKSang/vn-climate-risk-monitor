# Thiết kế ingestion Open-Meteo

**Hanoi Flood & Climate Risk Monitor** · v2.0 · 2026-08-21

**Trạng thái:** Phase 1–5 hoàn thành; pipeline hourly production đã collect và
load đủ 126 phường/xã vào Bronze DuckLake.

## 1. Mục tiêu

Pipeline phải:

- lưu response JSON nguồn bất biến để replay;
- giữ mọi forecast retrieval vintage;
- chạy incremental và retry không tạo duplicate downstream;
- tách metadata vận hành khỏi dữ liệu nguồn;
- giữ lineage từ Bronze table về response object và phường/xã yêu cầu;
- chạy production-like trên một node với stack miễn phí.

Phạm vi đầu tiên là forecast 72 giờ cho 126 phường/xã Hà Nội. KPI mưa/ngập
được định nghĩa tại [05-kpi-methodology.md](05-kpi-methodology.md), không nằm
trong collector.

## 2. Kiến trúc đã chốt

```text
Open-Meteo
    │
    ▼
Collector
    ├── PostgreSQL control plane
    │     ├── ingestion.ingestion_runs
    │     └── ingestion.ingestion_files
    │
    └── MinIO data plane
          └── bronze/files/.../response_NNN.json
                         │
                         ▼
                       Loader
                         │
                         ▼
              Bronze DuckLake table
              ├── catalog metadata → PostgreSQL ducklake_bronze
              └── Parquet          → MinIO bronze/tables
                         │
                         ▼
                    Silver → Gold
```

PostgreSQL là single source of truth cho ingestion metadata. MinIO không có
`_manifest.json`, `_SUCCESS`, `_FAILED.json` hoặc request envelope. `status =
SUCCEEDED` trong PostgreSQL là commit signal cho loader.

DuckLake không phải một layer sau Bronze. DuckLake quản lý Bronze/Silver/Gold
tables; catalog metadata nằm trong PostgreSQL và data files nằm trên MinIO.

## 3. Ba plane và quyền sở hữu

| Plane | Nơi lưu | Trách nhiệm |
|---|---|---|
| Control | PostgreSQL schema `ingestion` | logical run, attempt, file state, retry, lease, checksum |
| Object/data | MinIO `bronze/files` | response JSON nguồn, immutable |
| Analytical/table | DuckLake | snapshot/schema/table metadata và Parquet medallion |

Schema `ingestion` là native PostgreSQL, tách khỏi các schema nội bộ
`ducklake` và `ducklake_bronze`. Không tạo `ops` như một data layer thứ tư.
Migration `003_move_ingestion_control_to_postgres.py` loại hai relation DuckLake
`ops` cũ, nhưng từ chối chạy nếu chúng có dữ liệu.

## 4. Stack

| Nhiệm vụ | Công cụ |
|---|---|
| HTTP | `requests.Session` + `urllib3.Retry` |
| Control plane | `psycopg` + PostgreSQL |
| Object storage | MinIO Python SDK |
| Integrity | SHA-256 + size |
| Parse Phase 4 | PyArrow explicit schema |
| Bronze table | DuckDB + DuckLake `MERGE INTO` |
| Transform | dbt-duckdb |
| Test | pytest + `responses` |

Không dùng dlt, Airbyte, NiFi, Spark hoặc streaming service cho MVP. Forecast
chỉ tạo khoảng sáu HTTP batch mỗi run nên Python tuần tự dễ vận hành hơn.

## 5. Forecast contract

| Thuộc tính | Giá trị mặc định |
|---|---|
| Endpoint | `https://api.open-meteo.com/v1/forecast` |
| Model | `best_match` |
| Forecast horizon | 72 giờ |
| Batch size | 25 location |
| Concurrency | 1 |
| Schedule đề xuất | mỗi giờ, phút 15 |
| Grain Bronze tương lai | attempt × requested location × valid hour |

Biến hourly lõi:

```text
precipitation
rain
showers
precipitation_probability
weather_code
```

Location được đọc từ `gold.dim_hanoi_ward`, bắt buộc đủ 126 dòng, sắp xếp theo
`ward_key` rồi mới chia batch. Open-Meteo trả array theo thứ tự location request;
vì vậy `ingestion_files.ward_keys` lưu ordered mapping cho từng response file.

## 6. Logical run và execution attempt

Hai identity được tách nhưng vẫn chỉ dùng hai bảng:

```text
logical_run_id = UUIDv5(
    pipeline_name + dataset + scope + logical_schedule_key
)

attempt_id = UUIDv4 cho mỗi lần chạy job
```

Ví dụ:

```text
logical run: open_meteo_forecast / 2026-08-21T10:15:00Z / production
  attempt 1 → FAILED
  attempt 2 → SUCCEEDED
```

HTTP retry 429/5xx vẫn thuộc cùng attempt. Chỉ rerun cả collector mới tạo
attempt mới. `scope=production` và `scope=canary_N` tách canary khỏi scheduled
production run.

PostgreSQL dùng advisory transaction lock để cấp `attempt_number`, unique index
đảm bảo một logical run không có đồng thời hai attempt `RUNNING` và không có hơn
một attempt `SUCCEEDED`.

## 7. PostgreSQL control model

### `ingestion.ingestion_runs`

Một row đại diện cho một execution attempt. Các cột chính:

```text
attempt_id, logical_run_id, logical_key, attempt_number
pipeline_name, dataset, scope
scheduled_at_utc, started_at_utc, completed_at_utc
status, batch_count, location_count
source_endpoint, model_requested, forecast_hours, hourly_variables
collector_version, request_contract_version
error_type, error_message
```

Run state:

```text
RUNNING ──► SUCCEEDED
    └─────► FAILED
```

`SUCCEEDED` và `FAILED` là terminal cho một attempt. Retry job tạo attempt mới,
không sửa attempt cũ về `RUNNING`.

### `ingestion.ingestion_files`

Một row đại diện cho một response object/batch:

```text
file_id, attempt_id, batch_index, object_key
ward_keys
size_bytes, sha256, etag, content_type
http_status, request_attempt_count
expected_location_count, received_location_count
status, retry_count, worker_id, lease_expires_at_utc
rows_parsed, rows_inserted, parser_version
error_type, error_message
```

File state tối thiểu:

```text
PENDING ──► PROCESSING ──► COMMITTED
                 └──────► FAILED ──► PROCESSING
```

Chưa dùng `DISCOVERED` hoặc `QUARANTINED`. File được collector đăng ký trực tiếp
nên không cần discovery state riêng. Error không còn khả năng retry chỉ dừng ở
`FAILED` sau `max_retries` và được điều tra thủ công.

## 8. Collector Phase 2 sau refactor

Trình tự một run:

```text
validate và deterministic batch locations
  → INSERT ingestion_runs(status=RUNNING)
  → với từng batch:
       INSERT ingestion_files(status=PENDING, object_key, ward_keys)
       GET Open-Meteo với bounded retry
       ghi exact response bytes vào MinIO
       UPDATE size/hash/HTTP metadata
       validate HTTP, content type, JSON root và location count
       UPDATE received_location_count
  → UPDATE ingestion_runs(status=SUCCEEDED)
```

Nếu lỗi, response body đã nhận vẫn được giữ trên MinIO; file và run chuyển
`FAILED`. Loader chỉ claim file thuộc run `SUCCEEDED`, vì vậy partial run không
lọt xuống Bronze table.

Collector không ghi request JSON. Request-level fields cố định nằm trên run;
ordered `ward_keys` nằm trên file. Cách này tránh lưu cùng metadata ở cả MinIO
và PostgreSQL.

## 9. MinIO object layout

```text
bronze/files/
└── open_meteo/forecast/incremental/YYYY/MM/DD/HH/
    └── forecast_<logical-time>_a<attempt-number>_<attempt-id-prefix>/
        ├── response_000.json
        ├── response_001.json
        └── ...
```

Object write là write-once và từ chối overwrite. Mỗi object có size, SHA-256 và
ETag được ghi vào `ingestion_files`. Legacy canary trước refactor có thể còn
marker/request files trên MinIO nhưng collector mới không tạo hoặc sử dụng chúng.

## 10. Phase 3: incremental discovery và checkpoint

Discovery không scan MinIO. PostgreSQL query chính là nguồn file có thể xử lý:

```sql
run.status = 'SUCCEEDED'
AND run.scope = requested_scope
AND file.status IN ('PENDING', 'FAILED')
AND file.retry_count < max_retries
```

Claim dùng một PostgreSQL transaction với:

```text
SELECT ... FOR UPDATE SKIP LOCKED
UPDATE status=PROCESSING, worker_id, lease_expires_at_utc
```

Nhờ đó hai loader worker không claim cùng file. Nếu worker chết, lease hết hạn
sẽ đưa file về `FAILED`; claim tiếp theo tăng `retry_count` và xử lý lại.

Trước parse, `VerifiedObjectReader` đọc response từ MinIO và bắt buộc size cùng
SHA-256 khớp PostgreSQL. Chỉ worker sở hữu lease được phép chuyển file sang
`COMMITTED`.

Phase 3 đã triển khai control schema, run/file repository, deterministic logical
identity, claim, retry, lease recovery, commit ownership và integrity reader.

## 11. Retry và idempotency

HTTP GET retry các status `408`, `429`, `500`, `502`, `503`, `504`, dùng
exponential backoff, jitter và `Retry-After`. Số attempt bị giới hạn.

Idempotency có hai mức:

- collector: một logical run chỉ có tối đa một successful attempt;
- loader Phase 4: cùng source file phải sinh deterministic `bronze_row_id` và
  `MERGE`, nên crash sau Bronze commit nhưng trước checkpoint commit không tạo
  duplicate.

PostgreSQL và DuckLake Bronze catalog không có distributed transaction. Recovery
dựa vào lease, deterministic key và replay từ response JSON.

## 12. Phase 4: parse hourly vào Bronze

Loader đã triển khai:

1. claim file từ PostgreSQL;
2. xác minh checksum khi đọc MinIO;
3. map response array với ordered `ward_keys`;
4. validate `hourly.time` và độ dài mọi parallel array;
5. tạo Arrow table bằng explicit schema;
6. `MERGE` vào `bronze_store.tables.open_meteo_forecast_hourly`;
7. cập nhật file thành `COMMITTED` cùng row metrics và parser version.

Khóa idempotent:

```text
bronze_row_id = sha256(
    attempt_id || file_id || request_location_index || hourly_index
)
```

Giữ `attempt_id` trong khóa để không overwrite forecast vintage trước đó.
Parser hiện tại là `0.2.0`; schema Arrow có 38 cột typed. `location_id` của
multi-location response được lưu thành `source_location_id` và kiểm tra với
response order. Array lệch độ
dài, field mới hoặc giá trị sai type được giữ trong `_rescued_data`. JSON root,
location count hoặc `hourly.time` vi phạm contract làm file `FAILED`.

## 13. Recovery matrix

| Điểm lỗi | PostgreSQL | MinIO | Recovery |
|---|---|---|---|
| Trước gọi API | run/file `RUNNING/PENDING` | chưa có file | attempt `FAILED`, rerun job |
| Sau API, trước write | file `PENDING` | chưa có file | attempt `FAILED`, rerun job |
| Sau write, validation lỗi | run/file `FAILED` | giữ response lỗi | không load; điều tra/rerun |
| Run đủ batch | run `SUCCEEDED`, file `PENDING` | đủ response | loader được phép claim |
| Loader chết | file `PROCESSING` đến hết lease | không đổi | lease recovery rồi retry |
| Bronze commit, checkpoint lỗi | file chưa `COMMITTED` | không đổi | deterministic `MERGE` lại |

Object ghi được nhưng PostgreSQL update thất bại có thể thành orphan object. Với
single-node MVP, object này không được loader nhìn thấy và được xử lý bằng báo
cáo/lifecycle cleanup định kỳ; không xây distributed transaction.

## 14. Cost và vận hành

- Open-Meteo Free API, non-commercial, cần attribution CC BY 4.0.
- Batch 25 cho 126 location tạo sáu HTTP request mỗi forecast run.
- Concurrency giữ ở 1 cho workload single-node.
- Guardrail nội bộ: tối đa 1.000 effective API calls/ngày.
- Scheduler reserve worst-case HTTP retries trước khi cho phép một run bắt đầu.

Lệnh:

```bash
make ingest-weather-plan     # read-only
make ingest-weather-canary   # 1 location, scope canary_1
make load-weather-canary     # load file canary đang PENDING vào Bronze
make ingest-weather          # chỉ collect 126 locations
make load-weather            # load production files available-now
make run-weather-plan        # plan slot UTC gần nhất, không ghi dữ liệu
make run-weather             # collect 126 locations rồi drain loader
make weather-status          # metrics từ PostgreSQL control plane
make weather-healthcheck     # exit != 0 nếu không HEALTHY
```

## 15. Kế hoạch phase

| Phase | Phạm vi | Trạng thái |
|---|---|---|
| 1 | Typed config, HTTP policy, request/location contracts | ✅ |
| 2 | Forecast collector, exact immutable response; PostgreSQL state | ✅ Refactored |
| 3 | Native control schema, logical/attempt identity, claim/lease/checkpoint, integrity read | ✅ |
| 4 | Hourly parser, Arrow schema, rescued data, Bronze `MERGE` | ✅ |
| 5 | Full 126 run, recovery drill, schedule, metrics và runbook | ✅ |

Canary Phase 2–4 ngày 21/08/2026:

```text
attempt_id=a983684f-7780-4993-925a-9610020aa615
scope=canary_1
run_status=SUCCEEDED
file_status=COMMITTED
locations=1/1
objects=response_000.json
verified_bytes=2698
checksum=PASS
rows_parsed=72
rows_inserted=72
rescued_rows=0
distinct_bronze_row_id=72
valid_time_utc=2026-08-21T10:00:00Z..2026-08-24T09:00:00Z
```

Chạy lại loader canary trả `claimed=0`, chứng minh checkpoint bỏ qua file
`COMMITTED`. Canary cũng xác minh production discovery không claim nhầm
`canary_1`.

Production Phase 5 ngày 21/08/2026:

```text
logical_schedule=2026-08-21T11:15:00Z
attempt_id=655d49bf-523a-4628-b29c-e69f44fde694
attempt_number=2
run_status=SUCCEEDED
locations=126/126
source_files=6 COMMITTED
source_bytes=342539
rows_parsed=9072
rows_inserted=9072
rescued_rows=0
distinct_bronze_row_id=9072
distinct_ward_key=126
valid_time_utc=2026-08-21T11:00:00Z..2026-08-24T10:00:00Z
parser_version=0.2.0
health=HEALTHY
```

Rerun cùng logical slot trả `collection=ALREADY_SUCCEEDED`, `load_batches=0`,
`inserted=0`. Attempt 1 của slot được giữ `FAILED/OperationalRollback` để audit:
full run đầu đã phát hiện implicit PostgreSQL transaction làm rollback file
checkpoint. Code đã được sửa để mọi control-plane read có transaction boundary;
9.072 Bronze rows và sáu orphan objects của attempt lỗi được loại chính xác trước
khi attempt 2 chạy lại.

## 16. Phase 5: vận hành

Entrypoint `run-open-meteo-pipeline`:

1. chuẩn hóa thời gian hiện tại về hourly slot `HH:15 UTC`;
2. collect immutable responses hoặc nhận biết slot đã `SUCCEEDED`;
3. drain available-now loader đến khi checkpoint rỗng;
4. in summary và health; loader failure trả exit code khác 0.

Collector `RUNNING` quá 1.800 giây được đóng `FAILED` trước khi attempt mới bắt
đầu. File lease hết hạn được chuyển `FAILED` và reclaim ở lần load kế tiếp.
Cron template dùng `flock` để ngăn hai process single-node chạy chồng nhau.
Metrics được tính trực tiếp từ hai control table, không tạo metrics database mới.

Runbook chi tiết: [04b-ingestion-runbook.md](04b-ingestion-runbook.md).

## 17. Definition of Done Phase 1–5

- MinIO collector mới chỉ ghi response source object.
- PostgreSQL là nguồn duy nhất cho run và file metadata.
- Không tạo `_manifest.json`, `_SUCCESS`, `_FAILED.json` hoặc request object.
- Logical run tách khỏi execution attempt và canary tách khỏi production.
- Partial run không được loader claim.
- File claim không trùng giữa worker và có lease recovery.
- Size/SHA-256 được xác minh trước parse.
- Explicit Arrow schema parse đúng 72 hourly rows của canary.
- Bronze `MERGE` retry cùng row ID không tạo duplicate.
- Checkpoint chỉ chuyển `COMMITTED` sau Bronze transaction.
- Collector không chứa parsing hourly hoặc KPI.
- Schedule slot deterministic; rerun không tạo logical run mới.
- Daily API budget có guardrail trước khi gọi nguồn.
- Collector timeout và file lease recovery có integration test.
- Health report có freshness, backlog, retry và row metrics 24 giờ.
- Full 126-location production run và rerun idempotency đã pass.
- Unit, PostgreSQL repository và DuckLake production validation đều pass.

## Tham khảo

- [Open-Meteo Weather Forecast API](https://open-meteo.com/en/docs)
- [Open-Meteo pricing](https://open-meteo.com/en/pricing)
- [Requests Session](https://requests.readthedocs.io/en/latest/user/advanced/)
- [PostgreSQL explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html)
- [PostgreSQL `SKIP LOCKED`](https://www.postgresql.org/docs/current/sql-select.html)
- [Databricks Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [DuckLake `MERGE INTO`](https://ducklake.select/docs/stable/duckdb/usage/upserting)
