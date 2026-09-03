# Processing checkpoint framework

**Ngày:** 2026-09-03
**Thay thế:** `ingestion.gold_watermarks` + `scripts/gold_watermark.py` (thiết kế 2026-08-31)
**Phạm vi:** bước 1–4 của review. Bước 5 (strategy plugin registry) CHƯA làm — xem "Không làm".

---

## 1. Vấn đề đang sửa

### 1.1 Bug mất dữ liệu trong watermark hiện tại

`scripts/gold_watermark.py::cmd_advance` chạy **sau** `dbt build` và lấy:

```sql
SELECT MAX(_ingested_at) FROM silver.archive_hourly
```

Race:

```
11:02  dbt đọc Bronze
11:05  autoloader COMMIT một file       ← _ingested_at = 11:05
11:08  advance đọc MAX() = 11:05        ← checkpoint nhảy qua rows chưa xử lý
```

Rows lúc 11:05 không bao giờ vào Gold. Không có tín hiệu phát hiện.

`make ingest-*` và `make transform-archive` là hai target độc lập, không có lock — chồng nhau là chuyện bình thường, không phải edge case.

### 1.2 Start-time checkpoint một mình KHÔNG đủ

Chuyển sang `checkpoint = run_started_at` sửa được 1.1 nhưng còn một lỗ thứ hai.

`_ingested_at` trong `sources/*.sql` là `CURRENT_TIMESTAMP` của DuckDB — tức **transaction start time**, không phải commit time:

```
10:59:58  batch INSERT bắt đầu     → rows mang _ingested_at = 10:59:58
11:00:00  processing run bắt đầu   → đọc Bronze, KHÔNG thấy rows đó (chưa commit)
11:00:40  batch commit             → rows visible
11:00:00  run SUCCESS → checkpoint = 11:00:00
          lần sau đọc > 11:00:00 → 10:59:58 không lọt. MẤT.
```

Đây là khoảng cách giữa **thứ tự timestamp** và **thứ tự visibility**. Không có upper bound nào sửa được nó; `upper_bound = run_start` (đề xuất §13 của plan gốc) còn làm tệ hơn vì nó loại bỏ đúng phần overlap có thể cứu.

**Fix:** `lower_bound = last_successful_start_at − safety_lag`, không có upper bound. MERGE idempotent nên overlap chỉ tốn scan.

### 1.3 Hai đồng hồ khác nhau

`_ingested_at` do DuckDB trên máy worker sinh; `run_started_at` sẽ do orchestrator sinh. Skew vài giây = mất row, và không tái hiện được.

**Fix:** cả hai lấy từ **Postgres control plane** (`SELECT CURRENT_TIMESTAMP`). Engine inject `{{ ingested_at }}` vào source SQL, giống cách đã inject `{{ files }}`.

### 1.4 `INTERVAL '71 hours'` hardcode 3 lần

`fct_rainfall_historical_hourly.sql` có rolling window tới 72h. Một row mới ở giờ T làm sai 72 rows **đã tồn tại**. Model tự xử lý bằng cách nới scope ±71h — viết tay, lặp 3 lần, không test được.

---

## 2. Thiết kế

### 2.1 Ranh giới: framework KHÔNG thay dbt

```
processing framework SỞ HỮU      dbt SỞ HỮU
─────────────────────────────    ──────────────────────
processing_state (checkpoint)    DAG / ref()
processing_runs  (audit)         transform SQL
tính lower bound + safety lag    MERGE (incremental materialization)
inject vars vào dbt              data tests
advance CHỈ khi exit 0
reprocess_from() có audit
```

Đây chính là cái `Makefile:135-137` đang làm bằng shell, nâng lên thành Python có audit, có scope, có transaction.

### 2.2 Bảng control

```
processing.processing_state          -- 1 row / (process, source, scope)
    process_key, source_ref, scope   PK
    last_successful_start_at
    last_successful_run_id

processing.processing_runs           -- audit đầy đủ
    processing_run_id
    process_key, scope, target_ref
    started_at_utc, completed_at_utc
    status  RUNNING | SUCCEEDED | FAILED | REWIND
    bounds  JSONB   {source_ref: {checkpoint_before, lower_bound}}
    checkpoint_candidate             = started_at_utc
    actor, reason                    -- ai rewind và vì sao
    error_type, error_message
```

Key là `(process_key, source_ref, scope)` chứ không chỉ target:

- `source_ref` — một process đọc nhiều source thì rewind được từng source riêng.
- `scope` — `gold_watermarks` hiện chỉ có `pipeline_name`, dev và production **dùng chung một row**.

Partial unique index chặn hai run RUNNING cùng `(process_key, scope)`. Run chết để lại RUNNING → `abandon` có audit, không tự hết hạn (dbt build dài là hợp lệ).

### 2.3 State machine

```
run_started_at = control_now()          ← Postgres clock, TRƯỚC khi đọc gì
        ↓
checkpoint_before = read state
lower_bound = checkpoint_before − safety_lag   (None = full refresh)
        ↓
begin_run  → RUNNING
        ↓
execute(bounds)          ← dbt build --vars {...}
        ↓
   ┌────┴────┐
 FAIL      SUCCESS
   ↓          ↓
 state      state = run_started_at
 KHÔNG đổi  (KHÔNG phải MAX(), KHÔNG phải end time)
```

### 2.4 Recompute scope — khai báo ở model, không ở YAML

Sửa lại bước 4 của review: `expand_backward` **không** thuộc process config. Một process (`+tag:historical`) build nhiều gold model có window khác nhau (hourly 72h, daily khác, climatology khác). Một giá trị cấp process không phục vụ được.

Nơi khai báo đúng là chính model — versioned cùng SQL:

```sql
{{ incremental_input_scope(
    relation       = ref('archive_hourly'),
    source_ref     = 'archive_hourly',
    dimension      = 'valid_time_utc',
    expand_backward = '71 hours',
    expand_forward  = '71 hours',
    keys           = ['grid_cell_id']
) }}
```

Framework chỉ cấp `lower_bound` qua var `processing_bounds`.

Hai macro vì input và output scope KHÔNG đối xứng:

| | lower | upper |
|---|---|---|
| **input** (rows cần đọc để tính window) | `MIN(changed) − backward` | `MAX(changed) + forward` |
| **output** (rows thực sự bị ảnh hưởng) | `MIN(changed)` | `MAX(changed) + forward` |

Row mới ở T ảnh hưởng T..T+72h; để tính chúng phải đọc từ T−71h. Đây đúng là logic model đang viết tay.

---

## 3. Thay đổi theo file

### Mới

| File | Nội dung |
|---|---|
| `src/processing/schema.py` | DDL `processing_state` + `processing_runs` |
| `src/processing/state.py` | `ProcessingRepository` — checkpoint + run audit |
| `src/processing/config.py` | `ProcessConfig.from_yaml`, `parse_duration` |
| `src/processing/runner.py` | `compute_bounds`, `run_process` (state machine) |
| `processing/rainfall_historical_hourly.yml` | Process config đầu tiên |
| `transform/macros/incremental_scope.sql` | `incremental_input_scope` / `incremental_output_scope` |
| `scripts/run_processing.py` | CLI: `run` / `status` / `reprocess-from` / `abandon` / `migrate` |
| `tests/unit/test_processing_runner.py` | State machine + bounds (13 test) |
| `tests/unit/test_processing_config.py` | YAML + duration parser |
| `tests/unit/test_processing_dbt.py` | Bounds → `--vars`, exit code |
| `tests/unit/test_incremental_scope_macro.py` | Render macro bằng Jinja thuần, không cần DB |

### Sửa

| File | Thay đổi |
|---|---|
| `src/autoloader/checkpoint.py` | `+ control_now()` — Postgres clock |
| `src/autoloader/engine.py` | Inject `{{ ingested_at }}` |
| `sources/*.sql` (3 file) | `CURRENT_TIMESTAMP` → `{{ ingested_at }}` |
| `transform/macros/materializations.sql` | `gold_historical_watermark` → `processing_incremental` |
| `transform/models/gold/fct_rainfall_historical_hourly.sql` | 71h hardcode → macro |
| `Makefile` | `transform` / `transform-archive` gọi runner |
| `scripts/bootstrap.py` | Bootstrap thêm schema `processing` |
| `pyproject.toml` | `module-name += processing` |

### Xóa

| File | Lý do |
|---|---|
| `scripts/gold_watermark.py` | Thay bằng `scripts/run_processing.py` |
| `tests/unit/test_gold_watermark.py` | Thay bằng `test_processing_runner.py` |
| `read_gold_watermark` / `advance_gold_watermark` | API mang bug 1.1 |

`ingestion.gold_watermarks` **giữ nguyên DDL** để `migrate` đọc được; không ai ghi vào nữa.

---

## 4. Migration

```bash
uv run python scripts/run_processing.py migrate rainfall_historical_hourly
```

Copy `gold_watermarks.last_successful_ingestion_watermark` → `processing_state`.

⚠️ Giá trị cũ là `MAX(_ingested_at)` nên có thể **đi trước** thứ đã thực sự xử lý (bug 1.1). `migrate` in cảnh báo và gợi ý chạy một lần:

```bash
uv run python scripts/run_processing.py reprocess-from rainfall_historical_hourly \
    --from 2026-08-25T00:00:00Z --reason "close gap from MAX() watermark bug"
```

---

## 5. Không làm (cố ý)

- **Strategy plugin registry** (`CDC`, `SnapshotDiff`). Không source nào cần. Chỉ có `FileIncremental` (autoloader, đã chạy) + `PlatformTimestampIncremental` (bản này). Generic hoá sau khi chạy ổn trên ≥2 process.
- **Materialize Silver thành table.** Cả 7 model Silver là view, nên `_updated_at` cấp Silver không tồn tại. Chain thực tế là Bronze `_ingested_at` → Gold, MỘT checkpoint layer. Không viết contract như thể có hai.
- **Rename `_ingested_at` → `_inserted_at`.** Cosmetic, chạm 3 bronze SQL + 7 silver + hầu hết gold + dbt tests.
- **Hard delete detection.** Timestamp-based không bao giờ thấy delete. Source hiện tại append-only.

---

## 6. Verify

```bash
uv run pytest tests/unit/ -q
uv run ruff check .
uv run python scripts/run_processing.py status rainfall_historical_hourly
make transform-archive
uv run python scripts/run_processing.py status rainfall_historical_hourly   # checkpoint = run_started_at
```

Kiểm tra bằng mắt: `checkpoint` sau run phải bằng `started_at_utc` của run đó, KHÔNG bằng `completed_at_utc`, KHÔNG bằng `MAX(_ingested_at)`.
