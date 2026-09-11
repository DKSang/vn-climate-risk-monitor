# Runbook — Ingestion

`fetch.land` xử lý HTTP→MinIO; `autoloader` nạp file→bảng; cấu hình nguồn nằm ở
`sources/`.

---

## Kiến trúc hai bước, cố ý tách rời

```
0. map-grid  probe API 1 lần/model → transform/seeds/ward_grid_map_seed.csv
             126 phường → 12 ô era5 / 48 ô ecmwf_ifs
1. fetch     archive_tasks() → [FetchTask(url, key, units)]  (theo Ô, bỏ tháng đã đủ)
             pool N luồng chạy song song (không pacing chủ động — xem "Nhịp và
             song song" bên dưới)
               land(): GET rồi ghi JSON lên MinIO
                bronze/files/open_meteo/<dataset>/...
2. load      autoloader liệt kê MinIO, nạp file MỚI vào staging bằng SQL
                catalog1.silver.stg_weather_*
```

`land()` nằm trong package `fetch`; hàng đợi song song ở `fetch/pool.py`. Planner Open-Meteo (URL, định tuyến model, skip tháng) nằm trong `open_meteo.py`; bản đồ ô lưới ở `grid.py`. Object singleton được bọc thành array để khớp `read_json_auto` với file cũ.

### Archive: fetch theo ô lưới, hai model theo thời kỳ

| thời kỳ | model | ô Hà Nội | nguồn / dataset | bảng staging |
|---|---|---|---|---|
| trước 2017 | `era5` (0,25°) | 12 | `open_meteo_archive` | `catalog1.silver.stg_weather_archive_hourly` |
| từ 2017-01 | `ecmwf_ifs` (~9km) | 48 | `open_meteo_ifs` | `catalog1.silver.stg_weather_archive_hourly` |

Giữ ERA5 cho lịch sử trước 2017 và dùng `ecmwf_ifs` từ 2017. Archive fetch theo
ô lưới rồi chiếu về phường qua `bridge_ward_grid`; không fetch lặp theo 126
phường. `ecmwf_ifs` không phải reanalysis, nên không so trực tiếp baseline hai
phía mốc chuyển model.

Forecast vẫn fetch **theo phường**, cố ý: lưới `best_match` mịn hơn (48 ô/126 phường) và mesh của nó đổi khi Open-Meteo chuyển model nền, nên danh sách ô cache cứng sẽ mục.

Tách ra vì: lỗi mạng ở bước 1 không làm mất dữ liệu đã tải; bước 2 checkpoint
theo object key — file đã `COMMITTED` không nạp lại. Crash sau INSERT trước
checkpoint: lease hết hạn, INSERT lặp (at-least-once); Silver dedup.

Bước 2 **không quan tâm ai ghi file** — nó dùng directory listing. File do bước 1
ghi dở rồi tiến trình chết vẫn được nhặt ở lần chạy sau.

## Bootstrap dữ liệu địa lý

`docker compose up -d --build` đã seed và build geography trong bước bootstrap.
Khi seed tĩnh đổi, chạy lại phần geography trong runtime container:

```bash
docker compose exec -T airflow uv run python scripts/run_dbt.py seed \
  --project-dir transform --profiles-dir transform
docker compose exec -T airflow uv run python scripts/run_dbt.py run \
  --project-dir transform --profiles-dir transform \
  --select stg_seed__ward stg_seed__ward_grid stg_seed__flood_point \
  stg_seed__flood_observation dim_ward dim_flood_point fct_flood_event_observation
```

Target này seed dữ liệu geography và build các model tĩnh cần cho Gold, không
đụng các fact thời tiết chưa có raw source object.

## Lệnh hằng ngày

```bash
docker compose exec airflow airflow dags unpause open_meteo_forecast_hourly
docker compose exec airflow airflow dags trigger open_meteo_forecast_hourly
```

DAG chạy cố định `fetch forecast -> load open_meteo_forecast -> quality Silver
staging -> dbt build -> health`. Mỗi task phải thành công trước khi task sau chạy;
Provero fail sẽ chặn transform.

Các lệnh thành phần vẫn dùng được khi xử lý sự cố. Với fetch riêng, bỏ
`--execute` thì chỉ in kế hoạch, không gọi API.

## Backfill lịch sử

Repo đã có seed ánh xạ ô lưới. Chỉ map lại khi danh sách phường hoặc model archive
đổi.

```bash
docker compose exec -T airflow uv run fetch-open-meteo map-grid --execute
docker compose exec -T airflow uv run python scripts/run_dbt.py seed \
  --project-dir transform --profiles-dir transform
docker compose exec -T airflow uv run load-sources open_meteo_archive open_meteo_ifs
```

Fetch archive theo khoảng explicit. Bỏ `--execute` để xem kế hoạch. Planner tự bỏ
qua tháng đã đủ giờ trong raw landing, nên retry cùng khoảng là an toàn:

```bash
docker compose exec -T airflow uv run fetch-open-meteo archive \
  --start 2018-01-01 --end 2018-12-01 --execute
```

Resumable ở hai tầng: tháng đã đủ giờ trong raw landing thì bỏ hẳn; trong một tháng,
file `response_NNN.json` đã có thì bỏ từng file. Mỗi lần chạy ghi vào run dir
riêng nên không bao giờ ghi đè object cũ.

### Nhịp và song song

`fetch.pool` chạy `OPEN_METEO_FETCH_WORKERS` (mặc định 4) luồng song song và không
có pacing chủ động. Quota được xử lý bằng retry phản ứng trong `land()`; khi
backfill lớn, hạ số worker hoặc chia nhỏ `--start`/`--end`.

Ràng buộc quota giờ hoàn toàn dựa vào retry **phản ứng** trong `land()`
([fetch/__init__.py](../src/fetch/__init__.py)): gặp 429/500/502/503/504 thì
sleep 60s rồi thử lại, tối đa 5 lần; hết vẫn lỗi thì cả lô dừng (chính sách của
`fetch.pool`: một task hỏng là dừng, không đốt thêm request).

Hệ quả vận hành cần biết: với backfill nhiều tháng chạy `--execute` một lèo,
**429 có thể xảy ra thật** khi tổng request vượt trần Open-Meteo trong cùng cửa
sổ giờ/ngày — không có gì chủ động tránh trước. Giảm `OPEN_METEO_FETCH_WORKERS`
xuống 1 để gần với nhịp tuần tự cũ, hoặc chia nhỏ `--start`/`--end` từng đợt và
chờ giữa các đợt.

Kiểm tra tiến độ:

```bash
docker compose exec -T minio sh -lc '
mc alias set m http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$(cat /run/secrets/minio_secret_key)" >/dev/null
mc ls -r m/vn-climate/bronze/files/open_meteo/historical_weather_hourly/' \
  | grep -o 'year=[0-9]*/month=[0-9]*' | sort -u | wc -l
```

## Đặt lịch

Airflow DAG `open_meteo_archive_monthly` chạy `30 2 1 * *`, `catchup=False` và
`max_active_runs=1`. Mỗi run chỉ fetch tháng đã hoàn tất ngay trước
`data_interval_start`, sau đó load, quality, transform và health gate.

DAG mới bị pause mặc định. Bật một lần sau khi kiểm tra stack:

```bash
docker compose exec airflow airflow dags unpause open_meteo_archive_monthly
```

Backfill lịch sử vẫn chạy tay với khoảng `--start/--end`; planner bỏ qua tháng đã
đủ nên có thể retry cùng khoảng an toàn.

## Backup và restore metadata PostgreSQL

PostgreSQL chứa metadata DuckLake, ingestion checkpoint và dữ liệu tham chiếu.
Script mặc định backup toàn database bằng custom archive; có thể giới hạn bằng
`POSTGRES_SCHEMAS="ducklake ingestion processing"`. Đây là thao tác operator trên
host: cần Bash, PostgreSQL client (`pg_dump`, `pg_restore`) và credential riêng.
Default secret sinh trong Docker không được copy ra host tự động.

```bash
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5432
export POSTGRES_DB=vnclimate
export POSTGRES_USER=vnclimate
export PGPASSFILE=/secure/pgpass
BACKUP_DIR=/secure/backups bash scripts/backup_metadata.sh
```

Backup được ghi qua file tạm với `umask 077`, kiểm tra bằng `pg_restore --list`,
rồi mới rename atomically và tạo sidecar `.sha256`. Mặc định không ghi đè file
đã có; chỉ đặt `BACKUP_OVERWRITE=1` khi thực sự muốn thay thế một đường dẫn cố
định.

Restore sẽ `--clean` object hiện có trong một transaction. Dừng cron và mọi
writer dbt/autoloader trước, giữ PostgreSQL đang chạy, rồi xác nhận bằng đúng tên
database đích:

```bash
BACKUP_FILE=/secure/backups/vnclimate_metadata_20260830T010203Z.dump \
RESTORE_CONFIRM=vnclimate \
bash scripts/restore_metadata.sh
```

Sidecar SHA-256 là bắt buộc mặc định. Chỉ dùng `RESTORE_VERIFY_CHECKSUM=0` cho
archive tin cậy được tạo ngoài script này. Script ưu tiên
`POSTGRES_PASSWORD_FILE`; cũng hỗ trợ `PGPASSFILE` hoặc `POSTGRES_PASSWORD` khi
chạy ngoài container. Không đưa mật khẩu vào tên file, command line hay artifact.

Metadata backup không chứa object MinIO. Để có disaster recovery đầy đủ, cấu
hình MinIO Client alias, dừng mọi writer rồi dùng:

```bash
LAKEHOUSE_BACKUP_ROOT=/mnt/backup BACKUP_QUIESCED=1 \
  bash scripts/backup_lakehouse.sh
```

Thư mục đích phải nằm trên filesystem/volume độc lập với volume MinIO hiện hành.
Backup chỉ được publish sau khi tạo manifest, checksum từng object MinIO và
kiểm tra PostgreSQL custom archive. Kiểm tra artifact bất kỳ lúc nào mà không
chạm hệ thống đích:

```bash
LAKEHOUSE_BACKUP_DIR=/mnt/backup/lakehouse_<timestamp> \
  bash scripts/verify_lakehouse_backup.sh
```

Restore tự chạy lại verifier trước khi ghi và yêu cầu xác nhận cả trạng thái
quiesced lẫn đúng tên bucket:

```bash
LAKEHOUSE_BACKUP_DIR=/mnt/backup/lakehouse_<timestamp> \
RESTORE_QUIESCED=1 RESTORE_LAKEHOUSE_CONFIRM=vn-climate \
  bash scripts/restore_lakehouse.sh
```

## Xử lý sự cố

### Hết hạn mức Open-Meteo (429)

Không có pacing chủ động (xem "Nhịp và song song" ở trên) nên 429 là chuyện có
thể gặp thật, đặc biệt khi backfill nhiều tháng liền với `OPEN_METEO_FETCH_WORKERS`
cao. `land()` tự retry 5 lần, cooldown 60s giữa mỗi lần. Sống sót qua cả 5 lần
thì **không ghi** payload lỗi và cả lô dừng với exit code 2 (chính sách
`fetch.pool`: một task hỏng là dừng cả lô, tránh đốt thêm request vào một hạn
mức đã cạn). Không mất dữ liệu: file đã land tự bị bỏ qua khi chạy lại đúng lệnh
cũ.

Gặp 429 dồn dập thì hạ `OPEN_METEO_FETCH_WORKERS` (về 1 là tuần tự hoàn toàn)
hoặc chia nhỏ `--start`/`--end` và chờ giữa các đợt — không có biến môi trường
nào tự làm việc này thay bạn.

Lưu ý: chi phí "~N đơn vị" mà dry-run in ra là **ước lượng theo công thức xấp xỉ**
của Open-Meteo, có thể lệch với cách API tính thật.

### Một file JSON hỏng

Không chặn file khác. Engine chạy lại từng file để cô lập thủ phạm; file lành
vào bảng bình thường, file hỏng bị đánh `FAILED` và retry tối đa `max_retries`
(mặc định 3) rồi bị loại khỏi hàng đợi.

Xem file nào hỏng:

```sql
SELECT f.object_key, f.retry_count, f.error_type, f.error_message
FROM ingestion.ingestion_files f
JOIN ingestion.ingestion_runs r ON r.attempt_id = f.attempt_id
WHERE f.status = 'FAILED';
```

Sửa xong file trên MinIO thì reset để nạp lại:

```sql
UPDATE ingestion.ingestion_files
SET status = 'PENDING', retry_count = 0, error_type = NULL, error_message = NULL
WHERE object_key = '<object_key>';
```

### Nạp lại toàn bộ một nguồn

Xoá checkpoint của nguồn đó rồi làm rỗng bảng đích. **Không xoá file trên MinIO** —
chúng là bản gốc.

```sql
DELETE FROM ingestion.ingestion_files WHERE attempt_id IN (
  SELECT attempt_id FROM ingestion.ingestion_runs WHERE dataset = 'forecast');
DELETE FROM ingestion.ingestion_runs WHERE dataset = 'forecast';
```

### Lease treo ở PROCESSING

Tiến trình chết giữa chừng để lại file ở `PROCESSING`. `claim_files` tự thu hồi
khi `lease_expires_at_utc` quá hạn (mặc định 300s) — chỉ cần chờ rồi chạy lại
autoloader:

```bash
docker compose exec -T airflow uv run load-sources
```

### Processing run `RUNNING` mồ côi

Đây là checkpoint của dbt, khác với lease file ở trên. SIGTERM bình thường được
runner bắt và đổi run thành `FAILED`; mất điện, Docker daemon bị kill hoặc
SIGKILL vẫn có thể để lại một row `RUNNING`. Unique constraint cố ý chặn run mới
để hai writer không chạy đồng thời.

Chỉ sau khi đã xác minh process/container cũ không còn chạy, xem audit rồi đóng
run mồ côi với lý do cụ thể:

```bash
docker compose exec -T airflow uv run python scripts/run_processing.py status forecast_silver
docker compose exec -T airflow uv run python scripts/run_processing.py abandon forecast_silver \
  --reason 'Docker host restart; verified previous worker no longer exists'
```

Checkpoint thành công gần nhất không đổi, nên retry đọc lại cùng cửa sổ an toàn.
Không `abandon` chỉ vì một build chạy lâu. Lặp lại cho đúng `process_key` đang bị
khóa (`forecast_gold`, `silver_weather` hoặc `rain_gold`) nếu cần.

### Hết dung lượng MinIO

```bash
docker compose exec -T airflow uv run python scripts/maintain_lake.py \
  --snapshot-retention-days 7 --file-grace-days 2
docker compose exec -T airflow uv run python scripts/clean_lake.py
```

`maintain-lake` là đường production. `clean-lake` mất time-travel và chỉ
dùng khi thiếu disk nghiêm trọng.

## Thêm nguồn mới

**File đã nằm trên MinIO** — không viết Python. Thêm hai file vào `sources/`:

```
my_source.yml    khai báo discovery prefix, bảng đích (loader knobs chỉ khi lệch default)
my_source.sql    SELECT ... FROM read_json_auto({{ files }})
```

**REST API** — planner trong `vn_climate_risk_monitor/<tên>.py`, copy dùng `fetch.land`.

`{{ files }}` được engine thay bằng danh sách file đã claim. Không cần DDL tay:
lần `load-sources` đầu tiên tự `CREATE TABLE IF NOT EXISTS <target> AS (<sql>) WHERE
false`, nên schema bảng luôn khớp SELECT. Bảng đã có thì engine không đụng vào —
muốn partition/constraint riêng thì cứ tạo tay trước.

## Quyết định kiến trúc hiện tại

- MinIO chịu trách nhiệm integrity của object; discovery lưu `etag` và
  `size_bytes`, không tải lại file chỉ để tính checksum.
- Archive fetch theo ô lưới. ERA5 dùng trước 2017, `ecmwf_ifs` dùng từ 2017;
  `weather_model` nằm trong grain/join key phía sau.
- Fetch chạy song song theo `OPEN_METEO_FETCH_WORKERS`; 429 được retry trong
  `land()`. Backfill lớn nên giảm worker hoặc chia nhỏ khoảng thời gian.
- Autoloader tự tạo bảng đích từ SQL nguồn khi bảng chưa tồn tại. Bootstrap chỉ
  tạo schema/control plane.
- Staging giữ dữ liệu append-only. Dedup và MERGE change-aware nằm ở Silver
  intermediate; forecast giữ vintage theo `forecast_run_id`.
- Discovery dùng một logical run ổn định cho mỗi source; lease `PROCESSING` xử lý
  crash recovery.
