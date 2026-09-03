# Runbook — Ingestion

**Cập nhật 2026-08-28** — `fetch.land` (HTTP→MinIO); `autoloader` (file→bảng); cấu hình nguồn ở `sources/`.

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
2. load      autoloader liệt kê MinIO, nạp file MỚI vào bronze bằng SQL
                bronze_store.tables.open_meteo_*
```

`land()` nằm trong package ``fetch``; hàng đợi song song ở `fetch/pool.py`. Planner Open-Meteo (URL, định tuyến model, skip tháng) nằm trong ``open_meteo.py``; bản đồ ô lưới ở ``grid.py``. Object singleton được bọc thành array để khớp `read_json_auto` với file cũ.

### Archive: fetch theo ô lưới, hai model theo thời kỳ

| thời kỳ | model | ô Hà Nội | bảng bronze |
|---|---|---|---|
| trước 2017 | `era5` (0,25°) | 12 | `open_meteo_archive` |
| từ 2017-01 | `ecmwf_ifs` (~9km) | 48 | `open_meteo_ifs` |

**Giữ cả hai.** IFS không có dữ liệu trước 2017 (probe 2026-08-28: 2016 mọi quý NULL) — bỏ ERA5 là mất 17 năm baseline. Fetch theo ô: 126 phường chỉ rơi vào 12 ô ERA5 và **mọi bản sao trong cùng ô giống hệt nhau** (0 cặp (ô, giờ) nào lệch), nên fetch theo phường tiêu quota gấp ~10 lần mà không thêm thông tin. Silver chiếu ngược về phường qua `ward_grid_map`.

`ecmwf_ifs` cho tín hiệu khác nhau THẬT giữa các phường: cùng ngày mưa, ba phường mà ERA5 gộp thành một chuỗi 9,3mm thì IFS trả 105,0 / 137,9 / 116,2 mm. Khoảng cách phường→tâm ô giảm từ 18,9km (era5) xuống 5,5km.

**Không dùng** `era5_land` (không có biến mưa nào — đã probe) và `era5_seamless` (toạ độ mịn 0,1° nhưng giá trị mưa vẫn là ERA5 0,25° dán lại, làm trùng lặp bị GIẤU thay vì lộ ra và hỏng dedup theo ô).

`ecmwf_ifs` là chuỗi phân tích nghiệp vụ, **không phải reanalysis** — đồng nhất theo thời gian kém hơn era5. Đừng so trực tiếp trung bình trước/sau mốc 2017.

Forecast vẫn fetch **theo phường**, cố ý: lưới `best_match` mịn hơn (48 ô/126 phường) và mesh của nó đổi khi Open-Meteo chuyển model nền, nên danh sách ô cache cứng sẽ mục.

Tách ra vì: lỗi mạng ở bước 1 không làm mất dữ liệu đã tải; bước 2 checkpoint
theo object key — file đã `COMMITTED` không nạp lại. Crash sau INSERT trước
checkpoint: lease hết hạn, INSERT lặp (at-least-once); Silver dedup.

Bước 2 **không quan tâm ai ghi file** — nó dùng directory listing. File do bước 1
ghi dở rồi tiến trình chết vẫn được nhặt ở lần chạy sau.

## Bootstrap dữ liệu địa lý

Sau `make up && make bootstrap`, bảo đảm nguồn tham chiếu `public.wards` đã có
trong PostgreSQL rồi chạy:

```bash
make bootstrap-geography
```

Target seed `ward_coordinates_seed` và build đúng graph tổ tiên của
`gold.dim_hanoi_ward`. Nó tách khỏi bootstrap hạ tầng để không tạo vòng phụ
thuộc, đồng thời không build các fact thời tiết chưa có Bronze source.

## Lệnh hằng ngày

```bash
make forecast-pipeline
```

Target production chạy cố định `fetch forecast -> load open_meteo_forecast ->
quality Bronze -> dbt build`. Mỗi bước phải thành công trước khi sang bước kế;
đặc biệt Provero trả lỗi sẽ chặn transform. Cron chỉ gọi target này một lần nên
không có quality job chạy trùng.

Các lệnh thành phần vẫn dùng được khi xử lý sự cố. Với fetch riêng, bỏ `EXEC=1`
thì chỉ in kế hoạch, không gọi API.

## Backfill lịch sử

**Chạy `make map-grid EXEC=1` một lần trước** (≈252 đơn vị) để chốt ô lưới, rồi
`make seed`. Bản đồ chỉ cần làm lại khi danh sách phường đổi.

Còn thiếu 2014→nay = 152 tháng. Fetch theo ô nên chỉ **267 request / 13.121 đơn
vị ≈ 1,4 ngày**, thay vì 912 request / ~42.400 đơn vị ≈ 4,2 ngày nếu fetch theo
phường.

```bash
make map-grid EXEC=1 && make seed        # một lần
make fetch-archive EXEC=1                # dò kế hoạch trước khi thêm EXEC=1
make load SOURCE="open_meteo_archive open_meteo_ifs"
```

Bỏ `EXEC=1` để xem kế hoạch. Không cần `--start/--end`: planner tự bỏ qua tháng
đã ĐỦ GIỜ trong bronze, nên chạy lại dải mặc định 2000→nay vẫn ra đúng 267
request. Muốn chia nhỏ thì vẫn dùng `--start/--end` như cũ:

```bash
make fetch-archive EXEC=1 START=2018-01-01 END=2018-12-01
```

Resumable ở hai tầng: tháng đã đủ giờ trong bronze thì bỏ hẳn; trong một tháng,
file `response_NNN.json` đã có thì bỏ từng file. Mỗi lần chạy ghi vào run dir
riêng nên không bao giờ ghi đè object cũ.

### Nhịp và song song

**Không có pacing chủ động.** `fetch.pool` chạy `OPEN_METEO_FETCH_WORKERS`
(mặc định 4) luồng song song, mỗi luồng gọi `land()` ngay khi có việc — không
`sleep` trước, không token bucket, không biết trần giờ/ngày của Open-Meteo. Dự
án từng có một `QuotaLimiter` (token bucket, cửa sổ trượt, hai tầng trần, trạng
thái ghi đĩa) nhưng đã gỡ để đơn giản hoá; nếu cần lại pacing chủ động thì viết
mới, đừng tìm `fetch/pacing.py` — nó không còn trong cây code.

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
docker run --rm --network host --entrypoint sh minio/mc -c "
mc alias set m http://127.0.0.1:9000 minioadmin minioadmin >/dev/null
mc ls -r m/vn-climate/bronze/files/open_meteo/historical_weather_hourly/" \
  | grep -o 'year=[0-9]*/month=[0-9]*' | sort -u | wc -l
```

## Đặt lịch

Mẫu cron ở `orchestration/cron/*.cron.example`. Cả hai job dùng **chung một
`flock`** vì cùng tiêu vào một hạn mức rate limit của Open-Meteo.

Backfill **không đặt lịch** — chạy tay như trên. `scripts/backfill_archive.sh`
(lặp từng năm) vẫn chạy được nhưng không còn cần thiết: planner đã tự bỏ qua
tháng đã đủ, nên một lệnh `make fetch-archive EXEC=1` xử lý cả dải.

## Backup và restore metadata PostgreSQL

PostgreSQL chứa metadata DuckLake, ingestion checkpoint và dữ liệu tham chiếu.
Script mặc định backup toàn database bằng custom archive; có thể giới hạn bằng
`POSTGRES_SCHEMAS="ducklake ducklake_bronze ingestion"`. Cài PostgreSQL client
(`pg_dump`, `pg_restore`) trên máy chạy lệnh và truyền cấu hình bằng môi trường:

```bash
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5432
export POSTGRES_DB=vnclimate
export POSTGRES_USER=vnclimate
export POSTGRES_PASSWORD='…'  # chỉ ở môi trường tiến trình, không vào dump
BACKUP_DIR=/secure/backups make backup-metadata
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
make restore-metadata
```

Sidecar SHA-256 là bắt buộc mặc định. Chỉ dùng `RESTORE_VERIFY_CHECKSUM=0` cho
archive tin cậy được tạo ngoài script này. Có thể dùng `PGPASSFILE` thay cho
`POSTGRES_PASSWORD`; không đưa mật khẩu vào tên file, command line hay artifact.

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
`make load`.

### Hết dung lượng MinIO

```bash
make clean-lake     # squash snapshot, bỏ file Parquet của phiên bản cũ
```

Mất time-travel, giữ bản hiện tại. `on-run-end` của dbt đã tự dọn với chính sách
giữ 7 ngày; lệnh này là dọn mạnh tay.

## Thêm nguồn mới

**File đã nằm trên MinIO** — không viết Python. Thêm hai file vào `sources/`:

```
my_source.yml    khai báo discovery prefix, bảng đích (loader knobs chỉ khi lệch default)
my_source.sql    SELECT ... FROM read_json_auto({{ files }})
```

**REST API** — planner trong `vn_climate_risk_monitor/<tên>.py`, copy dùng `fetch.land`.

`{{ files }}` được engine thay bằng danh sách file đã claim. Không cần DDL tay:
lần `make load` đầu tiên tự `CREATE TABLE IF NOT EXISTS <target> AS (<sql>) WHERE
false`, nên schema bảng luôn khớp SELECT. Bảng đã có thì engine không đụng vào —
muốn partition/constraint riêng thì cứ tạo tay trước.

## ADR — các quyết định đã chốt của kiến trúc hiện tại

**Integrity delegated cho MinIO (2026-08-22, cột collector DROP 2026-08-27).**
Kiến trúc cũ tính SHA-256 lúc ghi và verify lúc đọc. Directory-listing discovery
không tải file về nên không băm được; hợp đồng checksum đã bỏ. Cơ chế bảo toàn
vẹn còn lại: MinIO bitrot protection + `etag`/`size_bytes` trong
`file_parameters` JSONB lúc discovery. Các cột collector trên
`ingestion.ingestion_files` (`sha256`, `http_status`, `rows_parsed`, …) đã DROP.

**Archive fetch theo ô lưới, hai model theo thời kỳ (2026-08-28).** Trước đó
fetch 126 phường cho mọi tháng. Đo trên chính bronze: 126 phường rơi vào 12 ô
ERA5 (seed; nearest-neighbour từng lệch 12 vs 13) và 0 cặp (ô, giờ) nào có
giá trị lệch nhau — tức trả quota gấp ~10 lần cho dữ liệu nhân bản. Nay fetch
theo ô, Silver chiếu ngược qua `ward_grid_map`. **Giữ cả hai model:** IFS
không có dữ liệu trước 2017 (probe 2026-08-28: 2016 mọi quý NULL), nên ERA5
là chuỗi lịch sử sâu duy nhất; từ 2017 dùng `ecmwf_ifs` ~9km cho tín hiệu
khác nhau thật giữa các phường. Backfill còn lại: 267 request / 13.121 đơn vị
thay vì 912 / ~42.400. Đánh đổi: `ecmwf_ifs` không phải reanalysis nên có bước
nhảy ở mốc 2017, và `weather_model` phải nằm trong mọi khoá join phía sau.

**Song song thay pacing chủ động, retry phản ứng gánh quota (2026-08-28).**
Nhịp tuần tự cũ `sleep(3600 × units / per_hour)` chỉ biết trần giờ: chạy đều
4.500/giờ thì cạn ngân sách NGÀY sau ~2,2 giờ rồi 429 hàng loạt và chết. Từng
thay bằng `QuotaLimiter` (token bucket, cửa sổ trượt, trần giờ VÀ ngày, state
ghi đĩa) nhưng đã **gỡ lại** để đơn giản hoá — `fetch.pool` giờ chỉ chạy
`OPEN_METEO_FETCH_WORKERS` luồng song song, không giữ nhịp dưới trần nào cả.
Quota hoàn toàn dựa vào retry phản ứng của `land()` (429 → sleep 60s, tối đa 5
lần) cộng chính sách dừng-cả-lô của pool. Đánh đổi: 429 có thể xảy ra thật khi
backfill nhiều tháng liền; giảm nhẹ bằng cách hạ `OPEN_METEO_FETCH_WORKERS`
hoặc chia nhỏ `--start`/`--end`.

**Bảng đích do engine tạo (2026-08-28).** Trước đó thêm nguồn mới phải chạy DDL
tay, và quên thì `make load` chết ở INSERT vào bảng không tồn tại — `bootstrap.py`
chỉ tạo *schema* `bronze_store.tables`, không tạo table. Nay engine tự
`CREATE TABLE IF NOT EXISTS ... AS (<sql>) WHERE false`, lấy schema từ chính SQL
của nguồn nên không có danh sách cột thứ hai để lệch. Probe `SELECT 1 FROM
<target> WHERE false` chạy trước vì `CREATE TABLE IF NOT EXISTS ... AS SELECT`
vẫn bind câu select kể cả khi bảng đã có, tức bắt `read_json_auto` đọc S3 để suy
schema; probe chỉ hỏi catalog. Cả hai chỉ chạy một lần cho mỗi tiến trình.

**Metrics per-file không ghi (2026-08-22).** Engine INSERT cả lô bằng một câu SQL
nên chỉ biết tổng; chia đều cho từng file là số giả. Tổng của lượt chạy in ra ở
stdout khi `make load`.

**Discovery run ổn định (2026-08-27).** Autoloader không mint `logical_key =
discovery:{timestamp}` mỗi lần load. Một run `SUCCEEDED` / nguồn
(`logical_key=discovery`) nhận thêm file PENDING. Lease trên `PROCESSING` vẫn
là crash recovery (flock chỉ chống hai process sống cùng lúc).

**Bronze INSERT, dedup ở Silver (2026-08-22).** Không MERGE theo row id ở Bronze:
forecast giữ MỌI vintage (mỗi vintage là dữ liệu phân tích, docs/04 §1), và
Silver đã `ROW_NUMBER() ... rn = 1` dedup theo (ô lưới, giờ). Chi phí: re-land
cùng tháng làm Bronze phình (đo 2026-08-21: 3.062.736 dòng thô cho 1.106.784
khóa duy nhất) — chấp nhận vì Parquet trên MinIO local gần như miễn phí.

## Bảng đối chiếu lệnh cũ → mới

Kiến trúc trước 2026-08-21 đã bị gỡ. Nếu gặp lệnh cũ trong tài liệu khác:

| Cũ (không còn) | Mới |
|---|---|
| `collect-open-meteo-forecast --execute` | `make fetch-forecast EXEC=1` |
| `collect-open-meteo-archive` | `make fetch-archive EXEC=1 START=… END=…` |
| `load-open-meteo-forecast` | `make load SOURCE=open_meteo_forecast` |
| `run-open-meteo-pipeline` | `make fetch-forecast EXEC=1 && make load` |
| `run-open-meteo-archive --tail` | `make fetch-archive EXEC=1 START=… && make load` |
| `observe-ingestion` | `make quality` (Provero) |
| `scripts/run_weather_pipeline.sh` | cron gọi thẳng hai `make` |
