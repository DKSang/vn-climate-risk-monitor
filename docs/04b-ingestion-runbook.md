# Runbook — Ingestion

**Cập nhật 2026-08-28** — `fetch.land` (HTTP→MinIO); `autoloader` (file→bảng); cấu hình nguồn ở `sources/`.

---

## Kiến trúc hai bước, cố ý tách rời

```
1. fetch   missing_rows() → [{url, key}] (126 phường, bỏ file đã có)
           for row in rows, pause theo OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR:
             land(): GET rồi ghi JSON lên MinIO
              bronze/files/open_meteo/<dataset>/...
2. load    autoloader liệt kê MinIO, nạp file MỚI vào bronze bằng SQL
              bronze_store.tables.open_meteo_*
```

`land()` nằm trong package ``fetch``. Planner Open-Meteo (URL, skip file, nhịp API) nằm trong ``open_meteo.py``. Object singleton được bọc thành array để khớp `read_json_auto` với file cũ.

Tách ra vì: lỗi mạng ở bước 1 không làm mất dữ liệu đã tải; bước 2 checkpoint
theo object key — file đã `COMMITTED` không nạp lại. Crash sau INSERT trước
checkpoint: lease hết hạn, INSERT lặp (at-least-once); Silver dedup.

Bước 2 **không quan tâm ai ghi file** — nó dùng directory listing. File do bước 1
ghi dở rồi tiến trình chết vẫn được nhặt ở lần chạy sau.

## Lệnh hằng ngày

```bash
make fetch-forecast EXEC=1              # dự báo cho slot giờ hiện tại
make load                               # nạp mọi nguồn có file mới
make quality                            # kiểm tra bronze (exit 1 nếu fail)
make transform                          # dbt: silver + gold
```

Bỏ `EXEC=1` thì chỉ in kế hoạch, không gọi API. Luôn chạy thử trước.

## Backfill lịch sử

ERA5 từ 2000-01 đến nay là **~320 tháng**, mỗi tháng **6 request** (126 phường ÷
25 phường/request) → **~1.920 request**. Open-Meteo free tier giới hạn theo phút
và giờ. Fetch tự giãn nhịp: `3600 × đơn vị mỗi request ÷
OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR` (archive ~54 đơn vị → ~43s/request,
forecast ~25 đơn vị → ~20s).

Chạy bằng script — nó lặp từng năm, fetch xong năm nào là `make load` năm đó:

```bash
make backfill-archive              # 2001 → nay
make backfill-archive FROM=2003    # từ 2003
```

Resumable: fetch tự bỏ qua từng file đã có nên interrupt rồi chạy lại chỉ đi
tiếp phần thiếu. Năm hiện tại script tự dừng ở tháng trước — 2 tháng gần nhất do
cron tail bồi hằng ngày. Muốn tay từng bước (xem kế hoạch trước, chạy một năm):

```bash
make fetch-archive START=2002-01-01 END=2002-12-01   # dry-run: xem kế hoạch
make fetch-archive EXEC=1 START=2002-01-01 END=2002-12-01
make load SOURCE=open_meteo_archive
```

`fetch` **tự bỏ qua từng file đã có** (không bỏ cả tháng) — chạy lại an toàn,
không tốn request, và crash giữa chừng tháng rồi chạy lại sẽ đi tiếp phần thiếu.
Mỗi lần chạy ghi vào run dir riêng (`…/month=01/run_<timestamp>/response_NNN.json`)
nên không bao giờ ghi đè object cũ — Bronze giữ cam kết immutable, và resume khớp
theo tên file nên nhận cả dữ liệu collector cũ ghi (`…/archive_…/response_NNN.json`).
Muốn tải lại tháng đã xong thì xoá run dir đó trên MinIO rồi chạy lại fetch.

Fetch chạy **tuần tự** — một GET tại một thời điểm — nên Open-Meteo chỉ thấy
một request đồng thời / IP. Muốn chậm/nhanh hơn thì chỉnh
`OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR`. Dry-run (`không EXEC=1`) in kế hoạch
trước khi gọi API.

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

Backfill **không đặt lịch** — chạy tay theo từng năm như trên.

## Xử lý sự cố

### Hết hạn mức Open-Meteo (429)

Khi 429 sống sót qua 5 lần GET (cooldown 60s), `land` **không ghi** payload lỗi
và dừng cả lô với exit code 2. Không mất dữ liệu: các file đã land sẽ tự bị bỏ
qua khi chạy lại. Chờ quota reset (5.000/giờ lăn, 10.000/ngày) rồi chạy lại đúng
lệnh cũ; script backfill cũng chỉ cần chạy lại.

Lưu ý: chi phí "~N đơn vị" mà dry-run in ra là **ước lượng theo công thức xấp xỉ**
của Open-Meteo. Nhịp lúc `--execute` tính từ
`OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR`. Nếu 429 đến sớm hơn dự kiến thì chia
nhỏ `--start`/`--end` hoặc hạ biến đó.

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

`{{ files }}` được engine thay bằng danh sách file đã claim. Tạo bảng đích trước
(`CREATE TABLE ... AS (<sql>) LIMIT 0`), rồi `make load`.

## ADR — các quyết định đã chốt của kiến trúc hiện tại

**Integrity delegated cho MinIO (2026-08-22, cột collector DROP 2026-08-27).**
Kiến trúc cũ tính SHA-256 lúc ghi và verify lúc đọc. Directory-listing discovery
không tải file về nên không băm được; hợp đồng checksum đã bỏ. Cơ chế bảo toàn
vẹn còn lại: MinIO bitrot protection + `etag`/`size_bytes` trong
`file_parameters` JSONB lúc discovery. Các cột collector trên
`ingestion.ingestion_files` (`sha256`, `http_status`, `rows_parsed`, …) đã DROP.

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
