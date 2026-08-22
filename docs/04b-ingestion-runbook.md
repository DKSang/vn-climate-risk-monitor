# Runbook — Ingestion

**Cập nhật 2026-08-21** sau khi gộp code về package `autoloader`. Mọi lệnh dưới
đây đã chạy thật trên máy, không phải chép từ thiết kế.

---

## Kiến trúc hai bước, cố ý tách rời

```
1. fetch   Python gọi Open-Meteo, ghi JSON as-is lên MinIO
              bronze/files/open_meteo/<dataset>/...
2. load    autoloader liệt kê MinIO, nạp file MỚI vào bronze bằng SQL
              bronze_store.tables.open_meteo_*
```

Tách ra vì: lỗi mạng ở bước 1 không làm mất dữ liệu đã tải; bước 2 chạy lại bao
nhiêu lần cũng an toàn (exactly-once theo object key trong checkpoint Postgres).

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
và giờ, `EffectiveCallPacer` tự giãn nhịp nên không cần tự sleep.

Chạy **từng năm** để không chiếm hết hạn mức trong ngày:

```bash
# 1. Xem kế hoạch trước
make fetch-archive START=2002-01-01 END=2002-12-01

# 2. Chạy thật (72 request cho 12 tháng)
make fetch-archive EXEC=1 START=2002-01-01 END=2002-12-01

# 3. Nạp vào bronze
make load SOURCE=open_meteo_archive
```

`fetch` **tự bỏ qua từng file đã có** (không bỏ cả tháng) — chạy lại an toàn,
không tốn request, và crash giữa chừng tháng rồi chạy lại sẽ đi tiếp phần thiếu.
Muốn tải đè thì thêm `--overwrite` (gọi thẳng `uv run fetch-open-meteo`).

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

Không cần viết Python. Thêm hai file vào `ingestion/sources/`:

```
my_source.yml    khai báo discovery prefix, bảng đích, batch size
my_source.sql    SELECT ... FROM read_json_auto({{ files }})
```

`{{ files }}` được engine thay bằng danh sách file đã claim. Tạo bảng đích trước
(`CREATE TABLE ... AS (<sql>) LIMIT 0`), rồi `make load`.

## ADR — các quyết định đã chốt của kiến trúc hiện tại

**Integrity delegated cho MinIO (2026-08-22).** Kiến trúc cũ tính SHA-256 lúc
ghi và verify lúc đọc. Directory-listing discovery không tải file về nên không
băm được (băm toàn bộ chỉ để verify là mất ý nghĩa của listing); hợp đồng
checksum đã bỏ. Cơ chế bảo toàn vẹn còn lại: MinIO bitrot protection + `etag`/
`size_bytes` ghi trong `file_parameters` JSONB ngay tại discovery. Các cột
`sha256`, `etag`, `content_type`, `http_status`, `request_attempt_count`,
`expected_item_count`, `received_item_count` trong `ingestion.ingestion_files`
là di sản của kiến trúc cũ, luôn NULL — sẽ DROP ở lần evolve schema tới, tránh
đổi schema khi backfill đang chạy.

**Metrics per-file không ghi (2026-08-22).** Engine INSERT cả lô bằng một câu SQL
nên chỉ biết tổng; chia đều cho từng file là số giả. `rows_parsed`/
`rows_inserted`/`rescued_rows` per-file để NULL; tổng của lượt chạy in ra ở
stdout khi `make load` và suy ra được từ số file COMMITTED.

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
