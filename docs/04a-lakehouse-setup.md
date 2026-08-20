# Bước 4a — Setup Lakehouse (DuckLake + Postgres + MinIO)

**Hanoi Flood & Climate Risk Monitor** · v0.1 · 2026-08-20 · *Trạng thái: CHỜ DUYỆT*

> POC của R13 đã có tham chiếu chuẩn: [github.com/linux-china/ducklake-demo](https://github.com/linux-china/ducklake-demo)
> (Postgres làm catalog metadata + MinIO làm storage + DuckDB làm compute). Tài liệu này áp
> đúng mô hình đó vào dự án: **một Postgres dùng kép** (Airflow metadata + DuckLake catalog, A12),
> **MinIO** làm lake, **DuckDB** làm engine.

## 1. Kiến trúc vật lý

```
┌─────────────────────────────────────────────────────────────┐
│ COMPUTE  DuckDB (dlt → dbt → FastAPI/Streamlit)              │
│   - đọc Bronze: Parquet thường trên MinIO (httpfs/secret)    │
│   - đọc/ghi Silver+Gold: bảng DuckLake (qua catalog)         │
└───────┬─────────────────────────────────────┬───────────────┘
        │ CREATE SECRET (MinIO)               │ ATTACH ducklake:catalog1
┌───────▼─────────────┐          ┌────────────▼──────────────┐
│ STORAGE  MinIO       │          │ CATALOG  PostgreSQL        │
│ s3://vn-climate/     │          │ db vnclimate, schema       │
│   bronze/… (dlt parquet)        │ ducklake (metadata tables) │
│   silver/… (ducklake tables)    └───────────────────────────┘
│   gold/…   (ducklake tables)
└─────────────────────┘
```

**Vai trò 3 thành phần (đúng tinh thần DuckLake):**
- **Catalog (Postgres)** — metadata: bảng nào, file nào, snapshot/version, ACID transaction.
- **Storage (MinIO)** — dữ liệu thật dạng Parquet, bất biến, có thể scale tách rời compute.
- **Compute (DuckDB)** — đọc metadata từ catalog + data từ storage để chạy query.

## 2. docker-compose.yml (thêm vào root repo)

```yaml
services:
  postgres:
    image: postgres:17.5
    ports: ["5432:5432"]
    environment:
      POSTGRES_DB: vnclimate
      POSTGRES_USER: vnclimate
      POSTGRES_PASSWORD: vnclimate
    volumes: [postgres_data:/var/lib/postgresql/data]

  minio:
    image: minio/minio
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server --console-address ":9001" /data
    volumes: [minio_data:/data]

volumes:
  postgres_data:
  minio_data:
```

```bash
docker compose up -d
```

## 3. Tạo lake trên DuckDB CLI (POC — chạy thử trước khi code)

Cài DuckDB CLI: `curl https://install.duckdb.org | sh` (hoặc qua `uv tool install duckdb`).

```bash
duckdb --cmd "
  -- 1) secret truy cập MinIO (S3-compatible)
  CREATE SECRET minio_secret (
    TYPE s3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT '127.0.0.1:9000',
    USE_SSL false,
    URL_STYLE 'path'
  );

  -- 2) catalog DuckLake nằm trong Postgres (dùng kép với Airflow, A12)
  ATTACH 'ducklake:postgres:dbname=vnclimate host=127.0.0.1 port=5432 user=vnclimate password=vnclimate'
    AS catalog1
    (DATA_PATH 's3://vn-climate', METADATA_SCHEMA 'ducklake');

  -- 3) phân lớp medallion bằng schema trong catalog
  CREATE SCHEMA catalog1.bronze;
  CREATE SCHEMA catalog1.silver;
  CREATE SCHEMA catalog1.gold;

  USE catalog1;
"
```

> Kết quả trong Postgres: 20 bảng `ducklake_*` (schema ducklake) được tạo tự động —
> kiểm tra bằng `psql postgres://vnclimate:vnclimate@127.0.0.1:5432/vnclimate` (`\dn`).

### Từ Python (những gì dbt/FastAPI sẽ dùng)

```python
import duckdb

con = duckdb.connect()
con.execute("""
  CREATE SECRET minio_secret (
    TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin',
    ENDPOINT '127.0.0.1:9000', USE_SSL false, URL_STYLE 'path'
  );
  ATTACH 'ducklake:postgres:dbname=vnclimate host=127.0.0.1 port=5432 user=vnclimate password=vnclimate'
    AS catalog1 (DATA_PATH 's3://vn-climate', METADATA_SCHEMA 'ducklake');
""")
con.execute("USE catalog1")
print(con.sql("SELECT * FROM gold.risk_hourly LIMIT 5"))
```

## 4. Vai trò từng lớp trong pipeline

| Lớp | Nơi lưu | Quản lý bởi | Ghi chú |
|---|---|---|---|
| **Bronze** | `s3://vn-climate/bronze/...` (Parquet thường) | **dlt** (filesystem destination) | Bất biến, append-only, không cần ACID — Parquet thường + partition ngày là đủ |
| **Silver** | `s3://vn-climate/silver/...` | **DuckLake** (qua catalog) | ACID/upsert khi làm lại mapping/luật — cần catalog quản lý version |
| **Gold** | `s3://vn-climate/gold/...` | **DuckLake** (qua catalog) | Mart qua DQ gate; time-travel để đối chiếu lịch sử báo cáo |

> **Vì sao Bronze không cần DuckLake?** Bronze chỉ ghi thêm (append) và không sửa (nguyên tắc
> medallion). ACID/versioning chỉ có giá trị ở lớp được **ghi lại** (Silver, Gold). Đây là lý do
> tách rõ: dlt ghi Parquet thường, dbt/duckdb ghi bảng DuckLake.

## 5. Tích hợp với dlt (Bước 4)

dlt **filesystem destination** viết thẳng lên MinIO bằng s3fs (đã cấu hình `endpoint_url` trong
`ingest/`): `s3://vn-climate/bronze/raw_forecast/year=2026/month=08/day=20/....parquet`.
Không cần qua DuckLake ở lớp Bronze — dbt ở Bước 5 sẽ `CREATE TABLE catalog1.silver.x AS
SELECT * FROM read_parquet('s3://vn-climate/bronze/...')`.

## 6. Lộ trình POC (xác minh R13)

1. [ ] `docker compose up -d` → MinIO console `localhost:9001`, Postgres `localhost:5432` lên.
2. [ ] Tạo secret MinIO + ATTACH catalog theo §3 (CLI) → thấy `ducklake_*` tables trong Postgres.
3. [ ] Tạo 1 bảng `gold.test` trong DuckLake, INSERT + SELECT lại → ACID hoạt động.
4. [ ] Chạy 1 pipeline dlt → thấy parquet trong MinIO (`bronze/raw_forecast/...`).
5. [ ] dbt đọc parquet Bronze → ghi `silver`/`gold` (Bước 5; nếu dbt-duckdb + DuckLake chưa chạy
     được → fallback R13: chạy dbt trên DuckDB file rồi `COPY` parquet lên MinIO).

## 7. Giả định & rủi ro mới

- **A18** — Bronze = Parquet thường trên MinIO (dlt, append-only); chỉ Silver/Gold là bảng
  DuckLake trong catalog. Giảm phức tạp, đúng bản chất từng lớp.
- **R17** — dlt **filesystem destination KHÔNG hỗ trợ `merge`** (tự rơi về `append`) → tránh
  dùng merge trên Bronze; trùng lặp xử lý ở Silver (dedup). Đã sửa lại 04-ingestion.md §2.

## 8. Tham chiếu

- Repo mẫu: github.com/linux-china/ducklake-demo (`justfile`, `demo.sql`, `docker-compose.yml`)
- DuckLake docs: ducklake.select/docs/stable/
- DuckLake tạo bảng metadata: `ducklake-schema.sql` (20 bảng `ducklake_*` trong catalog Postgres)