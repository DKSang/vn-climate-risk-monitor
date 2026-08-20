.PHONY: bootstrap bootstrap-env up down logs ingest-provinces dbt dbt-test transform dbt-docs lint

# ==== Setup ====
bootstrap-env:
	cp -n .env.example .env || true

bootstrap: bootstrap-env
	uv run python scripts/bootstrap.py

# ==== Infrastructure (Docker Compose: MinIO + Postgres + Airflow + API + Dashboard) ====
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# ==== Ingest (land file thô lên MinIO) ====
# Dữ liệu địa lý KHÔNG cần ingest qua dlt nữa: dbt đọc thẳng Postgres + CSV.
# dlt sẽ dùng lại khi làm Open-Meteo (cần HTTP retry + state + backfill theo lô).
ingest-provinces:
	uv run python ingest/raw_ingest_provinces.py

# ==== Transform (dbt + DuckDB + DuckLake) ====
# dbt project ở transform/, không phải transform/dbt/
dbt:
	cd transform && uv run dbt run --profiles-dir .

dbt-test:
	cd transform && uv run dbt test --profiles-dir .

# build = run + test, và tự dọn file cũ qua on-run-end
transform:
	cd transform && uv run dbt build --profiles-dir .

dbt-docs:
	cd transform && uv run dbt docs generate --profiles-dir . && uv run dbt docs serve --profiles-dir .

# ==== Serve (CHƯA CÀI ĐẶT — Bước 8) ====
# serving/api và serving/dashboard hiện là thư mục rỗng.

# ==== Bảo trì lakehouse ====
# on-run-end trong dbt_project.yml đã tự dọn sau mỗi lần build, NHƯNG giữ lại
# 7 ngày snapshot để còn time-travel -> file của các build trong 7 ngày vẫn nằm đó.
# Target này squash sạch: bỏ toàn bộ lịch sử snapshot, chỉ giữ phiên bản hiện tại.
clean-lake:
	uv run python -c "import duckdb; \
c = duckdb.connect(); \
c.execute(\"INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake;\"); \
c.execute(\"CREATE OR REPLACE SECRET s (TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin', ENDPOINT '127.0.0.1:9000', USE_SSL false, URL_STYLE 'path')\"); \
c.execute(\"ATTACH 'ducklake:postgres:dbname=vnclimate host=127.0.0.1 port=5432 user=vnclimate password=vnclimate' AS cat (DATA_PATH 's3://vn-climate', METADATA_SCHEMA 'ducklake')\"); \
c.execute(\"CALL ducklake_expire_snapshots('cat', older_than => now())\"); \
c.execute(\"CALL ducklake_cleanup_old_files('cat', cleanup_all => true)\"); \
print('lakehouse da duoc squash - mat time-travel, giu ban hien tai')"

# ==== Quality ====
lint:
	uv run ruff check .